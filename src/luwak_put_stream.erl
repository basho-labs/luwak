-module(luwak_put_stream).

-define(BUFFER_SIZE, 20).
-record(state, {file,offset,blocksize,ref,ttl,written=[],buffer=[],
                buffersize=0,checksumming=false,ctx=crypto:sha_init()}).

-include("luwak.hrl").

%% API
-export([start_link/4,
         start/4,
         send/2,
         ping/1,
         close/1,
         status/2,
         flush/1]).

%% @spec start_link(Riak :: riak(), File :: luwak_file(),
%%                  Offset :: int(), TTL :: int()) ->
%%        put_stream()
%% @doc Starts a stream for writing at the specified offset.  The
%%      stream will remain open for TTL milliseconds.  Every time the
%%      stream is written to or pinged it will reset the TTL counter.
%%      Therefore slow writers should set an appropriate TTL or use
%%      ping/1 to keep a stream open.  This function links the stream
%%      process to the calling process.
%% @equiv start(Riak, File, Offset, TTL)
start_link(Riak, File, Offset, TTL) ->
    Ref = make_ref(),
    BlockSize = luwak_file:get_property(File, block_size),
    Checksumming = luwak_file:get_property(File, checksumming),
    Pid = proc_lib:spawn_link(
            fun() ->
                    recv(Riak, #state{file=File,offset=Offset,
                                      blocksize=BlockSize,ref=Ref,ttl=TTL,
                                      checksumming=Checksumming})
            end),
    {put_stream, Ref, Pid}.

start(Riak, File, Offset, TTL) ->
    Ref = make_ref(),
    BlockSize = luwak_file:get_property(File, block_size),
    Checksumming = luwak_file:get_property(File, checksumming),
    Pid = proc_lib:spawn(
            fun() ->
                    recv(Riak, #state{file=File,offset=Offset,
                                      blocksize=BlockSize,ref=Ref,ttl=TTL,
                                      checksumming=Checksumming})
            end),
    {put_stream, Ref, Pid}.

%% @doc Sends the given binary to the stream for writing.  The put
%%      stream does its own internal buffering for performance
%%      reasons, so this is equivalent to writing to a regular file on
%%      a local filesystem.  If you want guarantees that your write is
%%      actually written then you need to call flush/1 on the put
%%      stream.
send({put_stream, Ref, Pid}, Data) ->
    Pid ! {put, Ref, Data}.

%% @doc Pings the stream, resetting the TTL timer.  Use this if you
%%      have a slow writer but don't necessarily want to extend the
%%      TTL time.
ping({put_stream, Ref, Pid}) ->
    Pid ! {ping, Ref}.

%% @doc Signals the stream to close.  The stream will immediately
%%      flush its buffers to the file and will stay open for TTL
%%      milliseconds waiting for one last status/2 call.
close({put_stream, Ref, Pid}) ->
    Pid ! {close, Ref}.

%% @spec status(Stream :: put_stream(), Timeout) -> {ok, File} | {error, Reason}
%% @doc Gets the current filehandle on which the stream is operating.
%%      Can be called at any time during the life of a stream,
%%      including after a call to close/1.  The returned file is not
%%      guaranteed to reflect the latest calls to send/2, since
%%      status/2 does not cause a flush to occur.
status({put_stream, Ref, Pid}, Timeout) ->
    Pid ! {status, self(), Ref},
    MonitorRef = erlang:monitor(process, Pid),
    receive
        {stream, Ref, File} ->
            erlang:demonitor(MonitorRef),
            {ok, File};
        {'DOWN', MonitorRef, _, Pid, Reason} ->
            {error, Reason}
    after Timeout ->
            erlang:demonitor(MonitorRef),
            {error, timeout}
    end.

%% @doc Causes the stream to immediately flush its buffers and commit
%%      any pending writes to the file.  Equivalent to an fsync call
%%      in most other filesystem API's.
flush({put_stream, Ref, Pid}) ->
    Pid ! {flush, Ref}.

recv(_Riak, {closed, _State = #state{file=File,ttl=TTL}}) ->
  receive
      {status, Client, Ref} -> Client ! {stream, Ref, File}
  after TTL ->
          ok
  end;
recv(Riak, State = #state{ref=Ref,ttl=TTL,buffer=Buffer,file=File}) ->
    recv(Riak,
         receive
             {put, Ref, Data} -> 
                 handle_data(Riak,
                            State#state{buffer=[Data|Buffer],
                                        buffersize=iolist_size([Data|Buffer])});
             {ping, Ref} ->
                 State;
             {flush, Ref} ->
                 flush(Riak, State);
             {close, Ref} ->
                 close(Riak, State);
             {status, Client, Ref} ->
                 Client ! {stream, Ref, File},
                 State
         after TTL ->
                 flush(Riak, State)
         end).

%% partial write on an unaligned block.  buffer better be empty
handle_data(Riak, State=#state{file=File,offset=Offset,blocksize=BlockSize,
                               buffer=Buffer,written=[],buffersize=BufferSize})
  when Offset rem BlockSize =/= 0, 
       BufferSize >= BlockSize - (Offset rem BlockSize) ->
    PartialSize = BlockSize - Offset rem BlockSize,
    <<PartialData:PartialSize/binary, TailData/binary>> =
        iolist_to_binary(lists:reverse(Buffer)),
    {ok, [W]} = luwak_io:no_tree_put_range(Riak, File, Offset, PartialData),
    update_tree(Riak, checksum(PartialData,
                               State#state{offset=Offset+PartialSize,
                                           buffer=[TailData],
                                           buffersize=byte_size(TailData),
                                           written=[W]}));
handle_data(Riak, State=#state{file=File,offset=Offset,blocksize=BlockSize,
                               buffer=Buffer,written=Written,
                               buffersize=BufferSize})
  when BufferSize >= BlockSize ->
    BufferSize = iolist_size(Buffer),
    PartialSize = BufferSize - BufferSize rem BlockSize,
    <<PartialData:PartialSize/binary, TailData/binary>> =
        iolist_to_binary(lists:reverse(Buffer)),
    {ok, Written1} =
        luwak_io:no_tree_put_range(Riak, File, Offset, PartialData),
    update_tree(Riak, checksum(PartialData,
                               State#state{offset=Offset+PartialSize,
                                           buffer=[TailData],
                                           buffersize=byte_size(TailData),
                                           written=Written ++ Written1}));
handle_data(Riak, State) ->
    update_tree(Riak, State).

update_tree(Riak, State=#state{offset=Offset,file=File,written=Written})
  when length(Written) >= ?BUFFER_SIZE ->
    OriginalOffset = Offset - luwak_tree_utils:blocklist_length(Written),
    {ok, NewFile} = luwak_tree:update(Riak, File, OriginalOffset, Written),
    update_checksum(Riak, State#state{file=NewFile});
update_tree(_Riak, State) ->
    State.

flush(Riak, State=#state{offset=Offset,file=File,buffer=Buffer,written=Written})
  when length(Buffer) > 0 ->
    ?debugFmt("A flush(Riak, ~p)~n", [State]),
    Data = iolist_to_binary(lists:reverse(Buffer)),
    {ok, Written1} = luwak_io:no_tree_put_range(Riak, File, Offset, Data),
    WriteSize = luwak_tree_utils:blocklist_length(Written1),
    flush(Riak, checksum(Data, State#state{offset=Offset+WriteSize,
                                           buffer=[],
                                           buffersize=0,
                                           written=Written++Written1}));
flush(Riak, State=#state{offset=Offset,file=File,written=Written,blocksize=BlockSize})
  when length(Written) > 0 ->
    ?debugFmt("B flush(Riak, ~p)~n", [State]),
    OriginalOffset = Offset - luwak_tree_utils:blocklist_length(Written),
    OriginalOffsetAligned = OriginalOffset - OriginalOffset rem BlockSize,  
    {ok, NewFile} = luwak_tree:update(Riak, File, OriginalOffsetAligned, Written),
    update_checksum(Riak, State#state{file=NewFile});
flush(_Riak, State) ->
    State.

checksum(_, State=#state{checksumming=false}) ->
    State;
checksum(Data, State=#state{ctx=Ctx}) ->
    State#state{ctx=crypto:sha_update(Ctx,Data)}.

update_checksum(_, State=#state{checksumming=false}) ->
    State;
update_checksum(Riak, State=#state{ctx=Ctx,file=File}) ->
    {ok, File1} = luwak_file:update_checksum(Riak,File,
                                            fun() -> crypto:sha_final(Ctx) end),
    State#state{file=File1}.

close(Riak, State) ->
    {closed, flush(Riak, State)}.
