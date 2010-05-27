-module(luwak_put_stream).

-define(BUFFER_SIZE, 20).
-record(state, {file,offset,blocksize,ref,ttl,written=[],buffer=[],buffersize=0}).
%% API
-export([start_link/4, start/4, send/2, ping/1, close/1, status/2]).

start_link(Riak, File, Offset, TTL) ->
  Ref = make_ref(),
  BlockSize = luwak_file:get_property(File, block_size),
  Pid = proc_lib:spawn_link(fun() ->
      recv(Riak, #state{file=File,offset=Offset,blocksize=BlockSize,ref=Ref,ttl=TTL})
    end),
  {put_stream, Ref, Pid}.
  
start(Riak, File, Offset, TTL) ->
  Ref = make_ref(),
  BlockSize = luwak_file:get_property(File, block_size),
  Pid = proc_lib:spawn(fun() ->
      recv(Riak, #state{file=File,offset=Offset,blocksize=BlockSize,ref=Ref,ttl=TTL})
    end),
  {put_stream, Ref, Pid}.

send({put_stream, Ref, Pid}, Data) ->
  Pid ! {put, Ref, Data}.

ping({put_stream, Ref, Pid}) ->
  Pid ! {ping, Ref}.

close({put_stream, Ref, Pid}) ->
  Pid ! {close, Ref}.
  
status({put_stream, Ref, Pid}, Timeout) ->
  Pid ! {status, self(), Ref},
  MonitorRef = erlang:monitor(process, Pid),
  receive
    {stream, Ref, File} ->
      erlang:demonitor(MonitorRef),
      {ok, File};
    {'DOWN', MonitorRef, _, Pid, Reason} -> {error, Reason}
  after Timeout ->
    erlang:demonitor(MonitorRef),
    {error, timeout}
  end.

recv(_, close) ->
  ok;
recv(Riak, {flushed, State = #state{file=File,ttl=TTL}}) ->
  receive
    {status, Client, Ref} -> Client ! {stream, Ref, File}
  after TTL ->
    ok
  end;
recv(Riak, State = #state{ref=Ref,ttl=TTL,buffer=Buffer,file=File}) ->
  recv(Riak,
    receive
      {put, Ref, Data} -> handle_data(Riak, State#state{buffer=[Data|Buffer],buffersize=iolist_size([Data|Buffer])});
      {ping, Ref} -> State;
      {close, Ref} -> flush(Riak, State);
      {status, Client, Ref} ->
        Client ! {stream, Ref, File},
        State
    after TTL ->
      flush(Riak, State)
    end).

%% partial write on an unaligned block.  buffer better be empty
handle_data(Riak, State=#state{file=File,offset=Offset,blocksize=BlockSize,buffer=Buffer,written=[],buffersize=BufferSize}) when Offset rem BlockSize =/= 0, BufferSize >= BlockSize - (Offset rem BlockSize) ->
  PartialSize = BlockSize - Offset rem BlockSize,
  <<PartialData:PartialSize/binary, TailData/binary>> = iolist_to_binary(lists:reverse(Buffer)),
  {ok, [W]} = luwak_io:no_tree_put_range(Riak, File, Offset, PartialData),
  update_tree(Riak, State#state{offset=Offset+PartialSize,buffer=[TailData],buffersize=byte_size(TailData),written=[W]});
handle_data(Riak, State=#state{file=File,offset=Offset,blocksize=BlockSize,buffer=Buffer,written=Written,buffersize=BufferSize}) when BufferSize >= BlockSize ->
  BufferSize = iolist_size(Buffer),
  PartialSize = BufferSize - BufferSize rem BlockSize,
  <<PartialData:PartialSize/binary, TailData/binary>> = iolist_to_binary(lists:reverse(Buffer)),
  {ok, Written1} = luwak_io:no_tree_put_range(Riak, File, Offset, PartialData),
  update_tree(Riak, State#state{offset=Offset+PartialSize,buffer=[TailData],buffersize=byte_size(TailData),written=Written ++ Written1});
handle_data(Riak, State) ->
  update_tree(Riak, State).

update_tree(Riak, State=#state{offset=Offset,file=File,written=Written}) when length(Written) >= ?BUFFER_SIZE ->
  OriginalOffset = Offset - luwak_tree_utils:blocklist_length(Written),
  {ok, NewFile} = luwak_tree:update(Riak, File, OriginalOffset, Written),
  State#state{file=NewFile};
update_tree(Riak, State) ->
  State.

flush(Riak, State=#state{offset=Offset,file=File,buffer=Buffer,written=Written}) when length(Buffer) > 0 ->
  error_logger:info_msg("A flush(Riak, ~p)~n", [State]),
  {ok, Written1} = luwak_io:no_tree_put_range(Riak, File, Offset, iolist_to_binary(lists:reverse(Buffer))),
  WriteSize = luwak_tree_utils:blocklist_length(Written1),
  flush(Riak, State#state{offset=Offset+WriteSize,buffer=[],buffersize=0,written=Written++Written1});
flush(Riak, State=#state{offset=Offset,file=File,written=Written}) when length(Written) > 0 ->
  error_logger:info_msg("B flush(Riak, ~p)~n", [State]),
  OriginalOffset = Offset - luwak_tree_utils:blocklist_length(Written),
  {ok, NewFile} = luwak_tree:update(Riak, File, OriginalOffset, Written),
  {flushed, State#state{file=NewFile}};
flush(Riak, State) ->
  {flushed, State}.
