-module(luwak_get_stream).

-export([get_stream/4, recv/2]).

-record(map, {riak,blocksize,ref,pid,offset,endoffset}).

-include_lib("luwak/include/luwak.hrl").


%% @spec get_stream(Riak :: riak(), File :: luwak_file(), Start :: int(), Length :: length()) ->
%%        get_stream() | {error, Reason}
get_stream(Riak, File, Start, Length) ->
  Ref = make_ref(),
  BlockSize = luwak_file:get_property(File, block_size),
  Root = luwak_file:get_property(File, root),
  MapStart = [{{?N_BUCKET, Root}, 0}],
  Map = #map{riak=Riak,blocksize=BlockSize,ref=Ref,offset=Start,endoffset=Start+Length},
  Receiver = proc_lib:spawn_link(receiver_fun(MapStart, self(), Map)),
  {get_stream, Ref, Receiver}.
  
recv({get_stream, Ref, Pid}, Timeout) ->
  receive
    {get, Ref, Data, Offset} -> {Data, Offset};
    {get, Ref, eos} -> eos;
    {get, Ref, closed} -> closed
  after Timeout ->
    {error, timeout}
  end.
  
nonblock_mr(Riak,Query,MapInput) ->
  case Riak:mapred_stream(Query,self(),30000) of
      {ok, {ReqId, FlowPid}} ->
          luke_flow:add_inputs(FlowPid, MapInput),
          luke_flow:finish_inputs(FlowPid);
      Error ->
          exit(Error)
  end.
  
receiver_fun(MapInput, Parent, Map=#map{riak=Riak,offset=Offset,endoffset=EndOffset,ref=Ref}) ->
  fun() ->
    Query = [{map,{qfun,map_fun()},Map#map{pid=self()}, false}],
    nonblock_mr(Riak, Query, MapInput),
    receive_loop(Ref, Offset, EndOffset, Parent)
  end.
      
receive_loop(Ref, Offset, EndOffset, Parent) when Offset >= EndOffset ->
  ?debugMsg("receive_loop sending eos~n"),
  Parent ! {get, Ref, eos},
  ok;
receive_loop(Ref, Offset, EndOffset, Parent) ->
  receive
    {get, Ref, Data, Offset} ->
      ?debugFmt("receive_loop got ~p~n", [{get, Ref, Data, Offset}]),
      Parent ! {get, Ref, Data, Offset},
      receive_loop(Ref, Offset+byte_size(Data), EndOffset, Parent);
    {close, Ref} ->
      ?debugFmt("receive_loop got ~p~n", [{close, Ref}]),
      Parent ! {get, Ref, closed},
      ok
  end.
  
map_fun() ->
  fun(Obj, TreeOffset, Map) ->
    case (catch map(riak_object:get_value(Obj), TreeOffset, Map)) of
      {'EXIT', Error} ->
        error_logger:error_msg("map failed with: ~p~n", [Error]),
        throw(Error);
      Result -> Result
    end
  end.
  
map(Parent=#n{}, TreeOffset, Map=#map{riak=Riak,offset=Offset,endoffset=EndOffset,blocksize=BlockSize}) ->
  ?debugFmt("A map(~p, ~p, ~p)~n", [Parent, TreeOffset, Map]),
  Fun = fun({Name,Length},AccOffset) ->
    {[{{?N_BUCKET, Name}, AccOffset}], AccOffset+Length}
  end,
  Blocks = luwak_tree:get_range(Riak, Fun, Parent, BlockSize, TreeOffset, Offset, EndOffset),
  Query = [{map,{qfun,map_fun()},Map,false}],
  spawn(fun() ->
    ?debugFmt("launching MR on ~p~n", [Blocks]),
    nonblock_mr(Riak, Query, Blocks)
  end),
  [];
map(Block, TreeOffset, Map=#map{riak=Riak,offset=Offset,endoffset=EndOffset,ref=Ref,pid=Pid,blocksize=BlockSize}) when TreeOffset < Offset ->
  ?debugFmt("B map(~p, ~p, ~p)~n", [Block, TreeOffset, Map]),
  PartialSize = Offset - TreeOffset,
  <<_:PartialSize/binary, Tail/binary>> = luwak_block:data(Block),
  ?debugFmt("sending ~p~n", [{get, Ref, Tail, Offset}]),
  Pid ! {get, Ref, Tail, Offset},
  [];
map(Block, TreeOffset, Map=#map{riak=Riak,offset=Offset,endoffset=EndOffset,ref=Ref,pid=Pid,blocksize=BlockSize}) when BlockSize >= EndOffset - TreeOffset ->
  ?debugFmt("C map(~p, ~p, ~p)~n", [Block, TreeOffset, Map]),
  PartialSize = EndOffset - TreeOffset,
  <<PartialData:PartialSize/binary, _/binary>> = luwak_block:data(Block),
  ?debugFmt("sending ~p~n", [{get, Ref, PartialData, TreeOffset}]),
  Pid ! {get, Ref, PartialData, TreeOffset},
  [];
map(Block, TreeOffset, Map=#map{riak=Riak,offset=Offset,endoffset=EndOffset,ref=Ref,pid=Pid,blocksize=BlockSize}) ->
  ?debugFmt("D map(~p, ~p, ~p)~n", [Block, TreeOffset, Map]),
  Data = luwak_block:data(Block),
  ?debugFmt("sending ~p~n", [{get, Ref, Data, TreeOffset}]),
  Pid ! {get, Ref, Data, TreeOffset},
  [].
  
ident_fun() ->
  fun({Name,Length}, NodeOffset) ->
    {[{Name,Length}], NodeOffset}
  end.

