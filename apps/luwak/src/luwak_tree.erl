-module(luwak_tree).

-export([update/4, get/2, block_at/3]).

-include_lib("luwak/include/luwak.hrl").

%%=======================================
%% Public API
%%=======================================
update(Riak, File, StartingPos, Blocks) ->
  Order = luwak_obj:get_property(File, tree_order),
  BlockSize = luwak_obj:get_property(File, block_size),
  if
    StartingPos rem BlockSize =/= 0 -> throw({error, "StartingPos must be a multiple of blocksize"});
    true -> ok
  end,
  case luwak_obj:get_property(File, root) of
    %% there is no root, therefore we create one
    undefined -> 
      {ok, RootObj} = create_tree(Riak, Order, Blocks),
      RootName = riak_object:key(RootObj),
      luwak_obj:update_root(Riak, File, RootName)
  end.

get(Riak, Name) when is_binary(Name) ->
  {ok, Obj} = Riak:get(?N_BUCKET, Name, 2),
  {ok, riak_object:get_value(Obj)}.

create_tree(Riak, Order, Children) ->
  if
    length(Children) > Order ->
      Written = map_sublist(fun(Sublist) ->
          Length = lists:foldr(fun({_,L},Acc) ->
              L+Acc
            end, 0, Sublist),
          {ok, Obj} = create_node(Riak, Sublist),
          {riak_object:key(Obj), Length}
        end, Order, Children),
      create_node(Riak, Written);
    true ->
      create_node(Riak, Children)
  end.


%% @spec block_at(Riak::riak(), File::luwak_obj(), Pos::int()) ->
%%          {ok, BlockObj} | {error, Reason}
block_at(Riak, File, Pos) ->
  BlockSize = luwak_obj:get_property(File, block_size),
  Length = luwak_obj:get_property(File, length),
  case luwak_obj:get_property(File, root) of
    undefined -> {error, notfound};
    RootName when Pos > Length -> eof;
    RootName ->
      block_at_retr(Riak, RootName, 0, Pos)
  end.

block_at_retr(Riak, NodeName, NodeOffset, Pos) ->
  case Riak:get(?N_BUCKET, NodeName, 2) of
    {ok, NodeObj} ->
      Type = luwak_obj:get_property(NodeObj, type),
      Links = luwak_obj:get_property(NodeObj, links),
      block_at_node(Riak, NodeObj, Type, Links, NodeOffset, Pos);
    Err -> Err
  end.
  
block_at_node(Riak, NodeObj, node, Links, NodeOffset, Pos) ->
  which_child(Riak, Links, NodeOffset, Pos);
block_at_node(Riak, NodeObj, block, _, NodeOffset, _) ->
  {ok, NodeObj}.
  
which_child(_, [], _, _) ->
  eof;
which_child(Riak, [{ChildName,EndOffset}|Tail], NodeOffset, Pos) when Pos > NodeOffset + EndOffset ->
  which_child(Riak, Tail, NodeOffset+EndOffset, Pos);
which_child(Riak, [{ChildName,EndOffset}|Tail], NodeOffset, Pos) when Pos =< NodeOffset + EndOffset ->
  block_at_retr(Riak, ChildName, NodeOffset, Pos).

map_sublist(Fun, N, List) ->
  map_sublist_1(Fun, N, List, [], []).
  
map_sublist_1(_, _, [], _, Acc) ->
  lists:reverse(Acc);
map_sublist_1(Fun, N, List, Sublist, Acc) when length(Sublist) >= N ->
  Result = Fun(lists:reverse(Sublist)),
  map_sublist_1(Fun, N, List, [], [Result|Acc]);
map_sublist_1(Fun, N, [E|List], Sublist, Acc) ->
  map_sublist_1(Fun, N, List, [E|Sublist], Acc).

create_node(Riak, Children) ->
  N = #n{created=now(),children=Children},
  Name = skerl:hexhash(?HASH_LEN, term_to_binary(Children)),
  Obj = riak_object:new(?N_BUCKET, Name, N),
  {Riak:put(Obj, 2), Obj}.
