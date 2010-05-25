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
      luwak_obj:update_root(Riak, File, RootName);
    RootName ->
      {ok, Root} = get(Riak, RootName),
      error_logger:info_msg("blocks~n"),
      WriteLength = luwak_tree_utils:blocklist_length(Blocks),
      error_logger:info_msg("children~n"),
      RootLength = luwak_tree_utils:blocklist_length(Root#n.children),
      {ok, NewRoot} = subtree_update(Riak, File, Order, StartingPos, 0, 
        Root, Blocks),
      NewRootName = riak_object:key(NewRoot),
      luwak_obj:update_root(Riak, File, NewRootName)
  end.

get(Riak, Name) when is_binary(Name) ->
  {ok, Obj} = Riak:get(?N_BUCKET, Name, 2),
  {ok, riak_object:get_value(Obj)}.

create_tree(Riak, Order, Children) when is_list(Children) ->
  error_logger:info_msg("create_tree(Riak, ~p, ~p)~n", [Order, Children]),
  if
    length(Children) > Order ->
      Written = list_into_nodes(Riak, Children, Order, 0),
      create_node(Riak, Written);
    true ->
      create_node(Riak, Children)
  end.

%% updating any node happens in up to 5 parts, depending on the coverage of the write list
subtree_update(Riak, File, Order, InsertPos, TreePos, Parent = #n{}, Blocks) ->
  error_logger:info_msg("subtree_update(Riak, File, ~p, ~p, ~p, ~p, ~p)~n", [Order, InsertPos, TreePos, Parent, Blocks]),
  {NodeSplit, BlockSplit} = luwak_tree_utils:five_way_split(TreePos, Parent#n.children, InsertPos, Blocks),
  error_logger:info_msg("NodeSplit ~p BlockSplit ~p~n", [NodeSplit, BlockSplit]),
  MidHeadStart = luwak_tree_utils:blocklist_length(NodeSplit#split.head) + TreePos,
  MidHeadReplacement = lists:map(fun({Name,Length}) ->
      {ok, ChildNode} = get(Riak, Name),
      {ok, ReplacementChild} = subtree_update(Riak, File, Order, 
        InsertPos, MidHeadStart, 
        ChildNode, BlockSplit#split.midhead),
      V = riak_object:get_value(ReplacementChild),
      {riak_object:key(ReplacementChild), luwak_tree_utils:blocklist_length(V#n.children)}
    end, NodeSplit#split.midhead),
  MiddleInsertStart = luwak_tree_utils:blocklist_length(BlockSplit#split.midhead) + MidHeadStart,
  MiddleReplacement = list_into_nodes(Riak, BlockSplit#split.middle, Order, MiddleInsertStart),
  MidTailStart = luwak_tree_utils:blocklist_length(BlockSplit#split.middle) + MiddleInsertStart,
  MidTailReplacement = lists:map(fun({Name,Length}) ->
      {ok, ChildNode} = get(Riak, Name),
      {ok, ReplacementChild} = subtree_update(Riak, File, Order,
        MidTailStart, MidTailStart,
        ChildNode, BlockSplit#split.midtail),
      V = riak_object:get_value(ReplacementChild),
      {riak_object:key(ReplacementChild), luwak_tree_utils:blocklist_length(V#n.children)}
    end, NodeSplit#split.midtail),
  create_tree(Riak, Order, NodeSplit#split.head ++ 
    MidHeadReplacement ++ 
    MiddleReplacement ++ 
    MidTailReplacement ++
    NodeSplit#split.tail).
  
list_into_nodes(Riak, Children, Order, StartingPos) ->
  map_sublist(fun(Sublist) ->
      Length = luwak_tree_utils:blocklist_length(Sublist),
      {ok, Obj} = create_node(Riak, Sublist),
      {riak_object:key(Obj), Length+StartingPos}
    end, Order, Children).
  

%% @spec block_at(Riak::riak(), File::luwak_obj(), Pos::int()) ->
%%          {ok, BlockObj} | {error, Reason}
block_at(Riak, File, Pos) ->
  BlockSize = luwak_obj:get_property(File, block_size),
  Length = luwak_obj:get_property(File, length),
  case luwak_obj:get_property(File, root) of
    undefined -> {error, notfound};
    % RootName when Pos > Length -> eof;
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
  ChildName = which_child(Links, NodeOffset, Pos),
  block_at_retr(Riak, ChildName, NodeOffset, Pos);
block_at_node(Riak, NodeObj, block, _, NodeOffset, _) ->
  {ok, NodeObj}.
  
which_child([{ChildName,_}|[]], _, _) ->
  ChildName;
which_child([{ChildName,Length}|Tail], NodeOffset, Pos) when Pos > NodeOffset + Length ->
%  error_logger:info_msg("which_child(~p, ~p, ~p)~n", [[{ChildName,Length}|Tail], NodeOffset, Pos]),
  which_child(Tail, NodeOffset+Length, Pos);
which_child([{ChildName,Length}|Tail], NodeOffset, Pos) when Pos =< NodeOffset + Length ->
%  error_logger:info_msg("which_child(~p, ~p, ~p)~n", [[{ChildName,Length}|Tail], NodeOffset, Pos]),
  ChildName.

map_sublist(Fun, N, List) ->
  map_sublist_1(Fun, N, List, [], []).
  
map_sublist_1(_, _, [], [], Acc) ->
  lists:reverse(Acc);
map_sublist_1(_, _, [], Sublist, []) ->
  lists:reverse(Sublist);
map_sublist_1(Fun, N, [], Sublist, Acc) ->
  lists:reverse([Fun(lists:reverse(Sublist))|Acc]);
map_sublist_1(Fun, N, List, Sublist, Acc) when length(Sublist) >= N ->
  Result = Fun(lists:reverse(Sublist)),
  map_sublist_1(Fun, N, List, [], [Result|Acc]);
map_sublist_1(Fun, N, [E|List], Sublist, Acc) ->
  map_sublist_1(Fun, N, List, [E|Sublist], Acc).

create_node(Riak, Children) ->
  error_logger:info_msg("create_node(Riak, ~p)~n", [Children]),
  N = #n{created=now(),children=Children},
  Name = skerl:hexhash(?HASH_LEN, term_to_binary(Children)),
  Obj = riak_object:new(?N_BUCKET, Name, N),
  {Riak:put(Obj, 2), Obj}.
    
floor(X) ->
  T = erlang:trunc(X),
  case (X - T) of
    Neg when Neg < 0 -> T - 1;
    Pos when Pos > 0 -> T;
    _ -> T
  end.

ceiling(X) ->
  T = erlang:trunc(X),
  case (X - T) of
    Neg when Neg < 0 -> T;
    Pos when Pos > 0 -> T + 1;
    _ -> T
  end.
