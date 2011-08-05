-module(luwak_tree_utils).

-include("luwak.hrl").

-export([longest_divisable_subtree/2,
         longest_divisable_subtree/4, 
         shortest_subtree_split/4,
         split_at_length/2,
         five_way_split/4,
         blocklist_length/1,
         foldrflatmap/3,
         split_at_length_left_bias/2]).

longest_divisable_subtree(NodesA, NodesB) ->
    longest_divisable_subtree(NodesA, NodesB, 0, 0).

longest_divisable_subtree(NodesA, NodesB, StartA, StartB) ->
    longest_divisable_subtree(NodesA, NodesB, StartA, StartB, [], []).

%% both lists were equal
longest_divisable_subtree([], [], _, _, AccA, AccB) ->
    {{lists:reverse(AccA), []}, {lists:reverse(AccB), []}};
%% listA was exhausted first, we favor listB as a special case here
longest_divisable_subtree([], NodesB, _, _LengthB, AccA, AccB) ->
    ?debugFmt("accA ~p accB ~p~n", [AccA, AccB]),
    {{lists:reverse(AccA), []}, {lists:reverse(AccB) ++ NodesB, []}};
%% listB was exhausted first, put back until equal
longest_divisable_subtree(NodesA, [], LengthA, _, AccA, AccB) ->
    ?debugFmt("accA ~p accB ~p~n", [AccA, AccB]),
    {NodesB, FinalAccB} = split_at_length(lists:reverse(AccB), LengthA),
    {{lists:reverse(AccA), NodesA}, {NodesB, FinalAccB}};
%% equal length subseq
longest_divisable_subtree([{NameA,LA}|NodesA], [{NameB,LB}|NodesB],
                          LengthA, LengthB, AccA, AccB)
  when LA + LengthA == LB + LengthB ->
    longest_divisable_subtree(NodesA, NodesB, LengthA+LA, LengthB+LB,
                              [{NameA,LA}|AccA], [{NameB,LB}|AccB]);
%% left is larger
longest_divisable_subtree([{NameA,LA}|NodesA], [{NameB,LB}|NodesB],
                          LengthA, LengthB, AccA, AccB)
  when LA + LengthA > LB + LengthB ->
    longest_divisable_subtree([{NameA,LA}|NodesA], NodesB, LengthA, LengthB+LB,
                              AccA, [{NameB,LB}|AccB]);
%% right is larger
longest_divisable_subtree([{NameA,LA}|NodesA], [{NameB,LB}|NodesB],
                          LengthA, LengthB, AccA, AccB)
  when LA + LengthA < LB + LengthB ->
    longest_divisable_subtree(NodesA, [{NameB,LB}|NodesB],
                              LengthA+LA, LengthB, [{NameA,LA}|AccA], AccB).

shortest_subtree_split(NodesA, NodesB, StartA, StartB) ->
    shortest_subtree_split(NodesA, NodesB, StartA, StartB, [], []).

shortest_subtree_split(NodesA, NodesB, StartA, StartB, AccA, AccB)
  when length(NodesA) == 0; length(NodesB) == 0; StartA == StartB ->
    {{lists:reverse(AccA),NodesA},{lists:reverse(AccB),NodesB}};
shortest_subtree_split([E={_NA,LA}|NodesA], NodesB, StartA, StartB, AccA, AccB)
  when StartA < StartB ->
    shortest_subtree_split(NodesA, NodesB, LA+StartA, StartB, [E|AccA], AccB);
shortest_subtree_split(NodesA, [E={_NB,LB}|NodesB], StartA, StartB, AccA, AccB)
  when StartA > StartB ->
    shortest_subtree_split(NodesA, NodesB, StartA, LB+StartB, AccA, [E|AccB]).

split_at_length_left_bias(Children, Length) ->
    split_at_length_left_bias(Children, Length, []).

split_at_length_left_bias([], _, Acc) ->
    {lists:reverse(Acc),[]};
split_at_length_left_bias(Children, Length, Acc) when Length =< 0 ->
    {lists:reverse(Acc), Children};
split_at_length_left_bias([E={_Name,L}|Tail], Length, Acc) ->
    split_at_length_left_bias(Tail, Length-L, [E|Acc]).

split_at_length(Children, Length) ->
    split_at_length(Children, Length, 0, []).

split_at_length(Children, 0, _, _) -> {[], Children};
split_at_length([], _Length, _AccLen, Acc) -> {lists:reverse(Acc), []};
split_at_length(Tail, Length, AccLen, Acc) when AccLen == Length ->
    {lists:reverse(Acc), Tail};
split_at_length(Tail = [{_Name,L}|_], Length, AccLen, Acc)
  when AccLen+L > Length ->
    {lists:reverse(Acc), Tail};
split_at_length([{Name,L}|Children], Length, AccLen, Acc) ->
    split_at_length(Children, Length, AccLen+L, [{Name,L}|Acc]).

five_way_split(TreePos, Nodes, InsertPos, Blocks) ->
    Offset = InsertPos - TreePos,
    ?debugFmt("offset ~p~n", [Offset]),
    {NoOverlapHeadNode, TailNode1} = split_at_length(Nodes, Offset),
    ?debugFmt("1: ~p~n", [{NoOverlapHeadNode, TailNode1}]),
    NegativeOverlap = InsertPos - (TreePos+blocklist_length(NoOverlapHeadNode)),
    ?debugFmt("neg overlap ~p~n", [NegativeOverlap]),
    {{OverlapHeadNode, TailNode2}, {OverlapHeadBlocks, TailBlocks1}} =
        shortest_subtree_split(TailNode1, Blocks, 0, NegativeOverlap),
    ?debugFmt("2: ~p~n", [{{OverlapHeadNode, TailNode2},
                           {OverlapHeadBlocks, TailBlocks1}}]),
    {{MiddleNode, TailNode3}, {MiddleBlocks, TailBlocks2}} =
        longest_divisable_subtree(TailNode2, TailBlocks1),
    ?debugFmt("3: ~p~n", [{{MiddleNode, TailNode3},
                           {MiddleBlocks, TailBlocks2}}]),
    {OverlapTailNode, NoOverlapTailNode} = case TailNode3 of
        [V|T] when length(TailBlocks2) > 0 -> {[V], T};
        _ -> {[], TailNode3}
    end,
    ?debugFmt("4: ~p~n", [{OverlapTailNode, NoOverlapTailNode}]),
    {#split{
      head=NoOverlapHeadNode,
      midhead=OverlapHeadNode,
      middle=MiddleNode,
      midtail=OverlapTailNode,
      tail=NoOverlapTailNode},
     #split{
      midhead=OverlapHeadBlocks,
      middle=MiddleBlocks,
      midtail=TailBlocks2}
    }.

blocklist_length(Children) ->
    ?debugFmt("blocklist_length(~p)~n", [Children]),
    lists:foldr(fun({_,L},Acc) -> L+Acc end, 0, Children).

foldrflatmap(_, [], _) -> [];
foldrflatmap(Fun, [Hd|Tail], Acc) ->
    {Result,Acc1} = Fun(Hd,Acc),
    Result ++ foldrflatmap(Fun, Tail, Acc1).
