-module(luwak_tree_utils_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("luwak/include/luwak.hrl").

left_list_larger_subtree_test() ->
    A = [{a,100}, {b,50}, {c,200}],
    B = [{d,50}, {e,50}, {f,50}, {g,50}, {h,50}],
    ?assertEqual({{[{a,100}, {b,50}], [{c,200}]},
                  {[{d,50}, {e,50}, {f,50}], [{g,50}, {h,50}]}},
                 luwak_tree_utils:longest_divisable_subtree(A, B)).

right_list_larger_subtree_test() ->
    A = [{a,100}, {b,50}, {c,200}],
    B = [{d,50}, {e,50}, {f,50}, {g,50}, {h,50}],
    ?assertEqual({{[{d,50}, {e,50}, {f,50}, {g,50}, {h,50}], []},
                  {[{a,100}, {b,50}, {c,200}], []}},
                 luwak_tree_utils:longest_divisable_subtree(B, A)).

equal_list_subtree_test() ->
    A = [{a,100}, {b,50}, {c,200}],
    B = [{d,50}, {e,50}, {f,50}, {g,50}, {h,50}, {i,50}, {j,50}],
    ?assertEqual({{A, []},
                  {B, []}},
                 luwak_tree_utils:longest_divisable_subtree(A, B)).

no_subtree_test() ->
    A = [{a,100}],
    B = [{b,50}],
    ?assertEqual({{[], A},
                  {[], B}},
                 luwak_tree_utils:longest_divisable_subtree(A, B)).

split_at_length_test() ->
    A = [{a, 100}, {b, 50}, {c,200}],
    ?assertEqual({[{a,100},{b,50}],
                  [{c,200}]},
                 luwak_tree_utils:split_at_length(A, 200)).

split_at_length_left_bias_test() ->
    A = [{a,100},{b,50},{c,200}],
    ?assertEqual({A,[]},
                 luwak_tree_utils:split_at_length_left_bias(A, 200)).

simple_five_way_split_test() ->
    Nodes = [{a,100},{b,100},{c,100},{d,100},{e,100}],
    Blocks = [{f,50},{g,50},{h,50},{i,50}],
    {NodeSplit,BlockSplit} = luwak_tree_utils:five_way_split(0, Nodes, 150, Blocks),
    ?assertEqual(#split{
                    head=[{a,100}],
                    midhead=[{b,100}],
                    middle=[{c,100}],
                    midtail=[{d,100}],
                    tail=[{e,100}]}, NodeSplit),
    ?assertEqual(#split{
                    midhead=[{f,50}],
                    middle=[{g,50},{h,50}],
                    midtail=[{i,50}]}, BlockSplit).

no_head_five_way_split_test() ->
    Nodes = [{a,100},{b,100},{c,100},{d,100},{e,100}],
    Blocks = [{f,50},{g,50},{h,50},{i,50}],
    {NodeSplit,BlockSplit} = luwak_tree_utils:five_way_split(0, Nodes, 50, Blocks),
    ?assertEqual(#split{
                    head=[],
                    midhead=[{a,100}],
                    middle=[{b,100}],
                    midtail=[{c,100}],
                    tail=[{d,100},{e,100}]}, NodeSplit),
    ?assertEqual(#split{
                    midhead=[{f,50}],
                    middle=[{g,50},{h,50}],
                    midtail=[{i,50}]}, BlockSplit).

equal_start_five_way_split_test() ->
    Nodes = [{a,100},{b,100},{c,100},{d,100},{e,100}],
    Blocks = [{f,50},{g,50},{h,50},{i,50}],
    {NodeSplit,BlockSplit} = luwak_tree_utils:five_way_split(0, Nodes, 0, Blocks),
    ?assertEqual(#split{
                    head=[],
                    midhead=[],
                    middle=[{a,100},{b,100}],
                    midtail=[],
                    tail=[{c,100},{d,100},{e,100}]}, NodeSplit),
    ?assertEqual(#split{
                    midhead=[],
                    middle=[{f,50},{g,50},{h,50},{i,50}],
                    midtail=[]}, BlockSplit).

appending_five_way_split_test() ->
    Nodes = [{a,6},{b,6},{c,1}],
    Blocks = [{d,2},{e,2},{f,2},{g,2},{h,2},{i,2},{j,2}],
    _A = {NodeSplit,_BlockSplit} = luwak_tree_utils:five_way_split(0, Nodes, 12, Blocks),
    ?assertEqual(#split{
                    head=[{a,6},{b,6}],
                    middle=[{c,1}]}, NodeSplit).

first_block_five_way_split_test() ->
    Nodes = [{a,1},{b,1},{c,1}],
    Blocks = [{a,1}],
    {NodeSplit, BlockSplit} = luwak_tree_utils:five_way_split(9, Nodes, 9, Blocks),
    ?assertEqual(#split{middle=[{a,1}],tail=[{b,1},{c,1}]}, NodeSplit),
    ?assertEqual(#split{middle=Blocks}, BlockSplit).

subtree_split_test() ->
    ListA = [{a,50},{b,50}, {c,50}],
    ListB = [{d,150}, {e,100}],
    ?assertEqual({{[{a,50},{b,50}],[{c,50}]},
                  {[{d,150}],[{e,100}]}},
                 luwak_tree_utils:shortest_subtree_split(ListA, ListB, 50, 0)).
