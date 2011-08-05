-module(luwak_tree_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("luwak.hrl").

create_simple_tree_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{tree_order,4},{block_size,5}], dict:new()),
      {ok, _Written, File2} = luwak_io:put_range(Riak, File, 0, <<"abcdefghijklm">>),
      BHash1 = <<"751fa004096ddee0b639b37d02fdb2dc68bd4eb7749bb2c17e5edc624d8a9eef4815e4cc1132cc14dfc493d1f46fc1b072653c0bcb3533e1da9676e96a4aeb30">>,
      BHash2 = <<"86a17f1a92887662099480d4bdd222d7e3372b347a1df276a183e3df5407d68ab44225967c0047f8e1a7c8d854198ac949a3d8f960385c7df4655a9143ad7fa7">>,
      BHash3 = <<"a472d1328b53ad058a383902e0a4170ad6b202f78fb1b25a5a41c8bb9f4a552f20ffebed176620f84990d2d60c9ecd85513af915e8f25bcb35e77c1b46efed67">>,
      RootHash = <<"5676ff048bedcf789df26fec4d118241089f71929579f023cbe7a26c620ad242106f91ee7b9f4d5f3c2c305bcbc5ba9c95b2e5d24706c8ab76305a32866e7de8">>,
      Root = luwak_file:get_property(File2, root),
      ?assertEqual(RootHash, Root),
      {ok, RootNode} = luwak_tree:get(Riak, Root),
      Children = RootNode#n.children,
      ?assertEqual([{BHash1,5},{BHash2,5},{BHash3,3}], Children)
    end).

create_and_overwrite_middle_tree_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{tree_order,4},{block_size,5}], dict:new()),
      {ok, _Written1, File2} = luwak_io:put_range(Riak, File, 0, <<"abcdefghijklm">>),
      BHash3 = <<"9b0b42ba73433d128142716ec92c0b6193fb5646eb4094aacd0246ac25c109ace790d6c97d08df257407f42a7c1ba8163fc209117a229c0236bc0994fae353a6">>,
      {ok, _Written2, File3} = luwak_io:put_range(Riak, File2, 1, <<"zyxwvutsrq">>),
      BHash1_2 = <<"dc9cb02788c70dda88e58b35c301d9824c07466dbc86298145327536524f3baa5e3d9cdbb40642e06c8ed9ce7f097923f39a0d3f38bb24729a0cbe9a4c8f2a01">>,
      BHash2_2 = <<"6f92ce05db424b6ea3b9bb9ad51d40b311490d5b3a15b8dbe2621e38796885369df197177bea421556dd2ef556301c240fcde02e41352ac9807a530af5a0ea46">>,
      RootHash2 = <<"14ec341979962a61404a79552754b12fc9faf9e747e1f13d2792ef5d9609b0a406d5fc25952da64eb61a6d99435c27cce4cf85a3d8bbea348bc7215bd8071eee">>,
      Root = luwak_file:get_property(File3, root),
      ?assertEqual(RootHash2, Root),
      {ok, RootNode} = luwak_tree:get(Riak, Root),
      Children = RootNode#n.children,
      ?assertEqual([{BHash1_2,5},{BHash2_2,5},{BHash3,3}], Children)
    end).

create_multilevel_tree_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{tree_order,5},{block_size,1}], dict:new()),
      {ok, _Written1, File2} = luwak_io:put_range(Riak, File, 0, <<"abcdefghijklm">>),
      Blocks = [ {skerl:hexhash(512, list_to_binary([C])), 1} || C <- binary_to_list(<<"abcdefghijklm">>) ],
      {FirstNodeChildren, Tail1} = lists:split(5, Blocks),
      {SecondNodeChildren, ThirdNodeChildren} = lists:split(5, Tail1),
      Node1 = skerl:hexhash(512, term_to_binary(FirstNodeChildren)),
      Node2 = skerl:hexhash(512, term_to_binary(SecondNodeChildren)),
      Node3 = skerl:hexhash(512, term_to_binary(ThirdNodeChildren)),
      RootChildren = [{Node1,5}, {Node2,5}, {Node3,3}],
      Root1 = skerl:hexhash(512, term_to_binary(RootChildren)),
      ?assertEqual(Root1, luwak_file:get_property(File2, root)),
      {ok, RootNode} = luwak_tree:get(Riak, Root1),
      ?assertEqual(RootChildren, RootNode#n.children),
      {ok, Node1Node} = luwak_tree:get(Riak, Node1),
      {ok, Node2Node} = luwak_tree:get(Riak, Node2),
      {ok, Node3Node} = luwak_tree:get(Riak, Node3),
      ?assertEqual(FirstNodeChildren, Node1Node#n.children),
      ?assertEqual(SecondNodeChildren, Node2Node#n.children),
      ?assertEqual(ThirdNodeChildren, Node3Node#n.children)
    end).

create_and_overwrite_multilevel_tree_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{tree_order,5},{block_size,1}], dict:new()),
      {ok, _Written1, File2} = luwak_io:put_range(Riak, File, 0, <<"abcdefghijklm">>),
      % ok = file:write_file("tree1.dot", luwak_tree:visualize_tree(Riak, luwak_file:get_property(File2, root))),
      {ok, _Written2, File3} = luwak_io:put_range(Riak, File2, 1, <<"zyxwvutsrq">>),
      Blocks = [ {skerl:hexhash(512, list_to_binary([C])), 1} || C <- binary_to_list(<<"azyxwvutsrqlm">>) ],
      {FirstNodeChildren, Tail1} = lists:split(5, Blocks),
      {SecondNodeChildren, ThirdNodeChildren} = lists:split(5, Tail1),
      Node1 = skerl:hexhash(512, term_to_binary(FirstNodeChildren)),
      Node2 = skerl:hexhash(512, term_to_binary(SecondNodeChildren)),
      Node3 = skerl:hexhash(512, term_to_binary(ThirdNodeChildren)),
      RootChildren = [{Node1,5}, {Node2,5}, {Node3,3}],
      Root1 = skerl:hexhash(512, term_to_binary(RootChildren)),
      ok = file:write_file("tree2.dot", luwak_tree:visualize_tree(Riak, luwak_file:get_property(File3, root))),
      ?assertEqual(Root1, luwak_file:get_property(File3, root)),
            {ok, RootNode} = luwak_tree:get(Riak, Root1),
      ?assertEqual(RootChildren, RootNode#n.children),
      {ok, Node1Node} = luwak_tree:get(Riak, Node1),
      {ok, Node2Node} = luwak_tree:get(Riak, Node2),
      {ok, Node3Node} = luwak_tree:get(Riak, Node3),
      ?assertEqual(FirstNodeChildren, Node1Node#n.children),
      ?assertEqual(SecondNodeChildren, Node2Node#n.children),
      ?assertEqual(ThirdNodeChildren, Node3Node#n.children)
    end).

create_and_append_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{tree_order,3},{block_size,2}], dict:new()),
      {ok, _Written1, File2} = luwak_io:put_range(Riak, File, 0, <<"abcdefghijklm">>),
      {ok, _Written2, File3} = luwak_io:put_range(Riak, File2, 13, <<"nopqrstuvwxyz">>),
      _Blocks = [ {skerl:hexhash(512, X), 2} || <<X:2/binary>> <= <<"abcdefghijklmnopqrstuvwxyz">> ],
      ok = file:write_file("tree3.dot", luwak_tree:visualize_tree(Riak, luwak_file:get_property(File3, root))),
      ?assertEqual(<<"abcdefghijklmnopqrstuvwxyz">>, iolist_to_binary(luwak_io:get_range(Riak, File3, 0, 26)))
    end).

append_beyond_pointer_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{tree_order,3},{block_size,2}], dict:new()),
      {ok, _Written1, File2} = luwak_io:put_range(Riak, File, 0, <<"abcdefghijklm">>),
      {ok, _Written2, File3} = luwak_io:put_range(Riak, File2, 15, <<"nopqrstuvwxyz">>),
      ?assertEqual(<<"abcdefghijklmnopqrstuvwxyz">>, iolist_to_binary(luwak_io:get_range(Riak, File3, 0, 26)))
    end).

block_at_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{tree_order,3},{block_size,3}], dict:new()),
      {ok, _Written1, File1} = luwak_io:put_range(Riak, File, 0, <<"abcdefghijklmno">>),
      {ok, Block} = luwak_tree:block_at(Riak, File1, 9),
      Data = luwak_block:data(Block),
      timer:sleep(1000),
      ?assertEqual(<<"jkl">>, Data)
    end).
