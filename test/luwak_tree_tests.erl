-module(luwak_tree_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("luwak/include/luwak.hrl").

create_simple_tree_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{tree_order,4},{block_size,5}], dict:new()),
      {ok, _Written, File2} = luwak_io:put_range(Riak, File, 0, <<"fuckyourcouch">>),
      BHash1 = <<"4622b193a65e6f2a7873b6bef7e3b0cf18867f687e48f40fd9eabf840d5f0ebbd65bfff586e5c38ba50e473516e8f270b6687a1f271586baf648a38aa489dd91">>,
      BHash2 = <<"08d5f211d13a9fb1e9b6902771b80459fedbb9e138b96d7a6dc3b92ad87997d24b65cc1a8594cc14b226cd511acf03eb3f4b24c7b67d270665d5bf5cb43f8fa6">>,
      BHash3 = <<"6f01ab53f4498ccfa5de27d9fa1fd1aa3c088958588db410bc35055012e6ed2795c39d8abe454402062436434b15acc78baddb016c370cd445579401562ea316">>,
      RootHash = <<"08b8b5439aecd6ab572f44fafd4cef1d1ae7adb09382046f02ad192437e7d7ff96f7fcd964c133c2039089eb03c7e219bbd272e60e3f2c6c7d1d12f8f01a5f7f">>,
      Root = luwak_file:get_property(File2, root),
      ?assertEqual(RootHash, Root),
      {ok, RootNode} = luwak_tree:get(Riak, Root),
      Children = RootNode#n.children,
      ?assertEqual([{BHash1,5},{BHash2,5},{BHash3,3}], Children)
    end).

create_and_overwrite_middle_tree_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{tree_order,4},{block_size,5}], dict:new()),
      {ok, _Written1, File2} = luwak_io:put_range(Riak, File, 0, <<"fuckyourcouch">>),
      _BHash1 = <<"4622b193a65e6f2a7873b6bef7e3b0cf18867f687e48f40fd9eabf840d5f0ebbd65bfff586e5c38ba50e473516e8f270b6687a1f271586baf648a38aa489dd91">>,
      _BHash2 = <<"08d5f211d13a9fb1e9b6902771b80459fedbb9e138b96d7a6dc3b92ad87997d24b65cc1a8594cc14b226cd511acf03eb3f4b24c7b67d270665d5bf5cb43f8fa6">>,
      BHash3 = <<"6f01ab53f4498ccfa5de27d9fa1fd1aa3c088958588db410bc35055012e6ed2795c39d8abe454402062436434b15acc78baddb016c370cd445579401562ea316">>,
      {ok, _Written2, File3} = luwak_io:put_range(Riak, File2, 1, <<"ballstoyou">>),
      BHash1_2 = <<"3717fd47d84a96cb99791d1c24d652a712e2841f2a3d0a282d22f9a477453bb7c394a1989115acf882fb867483cc2ba429f2280c1014cd82cc95aa8f1e8a1e4c">>,
      BHash2_2 = <<"16c46831340abea94de6a5369466c0e8f710c15e2eec684067b89b27ca48856d7833933d28a1213cff5887bc57e5fdbb1d5e19bff7c6bb1f412a5db74612314b">>,
      RootHash2 = <<"53bfd159bbc194ff331dbdcd19f9ab98480b554f88708a342ac2eb1f94229d0444a47a9d5fa5d8b14d1a6264d3df0dde3225f4c21536ed5874fbb0adfabc28eb">>,
      Root = luwak_file:get_property(File3, root),
      ?assertEqual(RootHash2, Root),
      {ok, RootNode} = luwak_tree:get(Riak, Root),
      Children = RootNode#n.children,
      ?assertEqual([{BHash1_2,5},{BHash2_2,5},{BHash3,3}], Children)
    end).

create_multilevel_tree_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{tree_order,5},{block_size,1}], dict:new()),
      {ok, _Written1, File2} = luwak_io:put_range(Riak, File, 0, <<"fuckyourcouch">>),
      Blocks = [ {skerl:hexhash(512, list_to_binary([C])), 1} || C <- binary_to_list(<<"fuckyourcouch">>) ],
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
      {ok, _Written1, File2} = luwak_io:put_range(Riak, File, 0, <<"fuckyourcouch">>),
      % ok = file:write_file("tree1.dot", luwak_tree:visualize_tree(Riak, luwak_file:get_property(File2, root))),
      {ok, _Written2, File3} = luwak_io:put_range(Riak, File2, 1, <<"ballstoyou">>),
      Blocks = [ {skerl:hexhash(512, list_to_binary([C])), 1} || C <- binary_to_list(<<"fballstoyouch">>) ],
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
      {ok, _Written1, File2} = luwak_io:put_range(Riak, File, 0, <<"wontyouplease">>),
      {ok, _Written2, File3} = luwak_io:put_range(Riak, File2, 13, <<"touchmymonkey">>),
      _Blocks = [ {skerl:hexhash(512, X), 2} || <<X:2/binary>> <= <<"wontyoupleasetouchmymonkey">> ],
      ok = file:write_file("tree3.dot", luwak_tree:visualize_tree(Riak, luwak_file:get_property(File3, root))),
      ?assertEqual(<<"wontyoupleasetouchmymonkey">>, iolist_to_binary(luwak_io:get_range(Riak, File3, 0, 26)))      
    end).

append_beyond_pointer_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{tree_order,3},{block_size,2}], dict:new()),
      {ok, _Written1, File2} = luwak_io:put_range(Riak, File, 0, <<"wontyouplease">>),
      {ok, _Written2, File3} = luwak_io:put_range(Riak, File2, 15, <<"touchmymonkey">>),
      ?assertEqual(<<"wontyoupleasetouchmymonkey">>, iolist_to_binary(luwak_io:get_range(Riak, File3, 0, 26)))
    end).

block_at_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{tree_order,3},{block_size,3}], dict:new()),
      {ok, _Written1, File1} = luwak_io:put_range(Riak, File, 0, <<"heywhyareyoudrp">>),
      {ok, Block} = luwak_tree:block_at(Riak, File1, 9),
      Data = luwak_block:data(Block),
      timer:sleep(1000),
      ?assertEqual(<<"you">>, Data)
    end).
