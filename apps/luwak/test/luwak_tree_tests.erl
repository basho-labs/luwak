-module(luwak_tree_tests).

-include_lib("luwak/include/luwak.hrl").
-include_lib("eunit/include/eunit.hrl").

create_simple_tree_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_obj:create(Riak, <<"file1">>, 
        dict:store(tree_order, 4, 
          dict:store(block_size, 5, dict:new()))),
      {ok, Written, File2} = luwak_io:put_range(Riak, <<"file1">>, 0, <<"fuckyourcouch">>),
      BHash1 = <<"4622b193a65e6f2a7873b6bef7e3b0cf18867f687e48f40fd9eabf840d5f0ebbd65bfff586e5c38ba50e473516e8f270b6687a1f271586baf648a38aa489dd91">>,
      BHash2 = <<"08d5f211d13a9fb1e9b6902771b80459fedbb9e138b96d7a6dc3b92ad87997d24b65cc1a8594cc14b226cd511acf03eb3f4b24c7b67d270665d5bf5cb43f8fa6">>,
      BHash3 = <<"6f01ab53f4498ccfa5de27d9fa1fd1aa3c088958588db410bc35055012e6ed2795c39d8abe454402062436434b15acc78baddb016c370cd445579401562ea316">>,
      RootHash = <<"08b8b5439aecd6ab572f44fafd4cef1d1ae7adb09382046f02ad192437e7d7ff96f7fcd964c133c2039089eb03c7e219bbd272e60e3f2c6c7d1d12f8f01a5f7f">>,
      Root = luwak_obj:get_property(File2, root),
      ?assertEqual(RootHash, Root),
      {ok, RootNode} = luwak_tree:get(Riak, Root),
      Children = RootNode#n.children,
      ?assertEqual([{BHash1,5},{BHash2,5},{BHash3,3}], Children)
    end).