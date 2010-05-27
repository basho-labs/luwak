-module(luwak_io_tests).

-include_lib("eunit/include/eunit.hrl").

simple_put_range_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{block_size,5}], dict:new()),
      {ok, Written, _} = luwak_io:put_range(Riak, File, 0, <<"fuckyourcouch">>),
      Hash1 = <<"4622b193a65e6f2a7873b6bef7e3b0cf18867f687e48f40fd9eabf840d5f0ebbd65bfff586e5c38ba50e473516e8f270b6687a1f271586baf648a38aa489dd91">>,
      Hash2 = <<"08d5f211d13a9fb1e9b6902771b80459fedbb9e138b96d7a6dc3b92ad87997d24b65cc1a8594cc14b226cd511acf03eb3f4b24c7b67d270665d5bf5cb43f8fa6">>,
      Hash3 = <<"6f01ab53f4498ccfa5de27d9fa1fd1aa3c088958588db410bc35055012e6ed2795c39d8abe454402062436434b15acc78baddb016c370cd445579401562ea316">>,
      {ok, Block1} = Riak:get(<<"luwak_node">>, Hash1, 2),
      {ok, Block2} = Riak:get(<<"luwak_node">>, Hash2, 2),
      {ok, Block3} = Riak:get(<<"luwak_node">>, Hash3, 2),
      ?assertEqual(<<"fucky">>, luwak_block:data(Block1)),
      ?assertEqual(<<"ourco">>, luwak_block:data(Block2)),
      ?assertEqual(<<"uch">>, luwak_block:data(Block3)),
      ?assertEqual([{Hash1,5},{Hash2,5},{Hash3,3}], Written)
    end).

simple_get_range_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{block_size,2},{tree_order,3}], dict:new()),
      {ok, Written, File1} = luwak_io:put_range(Riak, File, 0, <<"fuckyourcouch">>),
      Blocks = luwak_io:get_range(Riak, File1, 3, 5),
      ok = file:write_file("/Users/cliff/tree4.dot", luwak_tree:visualize_tree(Riak, luwak_file:get_property(File1, root))),
      ?assertEqual(<<"kyour">>, iolist_to_binary(Blocks))
    end).

multilevel_get_range_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{block_size,3},{tree_order,3}], dict:new()),
      {ok, Written, File1} = luwak_io:put_range(Riak, File, 0, <<"wontyoupleasetouchmymonkey">>),
      Blocks = luwak_io:get_range(Riak, File1, 4, 9),
      ok = file:write_file("/Users/cliff/tree5.dot", luwak_tree:visualize_tree(Riak, luwak_file:get_property(File1, root))),
      ?assertEqual(<<"youplease">>, iolist_to_binary(Blocks))
    end).

eof_get_range_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{block_size,3},{tree_order,3}], dict:new()),
      {ok, Written, File1} = luwak_io:put_range(Riak, File, 0, <<"wontyoupleasetouchmymonkey">>),
      ok = file:write_file("/Users/cliff/tree6.dot", luwak_tree:visualize_tree(Riak, luwak_file:get_property(File1, root))),
      Blocks = luwak_io:get_range(Riak, File1, 20, 20),
      ?assertEqual(<<"monkey">>, iolist_to_binary(Blocks))
    end).

truncate_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{block_size,3},{tree_order,3}], dict:new()),
      {ok, Written, File1} = luwak_io:put_range(Riak, File, 0, <<"wontyoupleasetouchmymonkey">>),
      {ok, File2} = luwak_io:truncate(Riak, File1, 7),
      ok = file:write_file("/Users/cliff/tree7.dot", luwak_tree:visualize_tree(Riak, luwak_file:get_property(File2, root))),
      Blocks = luwak_io:get_range(Riak, File2, 0, 7),
      ?assertEqual(<<"wontyou">>, iolist_to_binary(Blocks))
    end).
