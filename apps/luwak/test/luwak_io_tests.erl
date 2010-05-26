-module(luwak_io_tests).

-include_lib("eunit/include/eunit.hrl").

simple_put_range_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_obj:create(Riak, <<"file1">>, dict:store(block_size, 5, dict:new())),
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
