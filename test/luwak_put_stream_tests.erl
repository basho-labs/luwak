-module(luwak_put_stream_tests).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

put_stream_test_() ->
    {spawn,
     [{setup,
       fun test_helper:setup/0,
       fun test_helper:cleanup/1,
       [
        {timeout, 60000,
         [fun aligned_put_stream/0,
          fun unaligned_put_stream/0]}
       ]
      }
     ]
    }.

aligned_put_stream() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{block_size,3},{tree_order,3}], dict:new()),
      PutStream = luwak_put_stream:start(Riak, File, 0, 1000),
      Input = [<<"abc">>, <<"def">>, <<"ghi">>, <<"jkl">>, <<"mno">>],
      lists:foreach(fun(B) -> luwak_put_stream:send(PutStream, B) end, Input),
      luwak_put_stream:close(PutStream),
      {ok, File2} = luwak_put_stream:status(PutStream, 1000),
      Blocks = luwak_io:get_range(Riak, File2, 0, 15),
      ?assertEqual(<<"abcdefghijklmno">>, iolist_to_binary(Blocks))
    end).

unaligned_put_stream() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{block_size,3},{tree_order,3}], dict:new()),
      {ok, _, File1} = luwak_io:put_range(Riak, File, 0, <<"abcdefghijklmno">>),
      ok = file:write_file("tree8.dot", luwak_tree:visualize_tree(Riak, luwak_file:get_property(File1, root))),
      PutStream = luwak_put_stream:start(Riak, File1, 4, 1000),
      Input = [<<"zyx">>, <<"wvu">>, <<"t">>],
      lists:foreach(fun(B) -> luwak_put_stream:send(PutStream, B) end, Input),
      luwak_put_stream:close(PutStream),
      {ok, File2} = luwak_put_stream:status(PutStream, 1000),
      ok = file:write_file("tree9.dot", luwak_tree:visualize_tree(Riak, luwak_file:get_property(File2, root))),
      Blocks = luwak_io:get_range(Riak, File2, 0, 15),
      timer:sleep(100),
      ?assertEqual(<<"abcdzyxwvutlmno">>, iolist_to_binary(Blocks))
    end).

-endif.
