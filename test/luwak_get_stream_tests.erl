-module(luwak_get_stream_tests).

-include_lib("eunit/include/eunit.hrl").

simple_get_stream_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{block_size,3},{tree_order,3}], dict:new()),
      {ok, Written, File1} = luwak_io:put_range(Riak, File, 0, <<"wontyoupleasetouchmymonkey">>),
      GetStream = luwak_get_stream:start(Riak, File1, 0, 26),
      timer:sleep(100),
      ?assertEqual({<<"won">>, 0}, luwak_get_stream:recv(GetStream, 1000)),
      ?assertEqual({<<"tyo">>, 3}, luwak_get_stream:recv(GetStream, 1000)),
      ?assertEqual({<<"upl">>, 6}, luwak_get_stream:recv(GetStream, 1000)),
      ?assertEqual({<<"eas">>, 9}, luwak_get_stream:recv(GetStream, 1000)),
      ?assertEqual({<<"eto">>, 12}, luwak_get_stream:recv(GetStream, 1000)),
      ?assertEqual({<<"uch">>, 15}, luwak_get_stream:recv(GetStream, 1000)),
      ?assertEqual({<<"mym">>, 18}, luwak_get_stream:recv(GetStream, 1000)),
      ?assertEqual({<<"onk">>, 21}, luwak_get_stream:recv(GetStream, 1000)),
      ?assertEqual({<<"ey">>, 24}, luwak_get_stream:recv(GetStream, 1000)),
      ?assertEqual(eos, luwak_get_stream:recv(GetStream, 1000))
    end).
    
three_level_tree_stream_test_() ->
  Test = fun() ->
    test_helper:riak_test(fun(Riak) ->
        {ok, File} = luwak_file:create(Riak, <<"file1">>, [{block_size,2},{tree_order,2}], dict:new()),
        {ok, Written, File1} = luwak_io:put_range(Riak, File, 0, <<"wontyoupleasetouchmymonkey">>),
        GetStream = luwak_get_stream:start(Riak, File1, 3, 10),
        ?assertEqual({<<"t">>, 3}, luwak_get_stream:recv(GetStream, 1000)),
        ?assertEqual({<<"yo">>, 4}, luwak_get_stream:recv(GetStream, 1000)),
        ?assertEqual({<<"up">>, 6}, luwak_get_stream:recv(GetStream, 1000)),
        ?assertEqual({<<"le">>, 8}, luwak_get_stream:recv(GetStream, 1000)),
        ?assertEqual({<<"as">>, 10}, luwak_get_stream:recv(GetStream, 1000)),
        ?assertEqual({<<"e">>, 12}, luwak_get_stream:recv(GetStream, 1000)),
        ?assertEqual(eos, luwak_get_stream:recv(GetStream, 1000))
      end)
    end,
  {timeout, 30000, Test}.

read_beyond_file_end_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{block_size,2},{tree_order,2}], dict:new()),
      {ok, Written, File1} = luwak_io:put_range(Riak, File, 0, <<"wontyoupleasetouchmymonkey">>),
      GetStream = luwak_get_stream:start(Riak, File1, 30, 10),
      ?assertEqual(eos, luwak_get_stream:recv(GetStream, 1000))
    end).
