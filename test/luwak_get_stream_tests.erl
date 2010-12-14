-module(luwak_get_stream_tests).

-include_lib("eunit/include/eunit.hrl").

simple_get_stream_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{block_size,3},{tree_order,3}], dict:new()),
      {ok, _Written, File1} = luwak_io:put_range(Riak, File, 0, <<"wontyoupleasetouchmymonkey">>),
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
        {ok, _Written, File1} = luwak_io:put_range(Riak, File, 0, <<"wontyoupleasetouchmymonkey">>),
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
      {ok, _Written, File1} = luwak_io:put_range(Riak, File, 0, <<"wontyoupleasetouchmymonkey">>),
      GetStream = luwak_get_stream:start(Riak, File1, 30, 10),
      ?assertEqual(eos, luwak_get_stream:recv(GetStream, 1000))
    end).

%% @doc Makes a simple file, named Name, with BlockCount blocks, each
%%      filled with BlockSize characters.
%%      Ex: make_standard_range_file(R, <<"foo">>, 5, 3)
%%      will make a file named "foo" with the contents:
%%      aaaaabbbbbccccc
%% @spec make_standard_range_file(riak_client(), binary(),
%%                                integer(), integer())
%%         -> {ok, Contents::binary(), FileHandle::luwak_file()}
make_standard_range_file(Riak, Name, BlockSize, BlockCount) ->
    {ok, File} = luwak_file:create(Riak, Name,
                                   [{block_size, BlockSize}],
                                   dict:new()),
    Data = iolist_to_binary(
             [ lists:duplicate(BlockSize, $a+N) ||
                 N <- lists:seq(0, BlockCount-1) ]),
    {ok, _, NewFile} = luwak_io:put_range(Riak, File, 0, Data),
    {ok, Data, NewFile}.

%% tests a get_range that specifies an offset+length that ends in
%% the middle of a block
%% Ex: aaaaaaaaaa bbbbbbbbbb cccccccccc
%%                ^start      end^
partial_end_block_get_range_test() ->
    test_helper:riak_test(
      fun(Riak) ->
              {ok, Data, File} =  make_standard_range_file(
                                    Riak, <<"endblockrange">>, 10, 3),
              Read = read_stream(
                       luwak_get_stream:start(Riak, File, 10, 14)),
              <<_:10/binary, Expected:14/binary, _/binary>> = Data,
              ?assertEqual(Expected, Read)
      end).

%% tests a get_range that specifies an offset that lands in the
%% middle of a block
%% Ex: aaaaaaaaaa bbbbbbbbbb cccccccccc
%%          ^start       end^
partial_start_block_get_range_test() ->
    test_helper:riak_test(
      fun(Riak) ->
              {ok, Data, File} = make_standard_range_file(
                                   Riak, <<"startblockrange">>, 10, 3),
              Read = read_stream(
                       luwak_get_stream:start(Riak, File, 5, 15)),
              <<_:5/binary, Expected:15/binary, _/binary>> = Data,
              ?assertEqual(Expected, Read)
      end).


%% tests a get_range that starts and ends in the same block, but
%% is unaligned with that block
%% Ex: aaaaaaaaaa bbbbbbbbbb cccccccccc
%%                 ^st end^
partial_middle_block_get_range_test() ->
    test_helper:riak_test(
      fun(Riak) ->
              {ok, Data, File} = make_standard_range_file(
                                   Riak, <<"midblockrange">>, 10, 3),
               Read = read_stream(
                        luwak_get_stream:start(Riak, File, 11, 8)),
               <<_:11/binary, Expected:8/binary, _/binary>> = Data,
              ?assertEqual(Expected, Read)
      end).

read_stream(Stream) ->
    read_stream(Stream, luwak_get_stream:recv(Stream, 1000), []).

read_stream(_, eos, Acc) ->
    iolist_to_binary(lists:reverse(Acc));
read_stream(Stream, {Data, _}, Acc) ->
    read_stream(Stream,
                luwak_get_stream:recv(Stream, 1000),
                [Data|Acc]).
