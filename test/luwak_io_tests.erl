-module(luwak_io_tests).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

io_test_() ->
    {spawn,
     [{setup,
       fun test_helper:setup/0,
       fun test_helper:cleanup/1,
       [
        {timeout, 60000,
         [fun simple_put_range/0,
          fun simple_get_range/0,
          fun read_beyond_eof/0,
          fun multilevel_get_range/0,
          fun partial_end_block_get_range/0,
          fun partial_start_block_get_range/0,
          fun partial_middle_block_get_range/0,
          fun eof_get_range/0,
          fun truncate/0,
          fun sub_block_partial_write/0,
          fun overwrite_nonfull_block/0]}
       ]
      }
     ]
    }.

simple_put_range() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{block_size,5}], dict:new()),
      {ok, Written, _} = luwak_io:put_range(Riak, File, 0, <<"abcdefghijklm">>),
      Hash1 = <<"751fa004096ddee0b639b37d02fdb2dc68bd4eb7749bb2c17e5edc624d8a9eef4815e4cc1132cc14dfc493d1f46fc1b072653c0bcb3533e1da9676e96a4aeb30">>,
      Hash2 = <<"86a17f1a92887662099480d4bdd222d7e3372b347a1df276a183e3df5407d68ab44225967c0047f8e1a7c8d854198ac949a3d8f960385c7df4655a9143ad7fa7">>,
      Hash3 = <<"a472d1328b53ad058a383902e0a4170ad6b202f78fb1b25a5a41c8bb9f4a552f20ffebed176620f84990d2d60c9ecd85513af915e8f25bcb35e77c1b46efed67">>,
      {ok, Block1} = Riak:get(<<"luwak_node">>, Hash1, 2),
      {ok, Block2} = Riak:get(<<"luwak_node">>, Hash2, 2),
      {ok, Block3} = Riak:get(<<"luwak_node">>, Hash3, 2),
      ?assertEqual(<<"abcde">>, luwak_block:data(Block1)),
      ?assertEqual(<<"fghij">>, luwak_block:data(Block2)),
      ?assertEqual(<<"klm">>, luwak_block:data(Block3)),
      ?assertEqual([{Hash1,5},{Hash2,5},{Hash3,3}], Written)
    end).

simple_get_range() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{block_size,2},{tree_order,3}], dict:new()),
      {ok, _Written, File1} = luwak_io:put_range(Riak, File, 0, <<"abcdefghijklm">>),
      Blocks = luwak_io:get_range(Riak, File1, 3, 5),
      ok = file:write_file("tree4.dot", luwak_tree:visualize_tree(Riak, luwak_file:get_property(File1, root))),
      ?assertEqual(<<"defgh">>, iolist_to_binary(Blocks))
    end).

read_beyond_eof() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{block_size,2},{tree_order,3}], dict:new()),
      {ok, _Written, File1} = luwak_io:put_range(Riak, File, 0, <<"abcdefghijklm">>),
      Blocks = luwak_io:get_range(Riak, File1, 14, 5),
      ?assertEqual([], Blocks)
    end).

multilevel_get_range() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{block_size,3},{tree_order,3}], dict:new()),
      {ok, _Written, File1} = luwak_io:put_range(Riak, File, 0, <<"abcdefghijklmnopqrstuvwxyz">>),
      Blocks = luwak_io:get_range(Riak, File1, 4, 9),
      ok = file:write_file("tree5.dot", luwak_tree:visualize_tree(Riak, luwak_file:get_property(File1, root))),
      ?assertEqual(<<"efghijklm">>, iolist_to_binary(Blocks))
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
partial_end_block_get_range() ->
    test_helper:riak_test(
      fun(Riak) ->
              {ok, Data, File} =  make_standard_range_file(
                                    Riak, <<"endblockrange">>, 10, 3),
              Read = iolist_to_binary(
                       luwak_io:get_range(Riak, File, 10, 14)),
              <<_:10/binary, Expected:14/binary, _/binary>> = Data,
              ?assertEqual(Expected, Read)
      end).

%% tests a get_range that specifies an offset that lands in the
%% middle of a block
%% Ex: aaaaaaaaaa bbbbbbbbbb cccccccccc
%%          ^start       end^
partial_start_block_get_range() ->
    test_helper:riak_test(
      fun(Riak) ->
              {ok, Data, File} = make_standard_range_file(
                                   Riak, <<"startblockrange">>, 10, 3),
              Read = iolist_to_binary(
                       luwak_io:get_range(Riak, File, 5, 15)),
              <<_:5/binary, Expected:15/binary, _/binary>> = Data,
              ?assertEqual(Expected, Read)
      end).

%% tests a get_range that starts and ends in the same block, but
%% is unaligned with that block
%% Ex: aaaaaaaaaa bbbbbbbbbb cccccccccc
%%                 ^st end^
partial_middle_block_get_range() ->
    test_helper:riak_test(
      fun(Riak) ->
              {ok, Data, File} = make_standard_range_file(
                                   Riak, <<"midblockrange">>, 10, 3),
               Read = iolist_to_binary(
                        luwak_io:get_range(Riak, File, 11, 8)),
               <<_:11/binary, Expected:8/binary, _/binary>> = Data,
              ?assertEqual(Expected, Read)
      end).

eof_get_range() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{block_size,3},{tree_order,3}], dict:new()),
      {ok, _Written, File1} = luwak_io:put_range(Riak, File, 0, <<"abcdefghijklmnopqrstuvwxyz">>),
      ok = file:write_file("tree6.dot", luwak_tree:visualize_tree(Riak, luwak_file:get_property(File1, root))),
      Blocks = luwak_io:get_range(Riak, File1, 20, 20),
      ?assertEqual(<<"uvwxyz">>, iolist_to_binary(Blocks))
    end).

truncate() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{block_size,3},{tree_order,3}], dict:new()),
      {ok, _Written, File1} = luwak_io:put_range(Riak, File, 0, <<"abcdefghijklmnopqrstuvwxyz">>),
      {ok, File2} = luwak_io:truncate(Riak, File1, 7),
      ok = file:write_file("tree7.dot", luwak_tree:visualize_tree(Riak, luwak_file:get_property(File2, root))),
      Blocks = luwak_io:get_range(Riak, File2, 0, 7),
      ?assertEqual(<<"abcdefg">>, iolist_to_binary(Blocks))
    end).

sub_block_partial_write() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"basho">>, [{block_size,1000}], dict:new()),
      S = luwak_put_stream:start_link(Riak, File, 0, 1000),
      B1 = crypto:rand_bytes(33),
      luwak_put_stream:send(S, B1),
      luwak_put_stream:close(S),
      timer:sleep(100),
      {ok, File2} = luwak_file:get(Riak, <<"basho">>),
      S1 = luwak_put_stream:start_link(Riak, File2, 33, 1000),
      B2 = crypto:rand_bytes(33),
      luwak_put_stream:send(S1, B2),
      luwak_put_stream:close(S1),
      timer:sleep(100),
      {ok, File3} = luwak_file:get(Riak, <<"basho">>),
      Blocks = luwak_io:get_range(Riak, File3, 0, 66),
      ?debugFmt("blocks ~p~n", [Blocks]),
      ?assertEqual(<<B1/binary, B2/binary>>, iolist_to_binary(Blocks))
    end).

%% overwrite a file with more data than the file already had, but not
%% enough to touch the next block boundary
overwrite_nonfull_block() ->
    test_helper:riak_test(fun(Riak) ->
        {ok, F0} = luwak_file:create(Riak,
                                     <<"basho1">>,
                                     [{block_size, 4}],
                                     dict:new()),
        S0 = luwak_put_stream:start_link(Riak, F0, 0, 1000),
        luwak_put_stream:send(S0, <<"a">>),
        luwak_put_stream:close(S0),
        timer:sleep(100),

        {ok, F1} = luwak_file:get(Riak, <<"basho1">>),
        S1 = luwak_put_stream:start_link(Riak, F1, 0, 1000),
        luwak_put_stream:send(S1, <<"aa">>),
        luwak_put_stream:close(S1),
        timer:sleep(100),

        {ok, F2} = luwak_file:get(Riak, <<"basho1">>),
        Blocks = luwak_io:get_range(Riak, F2, 0, 4),
        ?debugFmt("blocks ~p~n", [Blocks]),
        ?assertEqual(<<"aa">>, iolist_to_binary(Blocks))
      end).

-endif.
