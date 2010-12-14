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
      {ok, _Written, File1} = luwak_io:put_range(Riak, File, 0, <<"fuckyourcouch">>),
      Blocks = luwak_io:get_range(Riak, File1, 3, 5),
      ok = file:write_file("tree4.dot", luwak_tree:visualize_tree(Riak, luwak_file:get_property(File1, root))),
      ?assertEqual(<<"kyour">>, iolist_to_binary(Blocks))
    end).
    
read_beyond_eof_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{block_size,2},{tree_order,3}], dict:new()),
      {ok, _Written, File1} = luwak_io:put_range(Riak, File, 0, <<"fuckyourcouch">>),
      Blocks = luwak_io:get_range(Riak, File1, 14, 5),
      ?assertEqual([], Blocks)
    end).

multilevel_get_range_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{block_size,3},{tree_order,3}], dict:new()),
      {ok, _Written, File1} = luwak_io:put_range(Riak, File, 0, <<"wontyoupleasetouchmymonkey">>),
      Blocks = luwak_io:get_range(Riak, File1, 4, 9),
      ok = file:write_file("tree5.dot", luwak_tree:visualize_tree(Riak, luwak_file:get_property(File1, root))),
      ?assertEqual(<<"youplease">>, iolist_to_binary(Blocks))
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
              Read = iolist_to_binary(
                       luwak_io:get_range(Riak, File, 10, 14)),
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
              Read = iolist_to_binary(
                       luwak_io:get_range(Riak, File, 5, 15)),
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
               Read = iolist_to_binary(
                        luwak_io:get_range(Riak, File, 11, 8)),
               <<_:11/binary, Expected:8/binary, _/binary>> = Data,
              ?assertEqual(Expected, Read)
      end).

eof_get_range_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{block_size,3},{tree_order,3}], dict:new()),
      {ok, _Written, File1} = luwak_io:put_range(Riak, File, 0, <<"wontyoupleasetouchmymonkey">>),
      ok = file:write_file("tree6.dot", luwak_tree:visualize_tree(Riak, luwak_file:get_property(File1, root))),
      Blocks = luwak_io:get_range(Riak, File1, 20, 20),
      ?assertEqual(<<"monkey">>, iolist_to_binary(Blocks))
    end).

truncate_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, File} = luwak_file:create(Riak, <<"file1">>, [{block_size,3},{tree_order,3}], dict:new()),
      {ok, _Written, File1} = luwak_io:put_range(Riak, File, 0, <<"wontyoupleasetouchmymonkey">>),
      {ok, File2} = luwak_io:truncate(Riak, File1, 7),
      ok = file:write_file("tree7.dot", luwak_tree:visualize_tree(Riak, luwak_file:get_property(File2, root))),
      Blocks = luwak_io:get_range(Riak, File2, 0, 7),
      ?assertEqual(<<"wontyou">>, iolist_to_binary(Blocks))
    end).

sub_block_partial_write_test() ->
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
overwrite_nonfull_block_test() ->
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
