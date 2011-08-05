-module(luwak_io).

-export([put_range/4,
         get_range/4,
         truncate/3,
         no_tree_put_range/4]).

-include("luwak.hrl").

%% @spec put_range(Riak :: riak(), File :: luwak_file(),
%%                 Start :: int(), Data :: binary()) ->
%%  {ok, WrittenBlocks, NewFile}
%% @doc Put range performs a synchronous write operation on the given
%%      luwak file.  The write will start at the offset specified by
%%      Start and overwrite anything at that position with the
%%      contents of Data.  Writes starting beyond the end of the file
%%      will occur at the end of the file.  Luwak does not allow for
%%      gaps in a file.
put_range(Riak, File, Start, Data) ->
    internal_put_range(Riak, File, Start, Data).

%% @spec get_range(Riak :: riak(), File :: luwak_file(),
%%                 Start :: int(), Length :: int()) ->
%%        iolist()
%% @doc Returns an iolist() for the starting offset and length specified.
get_range(Riak, File, Start, Length) ->
    Root = luwak_file:get_property(File, root),
    BlockSize = luwak_file:get_property(File, block_size),
    {ok, RootObj} = luwak_tree:get(Riak, Root),
    Blocks = luwak_tree:get_range(Riak, RootObj, BlockSize, 0,
                                  Start, Start+Length),
    ?debugFmt("blocks ~p~n", [Blocks]),
    ChopHead = Start rem BlockSize,
    ?debugFmt("chophead ~p length ~p~n", [ChopHead, Length]),
    retrieve_blocks(Riak, Blocks, ChopHead, Length).

%% @spec truncate(Riak :: riak(), File :: luwak_file(), Start :: int()) ->
%%       {ok, NewFile}
%% @doc Truncates a file to the specified offset.  All bytes beyond
%%      Start will no longer be readable in NewFile.
truncate(Riak, File, 0) ->
    luwak_file:update_root(Riak, File, undefined);
truncate(Riak, File, Start) ->
    RootName = luwak_file:get_property(File, root),
    BlockSize = luwak_file:get_property(File, block_size),
    Order = luwak_file:get_property(File, tree_order),
    {ok, Root} = luwak_tree:get(Riak, RootName),
    {ok, {NewRootName,_}} =
        luwak_tree:truncate(Riak, File, Start, Root, Order, 0, BlockSize),
    luwak_file:update_root(Riak, File, NewRootName).

%%==============================================
%% internal api
%%==============================================
%% @private
no_tree_put_range(Riak, File, Start, Data) ->
    ?debugFmt("no_tree_put_range(Riak, File, ~p, ~p)~n", [Start, Data]),
    BlockSize = luwak_file:get_property(File, block_size),
    case (Start rem BlockSize) of
        0 -> 
            write_blocks(Riak, File, undefined, Start, Data, BlockSize, []);
        _BlockOffset ->
            ?debugFmt("blockoffset ~p~n", [_BlockOffset]),
            {ok, Block} = luwak_tree:block_at(Riak, File, Start-_BlockOffset),
            write_blocks(Riak, File, Block, Start, Data, BlockSize, [])
    end.

internal_put_range(Riak, File, Start, Data) ->
    BlockSize = luwak_file:get_property(File, block_size),
    BlockAlignedStart = Start - (Start rem BlockSize),
    case (Start rem BlockSize) of
        0 ->
            {ok, Written} =
                write_blocks(Riak, File, undefined, Start, Data, BlockSize, []),
            {ok, NewFile} =
                luwak_tree:update(Riak, File, BlockAlignedStart, Written),
            {ok, NewFile1} =
                luwak_file:update_checksum(Riak, NewFile,
                                           fun() -> crypto:sha(Data) end),
            {ok, Written, NewFile1};
        _BlockOffset -> 
            {ok, Block} = luwak_tree:block_at(Riak, File, Start-_BlockOffset),
            {ok, Written} =
                write_blocks(Riak, File, Block, Start, Data, BlockSize, []),
            {ok, NewFile} =
                luwak_tree:update(Riak, File, BlockAlignedStart, Written),
            {ok, NewFile1} =
                luwak_file:update_checksum(Riak, NewFile,
                                           fun() -> crypto:sha(Data) end),
            {ok, Written, NewFile1}
    end.

write_blocks(_, _, _, _Start, <<>>, _, Written) when is_list(Written) ->
    ?debugFmt("A write_blocks(_, _, _, ~p, <<>>, _, ~p) ~n", [_Start, Written]),
    {ok, lists:reverse(Written)};
%% start aligned sub-block write
write_blocks(Riak, File, undefined, Start, Data, BlockSize, Written)
  when is_list(Written), byte_size(Data) < BlockSize ->
    ?debugFmt("B write_blocks(Riak, File, ~p, ~p, ~p, ~p, ~p) ~n",
              [undefined, Start, Data, BlockSize, Written]),
    DataSize = byte_size(Data),
    case luwak_tree:block_at(Riak, File, Start) of
        {ok, undefined} ->
            {ok, NewBlock} = luwak_block:create(Riak, Data),
            {ok, lists:reverse([{luwak_block:name(NewBlock),
                                 byte_size(Data)}|
                                Written])};
        {error, notfound} ->
            {ok, NewBlock} = luwak_block:create(Riak, Data),
            {ok, lists:reverse([{luwak_block:name(NewBlock),
                                 byte_size(Data)}|
                                Written])};
        {ok, Block} ->
            BlockData = case luwak_block:data(Block) of
                            <<_:DataSize/binary, Tail/binary>> ->
                                <<Data/binary, Tail/binary>>;
                            _ ->
                                %% block usage is less than the new
                                %% data for the block
                                Data
                        end,
            {ok, NewBlock} = luwak_block:create(Riak, BlockData),
            {ok, lists:reverse([{luwak_block:name(NewBlock),
                                 byte_size(BlockData)}|
                                Written])}
    end;
%% fully aligned write
write_blocks(Riak, File, undefined, Start, Data, BlockSize, Written)
  when is_list(Written) ->
    ?debugFmt("C write_blocks(Riak, File, ~p, ~p, ~p, ~p, ~p) ~n",
              [undefined, Start, Data, BlockSize, Written]),
    <<Slice:BlockSize/binary, Tail/binary>> = Data,
    {ok, Block} = luwak_block:create(Riak, Slice),
    write_blocks(Riak, File, undefined, Start+BlockSize, Tail, BlockSize,
                 [{luwak_block:name(Block),BlockSize}|Written]);
%% we are doing a sub-block write on a full block
write_blocks(Riak, _File, PartialStartBlock, Start, Data, BlockSize, Written)
  when is_list(Written), byte_size(Data) < BlockSize,
       byte_size(PartialStartBlock) == BlockSize ->
    ?debugFmt("D write_blocks(Riak, File, ~p, ~p, ~p, ~p, ~p) ~n",
              [PartialStartBlock, Start, Data, BlockSize, Written]),
    DataSize = byte_size(Data),
    PartialStart = Start rem BlockSize,
    <<Head:PartialStart/binary, _:DataSize/binary, Tail/binary>> =
        luwak_block:data(PartialStartBlock),
    BlockData = <<Head/binary, Data/binary, Tail/binary>>,
    {ok, Block} = luwak_block:create(Riak, BlockData),
    {ok, lists:reverse([{luwak_block:name(Block),
                         byte_size(BlockData)}|
                        Written])};
%% sub block write on a partial write
write_blocks(Riak, _File, PartialStartBlock, Start, Data, BlockSize, Written)
  when is_list(Written), byte_size(Data) < BlockSize ->
  PartialStartData = luwak_block:data(PartialStartBlock),
  DataSize = byte_size(Data),
  PartialStart = Start rem BlockSize,
  BlockData = case byte_size(PartialStartData) of
    S when S < PartialStart ->
      Difference = (PartialStart - S) * 8,
      <<PartialStartData/binary, 0:Difference, Data/binary>>;
    S when S == PartialStart ->
      <<PartialStartData/binary, Data/binary>>;
    S when S > PartialStart ->
      <<Head:PartialStart/binary, Left/binary>> = PartialStartData,
      if
        byte_size(Left) > DataSize ->
          <<_:DataSize/binary, Tail/binary>> = Left,
          <<Head/binary, Data/binary, Tail/binary>>;
        true ->
          <<Head/binary, Data/binary>>
      end
  end,
  {ok, Block} = luwak_block:create(Riak, BlockData),
  {ok, lists:reverse([{luwak_block:name(Block),byte_size(BlockData)}|Written])};
%% we are starting with a sub-block write
write_blocks(Riak, File, PartialStartBlock, Start, Data, BlockSize, Written)
  when Start > BlockSize ->
    ?debugFmt("E write_blocks(Riak, File, ~p, ~p, ~p, ~p, ~p) ~n",
              [PartialStartBlock, Start, Data, BlockSize, Written]),
    HeadSize = Start rem BlockSize,
    MidSize = BlockSize - HeadSize,
    ?debugFmt("headsize ~p midsize ~p~n", [HeadSize, MidSize]),
    <<Head:HeadSize/binary,_/binary>> = luwak_block:data(PartialStartBlock),
    <<Mid:MidSize/binary, Tail/binary>> = Data,
    BlockData = <<Head/binary, Mid/binary>>,
    {ok, Block} = luwak_block:create(Riak, BlockData),
    write_blocks(Riak, File, undefined, Start+byte_size(BlockData), Tail,
                 BlockSize,
                 [{luwak_block:name(Block),byte_size(BlockData)}|Written]);
write_blocks(Riak, File, PartialStartBlock, Start, Data, BlockSize, Written)
  when is_list(Written) ->
    ?debugFmt("F write_blocks(Riak, File, ~p, ~p, ~p, ~p, ~p) ~n",
              [PartialStartBlock, Start, Data, BlockSize, Written]),
    ChopDataSize = BlockSize - Start,
    ?debugFmt("chopdatasize ~p~n", [ChopDataSize]),
    <<ChopData:ChopDataSize/binary, Tail/binary>> = Data,
    <<Head:Start/binary, _/binary>> = luwak_block:data(PartialStartBlock),
    BlockData = <<Head/binary, ChopData/binary>>,
    {ok, Block} = luwak_block:create(Riak, BlockData),
    write_blocks(Riak, File, undefined, Start+byte_size(BlockData), Tail,
                 BlockSize,
                 [{luwak_block:name(Block),byte_size(BlockData)}|Written]).

retrieve_blocks(Riak, Blocks, ChopHead, Length) ->
    retrieve_blocks(Riak, Blocks, ChopHead, Length, []).

retrieve_blocks(_Riak, [], _, _, Acc) ->
    lists:reverse(Acc);
retrieve_blocks(_Riak, _, _, 0, Acc) ->
    lists:reverse(Acc);
retrieve_blocks(Riak, [{Name,_}], _, Length, Acc) ->
    {ok, Block} = luwak_tree:get(Riak, Name),
    PreData = luwak_block:data(Block),
    case byte_size(PreData) of
        DataSize when Length < DataSize ->
            <<Data:Length/binary, _/binary>> = PreData,
            retrieve_blocks(Riak, [], 0, 0, [Data|Acc]);
        _ ->
            retrieve_blocks(Riak, [], 0, 0, [PreData|Acc])
    end;
retrieve_blocks(Riak, [{Name,_}|Children], ChopHead, Length, Acc) ->
    {ok, Block} = luwak_tree:get(Riak, Name),
    <<_:ChopHead/binary, Data/binary>> = luwak_block:data(Block),
    case byte_size(Data) of
        ByteSize when ByteSize =< Length ->
            %% wanted the rest of the block
            retrieve_blocks(Riak, Children, 0, Length - ByteSize, [Data|Acc]);
        _ ->
            %% wanted only a middle chunk of the block
            <<SubData:Length/binary, _/binary>> = Data,
            retrieve_blocks(Riak, Children, 0, 0, [SubData|Acc])
    end.
