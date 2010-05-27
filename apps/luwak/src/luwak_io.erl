-module(luwak_io).

-export([put_range/4, get_range/4, truncate/3, no_tree_put_range/4]).

put_range(Riak, File, Start, Data) ->
  internal_put_range(Riak, File, Start, Data).
  
get_range(Riak, File, Start, Length) ->
  Root = luwak_file:get_property(File, root),
  BlockSize = luwak_file:get_property(File, block_size),
  {ok, RootObj} = luwak_tree:get(Riak, Root),
  Blocks = luwak_tree:get_range(Riak, RootObj, BlockSize, 0, Start, Start+Length),
  error_logger:info_msg("blocks ~p~n", [Blocks]),
  ChopHead = Start rem BlockSize,
  error_logger:info_msg("chophead ~p choptail ~p~n", [ChopHead, Length]),
  retrieve_blocks(Riak, Blocks, ChopHead, Length).
  
truncate(Riak, File, 0) ->
  luwak_file:update_root(Riak, File, undefined);
truncate(Riak, File, Start) ->
  RootName = luwak_file:get_property(File, root),
  BlockSize = luwak_file:get_property(File, block_size),
  Order = luwak_file:get_property(File, tree_order),
  {ok, Root} = luwak_tree:get(Riak, RootName),
  {ok, {NewRootName,_}} = luwak_tree:truncate(Riak, File, Start, Root, Order, 0, BlockSize),
  luwak_file:update_root(Riak, File, NewRootName).

%%==============================================
%% internal api
%%==============================================
no_tree_put_range(Riak, File, Start, Data) ->
  error_logger:info_msg("no_tree_put_range(Riak, File, ~p, ~p)~n", [Start, Data]),
  BlockSize = luwak_file:get_property(File, block_size),
  BlockAlignedStart = Start - (Start rem BlockSize),
  case (Start rem BlockSize) of
    0 -> write_blocks(Riak, File, undefined, Start, Data, BlockSize, []);
    BlockOffset ->
      error_logger:info_msg("blockoffset ~p~n", [BlockOffset]),
      {ok, Block} = luwak_tree:block_at(Riak, File, Start),
      write_blocks(Riak, File, Block, Start, Data, BlockSize, [])
  end.

internal_put_range(Riak, File, Start, Data) ->
  BlockSize = luwak_file:get_property(File, block_size),
  BlockAlignedStart = Start - (Start rem BlockSize),
  case (Start rem BlockSize) of
    0 ->
      {ok, Written} = write_blocks(Riak, File, undefined, Start, Data, BlockSize, []),
      {ok, NewFile} = luwak_tree:update(Riak, File, BlockAlignedStart, Written),
      {ok, Written, NewFile};
    BlockOffset -> 
      {ok, Block} = luwak_tree:block_at(Riak, File, Start),
      {ok, Written} = write_blocks(Riak, File, Block, Start, Data, BlockSize, []),
      {ok, NewFile} = luwak_tree:update(Riak, File, BlockAlignedStart, Written),
      {ok, Written, NewFile}
  end.

write_blocks(_, _, _, Start, <<>>, _, Written) when is_list(Written) ->
  % error_logger:info_msg("A write_blocks(_, _, _, ~p, <<>>, _, ~p) ~n", [Start, Written]),
  {ok, lists:reverse(Written)};
%% start aligned sub-block write
write_blocks(Riak, File, undefined, Start, Data, BlockSize, Written) when is_list(Written), byte_size(Data) < BlockSize ->
  % error_logger:info_msg("B write_blocks(Riak, File, ~p, ~p, ~p, ~p, ~p) ~n", [undefined, Start, Data, BlockSize, Written]),
  DataSize = byte_size(Data),
  case luwak_tree:block_at(Riak, File, Start) of
    {ok, undefined} ->
      {ok, NewBlock} = luwak_block:create(Riak, Data),
      {ok, lists:reverse([{luwak_block:name(NewBlock),byte_size(Data)}|Written])};
    {error, notfound} ->
      {ok, NewBlock} = luwak_block:create(Riak, Data),
      {ok, lists:reverse([{luwak_block:name(NewBlock),byte_size(Data)}|Written])};
    {ok, Block} ->
      <<_:DataSize/binary, Tail/binary>> = luwak_block:data(Block),
      BlockData = <<Data/binary, Tail/binary>>,
      {ok, NewBlock} = luwak_block:create(Riak, BlockData),
      {ok, lists:reverse([{luwak_block:name(NewBlock),byte_size(BlockData)}|Written])}
  end;
%% fully aligned write
write_blocks(Riak, File, undefined, Start, Data, BlockSize, Written) when is_list(Written) ->
  % error_logger:info_msg("C write_blocks(Riak, File, ~p, ~p, ~p, ~p, ~p) ~n", [undefined, Start, Data, BlockSize, Written]),
  <<Slice:BlockSize/binary, Tail/binary>> = Data,
  {ok, Block} = luwak_block:create(Riak, Slice),
  write_blocks(Riak, File, undefined, Start+BlockSize, Tail, BlockSize, [{luwak_block:name(Block),BlockSize}|Written]);
%% we are doing a sub-block write
write_blocks(Riak, File, PartialStartBlock, Start, Data, BlockSize, Written) when is_list(Written), byte_size(Data) < BlockSize ->
  % error_logger:info_msg("D write_blocks(Riak, File, ~p, ~p, ~p, ~p, ~p) ~n", [PartialStartBlock, Start, Data, BlockSize, Written]),
  DataSize = byte_size(Data),
  PartialStart = Start rem BlockSize,
  <<Head:PartialStart/binary, _:DataSize/binary, Tail/binary>> = luwak_block:data(PartialStartBlock),
  BlockData = <<Head/binary, Data/binary, Tail/binary>>,
  {ok, Block} = luwak_block:create(Riak, BlockData),
  {ok, lists:reverse([{luwak_block:name(Block),byte_size(BlockData)}|Written])};
%% we are starting with a sub-block write
write_blocks(Riak, File, PartialStartBlock, Start, Data, BlockSize, Written) when Start > BlockSize ->
  % error_logger:info_msg("E write_blocks(Riak, File, ~p, ~p, ~p, ~p, ~p) ~n", [PartialStartBlock, Start, Data, BlockSize, Written]),
  HeadSize = Start rem BlockSize,
  MidSize = BlockSize - HeadSize,
  % error_logger:info_msg("headsize ~p midsize ~p~n", [HeadSize, MidSize]),
  <<Head:HeadSize/binary,_/binary>> = luwak_block:data(PartialStartBlock),
  <<Mid:MidSize/binary, Tail/binary>> = Data,
  BlockData = <<Head/binary, Mid/binary>>,
  {ok, Block} = luwak_block:create(Riak, BlockData),
  write_blocks(Riak, File, undefined, Start+byte_size(BlockData), Tail, BlockSize, [{luwak_block:name(Block),byte_size(BlockData)}|Written]);
write_blocks(Riak, File, PartialStartBlock, Start, Data, BlockSize, Written) when is_list(Written) ->
  % error_logger:info_msg("F write_blocks(Riak, File, ~p, ~p, ~p, ~p, ~p) ~n", [PartialStartBlock, Start, Data, BlockSize, Written]),
  ChopDataSize = BlockSize - Start,
  % error_logger:info_msg("chopdatasize ~p~n", [ChopDataSize]),
  <<ChopData:ChopDataSize/binary, Tail/binary>> = Data,
  <<Head:Start/binary, _/binary>> = luwak_block:data(PartialStartBlock),
  BlockData = <<Head/binary, ChopData/binary>>,
  {ok, Block} = luwak_block:create(Riak, BlockData),
  write_blocks(Riak, File, undefined, Start+byte_size(BlockData), Tail, BlockSize, [{luwak_block:name(Block),byte_size(BlockData)}|Written]).

retrieve_blocks(Riak, Blocks, ChopHead, Length) ->
  retrieve_blocks(Riak, Blocks, ChopHead, Length, []).
  
retrieve_blocks(Riak, [], _, _, Acc) ->
  lists:reverse(Acc);
retrieve_blocks(Riak, [{Name,_}], _, 0, Acc) ->
  {ok, Block} = luwak_tree:get(Riak, Name),
  retrieve_blocks(Riak, [], 0, 0, [luwak_block:data(Block)|Acc]);
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
  retrieve_blocks(Riak, Children, 0, Length - byte_size(Data), [Data|Acc]).
