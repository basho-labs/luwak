-module(luwak_io).

-export([put_range/4, get_range/4, truncate/3]).

put_range(Riak, Name, Start, Data) ->
  case luwak_obj:get(Riak, Name) of
    {ok, File} -> internal_put_range(Riak, File, Start, Data);
    Err -> Err
  end.
  
get_range(Riak, Name, Start, Length) ->
  ok.
  
truncate(Riak, Name, Start) ->
  ok.


%%==============================================
%% internal api
%%==============================================
internal_put_range(Riak, File, Start, Data) ->
  BlockSize = luwak_obj:get_property(File, block_size),
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
    {ok, Block} ->
      <<_:DataSize/binary, Tail/binary>> = luwak_block:data(Block),
      BlockData = <<Data/binary, Tail/binary>>,
      {ok, NewBlock} = luwak_block:create(Riak, BlockData),
      {ok, lists:reverse([{luwak_block:name(NewBlock),byte_size(BlockData)}|Written])};
    {error, notfound} ->
      {ok, NewBlock} = luwak_block:create(Riak, Data),
      {ok, lists:reverse([{luwak_block:name(NewBlock),byte_size(Data)}|Written])}
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
  <<Head:Start/binary, _:DataSize/binary, Tail/binary>> = luwak_block:data(PartialStartBlock),
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
