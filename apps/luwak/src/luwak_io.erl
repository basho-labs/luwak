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
  case (Start rem BlockSize) of
    0 ->
      {ok, Written} = write_blocks(Riak, File, undefined, Start, Data, BlockSize, []),
      {ok, NewFile} = luwak_tree:update(Riak, File, Start, Written),
      {ok, Written, NewFile};
    BlockOffset -> 
      Block = luwak_tree:block_at(Riak, File, Start),
      {ok, Written} = write_blocks(Riak, File, Block, Start, Data, BlockSize, []),
      {ok, NewFile} = luwak_tree:update(Riak, File, Start, Written),
      {ok, Written, NewFile}
  end.

write_blocks(_, _, _, Start, <<>>, _, Written) ->
  {ok, lists:reverse(Written)};
%% start aligned sub-block write
write_blocks(Riak, File, undefined, Start, Data, BlockSize, Written) when byte_size(Data) < BlockSize ->
  DataSize = byte_size(Data),
  case luwak_tree:block_at(Riak, File, Start) of
    {ok, Block} ->
      <<_:DataSize/binary, Tail/binary>> = luwak_block:data(Block),
      BlockData = <<Data, Tail>>,
      {ok, NewBlock} = luwak_block:create(Riak, BlockData),
      {ok, lists:reverse([{luwak_block:name(NewBlock),byte_size(BlockData)}|Written])};
    {error, notfound} ->
      {ok, NewBlock} = luwak_block:create(Riak, Data),
      {ok, lists:reverse([{luwak_block:name(NewBlock),byte_size(Data)}|Written])}
  end;
%% fully aligned write
write_blocks(Riak, File, undefined, Start, Data, BlockSize, Written) ->
  <<Slice:BlockSize/binary, Tail/binary>> = Data,
  {ok, Block} = luwak_block:create(Riak, Slice),
  write_blocks(Riak, File, undefined, Start+BlockSize, Tail, BlockSize, [{luwak_block:name(Block),BlockSize}|Written]);
%% we are doing a sub-block write
write_blocks(Riak, File, PartialStartBlock, Start, Data, BlockSize, Written) when byte_size(Data) < BlockSize ->
  DataSize = byte_size(Data),
  <<Head:Start/binary, _:DataSize/binary, Tail/binary>> = luwak_block:data(PartialStartBlock),
  BlockData = <<Head, Data, Tail>>,
  {ok, Block} = luwak_block:create(Riak, BlockData),
  {ok, lists:reverse([{luwak_block:name(Block),byte_size(BlockData)}|Written])};
%% we are starting with a sub-block write
write_blocks(Riak, File, PartialStartBlock, Start, Data, BlockSize, Written) ->  
  <<Head:Start/binary, _/binary>> = luwak_block:data(PartialStartBlock),
  BlockData = <<Head, Data>>,
  {ok, Block} = luwak_block:create(Riak, BlockData),
  ChopDataSize = BlockSize - Start,
  <<_:ChopDataSize/binary, Tail/binary>> = Data,
  write_blocks(Riak, File, undefined, Start+byte_size(Data), Tail, BlockSize, [{luwak_block:name(Block),byte_size(BlockData)}|Written]).
