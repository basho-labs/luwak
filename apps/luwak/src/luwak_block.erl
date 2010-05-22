-module(luwak_block).

-include_lib("luwak/include/luwak.hrl").

-export([create/2, data/1, name/1]).

create(Riak, Data) ->
  Value = [
    {data, Data},
    {created, now()},
    {type, block}],
  {ok, Hash} = skerl:hash(?HASH_LEN, Data),
  Obj = riak_object:new(?N_BUCKET, list_to_binary(hex:bin_to_hexstr(Hash)), Value),
  {Riak:put(Obj,2), Obj}.
  
data(Object) ->
  proplists:get_value(data, riak_object:get_value(Object)).

name(Object) ->
  riak_object:key(Object).
