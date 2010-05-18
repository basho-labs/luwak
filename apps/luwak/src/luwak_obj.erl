-module(luwak_obj).

-export([create/3, set_attributes/3, get_attributes/1, exists/2, delete/2, get/2, get_property/2]).

-define(B_OBJ, <<"luwak_tld">>).
-define(BLOCK_DEFAULT, 1000000).

%% @spec create(Riak :: riak(), Name :: binary(), Attributes :: dict())
%%        -> {ok, }
create(Riak, Name, Attributes) when is_binary(Name) ->
  BlockSize = case dict:find(block_size, Attributes) of
    {ok, V} -> V;
    error -> ?BLOCK_DEFAULT
  end,
  Value = [
    {attributes, Attributes},
    {block_size, BlockSize},
    {length, 0},
    {created, now()},
    {modified, now()},
    {ancestors, []},
    {root, undefined}
  ],
  Obj = riak_object:new(?B_OBJ, Name, Value),
  {Riak:put(Obj, 2), Obj}.
  
set_attributes(Riak, Obj, Attributes) ->
  Value = lists:keyreplace(attributes, 1, riak_object:get_value(Obj), {attributes, Attributes}),
  Obj2 = riak_object:apply_updates(riak_object:update_value(Obj, Value)),
  {Riak:put(Obj2, 2), Obj2}.
  
get_attributes(Obj) ->
  proplists:get_value(attributes, riak_object:get_value(Obj)).
  
exists(Riak, Name) ->
  case Riak:get(?B_OBJ, Name, 2) of
    {ok, Obj} -> {ok, true};
    {error, notfound} -> {ok, false};
    Err -> Err
  end.
  
delete(Riak, Name) ->
  Riak:delete(?B_OBJ, Name, 2).

get(Riak, Name) ->
  Riak:get(?B_OBJ, Name, 2).
  
get_property(Obj, PropName) ->
  proplists:get_value(PropName, riak_object:get_value(Obj)).
