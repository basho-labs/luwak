-module(luwak_obj).

-export([create/3, set_attributes/3, get_attributes/2, exists/2, delete/2]).

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
    {links, []}
  ],
  Obj = riak_object:new(?B_OBJ, Name, Value),
  Riak:put(Obj, 2).
  
set_attributes(Riak, Name, Attributes) ->
  case Riak:get(?B_OBJ, Name, 2) of
    {ok, Obj} ->
      Value = lists:keyreplace(attributes, 1, riak_obj:get_value(Obj), {attributes, Attributes}),
      Obj2 = riak_obj:update_value(Obj, Value),
      Riak:put(Obj2, 2);
    Err -> Err
  end.
  
get_attributes(Riak, Name) ->
  case Riak:get(?B_OBJ, Name, 2) of
    {ok, Obj} ->
      {ok, proplist:get_value(attributes, riak_obj:get_value(Obj))};
    Err -> Err
  end.
  
exists(Riak, Name) ->
  case Riak:get(?B_OBJ, Name, 2) of
    {ok, Obj} -> {ok, true};
    Err -> Err
  end.
  
delete(Riak, Name) ->
  case Riak:delete(?B_OBJ, Name, 2).

