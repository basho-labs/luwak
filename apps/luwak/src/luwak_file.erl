-module(luwak_file).

-export([create/3, create/4, set_attributes/3, get_attributes/1, exists/2, 
         delete/2, get/2, get_property/2, update_root/3, name/1]).

-include_lib("luwak/include/luwak.hrl").

%% @spec create(Riak :: riak(), Name :: binary(), Attributes :: dict())
%%        -> {ok, File :: luwak_file()} | {error, Reason}
create(Riak, Name, Attributes) when is_binary(Name) ->
  create(Riak, Name, [], Attributes).

%% @spec create(Riak :: riak(), Name :: binary(), Properties :: proplist(), Attributes :: dict())
%%        -> {ok, File :: luwak_file()} | {error, Reason}
create(Riak, Name, Properties, Attributes) when is_binary(Name) ->
  BlockSize = proplists:get_value(block_size, Properties, ?BLOCK_DEFAULT),
  Order = proplists:get_value(tree_order, Properties, ?ORDER_DEFAULT),
  if
    Order < 2 -> throw("tree_order cannot be less than 2");
    BlockSize < 1 -> throw("block_size cannot be less than 1");
    true -> ok
  end,
  Value = [
    {attributes, Attributes},
    {block_size, BlockSize},
    {length, 0},
    {created, now()},
    {modified, now()},
    {tree_order, Order},
    {ancestors, []},
    {root, undefined}
  ],
  Obj = riak_object:new(?O_BUCKET, Name, Value),
  {Riak:put(Obj, 2), Obj}.
  
set_attributes(Riak, Obj, Attributes) ->
  Value = lists:keyreplace(attributes, 1, riak_object:get_value(Obj), {attributes, Attributes}),
  Obj2 = riak_object:apply_updates(riak_object:update_value(Obj, Value)),
  {Riak:put(Obj2, 2), Obj2}.
  
get_attributes(Obj) ->
  proplists:get_value(attributes, riak_object:get_value(Obj)).
  
exists(Riak, Name) ->
  case Riak:get(?O_BUCKET, Name, 2) of
    {ok, Obj} -> {ok, true};
    {error, notfound} -> {ok, false};
    Err -> Err
  end.
  
delete(Riak, Name) ->
  Riak:delete(?O_BUCKET, Name, 2).

get(Riak, Name) ->
  Riak:get(?O_BUCKET, Name, 2).
  
get_property(Obj, type) ->
  case riak_object:get_value(Obj) of
    List when is_list(List) -> proplists:get_value(type, List);
    #n{} -> node
  end;
get_property(Obj, links) ->
  case riak_object:get_value(Obj) of
    List when is_list(List) -> proplists:get_value(links, List);
    #n{children=Children} -> Children
  end;
get_property(Obj, PropName) ->
  case riak_object:get_value(Obj) of
    List when is_list(List) -> proplists:get_value(PropName, List);
    _ -> undefined
  end.

update_root(Riak, Obj, NewRoot) ->
  Values = riak_object:get_value(Obj),
  ObjVal1 = riak_object:get_value(Obj),
  OldRoot = proplists:get_value(root, ObjVal1),
  Ancestors = proplists:get_value(ancestors, ObjVal1),
  ObjVal2 = lists:keyreplace(ancestors, 1, ObjVal1, {ancestors, [OldRoot|Ancestors]}),
  ObjVal3 = lists:keyreplace(root, 1, ObjVal2, {root, NewRoot}),
  Obj2 = riak_object:apply_updates(riak_object:update_value(Obj, ObjVal3)),
  {Riak:put(Obj2, 2), Obj2}.

name(Obj) ->
  riak_object:key(Obj).