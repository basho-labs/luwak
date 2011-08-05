-module(luwak_file).

-export([create/3,
         create/4,
         set_attributes/3,
         get_attributes/1,
         exists/2, 
         delete/2,
         get/2,
         get_property/2,
         get_default_block_size/0,
         update_root/3,
         update_checksum/3, 
         name/1,
         length/2]).

-include("luwak.hrl").

%% @spec create(Riak :: riak(), Name :: binary(), Attributes :: dict())
%%        -> {ok, File :: luwak_file()}
%% @doc Create a luwak file handle with the given name and attributes.  Will
%%      overwrite an existing file of the same name.
%% @equiv create(Riak, Name, [], Attributes)
create(Riak, Name, Attributes) when is_binary(Name) ->
    create(Riak, Name, [], Attributes).

%% @spec create(Riak :: riak(), Name :: binary(), Properties :: proplist(),
%%              Attributes :: dict())
%%        -> {ok, File :: luwak_file()}
%% @doc Create a luwak file handle with the given name and attributes.
%%      Recognized properties:
%%      {block_size, int()}       - The maximum size of an individual data chunk
%%                                   in bytes.  Default is 1000000.
%%      {tree_order, int()}       - The maximum number of children for an
%%                                   individual tree node. Default is 250.
%%      {checksumming, boolean()} - This controls whether or not checksumming
%%                                   will occur.  Checksumming will leave a
%%                                   checksum of the most recent write request
%%                                   as a property of the filehandle.
create(Riak, Name, Properties, Attributes) when is_binary(Name) ->
    BlockSize = proplists:get_value(block_size, Properties,
                                    get_default_block_size()),
    Order = proplists:get_value(tree_order, Properties, ?ORDER_DEFAULT),
    Checksumming = proplists:get_value(checksumming, Properties, false),
    if
        Order < 2 -> throw("tree_order cannot be less than 2");
        BlockSize < 1 -> throw("block_size cannot be less than 1");
        true -> ok
    end,
    Value = [
             {attributes, Attributes},
             {block_size, BlockSize},
             {created, now()},
             {modified, now()},
             {checksumming, Checksumming},
             {checksum, undefined},
             {tree_order, Order},
             {ancestors, []},
             {root, undefined}
            ],
    Obj = riak_object:new(?O_BUCKET, Name, Value),
    Riak:put(Obj, 2, 2, ?TIMEOUT_DEFAULT, [{returnbody, true}]).

%% @spec set_attributes(Riak :: riak(), Obj :: luwak_file(),
%%                      Attributes :: dict())
%%        -> {ok, NewFile}
%% @doc Sets the new attributes, saves them, and returns a new file handle.
set_attributes(Riak, Obj, Attributes) ->
    Value = lists:keyreplace(attributes, 1, riak_object:get_value(Obj),
                             {attributes, Attributes}),
    Obj2 = riak_object:update_value(Obj, Value),
    Riak:put(Obj2, 2, 2, ?TIMEOUT_DEFAULT, [{returnbody, true}]).
  
%% @spec get_attributes(Obj :: luwak_file()) -> dict()
%% @doc Gets the attribute dictionary from the file handle.
get_attributes(Obj) ->
    proplists:get_value(attributes, riak_object:get_value(Obj)).
  
%% @spec exists(Riak :: riak(), Name :: binary())
%%       -> {ok, true} | {ok, false} | {error, Reason}
%% @doc Checks for the existence of the named file.
exists(Riak, Name) ->
    case Riak:get(?O_BUCKET, Name, 2) of
        {ok, _Obj} -> {ok, true};
        {error, notfound} -> {ok, false};
        Err -> Err
    end.
  
%% @spec length(Riak :: riak(), File :: luwak_file()) -> Length
%% @doc returns the length in bytes of the file.
length(Riak, File) ->
    case get_property(File, root) of
        undefined ->
            0;
        RootName ->
            {ok, Node} = luwak_tree:get(Riak, RootName),
            case Node of
                #n{children=Children} ->
                    luwak_tree_utils:blocklist_length(Children);
                Block ->
                    byte_size(luwak_block:data(Block))
            end
    end.

%% @spec delete(Riak :: riak(), Name :: binary()) -> ok | {error, Reason}
%% @doc deletes the named file from luwak.  This is a fast operation since
%%      the blocks and tree for that file remain untouched.  A GC operation
%%      (not yet implemented) will be required to clean them up properly.
delete(Riak, Name) ->
    Riak:delete(?O_BUCKET, Name, 2).

%% @spec get(Riak :: riak(), Name :: binary()) -> {ok, File} | {error, Reason}
%% @doc returns a filehandle for the named file.
get(Riak, Name) ->
    Riak:get(?O_BUCKET, Name, 2).

%% @spec get_property(Obj :: luwak_file(), PropName :: atom()) -> Property
%% @doc retrieves the named property from the filehandle.
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

get_default_block_size() ->
    app_helper:get_env(luwak, default_block_size, ?BLOCK_DEFAULT).

%% @private
update_root(Riak, Obj, NewRoot) ->
    ObjVal1 = riak_object:get_value(Obj),
    OldRoot = proplists:get_value(root, ObjVal1),
    Ancestors = proplists:get_value(ancestors, ObjVal1),
    ObjVal2 = lists:keyreplace(ancestors, 1, ObjVal1,
                               {ancestors, [OldRoot|Ancestors]}),
    ObjVal3 = lists:keyreplace(root, 1, ObjVal2, {root, NewRoot}),
    Obj2 = riak_object:update_value(Obj, ObjVal3),
    Riak:put(Obj2, 2, 2, ?TIMEOUT_DEFAULT, [{returnbody, true}]).

%% @private
update_checksum(Riak, Obj, ChecksumFun) ->
    case get_property(Obj, checksumming) of
        true ->
            ObjVal1 = riak_object:get_value(Obj),
            ObjVal2 = lists:keyreplace(checksum, 1, ObjVal1, 
                                       {checksum, {sha1, ChecksumFun()}}),
            Obj2 = riak_object:update_value(Obj, ObjVal2),
            Riak:put(Obj2, 2, 2, ?TIMEOUT_DEFAULT, [{returnbody, true}]);
        _ ->
            {ok, Obj}
  end.

%% @spec name(Obj :: luwak_file()) -> binary()
%% @doc returns the name of the given file handle.
name(Obj) ->
    riak_object:key(Obj).
