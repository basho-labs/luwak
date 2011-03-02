%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc Resource for serving large Riak objects over HTTP.
%%
%% Available operations:
%%
%% GET /Prefix/
%%   Get information about the luwak interface, in JSON form:
%%     {"props":{Prop1:Val1,Prop2:Val2,...},
%%      "keys":[Key1,Key2,...]}.
%%   Each property will be included in the "props" object.
%%   Including the query param "props=false" will cause the "props"
%%   field to be omitted from the response.
%%   Including the query param "keys=false" will cause the "keys"
%%   field to be omitted from the response.
%%
%% POST /Prefix/
%%   Equivalent to "PUT /Prefix/Key" where Key is chosen
%%   by the server.
%%
%% GET /Prefix/Key
%%   Get the data stored under the named Key.
%%   Content-type of the response will be whatever incoming
%%   Content-type was used in the request that stored the data.
%%   Additional headers will include:
%%     Etag: The Riak "vtag" metadata of the object
%%     Last-Modified: The last-modified time of the object
%%     Encoding: The value of the incoming Encoding header from
%%       the request that stored the data.
%%     X-Riak-Meta-: Any headers prefixed by X-Riak-Meta- supplied
%%       on PUT are returned verbatim
%%
%% PUT /Prefix/Key
%%   Store new data under the named Key.
%%   A Content-type header *must* be included in the request.  The
%%   value of this header will be used in the response to subsequent
%%   GET requests.
%%   The body of the request will be stored literally as the value
%%   of the riak_object, and will be served literally as the body of
%%   the response to subsequent GET requests.
%%   Include an Encoding header if you would like an Encoding header
%%   to be included in the response to subsequent GET requests.
%%   Include custom metadata using headers prefixed with X-Riak-Meta-.
%%   They will be returned verbatim on subsequent GET requests.
%%
%% POST /Prefix/Key
%%   Equivalent to "PUT /Prefix/Key" (useful for clients that
%%   do not support the PUT method).
%%
%% DELETE /Prefix/Key
%%   Delete the data stored under the named Key.
%%
%% Webmachine dispatch lines for this resource should look like:
%%
%%  {["luwak"],
%%   luwak_wm_file,
%%   [{prefix, "luwak"}
%%   ]}.
%%  {["luwak", key],
%%   luwak_wm_file,
%%   [{prefix, "luwak"}
%%   ]}.
%%
%% These example dispatch lines will expose this resource at
%% /luwak/ and /luwak/Key.  The resource will attempt to
%% connect to Riak on the same Erlang node one which the resource
%% is executing.
-module(luwak_wm_file).
-author('Bryan Fink <bryan@basho.com>').

%% webmachine resource exports
-export([
         init/1,
         service_available/2,
         allowed_methods/2,
         allow_missing_post/2,
         malformed_request/2,
         resource_exists/2,
         last_modified/2,
         generate_etag/2,
         content_types_provided/2,
         charsets_provided/2,
         encodings_provided/2,
         content_types_accepted/2,
         produce_toplevel_body/2,
         post_is_create/2,
         create_path/2,
         process_post/2,
         produce_doc_body/2,
         accept_doc_body/2,
         delete_resource/2
        ]).

%% @type context() = term()
-record(ctx, {key,          %% binary() - Key (from uri)
              client,       %% riak_client() - the store client
              prefix,       %% string() - prefix for resource uris
              handle,       %% {ok, riak_object()}|{error, term()}
                            %%   - the object found
              method        %% atom() - HTTP method for the request
             }).

-include_lib("webmachine/include/webmachine.hrl").
-include_lib("riak_kv/src/riak_kv_wm_raw.hrl").
-include_lib("luwak.hrl").

-define(HEAD_RANGE, "Range").
-define(HEAD_CRANGE, "Content-Range").
-define(HEAD_BLOCK_SZ, "X-Luwak-Block-Size").

%% @spec init(proplist()) -> {ok, context()}
%% @doc Initialize this resource.  This function extracts the
%%      'prefix' property from the dispatch args.
init(Props) ->
    {ok, #ctx{prefix=proplists:get_value(prefix, Props)}}.

%% @spec service_available(reqdata(), context()) ->
%%          {boolean(), reqdata(), context()}
%% @doc Determine whether or not a connection to Riak
%%      can be established.  This function also takes this
%%      opportunity to extract the 'bucket' and 'key' path
%%      bindings from the dispatch, as well as any vtag
%%      query parameter.
service_available(RD, Ctx) ->
    case riak:local_client(get_client_id(RD)) of
        {ok, C} ->
            {true,
             RD,
             Ctx#ctx{
               method=wrq:method(RD),
               client=C,
               key=case wrq:path_info(key, RD) of
                       undefined -> undefined;
                       K -> list_to_binary(K)
                   end
              }};
        Error ->
            {false,
             wrq:set_resp_body(
               io_lib:format("Unable to connect to Riak: ~p~n", [Error]),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx}
    end.

%% @spec get_client_id(reqdata()) -> term()
%% @doc Extract the request's preferred client id from the
%%      X-Riak-ClientId header.  Return value will be:
%%        'undefined' if no header was found
%%        32-bit binary() if the header could be base64-decoded
%%           into a 32-bit binary
%%        string() if the header could not be base64-decoded
%%           into a 32-bit binary
get_client_id(RD) ->
    case wrq:get_req_header(?HEAD_CLIENT, RD) of
        undefined -> undefined;
        RawId ->
            case catch base64:decode(RawId) of
                ClientId= <<_:32>> -> ClientId;
                _ -> RawId
            end
    end.

%% @spec allowed_methods(reqdata(), context()) ->
%%          {[method()], reqdata(), context()}
%% @doc Get the list of methods this resource supports.
%%      HEAD, GET, POST, and PUT are supported at both
%%      the bucket and key levels.  DELETE is supported
%%      at the key level only.
allowed_methods(RD, Ctx=#ctx{key=undefined}) ->
    %% bucket-level: no delete
    {['HEAD', 'GET', 'POST'], RD, Ctx};
allowed_methods(RD, Ctx) ->
    %% key-level: just about anything
    {['HEAD', 'GET', 'POST', 'PUT', 'DELETE'], RD, Ctx}.

%% @spec allow_missing_post(reqdata(), context()) ->
%%           {true, reqdata(), context()}
%% @doc Makes POST and PUT equivalent for creating new
%%      bucket entries.
allow_missing_post(RD, Ctx) ->
    {true, RD, Ctx}.

%% @spec malformed_request(reqdata(), context()) ->
%%          {boolean(), reqdata(), context()}
%% @doc Determine whether query parameters, request headers,
%%      and request body are badly-formed.
%%      Body format
%%      is not tested for a key-level request (since the
%%      body may be any content the client desires).
malformed_request(RD, Ctx) when Ctx#ctx.method =:= 'POST'
                                orelse Ctx#ctx.method =:= 'PUT' ->
    case wrq:get_req_header("Content-Type", RD) of
        undefined ->
            {true, missing_content_type(RD), Ctx};
        _ ->
            {false, RD, Ctx}
    end;
malformed_request(RD, Ctx) when Ctx#ctx.key =:= undefined ->
    {false, RD, Ctx};
malformed_request(RD, Ctx) ->
    HCtx = ensure_handle(Ctx),
    case HCtx#ctx.handle of
        {error, notfound} ->
            {{halt, 404},
             wrq:set_resp_header("Content-Type", "text/plain",
                                 wrq:append_to_response_body(
                                   io_lib:format("not found~n",[]),
                                   RD)),
             HCtx};
        {error, timeout} ->
            {{halt, 500},
             wrq:set_resp_header("Content-Type", "text/plain",
                                 wrq:append_to_response_body(
                                   io_lib:format("request timed out~n",[]),
                                   RD)),
             HCtx};
        {error, Err} ->
            {{halt, 500},
             wrq:set_resp_header("Content-Type", "text/plain",
                                 wrq:append_to_response_body(
                                   io_lib:format("Error:~n~p~n",[Err]),
                                   RD)),
             HCtx};
        _ ->
            {false, RD, HCtx}
    end.

%% @spec content_types_provided(reqdata(), context()) ->
%%          {[{ContentType::string(), Producer::atom()}], reqdata(), context()}
%% @doc List the content types available for representing this resource.
%%      "application/json" is the content-type for bucket-level GET requests
%%      The content-type for a key-level request is the content-type that
%%      was used in the PUT request that stored the document in Riak.
content_types_provided(RD, Ctx=#ctx{key=undefined}) ->
    %% top level: JSON description only
    {[{"application/json", produce_toplevel_body}], RD, Ctx};
content_types_provided(RD, Ctx=#ctx{method=Method}=Ctx) when Method =:= 'PUT';
                                                             Method =:= 'POST' ->
    {ContentType, _} = extract_content_type(RD),
    {[{ContentType, produce_doc_body}], RD, Ctx};
content_types_provided(RD, Ctx0) ->
    case defined_attribute(Ctx0, ?MD_CTYPE) of
        {undefined, Ctx} ->
            {[{"application/octet-stream", produce_doc_body}], RD, Ctx};
        {Ctype, Ctx} ->
            {[{Ctype, produce_doc_body}], RD, Ctx}
    end.

%% @spec charsets_provided(reqdata(), context()) ->
%%          {no_charset|[{Charset::string(), Producer::function()}],
%%           reqdata(), context()}
%% @doc List the charsets available for representing this resource.
%%      No charset will be specified for a bucket-level request.
%%      The charset for a key-level request is the charset that was used
%%      in the PUT request that stored the document in Riak (none if
%%      no charset was specified at PUT-time).
charsets_provided(RD, Ctx=#ctx{key=undefined}) ->
    %% default charset for bucket-level request
    {no_charset, RD, Ctx};
charsets_provided(RD, #ctx{method=Method}=Ctx) when Method =:= 'PUT';
                                                    Method =:= 'POST' ->
    case extract_content_type(RD) of
        {_, undefined} ->
            {no_charset, RD, Ctx};
        {_, Charset} ->
            {[{Charset, fun(X) -> X end}], RD, Ctx}
    end;
charsets_provided(RD, Ctx0) ->
    case defined_attribute(Ctx0, ?MD_CHARSET) of
        {undefined, Ctx} ->
            {no_charset, RD, Ctx};
        {Cset, Ctx} ->
            {[{Cset, fun(X) -> X end}], RD, Ctx}
    end.

%% @spec encodings_provided(reqdata(), context()) ->
%%          {[{Encoding::string(), Producer::function()}], reqdata(), context()}
%% @doc List the encodings available for representing this resource.
%%      "identity" and "gzip" are available for bucket-level requests.
%%      The encoding for a key-level request is the encoding that was
%%      used in the PUT request that stored the document in Riak, or
%%      "identity" and "gzip" if no encoding was specified at PUT-time.
encodings_provided(RD, Ctx=#ctx{key=undefined}) ->
    %% identity and gzip for bucket-level request
    {default_encodings(), RD, Ctx};
encodings_provided(RD, Ctx0) ->
    case defined_attribute(Ctx0, ?MD_ENCODING) of
        {undefined, Ctx} ->
            {default_encodings(), RD, Ctx};
        {Enc, Ctx} ->
            {[{Enc, fun(X) -> X end}], RD, Ctx}
    end.

defined_attribute(Ctx0, Attr) ->
    Ctx = ensure_handle(Ctx0),
    case Ctx#ctx.handle of
        {ok, H} ->
            As = luwak_file:get_attributes(H),
            case dict:find(Attr, As) of
                {ok, A} ->
                    {A, Ctx};
                error ->
                    {undefined, Ctx}
            end;
        {error, _} ->
                    {undefined, Ctx}
    end.

file_property(RD, Ctx0, Prop, Default) ->
    Ctx = ensure_handle(Ctx0),
    case Ctx#ctx.handle of
        {ok, H} ->
            case luwak_file:get_property(H, Prop) of
                undefined ->
                    {Default, RD, Ctx};
                P ->
                    {P, RD, Ctx}
            end;
        {error, _} ->
            {Default, RD, Ctx}
    end.

%% @spec default_encodings() -> [{Encoding::string(), Producer::function()}]
%% @doc The default encodings available: identity and gzip.
default_encodings() ->
    [{"identity", fun(X) -> X end},
     {"gzip", fun(X) -> zlib:gzip(X) end}].

%% @spec content_types_accepted(reqdata(), context()) ->
%%          {[{ContentType::string(), Acceptor::atom()}],
%%           reqdata(), context()}
%% @doc Get the list of content types this resource will accept.
%%      "application/json" is the only type accepted for bucket-PUT.
%%      Whatever content type is specified by the Content-Type header
%%      of a key-level PUT request will be accepted by this resource.
%%      (A key-level put *must* include a Content-Type header.)
content_types_accepted(RD, Ctx) ->
    case wrq:get_req_header(?HEAD_CTYPE, RD) of
        undefined ->
            %% user must specify content type of the data
            {[],
             wrq:set_resp_header(
               ?HEAD_CTYPE,
               "text/plain",
               wrq:set_resp_body(
                 ["Please include a valid Content-type header.\n"],
                 RD)),
             Ctx};
        CType ->
            Media = hd(string:tokens(CType, ";")),
            case string:tokens(Media, "/") of
                ["multipart", "byteranges"] ->
                    {[{Media, accept_byteranges}], RD, Ctx};
                [_Type, _Subtype] ->
                    %% accept whatever the user says
                    {[{Media, accept_doc_body}], RD, Ctx};
                _ ->
                    {[],
                     wrq:set_resp_header(
                       ?HEAD_CTYPE,
                       "text/plain",
                       wrq:set_resp_body(
                         ["\"", Media, "\""
                          " is not a valid media type"
                          " for the Content-type header.\n"],
                         RD)),
                     Ctx}
            end
    end.

%% @spec resource_exists(reqdata(), context()) -> {boolean(), reqdata(), context()}
%% @doc Determine whether or not the requested item exists.
%%      Documents exists if a read request to Riak returns {ok, riak_object()}.
resource_exists(RD, Ctx=#ctx{key=undefined}) ->
    %% the toplevel always exists
    {true, RD, Ctx};
resource_exists(RD, Ctx0) ->
    Ctx = ensure_handle(Ctx0),
    case Ctx#ctx.handle of
        {ok, _} ->
            {true, RD, Ctx};
        {error, notfound} ->
            {false, RD, Ctx};
        {error, timeout} ->
            {{halt, 503}, RD, Ctx}
    end.

%% @spec produce_toplevel_body(reqdata(), context()) -> {binary(), reqdata(), context()}
%% @doc Produce the JSON response to a bucket-level GET.
%%      Includes the bucket props unless the "props=false" query param
%%      is specified.
%%      Includes the keys of the documents in the bucket unless the
%%      "keys=false" query param is specified. If "keys=stream" query param
%%      is specified, keys will be streamed back to the client in JSON chunks
%%      like so: {"keys":[Key1, Key2,...]}.
produce_toplevel_body(RD, Ctx=#ctx{client=C}) ->
    SchemaPart =
        case wrq:get_qs_value(?Q_PROPS, RD) of
            ?Q_FALSE -> [];
            _ ->
                Props = [{<<"o_bucket">>, ?O_BUCKET},
                         {<<"n_bucket">>, ?N_BUCKET},
                         {<<"block_default">>,
                         luwak_file:get_default_block_size()}],
                [{?JSON_PROPS, {struct, Props}}]
        end,
    KeyPart =
        case wrq:get_qs_value(?Q_KEYS, RD) of
            ?Q_STREAM -> stream;
            ?Q_TRUE   -> {ok, KeyList} = C:list_keys(?O_BUCKET),
                         [{?Q_KEYS, KeyList}];
            _         -> []
        end,
    case KeyPart of
        stream -> {{stream, {mochijson2:encode({struct, SchemaPart}),
                             fun() ->
                                     {ok, ReqId} = C:stream_list_keys(?O_BUCKET),
                                     stream_keys(ReqId)
                             end}},
                   RD,
                   Ctx};
       _ ->
            {mochijson2:encode({struct, SchemaPart++KeyPart}), RD, Ctx}
    end.

stream_keys(ReqId) ->
    receive
        {ReqId, {keys, Keys}} ->
            {mochijson2:encode({struct, [{<<"keys">>, Keys}]}), fun() -> stream_keys(ReqId) end};
        {ReqId, done} -> {mochijson2:encode({struct, [{<<"keys">>, []}]}), done}
    end.

%% @spec post_is_create(reqdata(), context()) -> {boolean(), reqdata(), context()}
%% @doc POST is considered a document-creation operation for bucket-level
%%      requests (this makes webmachine call create_path/2, where the key
%%      for the created document will be chosen).
post_is_create(RD, Ctx=#ctx{key=undefined}) ->
    %% bucket-POST is create
    {true, RD, Ctx};
post_is_create(RD, Ctx) ->
    %% key-POST is not create
    {false, RD, Ctx}.

%% @spec create_path(reqdata(), context()) -> {string(), reqdata(), context()}
%% @doc Choose the Key for the document created during a bucket-level POST.
%%      This function also sets the Location header to generate a
%%      201 Created response.
create_path(RD, Ctx=#ctx{prefix=P}) ->
    K = riak_core_util:unique_id_62(),
    {K,
     wrq:set_resp_header("Location",
                         lists:append(["/",P,"/",K]),
                         RD),
     Ctx#ctx{key=list_to_binary(K)}}.

%% @spec process_post(reqdata(), context()) -> {true, reqdata(), context()}
%% @doc Pass-through for key-level requests to allow POST to function
%%      as PUT for clients that do not support PUT.
process_post(RD, Ctx) -> accept_doc_body(RD, Ctx).

%% @spec accept_doc_body(reqdata(), context()) -> {true, reqdat(), context()}
%% @doc Store the data the client is PUTing in the document.
%%      This function translates the headers and body of the HTTP request
%%      into their final riak_object() form, and executes the Riak put.
accept_doc_body(RD, Ctx=#ctx{key=K, client=C}) ->
    {CType, Charset} = extract_content_type(RD),
    UserMeta = extract_user_meta(RD),
    CTypeMD = dict:store(?MD_CTYPE, CType, dict:new()),
    CharsetMD = if Charset /= undefined ->
                        dict:store(?MD_CHARSET, Charset, CTypeMD);
                   true -> CTypeMD
                end,
    EncMD = case wrq:get_req_header(?HEAD_ENCODING, RD) of
                undefined -> CharsetMD;
                E -> dict:store(?MD_ENCODING, E, CharsetMD)
            end,
    UserMetaMD = dict:store(?MD_USERMETA, UserMeta, EncMD),
    H0 = case Ctx#ctx.handle of
             {ok, H} -> H;
             _ ->
                 FileProps = extract_file_props(RD),
                 {ok, H} = luwak_file:create(C, K, FileProps, dict:new()),
                 H
         end,
    {ok,H1} = luwak_file:set_attributes(C, H0, UserMetaMD),
    HCtx = Ctx#ctx{handle={ok,H1}},
    {accept_streambody(RD, HCtx), RD, HCtx}.

accept_streambody(RD, #ctx{handle={ok, H}, client=C}) ->
    Stream = luwak_put_stream:start_link(C, H, 0, 1000),
    Size = luwak_file:get_default_block_size(),
    accept_streambody1(Stream, 0, wrq:stream_req_body(RD, Size)).

accept_streambody1(Stream, Count0, {Data, Next}) ->
    Count = Count0+size(Data),
    luwak_put_stream:send(Stream, Data),
    if is_function(Next) ->
            accept_streambody1(Stream, Count, Next());
       Next =:= done ->
            luwak_put_stream:close(Stream),
            true
    end.

%% @spec extract_content_type(reqdata()) ->
%%          {ContentType::string(), Charset::string()|undefined}
%% @doc Interpret the Content-Type header in the client's PUT request.
%%      This function extracts the content type and charset for use
%%      in subsequent GET requests.
extract_content_type(RD) ->
    case wrq:get_req_header(?HEAD_CTYPE, RD) of
        undefined ->
            undefined;
        RawCType ->
            [CType|RawParams] = string:tokens(RawCType, "; "),
            Params = [ list_to_tuple(string:tokens(P, "=")) || P <- RawParams],
            {CType, proplists:get_value("charset", Params)}
    end.

%% @spec extract_user_meta(reqdata()) -> proplist()
%% @doc Extract headers prefixed by X-Riak-Meta- in the client's PUT request
%%      to be returned by subsequent GET requests.
extract_user_meta(RD) ->
    lists:filter(fun({K,_V}) ->
                    lists:prefix(
                        ?HEAD_USERMETA_PREFIX,
                        string:to_lower(any_to_list(K)))
                end,
                mochiweb_headers:to_list(wrq:req_headers(RD))).

%% @spec extract_file_props(reqdata()) -> proplists()
%% @doc Extract Luwak file properties from custom headers prefixed by
%%      X-Luwak- in the client's request.
extract_file_props(RD) ->
    extract_headers(RD, [{block_size, ?HEAD_BLOCK_SZ}], []).

extract_headers(_RD, [], Acc) ->
    Acc;
extract_headers(RD, [{Key, Header}|T], Acc) ->
    case wrq:get_req_header(Header, RD) of
        undefined ->
            extract_headers(RD, T, Acc);
        Val ->
            extract_headers(RD, T, [{Key, Val}|Acc])
    end.

%% @spec produce_doc_body(reqdata(), context()) -> {binary(), reqdata(), context()}
%% @doc Extract the value of the document, and place it in the response
%%      body of the request.
produce_doc_body(RD, Ctx=#ctx{handle={ok, H}, client=C}) ->
    {{stream, luwak_file:length(C, H), file_sender(C, H)},
     add_user_metadata(RD, H),
     Ctx}.

add_user_metadata(RD, Handle) ->
    Attr = luwak_file:get_attributes(Handle),
    case dict:find(?MD_USERMETA, Attr) of
        {ok, UserMeta} ->
            lists:foldl(fun({K,V},Acc) ->
                                wrq:merge_resp_headers([{K,V}],Acc)
                        end,
                        RD, UserMeta);
        error -> RD
    end.

file_sender(C, H) ->
    fun(Start, End) ->
            %% HTTP specifies the last byte to send,
            %% but luwak wants a number of bytes after offset
            Stream = luwak_get_stream:start(C, H, Start, 1+End-Start),
            (send_file_helper(Stream))()
    end.

-define(STREAM_TIMEOUT, 5000).

send_file_helper(Stream) ->
    fun() ->
            case luwak_get_stream:recv(Stream, ?STREAM_TIMEOUT) of
                {Data, _Offset} when is_binary(Data) ->
                    {Data, send_file_helper(Stream)};
                eos ->
                    {<<>>, done};
                closed ->
                    {<<>>, done};
                {error, timeout} ->
                    {<<>>, done}
            end
    end.

%% @spec ensure_handle(context()) -> context()
%% @doc Ensure that the 'handle' field of the context() has been filled
%%      with the result of a luwak_file:get request.  This is a
%%      convenience for memoizing the result of a get so it can be
%%      used in multiple places in this resource, without having to
%%      worry about the order of executing of those places.
ensure_handle(Ctx=#ctx{handle=undefined, key=K, client=C}) ->
    Ctx#ctx{handle=luwak_file:get(C, K)};
ensure_handle(Ctx) -> Ctx.

%% @spec delete_resource(reqdata(), context()) -> {true, reqdata(), context()}
%% @doc Delete the document specified.
delete_resource(RD, Ctx=#ctx{key=K, client=C}) ->
    case luwak_file:delete(C, K) of
        {error, precommit_fail} ->
            {{halt, 403}, send_precommit_error(RD, undefined), Ctx};
        {error, {precommit_fail, Reason}} ->
            {{halt, 403}, send_precommit_error(RD, Reason), Ctx};
        ok ->
            {true, RD, Ctx}
    end.

%% @spec generate_etag(reqdata(), context()) ->
%%          {undefined|string(), reqdata(), context()}
%% @doc Get the etag for this resource.
%%      Bucket requests will have no etag.
%%      Documents will have an etag equal to their vtag.  No etag will be
%%      given for documents with siblings, if no sibling was chosen with the
%%      vtag query param.
generate_etag(RD, Ctx=#ctx{key=undefined}) ->
    {undefined, RD, Ctx};
generate_etag(RD, Ctx) ->
    file_property(RD, Ctx, checksum, undefined).

%% @spec last_modified(reqdata(), context()) ->
%%          {undefined|datetime(), reqdata(), context()}
%% @doc Get the last-modified time for this resource.
%%      Bucket requests will have no last-modified time.
%%      Documents will have the last-modified time specified by the riak_object.
%%      No last-modified time will be given for documents with siblings, if no
%%      sibling was chosen with the vtag query param.
last_modified(RD, Ctx=#ctx{key=undefined}) ->
    {undefined, RD, Ctx};
last_modified(RD, Ctx) ->
    case file_property(RD, Ctx, modified, undefined) of
        {Time={_,_,_},NewRD,NewCtx} ->
            {calendar:now_to_universal_time(Time), NewRD, NewCtx};
        Other ->
            Other
    end.

any_to_list(V) when is_list(V) ->
    V;
any_to_list(V) when is_atom(V) ->
    atom_to_list(V);
any_to_list(V) when is_binary(V) ->
    binary_to_list(V).

missing_content_type(RD) ->
    RD1 = wrq:set_resp_header("Content-Type", "text/plain", RD),
    wrq:append_to_response_body(<<"Missing Content-Type request header">>, RD1).

send_precommit_error(RD, Reason) ->
    RD1 = wrq:set_resp_header("Content-Type", "text/plain", RD),
    Error = if
                Reason =:= undefined ->
                    list_to_binary([atom_to_binary(wrq:method(RD1), utf8),
                                    <<" aborted by pre-commit hook.">>]);
                true ->
                    Reason
            end,
    wrq:append_to_response_body(Error, RD1).
