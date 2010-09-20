-module(luwak_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    riak_core_util:start_app_deps(luwak),
    add_webmachine_routes(),
    {ok,self()}.

stop(_State) ->
    ok.

add_webmachine_routes() ->
    Name = app_helper:get_env(luwak, prefix, "luwak"),
    Props = [{prefix, Name}],
    [ webmachine_router:add_route(R)
      || R <- [{[Name, key], luwak_wm_file, Props},
               {[Name],      luwak_wm_file, Props}] ].
