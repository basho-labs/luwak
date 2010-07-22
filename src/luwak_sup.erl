%%%-------------------------------------------------------------------
%%% File    : luwak_sup.erl
%%% Author  : Bryan Fink <bryan@mashtun-2.local>
%%% Description : Basic supervisor for luwak app
%%%
%%% Created :  5 Jul 2010 by Bryan Fink <bryan@mashtun-2.local>
%%%-------------------------------------------------------------------
-module(luwak_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the supervisor
%%--------------------------------------------------------------------
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Func: init(Args) -> {ok,  {SupFlags,  [ChildSpec]}} |
%%                     ignore                          |
%%                     {error, Reason}
%% Description: Whenever a supervisor is started using 
%% supervisor:start_link/[2,3], this function is called by the new process 
%% to find out about restart strategy, maximum restart frequency and child 
%% specifications.
%%--------------------------------------------------------------------
init([]) ->
    add_webmachine_routes(),
    {ok,{{one_for_all,0,1}, []}}.

%%====================================================================
%% Internal functions
%%====================================================================
add_webmachine_routes() ->
    Name = app_helper:get_env(luwak, prefix, "luwak"),
    Props = [{prefix, Name}],
    [ webmachine_router:add_route(R)
      || R <- [{[Name, key], luwak_wm_file, Props},
               {[Name],      luwak_wm_file, Props}] ].
