-module(test_helper).

-export([riak_test/1]).

-define(APPS,
        [crypto,
         os_mon,
         runtime_tools,
         inets,
         mochiweb,
         webmachine,
         riak_sysmon,
         riak_core,
         luke,
         erlang_js,
         skerl,
         bitcask,
         riak_pipe,
         riak_kv]).

riak_test(Fun) ->
  stop_riak(),
  start_riak(),
  {ok, Riak} = riak:local_client(),
  Ret = (catch Fun(Riak)),
  case Ret of
    {'EXIT', Err} -> throw(Err);
    _ -> Ret
  end,
  net_kernel:stop().

start_riak() ->
    [] = os:cmd("epmd -daemon"),
    case net_kernel:start([test_luwak, shortnames]) of
        {ok,_} -> ok;
        {error,{already_started,_}} -> ok
    end,
  % Dir = "/tmp/ring-" ++ os:getpid(),
  % filelib:ensure_dir(Dir ++ "/"),
  % application:set_env(riak_core, ring_state_dir, Dir),
  application:set_env(riak_kv, storage_backend, riak_kv_cache_backend),
  error_logger:delete_report_handler(error_logger_tty_h),
  application:start(sasl),
  error_logger:delete_report_handler(sasl_report_tty_h),
  load_and_start_apps(?APPS).
    
stop_riak() ->
    case whereis(riak_core_ring_manager) of
        undefined ->
            ok;
        Pid ->
            %% when running from riak root
            exit(Pid, kill)
    end,
    Stop = fun(App) ->
                   application:stop(App)
           end,
    lists:foreach(Stop, lists:reverse(?APPS)).
  
load_and_start_apps([]) -> ok;
load_and_start_apps([App|Tail]) ->
  ensure_loaded(App),
  ensure_started(App),
  load_and_start_apps(Tail).

ensure_loaded(App) ->
  case application:load(App) of
      ok ->
          ok;
      {error,{already_loaded,App}} ->
          ok;
      Error ->
          throw({"failed to load", App, Error})
  end.

ensure_started(App) ->
  case application:start(App) of
      ok ->
          ok;
      {error,{already_started,App}} ->
          ok;
      Error ->
          throw({"failed to start", App, Error})
  end.
