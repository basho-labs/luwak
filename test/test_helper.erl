-module(test_helper).

-export([setup/0, cleanup/1, riak_test/1]).

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

setup() ->
    stop_riak(),
    start_riak().

cleanup(_) ->
    stop_riak(),
    net_kernel:stop().

riak_test(Fun) ->
  {ok, Riak} = riak:local_client(),
  Ret = (catch Fun(Riak)),
  case Ret of
    {'EXIT', Err} -> throw(Err);
    _ -> Ret
  end.


start_riak() ->
    [] = os:cmd("epmd -daemon"),
    case net_kernel:start([test_luwak, shortnames]) of
        {ok,_} -> ok;
        {error,{already_started,_}} -> ok
    end,
    application:set_env(riak_kv, storage_backend, riak_kv_cache_backend),
    error_logger:delete_report_handler(error_logger_tty_h),
    _ = application:load(sasl),
    put(old_sasl_l, app_helper:get_env(sasl, sasl_error_logger)),
    LogFile = "./luwak-eunit-sasl.log",
    ok = application:set_env(sasl, sasl_error_logger, {file, LogFile}),
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
    lists:foreach(Stop, lists:reverse(?APPS)),
    application:stop(sasl),
    application:set_env(sasl, sasl_error_logger, erase(old_sasl_l)).

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
