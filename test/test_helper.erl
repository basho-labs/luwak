-module(test_helper).

-export([riak_test/1]).

riak_test(Fun) ->
  start_riak(),
  {ok, Riak} = riak:local_client(),
  Ret = (catch Fun(Riak)),
  stop_riak(),
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
  % Dir = "/tmp/ring-" ++ os:getpid(),
  % filelib:ensure_dir(Dir ++ "/"),
  % application:set_env(riak_core, ring_state_dir, Dir),
  application:set_env(riak_kv, storage_backend, riak_kv_cache_backend),
  error_logger:delete_report_handler(error_logger_tty_h),
  application:start(sasl),
  error_logger:delete_report_handler(sasl_report_tty_h),
  load_and_start_apps([crypto,
                       os_mon,
                       runtime_tools,
                       mochiweb,
                       webmachine,
                       riak_core,
                       luke,
                       erlang_js,
                       skerl,
                       bitcask,
                       riak_kv]).
    
stop_riak() ->
  application:stop(riak_kv).
  
load_and_start_apps([]) -> ok;
load_and_start_apps([App|Tail]) ->
  application:load(App),
  application:start(App),
  load_and_start_apps(Tail).
