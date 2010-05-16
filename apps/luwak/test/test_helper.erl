-module(test_helper).

-export([riak_test/1]).

riak_test(Fun) ->
  start_riak(),
  Ret = (catch Fun()),
  stop_riak(),
  case Ret of
    {'EXIT', Err} -> throw(Err);
    _ -> Ret
  end.

start_riak() ->
  % Dir = "/tmp/ring-" ++ os:getpid(),
  % filelib:ensure_dir(Dir ++ "/"),
  % application:set_env(riak_core, ring_state_dir, Dir),
  application:set_env(riak_kv, storage_backend, riak_kv_cache_backend),
  load_and_start_apps([kernel, stdlib, sasl, crypto, webmachine,
    riak_core, riak_kv]).
    
stop_riak() ->
  application:stop(riak_kv),
  application:stop(riak_core).
  
load_and_start_apps([]) -> ok;
load_and_start_apps([App|Tail]) ->
  application:load(App),
  application:start(App),
  load_and_start_apps(Tail).
