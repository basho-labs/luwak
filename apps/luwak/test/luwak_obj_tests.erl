-module(luwak_obj_tests).

-include_lib("eunit/include/eunit.hrl").

object_creation_test() ->
  test_helper:riak_test(fun() ->
      {ok, Riak} = riak:local_client(),
      Dict = dict:new(),
      Ret = luwak_obj:create(Riak, <<"file1">>, dict:store(key, value, Dict)),
      {ok, Obj} = luwak_obj:get(Riak, <<"file1">>),
      Attr = luwak_obj:get_attributes(Obj),
      ?assertEqual({ok, value}, dict:find(key, Attr))
    end).
