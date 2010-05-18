-module(luwak_obj_tests).

-include_lib("eunit/include/eunit.hrl").

object_creation_test() ->
  test_helper:riak_test(fun(Riak) ->  
      Dict = dict:new(),
      luwak_obj:create(Riak, <<"file1">>, dict:store(key, value, Dict)),
      {ok, Obj} = luwak_obj:get(Riak, <<"file1">>),
      Attr = luwak_obj:get_attributes(Obj),
      ?assertEqual({ok, value}, dict:find(key, Attr))
    end).
    
set_attributes_test() ->
  test_helper:riak_test(fun(Riak) ->
      Dict = dict:new(),
      {ok, Obj} = luwak_obj:create(Riak, <<"file1">>, dict:store(key, value, Dict)),
      {ok, Obj2} = luwak_obj:set_attributes(Riak, Obj, dict:store(key, blah, Dict)),
      Attr = luwak_obj:get_attributes(Obj2),
      ?assertEqual({ok, blah}, dict:find(key, Attr))
    end).
    
exists_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, Obj} = luwak_obj:create(Riak, <<"file1">>, dict:new()),
      ?assertEqual({ok, true}, luwak_obj:exists(Riak, <<"file1">>))
    end).
  
delete_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, Obj} = luwak_obj:create(Riak, <<"file1">>, dict:new()),
      ok = luwak_obj:delete(Riak, <<"file1">>),
      ?assertEqual({ok, false}, luwak_obj:exists(Riak, <<"file1">>))
    end).

