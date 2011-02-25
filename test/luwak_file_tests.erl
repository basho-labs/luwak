-module(luwak_file_tests).

-include_lib("eunit/include/eunit.hrl").

object_creation_test() ->
  test_helper:riak_test(fun(Riak) ->  
      Dict = dict:new(),
      luwak_file:create(Riak, <<"file1">>, dict:store(key, value, Dict)),
      {ok, Obj} = luwak_file:get(Riak, <<"file1">>),
      Attr = luwak_file:get_attributes(Obj),
      ?assertEqual({ok, value}, dict:find(key, Attr))
    end).
    
set_attributes_test() ->
  test_helper:riak_test(fun(Riak) ->
      Dict = dict:new(),
      {ok, Obj} = luwak_file:create(Riak, <<"file1">>, dict:store(key, value, Dict)),
      {ok, Obj2} = luwak_file:set_attributes(Riak, Obj, dict:store(key, blah, Dict)),
      Attr = luwak_file:get_attributes(Obj2),
      ?assertEqual({ok, blah}, dict:find(key, Attr))
    end).
    
exists_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, _Obj} = luwak_file:create(Riak, <<"file1">>, dict:new()),
      ?assertEqual({ok, true}, luwak_file:exists(Riak, <<"file1">>))
    end).
  
delete_test() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, _Obj} = luwak_file:create(Riak, <<"file1">>, dict:new()),
      ok = luwak_file:delete(Riak, <<"file1">>),
      ?assertEqual({ok, false}, luwak_file:exists(Riak, <<"file1">>))
    end).

