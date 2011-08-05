-module(luwak_file_tests).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

file_test_() ->
    {spawn,
     [{setup,
       fun test_helper:setup/0,
       fun test_helper:cleanup/1,
       [
        {timeout, 60000,
         [fun object_creation/0,
          fun set_attributes/0,
          fun exists/0,
          fun delete/0]}
       ]
      }
     ]
    }.

object_creation() ->
  test_helper:riak_test(fun(Riak) ->
      Dict = dict:new(),
      luwak_file:create(Riak, <<"file1">>, dict:store(key, value, Dict)),
      {ok, Obj} = luwak_file:get(Riak, <<"file1">>),
      Attr = luwak_file:get_attributes(Obj),
      ?assertEqual({ok, value}, dict:find(key, Attr))
    end).

set_attributes() ->
  test_helper:riak_test(fun(Riak) ->
      Dict = dict:new(),
      {ok, Obj} = luwak_file:create(Riak, <<"file1">>, dict:store(key, value, Dict)),
      {ok, Obj2} = luwak_file:set_attributes(Riak, Obj, dict:store(key, blah, Dict)),
      Attr = luwak_file:get_attributes(Obj2),
      ?assertEqual({ok, blah}, dict:find(key, Attr))
    end).

exists() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, _Obj} = luwak_file:create(Riak, <<"file1">>, dict:new()),
      ?assertEqual({ok, true}, luwak_file:exists(Riak, <<"file1">>))
    end).

delete() ->
  test_helper:riak_test(fun(Riak) ->
      {ok, _Obj} = luwak_file:create(Riak, <<"file1">>, dict:new()),
      ok = luwak_file:delete(Riak, <<"file1">>),
      ?assertEqual({ok, false}, luwak_file:exists(Riak, <<"file1">>))
    end).

-endif.
