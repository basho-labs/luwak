{application, luwak,
 [
  {description, ""},
  {vsn, "1"},
  {modules, [
             luwak_app,
             luwak_sup,
             luwak_io,
             luwak_obj,
             luwak_stream,
             luwak_tree
            ]},
  {registered, []},
  {applications, [
                  kernel,
                  stdlib,
                  riak_core,
                  riak_kv,
                  skerl
                 ]},
  {mod, { luwak_app, []}},
  {env, []}
 ]}.
