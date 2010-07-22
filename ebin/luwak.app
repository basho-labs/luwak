{application, luwak,
 [
  {description, ""},
  {vsn, "1"},
  {modules, [
             luwak_app,
             luwak_sup,
             luwak_io,
             luwak_block,
             luwak_file,
             luwak_tree,
             luwak_tree_utils,
             luwak_put_stream,
             luwak_get_stream,
             luwak_checksum,
             luwak_wm_file
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
