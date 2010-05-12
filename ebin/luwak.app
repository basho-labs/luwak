{application, luwak,
 [
  {description, ""},
  {vsn, "1"},
  {modules, [
             luwak_app,
             luwak_sup
            ]},
  {registered, []},
  {applications, [
                  kernel,
                  stdlib
                 ]},
  {mod, { luwak_app, []}},
  {env, []}
 ]}.
