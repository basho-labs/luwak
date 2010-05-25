
-record(n, {created,children}).
-define(N_BUCKET, <<"luwak_node">>).
-define(O_BUCKET, <<"luwak_tld">>).
-define(ORDER_DEFAULT, 250).
-define(BLOCK_DEFAULT, 1000000).
-define(HASH_LEN, 512).

-record(split, {head=[], midhead=[], middle=[], midtail=[], tail=[]}).
