-module(luwak_checksum).

-export([sha1/2]).

sha1(Riak, File) ->
  Length = luwak_file:length(Riak, File),
  sha1_int(crypto:sha_init(), luwak_get_stream:start(Riak, File, 0, Length)).
  
sha1_int(Ctx, GetStream) ->
  case luwak_get_stream:recv(GetStream, 1000) of
    {error, timeout} -> {error, timeout};
    eos -> crypto:sha_final(Ctx);
    {Data, _} -> sha1_int(crypto:sha_update(Ctx,Data),GetStream)
  end.
