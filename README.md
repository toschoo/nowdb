# NoWDB

NoWDB is an experimental prototype for timeseries database with a graph-oriented datamodell.
It supports a subset of SQL and supports stored procedures implemented as Lua scripts.

There are clients for C, C++, [Go](https://github.com/toschoo/gonow),
Lua, Python and Julia.
There is also a [package](https://github.com/toschoo/luanow)
providing server-side functionality implemented in Lua.

More documentation can be found
[here](https://github.com/toschoo/nowdb/blob/master/doc/manual/manual.pdf).

## Dependencies

NoWDB is implemented in C99 and POSIX-compliant.
It depends on:

- [tsalgo](https://github.com/toschoo/tsalgo)
- [beet](https://github.com/toschoo/beet)
- [zstd](https://github.com/facebook/zstd)
- Flex (the flexical analyser)
- csvlib
