export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:./lib:../lib:/usr/local/lib
export PATH=$PATH:./bin:./todo/bin
export PYTHONPATH=$PYTHONATH:./pynow:./test/server
export LUA_PATH="$LUA_PATH;./lua/?.lua;./test/server/?.lua;./test/luasmoke/?.lua;/home/pj/luanow/?.lua;../luanow/?.lua"
export LUA_CPATH="$LUA_CPATH;./lua/?.so;./test/server/?.so"
export NOWDB_LUA_PATH="*:rsc/lua"
export NOWDB_HOME="rsc"
