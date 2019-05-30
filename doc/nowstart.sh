export LD_LIBRARY_PATH=/lib:/usr/lib:/usr/local/lib
export PYTHONPATH=/usr/local/pynow:$PYTHONPATH
export LUA_PATH="./lua/?.lua;/opt/lua/?.lua;"
export LUA_CPATH="$LUA_CPATH;./lua/?.so;/opt/lua/?.so"
export NOWDB_LUA_PATH="*:/opt/lua"
export NOWDB_HOME="/usr/local"

nowdbd -p 50677 -b /opt/dbs -l >/opt/log/nowdb.log 2>&1
