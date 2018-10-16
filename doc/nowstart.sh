export LD_LIBRARY_PATH=/lib:/usr/lib:/usr/local/lib
export PYTHONPATH=/usr/local/pynow:$PYTHONPATH

nowdbd -b /dbs -y 2>log/nowdb.log
