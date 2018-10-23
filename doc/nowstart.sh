export LD_LIBRARY_PATH=/lib:/usr/lib:/usr/local/lib
export PYTHONPATH=/usr/local/pynow:$PYTHONPATH

nowdbd -b /opt/dbs -y 2>>/opt/log/nowdb.log
