# -------------------------------------------------------------------------
# running python base api test battery with either Python 2 or 3
# -------------------------------------------------------------------------
v=2

if [ $# -gt 0 ]
then
  if [ $1 -eq 3 ]
  then
     v=3
  fi
fi

if [ $v -eq 2 ]
then
  PY="python"
else
  PY="python3"
fi

echo "RUNNING TEST BATTERY 'PYSMOKE' using Python $v" > log/pysmoke.log

nohup bin/nowdbd -b rsc > log/nowdb.log 2>&1 &
if [ $? -ne 0 ]
then
	echo "FAILED: cannot run server"
	exit 1
fi

p=$!

echo "SERVER $p RUNNING" >> log/pysmoke.log

echo "RUNNING baicsmoke.py" >> log/pysmoke.log
$PY test/pysmoke/basicsmoke.py >> log/pysmoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: basicsmoke.py failed"
	kill -2 $p
	exit 1
fi


echo "RUNNING curedge.py" >> log/pysmoke.log
$PY test/pysmoke/curedge.py >> log/pysmoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: curedge.py failed"
	kill -2 $p
	exit 1
fi

echo "RUNNING curvertex.py" >> log/pysmoke.log
$PY test/pysmoke/curvertex.py >> log/pysmoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: curvertex.py failed"
	kill -2 $p
	exit 1
fi

echo "RUNNING wedge.py" >> log/pysmoke.log
$PY test/pysmoke/wedge.py >> log/pysmoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: wedge.py failed"
	kill -2 $p
	exit 1
fi

echo "RUNNING wvertex.py" >> log/pysmoke.log
$PY test/pysmoke/wvertex.py >> log/pysmoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: wvertex.py failed"
	kill -2 $p
	exit 1
fi

echo "RUNNING strings.py" >> log/pysmoke.log
$PY test/pysmoke/strings.py >> log/pysmoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: strings.py failed"
	kill -2 $p
	exit 1
fi

echo "RUNNING formulas.py" >> log/pysmoke.log
$PY test/pysmoke/formulas.py >> log/pysmoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: formulas.py failed"
	kill -2 $p
	exit 1
fi

echo "RUNNING lock.py" >> log/pysmoke.log
$PY test/pysmoke/lock.py >> log/pysmoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: lock.py failed"
	kill -2 $p
	exit 1
fi

echo "RUNNING bugs.py" >> log/pysmoke.log
$PY test/pysmoke/bugs.py >> log/pysmoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: bugs.py failed"
	kill -2 $p
	exit 1
fi

echo "RUNNING insert.py" >> log/pysmoke.log
$PY test/pysmoke/insert.py >> log/pysmoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: insert.py failed"
	kill -2 $p
	exit 1
fi

echo "RUNNING loader.py" >> log/pysmoke.log
$PY test/pysmoke/loader.py >> log/pysmoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: loader.py failed"
	kill -2 $p
	exit 1
fi

sleep 1

kill -2 $p

echo "PASSED"
