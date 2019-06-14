# -------------------------------------------------------------------------
# running python db api test battery with either Python 2 or 3
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

echo "RUNNING TEST BATTERY 'PAPISMOKE' with python $v" > log/papismoke.log

export PYTHONPATH=$PYTHONPATH:./test/server
nohup bin/nowdbd -b rsc -ly > log/nowdb.log 2>&1 &
if [ $? -ne 0 ]
then
	echo "FAILED: cannot run server"
	exit 1
fi

p=$!

echo "SERVER $p RUNNING" >> log/papismoke.log

sleep 1

echo "RUNNING baicsmoke.py" >> log/papismoke.log
$PY test/papismoke/basicsmoke.py >> log/papismoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: basicsmoke.py failed"
	kill -2 $p
	exit 1
fi

echo "RUNNING formulas.py" >> log/papismoke.log
$PY test/papismoke/formulas.py >> log/papismoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: formulas.py failed"
	kill -2 $p
	exit 1
fi

echo "RUNNING pdbasic.py" >> log/papismoke.log
$PY test/papismoke/pdbasic.py >> log/papismoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: pdbasic.py failed"
	kill -2 $p
	exit 1
fi

echo "RUNNING alias.py" >> log/papismoke.log
$PY test/papismoke/alias.py >> log/papismoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: alias.py failed"
	kill -2 $p
	exit 1
fi

echo "RUNNING insert.py" >> log/papismoke.log
$PY test/papismoke/insert.py >> log/papismoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: insert.py failed"
	kill -2 $p
	exit 1
fi

echo "RUNNING exec.py lua" >> log/papismoke.log
$PY test/papismoke/exec.py lua >> log/papismoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: exec.py lua failed"
	kill -2 $p
	exit 1
fi

echo "RUNNING exec2.py" >> log/papismoke.log
$PY test/papismoke/exec2.py >> log/papismoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: exec2.py python failed"
	kill -2 $p
	exit 1
fi

echo "RUNNING exec.py python" >> log/papismoke.log
$PY test/papismoke/exec.py python >> log/papismoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: exec.py python failed"
	kill -2 $p
	exit 1
fi

kill -2 $p

sleep 1
echo "PASSED"
