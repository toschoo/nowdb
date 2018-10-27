
echo "RUNNING TEST BATTERY 'PYSMOKE'" > log/pysmoke.log

nohup nowdbd -b rsc > log/nowdb.log 2>&1 &
if [ $? -ne 0 ]
then
	echo "FAILED: cannot run server"
	exit 1
fi

p=$!

echo "SERVER $p RUNNING" >> log/pysmoke.log

echo "RUNNING baicsmoke.py" >> log/pysmoke.log
test/pysmoke/basicsmoke.py >> log/pysmoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: basicsmoke.py failed"
	kill -2 $p
	exit 1
fi

echo "RUNNING bugs.py" >> log/pysmoke.log
test/pysmoke/bugs.py >> log/pysmoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: bugs.py failed"
	kill -2 $p
	exit 1
fi

echo "RUNNING insert.py" >> log/pysmoke.log
test/pysmoke/insert.py >> log/pysmoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: insert.py failed"
	kill -2 $p
	exit 1
fi


kill -2 $p

echo "PASSED"
