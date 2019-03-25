
echo "RUNNING TEST BATTERY 'PAPISMOKE'" > log/papismoke.log

export PYTHONPATH=$PYTHONPATH:./test/server
nohup nowdbd -b rsc -y > log/nowdb.log 2>&1 &
if [ $? -ne 0 ]
then
	echo "FAILED: cannot run server"
	exit 1
fi

p=$!

echo "SERVER $p RUNNING" >> log/papismoke.log

echo "RUNNING baicsmoke.py" >> log/papismoke.log
test/papismoke/basicsmoke.py >> log/papismoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: basicsmoke.py failed"
	kill -2 $p
	exit 1
fi

echo "RUNNING formulas.py" >> log/papismoke.log
test/papismoke/formulas.py >> log/papismoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: formulas.py failed"
	kill -2 $p
	exit 1
fi

echo "RUNNING pdbasic.py" >> log/papismoke.log
test/papismoke/pdbasic.py >> log/papismoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: pdbasic.py failed"
	kill -2 $p
	exit 1
fi

echo "RUNNING alias.py" >> log/papismoke.log
test/papismoke/alias.py >> log/papismoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: alias.py failed"
	kill -2 $p
	exit 1
fi

echo "RUNNING insert.py" >> log/papismoke.log
test/papismoke/insert.py >> log/papismoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: insert.py failed"
	kill -2 $p
	exit 1
fi

echo "RUNNING exec.py" >> log/papismoke.log
test/papismoke/exec.py >> log/papismoke.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: exec.py failed"
	kill -2 $p
	exit 1
fi

kill -2 $p

sleep 1
echo "PASSED"
