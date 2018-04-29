make smoke
if [ $? -ne 0 ]
then
	echo "FAILED: cannot make tests"
fi

echo "RUNNING TEST BATTERY 'SMOKE'" > log/test.log

test/smoke/errsmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: errsmoke failed"
fi

test/smoke/timesmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: timesmoke failed"
fi

test/smoke/pathsmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: pathsmoke failed"
fi

test/smoke/tasksmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: tasksmoke failed"
	exit 1
fi

test/smoke/queuesmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: queuesmoke failed"
	exit 1
fi

test/smoke/workersmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: workersmoke failed"
	exit 1
fi

test/smoke/filesmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: filesmoke failed"
	exit 1
fi

test/smoke/storesmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: storesmoke failed"
	exit 1
fi

test/smoke/insertstoresmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: insertstoresmoke failed"
	exit 1
fi

test/smoke/insertandsortstoresmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: insertandsortstoresmoke failed"
	exit 1
fi

echo "PASSED"
