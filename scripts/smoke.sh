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
fi

test/smoke/queuesmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: queuesmoke failed"
fi

test/smoke/workersmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: workersmoke failed"
fi

test/smoke/filesmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: filesmoke failed"
fi

test/smoke/storesmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: storesmoke failed"
fi

test/smoke/insertstoresmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: insertstoresmoke failed"
fi

echo "PASSED"
