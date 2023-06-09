make smoke
if [ $? -ne 0 ]
then
	echo "FAILED: cannot make tests"
	exit 1
fi

echo "RUNNING TEST BATTERY 'SMOKE'" > log/test.log

echo "running errsmoke" >> log/test.log
test/smoke/errsmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: errsmoke failed"
	exit 1
fi

echo "running timesmoke" >> log/test.log
test/smoke/timesmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: timesmoke failed"
	exit 1
fi

echo "running pathsmoke" >> log/test.log
test/smoke/pathsmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: pathsmoke failed"
	exit 1
fi

echo "running tasksmoke" >> log/test.log
test/smoke/tasksmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: tasksmoke failed"
	exit 1
fi

echo "running queuesmoke" >> log/test.log
test/smoke/queuesmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: queuesmoke failed"
	exit 1
fi

echo "running workersmoke" >> log/test.log
test/smoke/workersmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: workersmoke failed"
	exit 1
fi

echo "running filesmoke" >> log/test.log
test/smoke/filesmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: filesmoke failed"
	exit 1
fi

echo "running filtersmoke" >> log/test.log
test/smoke/filtersmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: filtersmoke failed"
	exit 1
fi

echo "running storesmoke" >> log/test.log
test/smoke/storesmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: storesmoke failed"
	exit 1
fi

echo "running insertstoresmoke" >> log/test.log
test/smoke/insertstoresmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: insertstoresmoke failed"
	exit 1
fi

echo "running insertandsortstoresmoke" >> log/test.log
test/smoke/insertandsortstoresmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: insertandsortstoresmoke failed"
	exit 1
fi

echo "running scopesmoke" >> log/test.log
test/smoke/scopesmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: scopesmoke failed"
	exit 1
fi

echo "running readersmoke" >> log/test.log
test/smoke/readersmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: readersmoke failed"
	exit 1
fi

echo "running indexsmoke" >> log/test.log
test/smoke/indexsmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: indexsmoke failed"
	exit 1
fi

echo "running imansmoke" >> log/test.log
test/smoke/imansmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: imansmoke failed"
	exit 1
fi

echo "running indexersmoke" >> log/test.log
test/smoke/indexersmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: indexersmoke failed"
	exit 1
fi

echo "running modelsmoke" >> log/test.log
test/smoke/modelsmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: modelsmoke failed"
	exit 1
fi

echo "running pmansmoke" >> log/test.log
test/smoke/pmansmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: pmansmoke failed"
	exit 1
fi

echo "running textsmoke" >> log/test.log
test/smoke/textsmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: textsmoke failed"
	exit 1
fi

echo "running sortsmoke" >> log/test.log
test/smoke/sortsmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: sortsmoke failed"
	exit 1
fi

echo "running msortsmoke" >> log/test.log
test/smoke/msortsmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: msortsmoke failed"
	exit 1
fi

echo "running rowsmoke" >> log/test.log
test/smoke/rowsmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: rowsmoke failed"
	exit 1
fi

echo "running exprsmoke" >> log/test.log
test/smoke/exprsmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: exprsmoke failed"
	exit 1
fi

echo "running funsmoke" >> log/test.log
test/smoke/funsmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: funsmoke failed"
	exit 1
fi

echo "running scopesmoke2" >> log/test.log
test/smoke/scopesmoke2 >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: scopesmoke2 failed"
	exit 1
fi

echo "running mergesmoke" >> log/test.log
test/smoke/mergesmoke >> log/test.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: mergesmoke failed"
	exit 1
fi

echo "PASSED"
