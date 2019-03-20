echo "RUNNING TEST BATTERY"
make all

echo "RUNNING SMOKE TESTS IN C"
scripts/smoke.sh
if [ $? -ne 0 ]
then
	exit 1
fi

echo "RUNNING CLIENT TESTS"
scripts/client.sh
if [ $? -ne 0 ]
then
	exit 1
fi

sleep 1

echo "RUNNING VWHERE TESTS"
echo "RUNNING VWHERE TESTS" > log/where.log
scripts/vwhere.sh 10 >>log/where.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: vwhere failed"
	exit 1
fi

echo "RUNNING SMOKE TESTS IN PYTHON"
scripts/pysmoke.sh
if [ $? -ne 0 ]
then
	exit 1
fi

sleep 1

echo "RUNNING EWHERE TESTS"
echo "RUNNING EWHERE TESTS" >> log/where.log
scripts/ewhere.sh 10 1>&2 >>log/where.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: ewhere failed"
	exit 1
fi

echo "RUNNING PYTHON DB API TESTS IN PYTHON"
scripts/papismoke.sh
if [ $? -ne 0 ]
then
	exit 1
fi

sleep 1

echo "PASSED"
