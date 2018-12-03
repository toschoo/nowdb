echo "RUNNING TEST BATTERY"

scripts/smoke.sh
if [ $? -ne 0 ]
then
	exit 1
fi

scripts/client.sh
if [ $? -ne 0 ]
then
	exit 1
fi

sleep 1

echo "RUNNING VWHERE TESTS" > log/where.log
scripts/vwhere.sh 10 >>log/where.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: vwhere failed"
	exit 1
fi

scripts/pysmoke.sh
if [ $? -ne 0 ]
then
	exit 1
fi

sleep 1

echo "RUNNING EWHERE TESTS" >> log/where.log
scripts/ewhere.sh 10 1>&2 >>log/where.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: ewhere failed"
	exit 1
fi
