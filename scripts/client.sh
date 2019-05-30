make clientsmoke
make server
make client
if [ $? -ne 0 ]
then
	echo "FAILED: cannot make tests"
	exit 1
fi

echo "RUNNING TEST BATTERY 'CLIENT'" > log/client.log

nohup bin/nowdbd -b rsc > log/nowdb.log 2>&1 &
if [ $? -ne 0 ]
then
	echo "FAILED: cannot run server"
	exit 1
fi

p=$!

echo "SERVER $p RUNNING" >> log/client.log

echo "running explain.py" >> log/client.log
test/client/explain.py 1000 >> log/client.log
if [ $? -ne 0 ]
then
	echo "FAILED: explain.py failed"
	kill -2 $p
	exit 1
fi

echo "running time.py" >> log/client.log
test/client/time.py 1000 >> log/client.log
if [ $? -ne 0 ]
then
	echo "FAILED: time.py failed"
	kill -2 $p
	exit 1
fi

echo "running clientsmoke (1)" >> log/client.log
test/client/clientsmoke >> log/client.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: clientsmoke failed"
	kill -2 $p
	exit 1
fi

echo "running clientsmoke (2)" >> log/client.log
test/client/clientsmoke >> log/client.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: clientsmoke (2) failed"
	kill -2 $p
	exit 1
fi

echo "running clientsmoke (3)" >> log/client.log
test/client/clientsmoke >> log/client.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: clientsmoke (3) failed"
	kill -2 $p
	exit 1
fi

kill -2 $p
sleep 1

echo "PASSED"
