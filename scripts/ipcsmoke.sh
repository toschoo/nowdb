echo "RUNNING baicsmoke.py" >> log/lock.log
test/pysmoke/basicsmoke.py >> log/lock.log 2>&1
if [ $? -ne 0 ]
then
	echo "FAILED: basicsmoke.py failed"
	# kill -2 $p
	exit 1
fi

test/pysmoke/lock.py >> log/lock.log 2>&1  &
test/pysmoke/lock.py >> log/lock.log 2>&1  &
test/pysmoke/lock.py >> log/lock.log 2>&1  &
test/pysmoke/lock.py >> log/lock.log 2>&1  &
test/pysmoke/lock.py >> log/lock.log 2>&1  &

# sleep 10

# kill -2 $p

echo "PASSED"
