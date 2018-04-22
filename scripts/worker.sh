rm -f log/worker.log
for i in {1..1000}
do
	printf "Test %05d\n" $i
	test/smoke/workersmoke >> log/worker.log 2>&1
	if [ $? -ne 0 ]
	then
		echo "FAILED"
		exit 1
	fi
done
echo "PASSED"
