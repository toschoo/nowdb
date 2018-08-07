if [ $# -lt 1 ]
then
	path=/opt/dbs/bench10
else
	path=$1
fi

if [ $# -lt 2 ]
then
	target=raw.txt
else
	target=$2
fi

if [ $# -lt 3 ]
then
	cnt=1000000
	wrk=1
	blk=1
	lrg=8

elif [ "$3" = "large" ]
then
	cnt=1000000000
	wrk=3
	blk=8
	lrg=1024

elif [ "$3" = "big" ]
then
	cnt=100000000
	wrk=3
	blk=8
	lrg=1024

fi

if [ $# -gt 3 ]
then
	wrk=$4
fi

echo "path  : $path"
echo "target: $target"
echo "cnt=$cnt"
echo "wrk=$wrk"
echo "blk=$blk"
echo "lrg=$lrg"

bin/writestorebench $path -count $cnt \
                          -sort 1 -sorter $wrk \
                          -comp zstd \
                          -block $blk -large $lrg \
                          -report 10000 > $target
if [ $? -ne 0 ]
then
	echo "ERROR in writestorebench!"
	exit 1
fi

scripts/benchreport.sh $target quick.txt
if [ $? -ne 0 ]
then
	echo "ERROR in benchreport!"
	exit 1
fi

scripts/percentile.sh quick.txt
