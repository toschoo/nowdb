if [ $# -lt 1 ]
then
	scope=bench20
else
	scope=$1
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
else
	cnt=$3
fi

echo "path  : $path"
echo "target: $target"
echo "count : $cnt"

sql="create scope $scope if not exists"
sql="$sql; use $scope"
sql="$sql; create big context ctx_bench if not exists"
sql="$sql; create big index idx_bench1 on ctx_bench (origin, edge) if not exists"
sql="$sql; create big index idx_bench2 on ctx_bench (destin, edge) if not exists"

echo "$sql" | bin/scopetool /opt/dbs

if [ $? -ne 0 ]
then
	exit 1
fi

bin/writecontextbench /opt/dbs/$scope -count $cnt \
	                              -report 10000 \
				      -context ctx_bench > $target
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
