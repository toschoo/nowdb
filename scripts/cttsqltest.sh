# ========================================================================
# Test Script for SQL (scopetool)
# -------------------
# Prerequisits:
# - file /opt/clients/ctt/ctt.count.125.csv
#   with the format count;edge;origin;timestamp
#   where count is the number of occurrences of
#   the specific combination of edge, origin and timestamp
#   in /opt/clients/ctt/ctt.edges.csv
# - database /opt/dbs/ctt derived from
#   /opt/clients/ctt/ctt.edges.csv
#
# The script will n times (n either 10 or $1):
# - select a random line from file
# - perform a query on the database with edge, origin and timestamp
# - and compare the number of results with count
# ========================================================================
if [ $# -lt 1 ]
then
	iter=10
else
	iter=$1
fi

file=/opt/clients/ctt/ctt.count.125.csv
base=/opt/dbs
db=ctt
ctx=ctt

mx=$(wc -l $file | cut -d" " -f1)

i=0
while [ $i -lt $iter ]
do
	i=$(($i+1))
	r=$((1+$RANDOM%mx))
	line=$(head -$r $file | tail -1)
	#echo $line
	expected=$(echo $line | cut -d";" -f1)
	edge=$(echo $line | cut -d";" -f2)
	orig=$(echo $line | cut -d";" -f3)
	tstp=$(echo $line | cut -d";" -f4)

	printf "Test %06d) Line %06d: " $i $r

	sql="use ctt;select * from ctt"
	sql="$sql where edge=$edge and origin=$orig"
	sql="$sql and timestamp = $tstp;"

	res=$(echo "$sql" | bin/scopetool $base 2>&1 | \
		grep Read | sed 's/^.*Read: //g')

	cnt=$(echo $res | cut -d":" -f2 | cut -d" " -f2)

	#echo $res

	if [ $cnt -ne $expected ]
	then
		echo "FAILED: $cnt != $expected!"
		exit 1
	else
		printf "%06d - OK.\n" $cnt
	fi
done
echo "PASSED"
