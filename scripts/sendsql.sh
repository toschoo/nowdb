sql1="create scope test"
sql2="create big context big_ctx set sorters=1, largesize=8, allocsize=8"
sql3="load '/opt/dbs/csv/myfile.csv' into big_ctx ignore header"
sql4="select * from big_ctx"
sql5="select * from big_ctx where timestamp <= 1527182572603300000 or timestamp >= 1527182572603344059"
sql6="select * from big_ctx where edge = 1 and (origin = 50 or origin = 100) and (destin = 10 or destin = 90)"

if [ $# -lt 1 ]
then
	count=1000
else
	count=$1
fi

echo ""
for i in $(seq 1 $count)
do
	k=$(($RANDOM%6))
	case $k in
		0) sql=$sql1 ;;
		1) sql=$sql2 ;;
		2) sql=$sql3 ;;
		3) sql=$sql4 ;;
		4) sql=$sql5 ;;
		5) sql=$sql6 ;;
	esac
	printf "%s;\n" "$sql"
done
