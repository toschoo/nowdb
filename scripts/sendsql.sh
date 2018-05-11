sql1="create scope test"
sql2="create big context big_ctx set sorters=1, largesize=8, allocsize=8"
sql3="load '/opt/dbs/csv/myfile.csv' into big_ctx ignore header"

if [ $# -lt 1 ]
then
	count=1000
else
	count=$1
fi

echo ""
for i in $(seq 1 $count)
do
	k=$(($RANDOM%3))
	case $k in
		0) sql=$sql1 ;;
		1) sql=$sql2 ;;
		2) sql=$sql3 ;;
	esac
	printf "%s;\n" "$sql"

done
