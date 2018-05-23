if [ $# -lt 1 ]
then
	mx=100
else
	mx=$1
fi

# sqlprefix
csvsqlprfx="select count(*) from context "
nowsqlprfx="use test;select *  from ctx_tiny "
csvsqlsufx=""
nowsqlsufx=";"

# make sure everything we need is there
make all
if [ $? -ne 0 ]
then
	echo "cannot make stuff"
	exit 1
fi

file=rsc/kilo.csv
csv=rsc/context.csv
base=/opt/dbs
loader=test/sql/load2.sql

# how we build our where clause
function mkwhere() {
	l=$1

	line=$(head -$l $file | tail -1)
	vars=$(echo $line | cut -d";" -f1-5)
	edge=$(echo $vars | cut -d";" -f1)
	origin=$(echo $vars | cut -d";" -f2)
	destin=$(echo $vars | cut -d";" -f3)
	tmstmp=$(echo $vars | cut -d";" -f5)

	for i in {0..3}
	do
		x=$(($RANDOM%2))
		if [ $x -eq 0 ]
		then
			case $i in
			0) c1="or" ;;
			1) c2="or" ;;
			2) c3="or" ;;
			esac
		else 
			case $i in
			0) c1="and" ;;
			1) c2="and" ;;
			2) c3="and" ;;
			esac
		fi
	done

	# we still need parentheses
	x=$(($RANDOM%4))
	case $x in
		0) 
			e="edge=$edge"; o="origin=$origin";
			d="destin=$destin"; t="timestamp=$tmstmp"
			;;
		1) 
			e="(edge=$edge"; o="origin=$origin"; 
			d="destin=$destin)"; t="timestamp=$tmstmp" 
			;;
		2) 
			e="edge=$edge";o="(origin=$origin"; 
			d="destin=$destin)"; t="timestamp=$tmstmp"
			;;
		3) 
			e="edge=$edge";o="(origin=$origin"; 
			d="destin=$destin)"; t="timestamp=$tmstmp"
			;;
	esac

	w="where $e $c1 $o $c2 $d $c3 $t"

	printf "%s\n" "$w"
}

# create the data
bin/writecsv > $file

# make them csvsql-readable
echo "edge;origin;destin;label;timestamp;weight;weight2;wtype;wtype2" > $csv
cat $file >> $csv

# load them into database
cat $loader | bin/scopetool $base

cnt=$(wc -l rsc/kilo.csv | awk -F" " '{print $1}')

i=0
IFS=$'\n'
while [ $i -lt $mx ]
do
	line=$((1 + $RANDOM%$cnt))
	i=$(($i+1))
	printf "Test %06d) line %d: " $i $line
	whereclause=$(mkwhere $line)
	csvsql="$csvsqlprfx$whereclause$csvsqlsufx"
	nowdbsql="$nowsqlprfx$whereclause$nowsqlsufx"
	csvout=$(csvsql -I -d";" --query="$csvsql" $csv 2>&1)
	if [ $? -ne 0 ]
	then
		printf "ERROR in csvsql '%s':\n" "$csvsql"
		echo "$csvout"
		exit 1
	fi
	csvres=$(echo $csvout | awk -F" " '{print $2}')
	nowout=$(echo $nowdbsql | bin/scopetool $base 2>&1 >/dev/null)
	if [ $? -ne 0 ]
	then
		printf "ERROR in scopetool '%s':\n" "$nowdbsql"
		echo "$nowout"
		exit 1
	fi
	nowres=$(echo $nowout | cut -d" " -f2)
	if [ $csvres -ne $nowres ]
	then
		printf "FAILED in '%s': %d != %d\n" $csvsql $csvres $nowres
		exit 1
	fi
	printf "%d = %d (%s)\n" $csvres $nowres "$csvsql"
done
unset IFS

echo "PASSED!"
exit 0
