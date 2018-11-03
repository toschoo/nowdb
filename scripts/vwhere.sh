# ========================================================================
# Test script for where on vertex
# -------------------------------
# Prerequisits:
# - csvkit should be installed
#
# The script
# - creates the database rsc/db500
# - creates an edge and a context
# - creates rsc/loc.csv with 1000 vertices using writecsv -vertex 1
# - it then generates random wheres containing
#   - name, lon, lat
#   - with and & ors (randomly)
#   - random parentheses around the expressions
# - it executes the resulting query with 
#   - scopetool
#   - csvsql
# - and compares the results
# ========================================================================
if [ $# -lt 1 ]
then
	mx=10
else
	mx=$1
fi

# sqlprefix
csvsqlprfx="select count(*) from loc "
nowsqlprfx="use db500;select id,name,lon,lat from loc "
csvsqlsufx=""
nowsqlsufx=";"

# make sure everything we need is there
make all
if [ $? -ne 0 ]
then
	echo "cannot make stuff"
	exit 1
fi

efile=rsc/kilo.csv
file=rsc/loc.csv
csv=$file
base=rsc
loader=test/sql/load2.sql

# how we build our where clause
function mkwhere() {
	l=$1

	line=$(head -$l $file | tail -1)
	id=$(echo $line | cut -d";" -f1)
	name=$(echo $line | cut -d";" -f2)
	lon=$(echo $line | cut -d";" -f3)
	lat=$(echo $line | cut -d";" -f4)

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

	x=$(($RANDOM%4))
	case $x in
		0) 
			e="id=$id"; n="name='$name'";
			o="lon=$lon"; a="lat=$lat"
			;;
		1) 
			e="id=$id"; n="name='$name'"; 
			o="(lon=$lon"; a="lat=$lat)" 
			;;
		2) 
			e="id=$id";n="(name='$name'"; 
			o="lon=$lon)"; a="lat=$lat"
			;;
		3) 
			e="id=$id";n="(name='$name'"; 
			o="lon=$lon)"; a="lat=$lat"
			;;
	esac

	# w="where $e $c1 $n $c2 $o $c3 $a"
	w="where $n $c2 $o $c3 $a"

	printf "%s\n" "$w"
}

# create the data
bin/writecsv -vertex 1 > $file
bin/writecsv > $efile

# load them into database
cat $loader | bin/scopetool $base

cnt=$(wc -l $file | awk -F" " '{print $1}')

i=0
IFS=$'\n'

# noinference parameter for csvsql
csvchk=$(csvsql -h | tail -1 | cut -d" " -f3 | cut -d"," -f1)

if [ "$csvchk" = "-I" ]
then
	echo "no type inference"
	noinf="-I"
else
	noinf=""
fi

while [ $i -lt $mx ]
do
	while [ true ]
	do
		line=$((2 + $RANDOM%$cnt))
		z=$(head -$line $file | tail -1 | grep ";NA;")
		if [ "$z" == "" ]
		then
			break
		fi 
	done

	i=$(($i+1))
	
	printf "Test %06d) line %d: " $i $line
	whereclause=$(mkwhere $line)
	csvsql="$csvsqlprfx$whereclause$csvsqlsufx"
	nowdbsql="$nowsqlprfx$whereclause$nowsqlsufx"
	csvout=$(csvsql $noinf -d";" --query="$csvsql" $csv 2>&1)
	if [ $? -ne 0 ]
	then
		printf "ERROR in csvsql '%s':\n" "$csvsql"
		echo "$csvout"
		exit 1
	fi
	csvres=$(echo $csvout | awk -F" " '{print $2}')
	nowout=$(echo $nowdbsql | bin/scopetool $base 2>&1 >/dev/null | grep Read)
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
		echo "$nowout"
		exit 1
	fi
	printf "%d = %d (%s)\n" $csvres $nowres "$csvsql"
done
unset IFS

echo "PASSED!"
exit 0
