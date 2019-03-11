# ========================================================================
# Test script for where on edge
# -----------------------------
# Prerequisits:
# - csvkit should be installed
#
# The script
# - creates the database rsc/db500
# - creates an edge and a context
# - creates rsc/kilo.csv with 1000 edges using writecsv
# - copies this file to rsc/measure.csv and adds a header to it
# - it then generates random wheres containing
#   edge, origin, destin and timestamp
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
csvsqlprfx="select count(*) from measure "
nowsqlprfx="use db500;select count(*) from measure "
csvsqlsufx=""
nowsqlsufx=";"

# make sure everything we need is there
make bin/scopetool
make bin/writecsv
if [ $? -ne 0 ]
then
	echo "cannot make stuff"
	exit 1
fi

file=rsc/kilo.csv
vfile=rsc/loc.csv
csv=rsc/measure.csv
base=rsc
loader=test/sql/load2.sql

# how we build our where clause
function mkwhere() {
	l=$1

	line=$(head -$l $file | tail -1)
	vars=$(echo $line | cut -d";" -f1-5)
	origin=$(echo $vars | cut -d";" -f1)
	destin=$(echo $vars | cut -d";" -f2)
	label=$(echo $vars | cut -d";" -f4)

	for i in {0..2}
	do
		x=$(($RANDOM%2))
		if [ $x -eq 0 ]
		then
			case $i in
			1) c1="or" ;;
			2) c2="or" ;;
			esac
		else 
			case $i in
			1) c1="and" ;;
			2) c2="and" ;;
			esac
		fi
	done

	# we still need parentheses
	x=$(($RANDOM%4))
	case $x in
		0) 
			o="origin=$origin";
			d="destin=$destin"; l="label=$label"
			;;
		1) 
			o="(origin=$origin"; 
			d="destin=$destin)"; l="label=$label" 
			;;
		2) 
			o="(origin=$origin"; 
			d="destin=$destin"; l="label=$label)"
			;;
		3) 
			o="origin=$origin"; 
			d="(destin=$destin"; l="label=$label)"
			;;
	esac

	w="where $o $c1 $d $c2 $l"

	printf "%s\n" "$w"
}

# create the data
bin/writecsv > $file
bin/writecsv -vertex 1 > $vfile

# make them csvsql-readable
echo "origin;destin;stamp;label;weight" > $csv
cat $file >> $csv

# load them into database
cat $loader | bin/scopetool $base

cnt=$(wc -l rsc/kilo.csv | awk -F" " '{print $1}')

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
	line=$((1 + $RANDOM%$cnt))
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
	nowres=$(echo $nowdbsql | bin/scopetool $base 2>/dev/null)
	if [ $? -ne 0 ]
	then
		printf "ERROR in scopetool '%s':\n" "$nowdbsql"
		echo "$nowout"
		exit 1
	fi
	# nowres=$(echo $nowout | cut -d" " -f2)
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
