if [ $# -lt 1 ]
then
	source=raw.txt
else
	source=$1
fi

if [ $# -lt 2 ]
then
	target=report.txt
else
	target=$2
fi

grep "10000:" $source | cut -d" " -f2 | cut -d"u" -f1 > $target

