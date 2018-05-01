
if [ $# -lt 1 ]
then
	file=report.txt
else
	file=$1
fi

if [ "$file" = "" ]
then
	echo "no file"
	exit 1
fi

if [ $# -lt 2 ]
then
	p=99
else
	p=$2
fi


n=$(wc -l report.txt | awk '{print $1}')
k=$(($n*$p/100))

sort -n $file | head -$k | tail -1
