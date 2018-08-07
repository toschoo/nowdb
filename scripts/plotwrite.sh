if [ $# -lt 1 ]
then
	w=$(wc -l report.txt | awk '{print $1}')	
	n=$(($RANDOM%$w))
else
	n=$1
fi

head -$n report.txt | tail -100 | \
	feedgnuplot --terminal 'dumb 120,35' --lines --unset grid --exit

echo ""
echo "from first $n lines"
