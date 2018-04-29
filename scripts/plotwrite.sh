if [ $# -lt 1 ]
then
	n=$RANDOM
else
	n=$1
fi

head -$n report.txt | tail -100 | \
	feedgnuplot --terminal 'dumb 120,35' --lines --unset grid --exit

echo ""
echo "from first $n lines"
