if [ $# -lt 1 ]
then
	n=$RANDOM
else
	n=$1
fi

echo "from first $nm lines"
head -$n report.txt | tail -100 | \
	feedgnuplot --terminal 'dumb 120,35' --lines --unset grid --exit
