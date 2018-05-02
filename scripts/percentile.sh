
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

n=$(wc -l $file | awk '{print $1}')

sf=sorted.$file.tmp
sort -n $file > $sf

cat $sf | tr "\n" "+" > sum.$file.tmp
echo 0 >> sum.$file.tmp
s=$(cat sum.$file.tmp | bc)
avg=$(($s/$n))

tail -n+5 $sf | head -n-5 | tr "\n" "+" > sum.$file.tmp
echo 0 >> sum.$file.tmp
s2=$(cat sum.$file.tmp | bc)
avg2=$(($s2/($n-10)))

rm sum.$file.tmp

min=$(head -1 $sf)
max=$(tail -1 $sf)

k=$(($n*50/100))
median=$(sort -n $file | head -$k | tail -1)

k=$(($n*75/100))
seven5=$(sort -n $file | head -$k | tail -1)

k=$(($n*95/100))
nine5=$(sort -n $file | head -$k | tail -1)

k=$(($n*99/100))
nine9=$(sort -n $file | head -$k | tail -1)

rm $sf

printf "sum   : %010d\n" $s
printf "avg   : %010d\n" $avg
printf "avg-5 : %010d\n" $avg2
printf "min   : %010d\n" $min
printf "max   : %010d\n" $max
printf "median: %010d\n" $median
printf "75%%   : %010d\n" $seven5
printf "95%%   : %010d\n" $nine5
printf "99%%   : %010d\n" $nine9

