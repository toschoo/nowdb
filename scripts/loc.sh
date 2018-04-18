c=0
for d in $(ls src/nowdb)
do
	stp=0
	for f in $(ls src/nowdb/$d)
	do
		tmp=$(wc -l src/nowdb/$d/$f | awk '{print $1}')
		stp=$(($stp+$tmp))
	done
	c=$(($c+$stp))
	printf "% 7s: % 6d\n" $d $stp
done
printf -- "---------------\n"
printf "  total: % 6d\n" $c
