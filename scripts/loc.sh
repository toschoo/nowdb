stp=0
for f in $(ls src/nowdbd)
do
	tmp=$(wc -l src/nowdbd/$f | awk '{print $1}')
	stp=$(($stp+$tmp))
done
echo ""
printf "nowdb server: % 6d\n" $stp

stp=0
for f in $(ls src/nowdbclient)
do
	tmp=$(wc -l src/nowdbclient/$f | awk '{print $1}')
	stp=$(($stp+$tmp))
done
printf "nowdb client: % 6d\n" $stp
echo "nowdb library"
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

