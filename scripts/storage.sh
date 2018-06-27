ctx=$1
if [ "$ctx" == "" ]
then
	echo "no context"
	exit 1
fi

s=$(bin/catalog $ctx | grep Size | cut -d":" -f2 | cut -d" " -f2 | awk '{print $1 "+"}')
s=${s//$'\n'/}

echo "$s 0" | bc
