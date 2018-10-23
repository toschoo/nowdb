
dname="nowdbcontainer"

if [ $# -eq 1 ]
then
	dname="nowdbcontainer"
fi

if [ "$dname" == "" ]
then
	echo "docker name is empty"
fi

docker exec -ti $dname /bin/bash -c "./nowstop.sh"
