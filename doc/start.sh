docker run --rm -d                                 \
	   -p 55505:55505                          \
	   -v /opt:/opt                            \
	   --name nowdbcontainer                   \
	   nowdbdocker /bin/bash -c "./nowstart.sh"
