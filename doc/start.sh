docker run --rm -d                                 \
	   -p 50677:50677                          \
	   -v /opt:/opt                            \
	   --name nowdbcontainer                   \
	   nowdbdocker /bin/bash -c "./nowstart.sh"
