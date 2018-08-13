#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <ctype.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>

char buf[8192];

#define SENDSQL(sql) \
	s = strlen(sql); \
	memcpy(msg, &s, 4); \
	if (write(sock, msg, 4) != 4) { \
		perror("cannot send size"); \
		close(sock); \
		return EXIT_FAILURE; \
	} \
	memcpy(msg, sql, s); \
	if (write(sock, msg, s) != s) { \
		perror("cannot send SQL"); \
		close(sock); \
		return EXIT_FAILURE; \
	}

#define READACK() \
	s = read(sock, msg, 8192); \

int main(int argc, char **argv) {
	size_t s;
	int sock;
	struct sockaddr_in adr;
	char *msg=buf;

	/*
	if (argc > 1) {
		s = strnlen(argv[1], 128);
		if (s > 127) {
			fprintf(stderr, "message too long!\n");
			return EXIT_FAILURE;
		}
		if (strcmp(argv[1], "-?") == 0) {
			fprintf(stderr, "%s message\n", argv[0]);
			return EXIT_SUCCESS; 
		}
		msg = argv[1];
	}
	*/

	if ((sock = socket(AF_INET, SOCK_STREAM, IPPROTO_IP)) < 0) {
		perror("cannot create socket");
		return EXIT_FAILURE;
	}
	adr.sin_family = AF_INET;
	adr.sin_port   = htons(55505);
	if (inet_aton("127.0.0.1", &adr.sin_addr) == 0) {
	// if (inet_aton("148.251.50.235", &adr.sin_addr) == 0) {
		perror("cannot set address");
		return EXIT_FAILURE;
	}
	if (connect(sock, (struct sockaddr*)&adr, sizeof(adr)) != 0) {
		perror("cannot connect");
		return EXIT_FAILURE;
	}
	memcpy(msg, "SQLTX0  ", 8);
	if (write(sock, msg, 8) != 8) {
		perror("cannot send header");
		close(sock);
		return EXIT_FAILURE;
	}
	SENDSQL("use retail");
	READACK();

	SENDSQL("select count(*) from tx");

	s = read(sock, msg, 8192);
	write(1, msg, s); write(1, "\n", 1);

	fprintf(stderr, "blocking!\n");
	s = read(sock, msg, 8192); // just block

	fprintf(stderr, "received: %zu\n", s);
	write(1, msg, 1); write(1,"\n",1);
	
	close(sock);
	return EXIT_SUCCESS;
}
