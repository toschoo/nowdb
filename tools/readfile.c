/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Read edge file
 * ========================================================================
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <error.h>

#include <nowdb/types/types.h>

void helptxt(char *program) {
	fprintf(stderr, "%s <path-to-file>\n", program);
}

char buf[8192];

ssize_t filesize(char *path) {
	struct stat st;

	if (stat(path, &st) != 0) {
		perror("cannot stat file");
		return -1;
	}
	return st.st_size;
}

int readfile(char *path) {
	int rc = 0;
	ssize_t s;
	int x = 0;
	FILE *f;
	nowdb_edge_t *e;

	s = filesize(path);

	f = fopen(path, "r");
	if (f == NULL) {
		perror("cannot open file");
		return -1;
	}

	for(ssize_t i = 0; i<s; i+=8192) {
		x = fread(buf, 1, 8192, f);
		if (x != 8192) {
			if (x <= 0) {
				perror("cannot read file");
				rc = -1; break;
			}
			fprintf(stderr, "incomplete read: %d\n", x);
		}
		for(int z=0;z<8192;z+=64) {
			e = (nowdb_edge_t*)(buf+z);
			fprintf(stdout, "%lu.%lu(%lu)\n",
			  e->origin, e->edge, e->destin);
		}
	}
	fclose(f);
	return rc;
}

int main(int argc, char **argv) {
	char *path;

	if (argc < 2) {
		helptxt(argv[0]);
		return EXIT_FAILURE;
	}
	path = argv[1];

	if (readfile(path) != 0) return EXIT_FAILURE;
	return EXIT_SUCCESS;
}
