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

#define LOOP_INIT(stm, atts) \
	uint32_t recsz = nowdb_edge_recSize(atts); \
	uint32_t bufsz = (NOWDB_IDX_PAGE/recsz)*recsz; \
	uint32_t remsz = NOWDB_IDX_PAGE - bufsz;

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

	int recsz = nowdb_recSize(3);
	int bufsz = (8192/recsz)*recsz;

	s = filesize(path);

	f = fopen(path, "r");
	if (f == NULL) {
		perror("cannot open file");
		return -1;
	}

	for(int i = 0; i<s; i+=8192) {
		x = fread(buf, 1, 8192, f);
		if (x != 8192) {
			if (x <= 0) {
				perror("cannot read file");
				rc = -1; break;
			}
			fprintf(stderr, "incomplete read: %d\n", x);
		}
		for(int z=0;z<8192;z+=recsz) {
			if (z%8192 >= bufsz) break;

			char *e = buf+z;

			fprintf(stdout, "%lu-->%lu @%ld\n",
			  *(uint64_t*)(e+NOWDB_OFF_ORIGIN),
			  *(uint64_t*)(e+NOWDB_OFF_DESTIN),
			  *(int64_t*)(e+NOWDB_OFF_STAMP));
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
