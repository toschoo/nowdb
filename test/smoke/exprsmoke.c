/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for manually creating rows
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/time.h>
#include <nowdb/fun/expr.h>

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>
#include <math.h>
#include <limits.h>
#include <float.h>

#define IT 1000
#define MAXSTR 20

char *randstr(uint32_t *sz) {
	char *str;
	uint32_t s;

	do {s = rand()%MAXSTR;} while(s==0);

	str = malloc(s+1);

	for(int i=0; i<s; i++) {
		str[i] = rand()%25;
		str[i]+=65;
	}
	str[s] = 0; *sz = s;
	// fprintf(stderr, "'%s'\n", str);
	return str;
}

int main() {
	int rc = EXIT_SUCCESS;

	srand(time(NULL));

	if (!nowdb_init()) {
		fprintf(stderr, "cannot init library\n");
		return EXIT_FAILURE;
	}

	for(int i=0;i<IT;i++) {
	}

	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	nowdb_close();
	return rc;
}

