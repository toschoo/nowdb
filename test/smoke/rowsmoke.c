/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for manually creating rows
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/time.h>
#include <nowdb/query/row.h>

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

char *testSingle(char *row2) {
	nowdb_err_t err;
	char *row=NULL;
	uint32_t sz=0;
	uint32_t s=0;
	uint32_t l=0; // , k=0;
	uint64_t u;
	int64_t  i;
	double   d,d2;
	char     b;
	nowdb_time_t tm;
	nowdb_type_t t=NOWDB_TYP_NOTHING;
	void *v=NULL;
	char *txt=NULL;
	int x;

	x = rand()%6;
	switch(x) {
	case 0:
		t = NOWDB_TYP_UINT;
		u=rand();
		v=&u; s=8;
		break;
	case 1:
		t = NOWDB_TYP_INT;
		i=rand()%1000000;
		if (rand()%2 == 0) i*=-1;
		v=&i; s=8;
		break;
	case 2:
		t = NOWDB_TYP_TIME;
		tm=rand()%1000000;
		if (rand()%2 == 0) tm*=-1;
		v=&tm; s=8;
		break;
	case 3:
		t = NOWDB_TYP_BOOL;
		b = rand()%2;
		v=&b; s=1;
		break;
	case 4:
		t = NOWDB_TYP_FLOAT;
		d = (double)(rand()%10000);
		d2 = (double)(rand()%10000);
		d /= d2;
		v=&d; s=8;
		break;
	case 5:
		t = NOWDB_TYP_TEXT;
		txt = randstr(&s);
		if (txt == NULL) {
			fprintf(stderr, "out-of-mem\n");
			return NULL;
		}
		v=txt;
	}

	// fprintf(stderr, "testing %d\n", t);

	if (row2 != NULL) {
		l = nowdb_row_len(row2);
		row = nowdb_row_addValue(row2, t, v, &sz);
	} else {
		row = nowdb_row_fromValue(t, v, &sz);
	}
	if (row == NULL) {
		fprintf(stderr, "out-of-mem?\n");
		if (txt != NULL) free(txt);
		if (row2 != NULL) free(row2);
		return NULL;
	}
	if (t == NOWDB_TYP_TEXT && sz != l + s + 3) {
		fprintf(stderr, "wrong size (text): %u + %u = %u\n", l,s,sz);
		if (txt != NULL) free(txt);
		free(row); row=NULL;
		return NULL;

	} else if (t != NOWDB_TYP_BOOL &&
	           t != NOWDB_TYP_TEXT && sz != l + 10) {
		fprintf(stderr, "wrong size: %u + %u = %u\n", l,s,sz);
		if (txt != NULL) free(txt);
		free(row); row=NULL;
		return NULL;

	} else if (t == NOWDB_TYP_BOOL && sz != l + 3) {
		fprintf(stderr, "wrong size (bool): %u + %u = %u\n", l,s,sz);
		if (txt != NULL) free(txt);
		free(row); row=NULL;
		return NULL;
	} 
	if (memcmp(row+l+1, v, s) != 0) {
		fprintf(stderr, "not the same: %d/%d\n", row[l], t);
		for(int j=0; j<sz; j++) {
			fprintf(stderr, "%u ", row[j]);
		}
		fprintf(stderr, "\n");
		if (txt != NULL) free(txt);
		free(row); row=NULL;
		return NULL;
	}
	err = nowdb_row_write(row, sz, stderr);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot write %d (%d, %u) row: \n", t, l, sz);
		nowdb_err_print(err);
		nowdb_err_release(err);
		if (txt != NULL) fprintf(stderr, "'%s'\n", txt);
		for(int j=0;j<sz;j++) {
			fprintf(stderr, "%d ", row[j]);
		}
		fprintf(stderr, "\n");
		if (txt != NULL) free(txt);
		free(row); row=NULL;
		return NULL;
	}
	if (txt != NULL) free(txt);
	return row;
}

int main() {
	int rc = EXIT_SUCCESS;
	char *row, *row2;

	srand(time(NULL));

	if (!nowdb_init()) {
		fprintf(stderr, "cannot init library\n");
		return EXIT_FAILURE;
	}

	for(int i=0;i<IT;i++) {
		row = testSingle(NULL);
		if (row == NULL) {
			fprintf(stderr, "testSingle failed\n");
			rc = EXIT_FAILURE;
			break;
		}
		if (rand()%2) {
			free(row); continue;
		}
		row2 = testSingle(row);
		if (row2 == NULL) {
			fprintf(stderr, "testSingle failed\n");
			rc = EXIT_FAILURE;
			break;
		}
		if (rand()%2) {
			free(row2); continue;
		}
		row2 = testSingle(row2);
		if (row2 == NULL) {
			fprintf(stderr, "testSingle failed\n");
			rc = EXIT_FAILURE;
			break;
		}
		free(row2);
	}

	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	nowdb_close();
	return rc;
}

