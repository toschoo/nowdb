/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for mergesort with remainder
 * ========================================================================
 */
#include <nowdb/sort/sort.h>
#include <nowdb/types/types.h>
#include <nowdb/types/time.h>

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

#define ONE    16384

#define BUFSZ 8192

#define EDGE_OFF  25
#define LABEL_OFF 33
#define WEIGHT_OFF 41

void setRandomValue(char *e, uint32_t off) {
	uint64_t x=0;
	do x = rand()%100; while(x == 0);
	memcpy(e+off, &x, 8);
}

int insertEdges(char *e, int items, uint32_t size) {
	int rc;
	uint32_t sz;
	int rm;
	int z=0;

	sz = nowdb_edge_recSize(1, 3);
	rm = BUFSZ-(BUFSZ/sz)*sz;

	fprintf(stderr, "inserting %d random edges\n", items);

	for(uint32_t i=0; i<size;) {

		if (BUFSZ - (i%BUFSZ) < sz) {
			i+=rm; continue;
		}

		setRandomValue(e+i, NOWDB_OFF_ORIGIN);
		setRandomValue(e+i, NOWDB_OFF_DESTIN);
		setRandomValue(e+i, EDGE_OFF);
		setRandomValue(e+i, LABEL_OFF);
		rc = nowdb_time_now((nowdb_time_t*)(e+i+NOWDB_OFF_STAMP));
		if (rc != 0) {
			fprintf(stderr, "time error: %d\n", rc);
			return -1;
		}

		uint64_t w = (uint64_t)i;
		memcpy(e+i+WEIGHT_OFF, &w, sizeof(uint64_t));

		i+=sz;
		z++;
		if (z == items) break;
	}
	return 0;
}

char *createBuf(int items, uint32_t size) {
	char *buf;

	fprintf(stderr, "allocating %u bytes\n", size);

	buf = calloc(1, size);
	if (buf == NULL) return NULL;

	if (insertEdges(buf, items, size) != 0) {
		free(buf); return NULL;
	}
	/*
	int sz = nowdb_edge_recSize(1,3);
	int rm = BUFSZ-(BUFSZ/sz)*sz;
	for(int i=0; i<size;) {
		if (BUFSZ - (i%BUFSZ) <= rm) {
			i+=rm; continue;
		}
		fprintf(stderr, "origin: %lu\n", *(uint64_t*)(buf+i));
		i+=sz;
	}
	*/
	return buf;
}

int checkSorted(char *e, int items, uint32_t size) {
	int rc=0;
	char *p=NULL;
	int sz, rm;
	int cnt;
	int b=0;
	int sd=0; // stepdown

	sz = nowdb_edge_recSize(1, 3);
	rm = BUFSZ-(BUFSZ/sz)*sz;

	cnt = 1;

	for(int i=0; i<size;) {

		if (BUFSZ - (i%BUFSZ) <= rm) {
			i+=rm; continue;
		}
		if (p == NULL) {
			p=e+i; i+=sz; continue;
		}

		b = i/BUFSZ+1;

		/*
		fprintf(stderr, "%lu / %lu / %lu\n",
		    *(uint64_t*)(p+NOWDB_OFF_ORIGIN),
		    *(uint64_t*)(p+NOWDB_OFF_DESTIN),
		    *(int64_t*)(p+NOWDB_OFF_STAMP));
		*/

		// end of buffer
		if (*(uint64_t*)(e+i+NOWDB_OFF_ORIGIN) == 0 ||
		    *(uint64_t*)(e+i+NOWDB_OFF_DESTIN) == 0 ||
		    *(int64_t*)(e+i+NOWDB_OFF_STAMP)   == 0) {
			break;
		}

		if (*(uint64_t*)(e+i+NOWDB_OFF_ORIGIN) <
		    *(uint64_t*)(p+NOWDB_OFF_ORIGIN)) {
			fprintf(stderr, "%d. buffer is not sorted\n", b);
			sd++;
		}
		if (*(uint64_t*)(e+i+NOWDB_OFF_ORIGIN) ==
		    *(uint64_t*)(p+NOWDB_OFF_ORIGIN) &&
		    *(uint64_t*)(e+i+NOWDB_OFF_DESTIN) < 
		    *(uint64_t*)(p+NOWDB_OFF_DESTIN)) {
			fprintf(stderr, "%d. buffer is not sorted\n", b);
			sd++;
		}
		if (*(uint64_t*)(e+i+NOWDB_OFF_ORIGIN) ==
		    *(uint64_t*)(p+NOWDB_OFF_ORIGIN) &&
		    *(uint64_t*)(e+i+NOWDB_OFF_DESTIN) ==
		    *(uint64_t*)(p+NOWDB_OFF_DESTIN) &&
		    *(int64_t*)(e+i+NOWDB_OFF_STAMP) < 
		    *(int64_t*)(p+NOWDB_OFF_STAMP)) {
			fprintf(stderr, "%d. buffer is not sorted\n", b);
			sd++;
		}
		p=e+i;i+=sz;cnt++;
	}
	if (rc != 0) {
		fprintf(stderr, "invalid value (%d. item)\n", cnt);
		return -1;
	}
	if (sd > 0) {
		fprintf(stderr,
		"NOT SORTED for %u buffers: %d stepdowns (edges: %d)\n",
		                                size/BUFSZ, sd, cnt);
		return -1;
	}
	if (cnt != items) {
		fprintf(stderr, "number of edges do not match: %d / %d\n",
		                items, cnt);
		return -1;
	}
	fprintf(stderr, "SORTED: %d\n", cnt);
	return 0;
}

int testSort(int items) {
	int sz, size;
	int k=1;

	sz = nowdb_edge_recSize(1, 3);
	int perbuf = BUFSZ/sz;
	while(perbuf*k<items) k+=1;

	size = k*BUFSZ;

	char *buf = createBuf(items, size);
	if (buf == NULL) return -1;

	if (nowdb_mem_merge(buf, size, BUFSZ, sz,
	           nowdb_sort_edge_compare, NULL) != 0) {
		fprintf(stderr, "sort failed\n");
		free(buf); return -1;
	}
	if (checkSorted(buf, items, size) != 0) {
		fprintf(stderr, "buffer is not sorted\n");
		free(buf); return -1;
	}
	free(buf);
	return 0;
}

int main() {
	int perbuf = BUFSZ/nowdb_edge_recSize(1,3);
	int rc = EXIT_SUCCESS;
	if (!nowdb_init()) {
		fprintf(stderr, "cannot init environment\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	for(int i=0; i<100; i++) {
		/*
		int k = 1<<i;
		int sz = k*BUFSZ;
		int x = rand()%6;
		if (x > 1) sz += rand()%(BUFSZ/x);
		*/
		int k=0, r;
		while(k==0) k=rand()%10;
		r=rand()%10;
		int items = k*perbuf+r;

		fprintf(stderr, "testing with %d items\n", items);
		if (testSort(items) != 0) {
			fprintf(stderr, "%07d failed\n", items);
			rc = EXIT_FAILURE;
			break;
		}
	}

cleanup:
	srand(time(NULL) & (uint64_t)&printf);
	nowdb_close();
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;


}


