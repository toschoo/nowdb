/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for sorting
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/sort/sort.h>

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <limits.h>

#define NELEMENTS 4096

nowdb_cmp_t intcmp(const void *one, const void *two, void *ignore) {
	if (*(int*)one < *(int*)two) return NOWDB_SORT_LESS;
	if (*(int*)one > *(int*)two) return NOWDB_SORT_GREATER;
	return NOWDB_SORT_EQUAL;
}

int testBlistSort() {
	int rc = 0;
	nowdb_err_t err;
	nowdb_blist_t  flist;
	ts_algo_list_t blocks;
	ts_algo_list_node_t *runner;
	nowdb_block_t  *b=NULL;
	int *buf;
	int rem;
	int e;
	int j=0;
	int t=0;

	rem = rand()%100;
	if (rem%2 == 0) rem++;

	e = NELEMENTS+rem;
	
	buf = malloc(sizeof(int)*e);
	if (buf == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return -1;
	}
	for(int i=0; i<e; i++) {
		buf[i] = rand()%1000;
	}
	ts_algo_list_init(&blocks);
	err = nowdb_blist_init(&flist, 256);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot init free list\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		rc = -1; goto cleanup;
	}
	for(int i=0; i<e; i++) {
		if (i%64 == 0) {
			err = nowdb_blist_give(&flist, &blocks);
			if (err != NOWDB_OK) {
				fprintf(stderr, "blist won't give\n");
				nowdb_err_print(err);
				nowdb_err_release(err);
				rc = -1; goto cleanup;
			}
			b = blocks.head->cont; j=0;
		}
		memcpy(b->block+j, buf+i, sizeof(int));
		j+=sizeof(int); b->sz += sizeof(int);
	}
	// fprintf(stderr, "blocks in input set: %d\n", blocks.len);

	// sort buf
	nowdb_mem_sort((char*)buf, e, sizeof(int), &intcmp, NULL);

	// sort blocks
	err = nowdb_block_sort(&blocks, &flist, sizeof(int), &intcmp);
	if (err != NOWDB_OK) {
		fprintf(stderr, "blist won't give\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		rc = -1; goto cleanup;
	}
	// fprintf(stderr, "blocks in result set: %d\n", blocks.len);

	// check no of elements
	for(runner=blocks.head; runner!=NULL; runner=runner->nxt) {
		b = runner->cont;
		t += b->sz/sizeof(int);
	}
	fprintf(stderr, "in result set: %d (%d)\n", t, e);
	if (t != e) {
		fprintf(stderr,
		"result does not equal input: %d (%d)\n", t, e);
		rc = -1; goto cleanup;
	}
	// compare
	runner = blocks.head;
	if (runner == NULL) {
		fprintf(stderr, "head of result is NULL\n");
		rc = -1; goto cleanup;
	}
	b = runner->cont; j=0;
	for(int i=0; i<e; i++) {
		if (j>=b->sz) {
			runner=runner->nxt;
			if (runner==NULL) {
				fprintf(stderr, "result is inclomplete\n");
				rc = -1; goto cleanup;
			}
			b = runner->cont; j=0;
			// fprintf(stderr, "====================\n");
		}
		// fprintf(stderr, "%d\n", *(int*)(b->block+j));
		if (buf[i] != *(int*)(b->block+j)) {
			fprintf(stderr, "elements differ at %d: %d / %d\n",
			                i, buf[i], *(int*)(b->block+j));
			rc = -1; goto cleanup;
		}
		j+=sizeof(int);
	}

cleanup:
	nowdb_blist_destroyBlockList(&blocks, &flist);
	nowdb_blist_destroy(&flist);
	free(buf);
	return rc;
}

int main() {
	int rc = EXIT_SUCCESS;

	srand(time(NULL) ^ (uint64_t)&printf);

	for(int i=0; i<100; i++) {
		if (testBlistSort() != 0) {
			fprintf(stderr, "testBlistSort failed\n");
			rc = -1; goto cleanup;
		}
	}

cleanup:
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED.\n");
	} else {
		fprintf(stderr, "FAILED.\n");
	}
	return rc;
}

