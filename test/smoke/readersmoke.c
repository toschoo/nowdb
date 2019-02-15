/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for readers
 * ========================================================================
 */
#include <nowdb/io/file.h>
#include <nowdb/task/task.h>
#include <nowdb/task/lock.h>
#include <nowdb/store/store.h>
#include <nowdb/reader/reader.h>
#include <common/stores.h>
#include <common/bench.h>

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

typedef struct {
	uint64_t origin;
	uint64_t destin;
	nowdb_time_t timestamp;
	uint64_t value1;
	uint64_t value2;
	uint64_t value3;
} myedge_t;

#define RECSIZE sizeof(myedge_t)

void makeEdgePattern(myedge_t *e) {
	e->origin   = 1;
	e->destin   = 1;
	e->value1   = 0;
	e->value2   = 0;
	e->value3   = 0;
}

#define RECPAGE (NOWDB_IDX_PAGE/RECSIZE)
#define FULL (128*RECPAGE)
#define HALF (FULL/2)

nowdb_bool_t insertEdges(nowdb_store_t *store, uint32_t count, uint64_t start) {
	int rc;
	nowdb_err_t err;
	myedge_t e;
	uint64_t max = start + count;

	makeEdgePattern(&e);
	rc = nowdb_time_now(&e.timestamp);
	if (rc != 0) {
		fprintf(stderr, "cannot get timestamp: %d\n", rc);
		return FALSE;
	}
	for(uint64_t i=start; i<max; i++) {
		e.value1 = i;
		err = nowdb_store_insert(store, &e);
		if (err != NOWDB_OK) {
			fprintf(stderr, "insert error\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
	}
	fprintf(stderr, "inserted %u from %lu to %lu (%lu)\n",
	                         count, start, max, e.value1);
	return TRUE;
}

nowdb_bool_t checkInitial(nowdb_store_t *store) {
	if (store->writer == NULL) {
		fprintf(stderr, "store without writer\n");
		return FALSE;
	}
	if (store->writer->size != 0) {
		fprintf(stderr, "store not at initial position\n");
		return FALSE;
	}
	if (store->spares.len == 0) {
		fprintf(stderr, "store has no spares\n");
		return FALSE;
	}
	if (store->spares.len < 2) {
		fprintf(stderr, "store has too few spares\n");
		return FALSE;
	}
	if (store->waiting.len > 0) {
		fprintf(stderr, "store has pending files\n");
		return FALSE;
	}
	return TRUE;
}

#define DELAY 10000000
nowdb_bool_t waitSorted(nowdb_store_t *store) {
	nowdb_err_t err;
	nowdb_bool_t ok = FALSE;

	for(;;) {
		err = nowdb_lock_read(&store->lock);
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot lock\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
		ok = (store->waiting.len == 0);
		err = nowdb_unlock_read(&store->lock);
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot unlock\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
		if (ok) break;
		err = nowdb_task_sleep(DELAY);
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot sleep\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
	}
	return ok;
}

nowdb_bool_t testFullscan(nowdb_store_t *store) {
	nowdb_err_t err;
	nowdb_reader_t *reader;
	ts_algo_list_t files;
	uint64_t s = 0;
	uint32_t mx = (NOWDB_IDX_PAGE/RECSIZE)*RECSIZE;

	ts_algo_list_init(&files);
	err = nowdb_store_getReaders(store, &files, NOWDB_TIME_DAWN,
	                                            NOWDB_TIME_DUSK);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot get readers :-(\n");
		nowdb_err_print(err); nowdb_err_release(err);
		return FALSE;
	}
	err = nowdb_reader_fullscan(&reader, &files, NULL);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create reader :-(\n");
		nowdb_err_print(err); nowdb_err_release(err);
		nowdb_store_destroyFiles(store, &files);
		return FALSE;
	}
	for(;;) {
		err = nowdb_reader_move(reader);
		if (err != NOWDB_OK) {
			if (err->errcode != nowdb_err_eof) {
				fprintf(stderr, "move failed\n");
				nowdb_err_print(err);
				nowdb_err_release(err);
				nowdb_reader_destroy(reader); free(reader);
				nowdb_store_destroyFiles(store, &files);
				return FALSE;
			}
			nowdb_err_release(err); break;
		}
		for(int i=0; i<mx; i+= reader->recsize) {
			s++;
		}
	}
	nowdb_reader_destroy(reader); free(reader);
	nowdb_store_destroyFiles(store, &files);
	if (s != 5*FULL) {
		fprintf(stderr, "count does not match: %lu/%lu\n", s, 5*FULL);
		return FALSE;
	}
	return TRUE;
}

int main() {
	nowdb_err_t err;
	struct timespec t1, t2;
	int rc = EXIT_SUCCESS;
	nowdb_store_t *store1=NULL, *store2=NULL;

	fprintf(stderr, "RECSIZE: %lu, FULL: %lu\n", RECSIZE, FULL);

	nowdb_err_init();
	fprintf(stderr, "uncompressed...\n");
	store1 = bootstrap("rsc/store40", RECSIZE);
	if (store1 == NULL) {
		fprintf(stderr, "cannot bootstrap\n");
		return EXIT_FAILURE;
	}
	if (!insertEdges(store1, 5*FULL+HALF, 0)) {
		fprintf(stderr, "cannot insert edges\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!waitSorted(store1)) {
		fprintf(stderr, "cannot wait for sorting\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	fprintf(stderr, "starting to read\n");
	timestamp(&t1);
	if (!testFullscan(store1)) {
		fprintf(stderr, "cannot perform fullscan\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	timestamp(&t2);
	fprintf(stderr, "Fullscan (uncompressed): %ldus\n",
	                             minus(&t2, &t1)/1000);

	err = nowdb_store_close(store1);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_store_destroy(store1);
		free(store1); store1 = NULL;
		rc = EXIT_FAILURE; goto cleanup;
	}
	nowdb_store_destroy(store1); free(store1); store1=NULL;

	store2 = xBootstrap("rsc/store50", &nowdb_sort_edge_compare,
	        NOWDB_COMP_ZSTD, 2, RECSIZE, NOWDB_MEGA, NOWDB_MEGA);
	if (store2 == NULL) {
		fprintf(stderr, "cannot bootstrap\n");
		return EXIT_FAILURE;
	}
	if (!insertEdges(store2, 5*FULL+HALF, 0)) {
		fprintf(stderr, "cannot insert edges\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!waitSorted(store2)) {
		fprintf(stderr, "cannot wait for sorting\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	fprintf(stderr, "starting to read\n");
	timestamp(&t1);
	if (!testFullscan(store2)) {
		fprintf(stderr, "cannot perform fullscan\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	timestamp(&t2);
	fprintf(stderr, "Fullscan (compressed)  : %ldus\n",
	                             minus(&t2, &t1)/1000);

cleanup:
	if (store1 != NULL) {
		NOWDB_IGNORE(nowdb_store_close(store1));
		nowdb_store_destroy(store1); free(store1);
	}
	if (store2 != NULL) {
		NOWDB_IGNORE(nowdb_store_close(store2));
		nowdb_store_destroy(store2); free(store2);
	}
	nowdb_err_destroy();
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}

