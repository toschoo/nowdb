/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple insert tests for store
 * ========================================================================
 */
#include <nowdb/io/file.h>
#include <nowdb/store/store.h>
#include <common/stores.h>

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

#define HALF 8192
#define FULL 16384

void makeEdgePattern(nowdb_edge_t *e) {
	e->origin   = 1;
	e->destin   = 1;
	e->edge     = 1;
	e->label    = 0;
	e->weight2  = 0;
	e->wtype[0] = NOWDB_TYP_UINT;
	e->wtype[1] = NOWDB_TYP_NOTHING;
}

nowdb_bool_t insertEdges(nowdb_store_t *store, uint32_t count, uint64_t start) {
	nowdb_err_t err;
	nowdb_edge_t e;
	uint64_t max = start + count;

	makeEdgePattern(&e);
	err = nowdb_time_now(&e.timestamp);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	for(uint64_t i=start; i<max; i++) {
		e.weight = i;
		err = nowdb_store_insert(store, &e);
		if (err != NOWDB_OK) {
			fprintf(stderr, "insert error\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
	}
	fprintf(stderr, "inserted %u from %lu to %lu (%lu)\n",
	                         count, start, max, e.weight);
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

nowdb_bool_t checkHalf(nowdb_store_t *store) {
	if (store->writer == NULL) {
		fprintf(stderr, "store without writer\n");
		return FALSE;
	}
	if (store->writer->size != HALF*store->recsize) {
		fprintf(stderr, "store not at 'half' position: %u\n",
		                                store->writer->size);
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

nowdb_bool_t checkFull(nowdb_store_t *store) {
	if (store->writer == NULL) {
		fprintf(stderr, "store without writer\n");
		return FALSE;
	}
	if (store->writer->size != HALF*store->recsize) {
		fprintf(stderr, "store not at 'half' position: %u\n",
		                                store->writer->size);
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
	if (store->waiting.len != 1) {
		fprintf(stderr, "store has no pending file\n");
		return FALSE;
	}
	return TRUE;
}

nowdb_bool_t find(nowdb_file_t *file, uint32_t count, uint64_t start) {
	nowdb_err_t err;
	nowdb_edge_t *e=NULL;
	uint64_t j = start;
	// uint32_t max = file->size / file->bufsize;

	err = nowdb_file_open(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	err = nowdb_file_rewind(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		NOWDB_IGNORE(nowdb_file_close(file));
		return FALSE;
	}
	fprintf(stderr, "file size: %u\n", file->size);
	for(;;) {
		if (file->pos == file->size) break;
		err = nowdb_file_move(file);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			NOWDB_IGNORE(nowdb_file_close(file));
			return FALSE;
		}
		for(int i=0;i<file->bufsize;i+=file->recordsize) {
			e = (nowdb_edge_t*)(file->bptr+i);
			if (e->weight != j) {
				fprintf(stderr, "wrong number: %lu\n", e->weight);
				NOWDB_IGNORE(nowdb_file_close(file));
				return FALSE;
			}
			j++;
			fprintf(stderr, "edge: %lu\n", e->weight);
		}
	}
	if (e == NULL) {
		fprintf(stderr, "no edge\n");
		NOWDB_IGNORE(nowdb_file_close(file));
		return FALSE;
	}
	if (e->weight != start + count) {
		fprintf(stderr, "last number is wrong: %lu\n", e->weight);
		NOWDB_IGNORE(nowdb_file_close(file));
		return FALSE;
	}
	err = nowdb_file_close(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		NOWDB_IGNORE(nowdb_file_close(file));
		return FALSE;
	}
	return TRUE;
}

nowdb_bool_t checkFiles(nowdb_store_t *store) {
	nowdb_err_t      err;
	ts_algo_list_t files;
	nowdb_file_t   *file;
	ts_algo_list_node_t *runner;
	ts_algo_list_init(&files);
	err = nowdb_store_getFiles(store, &files,
	                           NOWDB_TIME_DAWN,
	                           NOWDB_TIME_DUSK);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		ts_algo_list_destroy(&files);
		return FALSE;
	}
	if (files.len != 2) {
		fprintf(stderr, "expecting 2 files: %d\n", files.len);
		ts_algo_list_destroy(&files);
		return FALSE;
	}
	for(runner=files.head; runner!=NULL; runner=runner->nxt) {
		file = runner->cont;
		if (file->id != store->writer->id &&
		    nowdb_store_findWaiting(store, file) == NULL)
		{
			ts_algo_list_destroy(&files);
			return FALSE;
		}
		/* if waiting find from 0...16383 */
		/* if writer find from 16384 ...24575 */
		if (file->id == store->writer->id) {
			if (!find(file, FULL, 16384)) {
				ts_algo_list_destroy(&files);
				return FALSE;
			}
		}
	}
	ts_algo_list_destroy(&files);
	return TRUE;
}

int main() {
	int rc = EXIT_SUCCESS;
	nowdb_store_t *store;

	nowdb_err_init();
	store = bootstrap("rsc/store10");
	if (store == NULL) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!checkInitial(store)) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!insertEdges(store, HALF, 0)) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!checkHalf(store)) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!insertEdges(store, FULL, HALF)) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!checkFull(store)) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!checkFiles(store)) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!closeStore(store)) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!openStore(store)) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!checkFull(store)) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!checkFiles(store)) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	
cleanup:
	if (store != NULL) {
		closeStore(store);
		nowdb_store_destroy(store);
		free(store);
	}
	nowdb_err_destroy();
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}

