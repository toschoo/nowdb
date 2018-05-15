/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple insert tests for store
 * ========================================================================
 */
#include <nowdb/io/file.h>
#include <nowdb/store/store.h>
#include <common/stores.h>
#include <common/bench.h>

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
	uint64_t c = 0;
	nowdb_bool_t closeit = FALSE;
	// uint32_t max = file->size / file->bufsize;

	if (file->state == nowdb_file_state_closed) {
		err = nowdb_file_open(file);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
		closeit = TRUE;
	}
	err = nowdb_file_rewind(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		if (closeit) NOWDB_IGNORE(nowdb_file_close(file));
		return FALSE;
	}
	fprintf(stderr, "file size: %u\n", file->size);
	fprintf(stderr, "file  pos: %u\n", file->pos);
	fprintf(stderr, "file  buf: %u\n", file->bufsize);
	for(;;) {
		if (file->pos >= file->size) break;
		err = nowdb_file_move(file);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			if (closeit) NOWDB_IGNORE(nowdb_file_close(file));
			return FALSE;
		}
		for(int i=0;i<file->bufsize;i+=file->recordsize) {
			e = (nowdb_edge_t*)(file->bptr+i);
			if (e->weight != j) {
				fprintf(stderr, "wrong number: %lu\n", e->weight);
				if (closeit) NOWDB_IGNORE(nowdb_file_close(file));
				return FALSE;
			}
			j++; c++;
		}
	}
	if (e == NULL) {
		fprintf(stderr, "no edge\n");
		if (closeit) NOWDB_IGNORE(nowdb_file_close(file));
		return FALSE;
	}
	if (e->weight != start + count) {
		fprintf(stderr, "last number is wrong: %lu (%lu)\n",
		                            e->weight, start+count);
		if (closeit) NOWDB_IGNORE(nowdb_file_close(file));
		return FALSE;
	}
	if (closeit) {
		err = nowdb_file_close(file);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			NOWDB_IGNORE(nowdb_file_close(file));
			return FALSE;
		}
	}
	return TRUE;
}

nowdb_bool_t checkFiles(nowdb_store_t *store) {
	nowdb_bool_t   found;
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
		nowdb_store_destroyFiles(store, &files);
		return FALSE;
	}
	if (files.len != 2) {
		fprintf(stderr, "expecting 2 files: %d\n", files.len);
		nowdb_store_destroyFiles(store, &files);
		return FALSE;
	}
	for(runner=files.head; runner!=NULL; runner=runner->nxt) {
		file = runner->cont;
		err = nowdb_store_findWaiting(store, file, &found);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			nowdb_store_destroyFiles(store, &files);
			return FALSE;
		}
		if (file->id != store->writer->id && !found) {
			nowdb_store_destroyFiles(store, &files);
			return FALSE;
		}
		/* if waiting find from 0...16383 */
		/* CAUTION: waiting may have been sorted already !!! */
		if (file->id != store->writer->id) {
			if (!find(file, FULL-1, 0)) {
				nowdb_store_destroyFiles(store, &files);
				return FALSE;
			}
		}
		/* if writer find from 16384 ...24575 */
		if (file->id == store->writer->id) {
			if (!find(file, HALF-1, FULL)) {
				nowdb_store_destroyFiles(store, &files);
				return FALSE;
			}
		}
	}
	nowdb_store_destroyFiles(store, &files);
	return TRUE;
}

int main() {
	struct timespec t1, t2;
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
		nowdb_bool_t x;
		timestamp(&t1);
		x = closeStore(store);
		timestamp(&t2);
		fprintf(stderr, "closing took %luus\n",
		                 minus(&t2, &t1)/1000);
		if (x) {
			nowdb_store_destroy(store);
			free(store);
		}
	}
	nowdb_err_destroy();
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}

