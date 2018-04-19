/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple insert tests for store
 * ========================================================================
 */
#include <nowdb/io/file.h>
#include <nowdb/store/store.h>

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

nowdb_store_t *mkStore() {
	nowdb_store_t *store;
	nowdb_err_t err;
	err = nowdb_store_new(&store, "rsc/store10", 1, 64, NOWDB_MEGA);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	return store;
}

nowdb_bool_t createStore(nowdb_store_t *store) {
	nowdb_err_t     err;
	err = nowdb_store_create(store);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	return TRUE;
}

nowdb_bool_t dropStore(nowdb_store_t *store) {
	nowdb_err_t     err;
	err = nowdb_store_drop(store);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	return TRUE;
}

nowdb_bool_t openStore(nowdb_store_t *store) {
	nowdb_err_t     err;
	err = nowdb_store_open(store);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	return TRUE;
}

nowdb_bool_t closeStore(nowdb_store_t *store) {
	nowdb_err_t     err;
	err = nowdb_store_close(store);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	return TRUE;
}

nowdb_store_t *bootstrap() {
	nowdb_store_t *store;
	store = mkStore();
	if (store == NULL) return NULL;
	if (openStore(store)) {
		if (!closeStore(store)) goto failure;
		if (!dropStore(store)) goto failure;
	}
	if (!createStore(store)) goto failure;
	if (!openStore(store)) goto failure;
	return store;

failure:
	closeStore(store);
	nowdb_store_destroy(store);
	return NULL;
	
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

int main() {
	int rc = EXIT_SUCCESS;
	nowdb_store_t *store;

	nowdb_err_init();
	store = bootstrap();
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
	/*
	 * read files
	 */
	if (!closeStore(store)) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!openStore(store)) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!checkFull(store)) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	/*
	 * read files
	 */
	
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

