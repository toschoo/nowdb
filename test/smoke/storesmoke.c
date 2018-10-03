/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for store
 * ========================================================================
 */
#include <nowdb/io/file.h>
#include <nowdb/store/store.h>

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

nowdb_bool_t createStore() {
	nowdb_store_t store;
	nowdb_err_t     err;

	/*
	if (mkdir("rsc/teststore", NOWDB_FILE_MODE) != 0) {
		perror("cannot create 'rsc/teststore'");
		return FALSE;
	}
	*/
	err = nowdb_store_init(&store, "rsc/teststore", 1, 64,
	                              NOWDB_MEGA, NOWDB_MEGA);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	err = nowdb_store_create(&store);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_store_destroy(&store);
		return FALSE;
	}
	nowdb_store_destroy(&store);
	return TRUE;
}

nowdb_bool_t dropStore() {
	nowdb_store_t store;
	nowdb_err_t     err;

	err = nowdb_store_init(&store, "rsc/teststore", 1, 64,
	                              NOWDB_MEGA, NOWDB_MEGA);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	err = nowdb_store_drop(&store);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_store_destroy(&store);
		return FALSE;
	}
	nowdb_store_destroy(&store);
	return TRUE;
}

nowdb_bool_t testInitDestroyStore() {
	nowdb_store_t store;
	nowdb_err_t     err;

	err = nowdb_store_init(&store, "rsc/teststore", 1, 64,
	                              NOWDB_MEGA, NOWDB_MEGA);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	nowdb_store_destroy(&store);
	return TRUE;
}

nowdb_bool_t testNewDestroyStore() {
	nowdb_store_t *store;
	nowdb_err_t      err;

	err = nowdb_store_new(&store, "rsc/teststore", 1, 64,
	                              NOWDB_MEGA, NOWDB_MEGA);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	nowdb_store_destroy(store); free(store);
	return TRUE;
}

nowdb_bool_t testOpenStore() {
	nowdb_store_t store;
	nowdb_err_t     err;

	err = nowdb_store_init(&store, "rsc/teststore", 1, 64,
	                              NOWDB_MEGA, NOWDB_MEGA);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	err = nowdb_store_open(&store);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	nowdb_store_destroy(&store);
	return TRUE;
}

nowdb_bool_t testOpenCloseStore() {
	nowdb_store_t store;
	nowdb_err_t     err;

	err = nowdb_store_init(&store, "rsc/teststore", 1, 64,
	                              NOWDB_MEGA, NOWDB_MEGA);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	err = nowdb_store_open(&store);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	err = nowdb_store_close(&store);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	nowdb_store_destroy(&store);
	return TRUE;
}

nowdb_bool_t testInsert() {
	nowdb_store_t store;
	nowdb_err_t     err;
	nowdb_edge_t    e,k;
	int rc;

	err = nowdb_store_init(&store, "rsc/teststore", 1, 64,
	                              NOWDB_MEGA, NOWDB_MEGA);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	err = nowdb_store_open(&store);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}

	e.edge = 3;
	e.origin = 1;
	e.destin = 2;
	e.label = 5;
	e.weight = 42;
	e.weight2 = 0;
	e.wtype[0] = NOWDB_TYP_UINT;
	e.wtype[1] = NOWDB_TYP_NOTHING;

	rc = nowdb_time_now(&e.timestamp);
	if (rc != 0) {
		fprintf(stderr, "cannot get timestamp: %d\n", rc);
		return FALSE;
	}

	err = nowdb_store_insert(&store, &e);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}

	e.weight++;
	err = nowdb_store_insert(&store, &e);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}

	e.weight++;
	err = nowdb_store_insert(&store, &e);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}

	memcpy(&k, store.writer->mptr, 64);
	if (k.weight != e.weight-2) {
		fprintf(stderr, "edges differ\n");
		return FALSE;
	}
	memcpy(&k, store.writer->mptr+64, 64);
	if (k.weight != e.weight-1) {
		fprintf(stderr, "edges differ\n");
		return FALSE;
	}
	memcpy(&k, store.writer->mptr+128, 64);
	if (k.weight != e.weight) {
		fprintf(stderr, "edges differ\n");
		return FALSE;
	}
	
	err = nowdb_store_close(&store);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	nowdb_store_destroy(&store);
	return TRUE;
}


int main() {
	int rc = EXIT_SUCCESS;

	if (!nowdb_err_init()) {
		fprintf(stderr, "cannot init\n");
		return EXIT_FAILURE;
	}

	if (!testInitDestroyStore()) {
		fprintf(stderr, "testInitDestroy failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testNewDestroyStore()) {
		fprintf(stderr, "testNewDestroy failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!createStore()) {
		fprintf(stderr, "createStore failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testOpenCloseStore()) {
		fprintf(stderr, "testOpenCloseStore failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testInsert()) {
		fprintf(stderr, "testInsert failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!dropStore()) {
		fprintf(stderr, "dropStore failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	
cleanup:
	nowdb_err_destroy();
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}
