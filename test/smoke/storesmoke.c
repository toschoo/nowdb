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
	err = nowdb_store_init(&store, "rsc/teststore", 1, 64, NOWDB_MEGA);
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

	err = nowdb_store_init(&store, "rsc/teststore", 1, 64, NOWDB_MEGA);
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

	err = nowdb_store_init(&store, "rsc/teststore", 1, 64, NOWDB_MEGA);
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

	err = nowdb_store_new(&store, "rsc/teststore", 1, 64, NOWDB_MEGA);
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

	err = nowdb_store_init(&store, "rsc/teststore", 1, 64, NOWDB_MEGA);
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

	err = nowdb_store_init(&store, "rsc/teststore", 1, 64, NOWDB_MEGA);
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
