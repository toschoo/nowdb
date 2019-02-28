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

typedef struct {
	uint64_t origin;
	uint64_t destin;
	nowdb_time_t stamp;
	uint64_t reg1;
	uint64_t reg2;
	uint64_t reg3;
	uint64_t reg4;
	uint64_t reg5;
} myedge_t;

#define RECSIZE sizeof(myedge_t)

nowdb_storage_t *createStorage() {
	nowdb_storage_config_t cfg;
	nowdb_storage_t *strg;
	nowdb_err_t       err;
	nowdb_bitmap64_t   os;

	os = NOWDB_CONFIG_SIZE_TINY       |
	     NOWDB_CONFIG_INSERT_MODERATE |
	     NOWDB_CONFIG_DISK_HDD;
	nowdb_storage_config(&cfg, os);

	err = nowdb_storage_new(&strg, "default", &cfg);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create default storage\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}

	return strg;
}

nowdb_bool_t createStore() {
	nowdb_storage_t *strg;
	nowdb_store_t   store;
	nowdb_err_t       err;

	/*
	if (mkdir("rsc/teststore", NOWDB_FILE_MODE) != 0) {
		perror("cannot create 'rsc/teststore'");
		return FALSE;
	}
	*/
	strg = createStorage();
	if (strg == NULL) return FALSE;

	err = nowdb_store_init(&store, "rsc/teststore", NULL, 1,
	                               NOWDB_CONT_EDGE, strg,
                                       RECSIZE, 1);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_storage_destroy(strg); free(strg);
		return FALSE;
	}
	err = nowdb_store_create(&store);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_store_destroy(&store);
		nowdb_storage_destroy(strg); free(strg);
		return FALSE;
	}
	nowdb_store_destroy(&store);
	nowdb_storage_destroy(strg); free(strg);
	return TRUE;
}

nowdb_bool_t dropStore() {
	nowdb_storage_t *strg;
	nowdb_store_t   store;
	nowdb_err_t       err;

	strg = createStorage();
	if (strg == NULL) return FALSE;

	err = nowdb_store_init(&store, "rsc/teststore", NULL, 1,
	                               NOWDB_CONT_EDGE, strg,
                                       RECSIZE,1);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_storage_destroy(strg); free(strg);
		return FALSE;
	}
	err = nowdb_store_drop(&store);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_store_destroy(&store);
		nowdb_storage_destroy(strg); free(strg);
		return FALSE;
	}
	nowdb_store_destroy(&store);
	nowdb_storage_destroy(strg); free(strg);
	return TRUE;
}

nowdb_bool_t testInitDestroyStore() {
	nowdb_storage_t *strg;
	nowdb_store_t   store;
	nowdb_err_t       err;

	strg = createStorage();
	if (strg == NULL) return FALSE;

	err = nowdb_store_init(&store, "rsc/teststore", NULL, 1,
	                      NOWDB_CONT_EDGE, strg, RECSIZE, 1);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_storage_destroy(strg); free(strg);
		return FALSE;
	}
	nowdb_store_destroy(&store);
	nowdb_storage_destroy(strg); free(strg);
	return TRUE;
}

nowdb_bool_t testNewDestroyStore() {
	nowdb_storage_t *strg;
	nowdb_store_t  *store;
	nowdb_err_t       err;

	strg = createStorage();
	if (strg == NULL) return FALSE;

	err = nowdb_store_new(&store, "rsc/teststore", NULL, 1,
	                     NOWDB_CONT_EDGE, strg, RECSIZE, 1);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_storage_destroy(strg); free(strg);
		return FALSE;
	}
	nowdb_store_destroy(store); free(store);
	nowdb_storage_destroy(strg); free(strg);
	return TRUE;
}

nowdb_bool_t testOpenStore() {
	nowdb_storage_t *strg;
	nowdb_store_t   store;
	nowdb_err_t       err;

	strg = createStorage();
	if (strg == NULL) return FALSE;

	err = nowdb_storage_start(strg);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot start storage\n");
		nowdb_err_print(err); nowdb_err_release(err);
		nowdb_storage_destroy(strg); free(strg);
		return FALSE;
	}

	err = nowdb_store_init(&store, "rsc/teststore", NULL, 1,
	                      NOWDB_CONT_EDGE, strg, RECSIZE, 1);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_storage_stop(strg);
		nowdb_storage_destroy(strg); free(strg);
		return FALSE;
	}
	err = nowdb_store_open(&store);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_storage_stop(strg);
		nowdb_storage_destroy(strg); free(strg);
		return FALSE;
	}
	nowdb_store_destroy(&store);
	nowdb_storage_stop(strg);
	nowdb_storage_destroy(strg); free(strg);
	return TRUE;
}

nowdb_bool_t testOpenCloseStore() {
	nowdb_storage_t *strg;
	nowdb_store_t   store;
	nowdb_err_t       err;

	strg = createStorage();
	if (strg == NULL) return FALSE;

	err = nowdb_storage_start(strg);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot start storage\n");
		nowdb_err_print(err); nowdb_err_release(err);
		nowdb_storage_destroy(strg); free(strg);
		return FALSE;
	}

	err = nowdb_store_init(&store, "rsc/teststore", NULL, 1,
	                      NOWDB_CONT_EDGE, strg, RECSIZE, 1);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		NOWDB_IGNORE(nowdb_storage_stop(strg));
		nowdb_storage_destroy(strg); free(strg);
		return FALSE;
	}
	err = nowdb_store_open(&store);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		NOWDB_IGNORE(nowdb_storage_stop(strg));
		nowdb_storage_destroy(strg); free(strg);
		return FALSE;
	}
	err = nowdb_storage_stop(strg);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_storage_destroy(strg); free(strg);
		return FALSE;
	}
	err = nowdb_store_close(&store);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_storage_destroy(strg); free(strg);
		return FALSE;
	}
	nowdb_storage_destroy(strg); free(strg);
	nowdb_store_destroy(&store);
	return TRUE;
}

nowdb_bool_t testInsert() {
	nowdb_storage_t *strg;
	nowdb_store_t   store;
	nowdb_err_t       err;
	myedge_t          e,k;
	int rc;

	strg = createStorage();
	if (strg == NULL) return FALSE;

	err = nowdb_storage_start(strg);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot start storage\n");
		nowdb_err_print(err); nowdb_err_release(err);
		nowdb_storage_destroy(strg); free(strg);
		return FALSE;
	}

	err = nowdb_store_init(&store, "rsc/teststore", NULL, 1,
	                      NOWDB_CONT_EDGE, strg, RECSIZE, 1);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		NOWDB_IGNORE(nowdb_storage_stop(strg));
		nowdb_storage_destroy(strg); free(strg);
		return FALSE;
	}

	err = nowdb_store_open(&store);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		NOWDB_IGNORE(nowdb_storage_stop(strg));
		nowdb_storage_destroy(strg); free(strg);
		return FALSE;
	}

	e.origin = 1;
	e.destin = 2;
	e.reg1   = 5;
	e.reg2   = 42;
	e.reg3   = 0;
	e.reg4   = 0;
	e.reg5   = 0;

	rc = nowdb_time_now(&e.stamp);
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

	e.reg2++;
	err = nowdb_store_insert(&store, &e);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}

	e.reg2++;
	err = nowdb_store_insert(&store, &e);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}

	memcpy(&k, store.writer->mptr, RECSIZE);
	if (k.reg2 != e.reg2-2) {
		fprintf(stderr, "edges differ\n");
		return FALSE;
	}
	memcpy(&k, store.writer->mptr+RECSIZE, RECSIZE);
	if (k.reg2 != e.reg2-1) {
		fprintf(stderr, "edges differ\n");
		return FALSE;
	}
	memcpy(&k, store.writer->mptr+2*RECSIZE, RECSIZE);
	if (k.reg2 != e.reg2) {
		fprintf(stderr, "edges differ\n");
		return FALSE;
	}
	
	err = nowdb_storage_stop(strg);
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
	nowdb_storage_destroy(strg); free(strg);
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
