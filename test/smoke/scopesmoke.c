/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for scope
 * ========================================================================
 */
#include <nowdb/io/file.h>
#include <nowdb/scope/scope.h>

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

int testNewDestroy() {
	nowdb_err_t err;
	nowdb_scope_t *scope;

	err = nowdb_scope_new(&scope, "rsc/testscope", 1);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot allocate scope\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return 0;
	}
	nowdb_scope_destroy(scope); free(scope);
	return 1;
}

int testInitDestroy() {
	nowdb_err_t err;
	nowdb_scope_t scope;

	err = nowdb_scope_init(&scope, "rsc/testscope", 1);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot init scope\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return 0;
	}
	nowdb_scope_destroy(&scope);
	return 1;
}

nowdb_scope_t *mkScope(nowdb_path_t path) {
	nowdb_err_t err;
	nowdb_scope_t *scope;

	err = nowdb_scope_new(&scope, path, 1);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot allocate scope\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	return scope;
}

nowdb_scope_t *createScope(nowdb_path_t path) {
	nowdb_err_t err;
	nowdb_scope_t *scope;

	scope = mkScope(path);
	if (scope == NULL) return NULL;

	err = nowdb_scope_create(scope);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_scope_destroy(scope); free(scope);
		return NULL;
	}
	return scope;
}

int dropScope(nowdb_scope_t *scope) {
	nowdb_err_t err;
	err = nowdb_scope_drop(scope);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return 0;
	}
	return 1;
}

int testCreateDrop(nowdb_path_t path) {
	nowdb_err_t err;
	nowdb_scope_t *scope;

	scope = mkScope(path);
	if (scope == NULL) return 0;

	err = nowdb_scope_create(scope);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_scope_destroy(scope); free(scope);
		return 0;
	}
	err = nowdb_scope_drop(scope);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_scope_destroy(scope); free(scope);
		return 0;
	}
	nowdb_scope_destroy(scope); free(scope);
	return 1;
}

int testOpenClose(nowdb_scope_t *scope) {
	nowdb_err_t err;

	err = nowdb_scope_open(scope);
	if (err != NOWDB_OK) {
		fprintf(stderr, "could not open scope\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return 0;
	}
	err = nowdb_scope_close(scope);
	if (err != NOWDB_OK) {
		fprintf(stderr, "could not close scope\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return 0;
	}
	return 1;
}

int main() {
	int rc = EXIT_SUCCESS;
	nowdb_scope_t *scope = NULL;

	if (!nowdb_err_init()) {
		fprintf(stderr, "cannot init environment\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testNewDestroy()) {
		fprintf(stderr, "testNewDestroy failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testInitDestroy()) {
		fprintf(stderr, "testInitDestroy failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	scope = mkScope("rsc/scope1");
	if (scope == NULL) {
		fprintf(stderr, "mkScope failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	NOWDB_IGNORE(nowdb_scope_drop(scope));
	nowdb_scope_destroy(scope); free(scope);

	if (!testCreateDrop("rsc/scope1")) {
		fprintf(stderr, "testCreateDrop failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	scope = createScope("rsc/scope1");
	if (scope == NULL) {
		fprintf(stderr, "createScope failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testOpenClose(scope)) {
		fprintf(stderr, "testOpenClose failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

cleanup:
	if (scope != NULL) {
		/*
		if (!dropScope(scope)) {
			fprintf(stderr, "dropScope failed\n");
			rc = EXIT_FAILURE;
		}
		*/
		nowdb_scope_destroy(scope); free(scope);
	}
	nowdb_err_destroy();
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}
