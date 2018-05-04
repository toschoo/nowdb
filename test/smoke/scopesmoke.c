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

	fprintf(stderr, "creating %s\n", path);

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
	fprintf(stderr, "dropping %s\n", scope->path);
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

	fprintf(stderr, "creating %s\n", path);

	err = nowdb_scope_create(scope);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create scope\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_scope_destroy(scope); free(scope);
		return 0;
	}
	fprintf(stderr, "dropping %s\n", path);
	err = nowdb_scope_drop(scope);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot drop scope\n");
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

	fprintf(stderr, "opening %s\n", scope->path);
	err = nowdb_scope_open(scope);
	if (err != NOWDB_OK) {
		fprintf(stderr, "could not open scope\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return 0;
	}
	fprintf(stderr, "closing %s\n", scope->path);
	err = nowdb_scope_close(scope);
	if (err != NOWDB_OK) {
		fprintf(stderr, "could not close scope\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return 0;
	}
	return 1;
}

int openScope(nowdb_scope_t *scope) {
	nowdb_err_t err;

	fprintf(stderr, "opening %s\n", scope->path);
	err = nowdb_scope_open(scope);
	if (err != NOWDB_OK) {
		nowdb_err_print(err); nowdb_err_release(err);
		return 0;
	}
	return 1;
}

int closeScope(nowdb_scope_t *scope) {
	nowdb_err_t err;

	fprintf(stderr, "closing %s\n", scope->path);
	err = nowdb_scope_close(scope);
	if (err != NOWDB_OK) {
		nowdb_err_print(err); nowdb_err_release(err);
		return 0;
	}
	return 1;
}

int createContext(nowdb_scope_t *scope, char *name) {
	nowdb_err_t err;
	nowdb_ctx_config_t cfg;

	cfg.allocsize = 1;
	cfg.largesize = 1;
	cfg.sorters   = 1;
	cfg.sort      = 1;
	cfg.comp      = NOWDB_COMP_ZSTD;
	cfg.encp      = NOWDB_ENCP_NONE;

	fprintf(stderr, "creating context %s\n", name);
	err = nowdb_scope_createContext(scope, name, &cfg);
	if (err != NOWDB_OK) {
		fprintf(stderr, "create context failed\n");
		nowdb_err_print(err); nowdb_err_release(err);
		return 0;
	}
	return 1;
}

int dropContext(nowdb_scope_t *scope, char *name) {
	nowdb_err_t err;

	fprintf(stderr, "dropping context %s\n", name);
	err = nowdb_scope_dropContext(scope, name);
	if (err != NOWDB_OK) {
		fprintf(stderr, "drop context failed\n");
		nowdb_err_print(err); nowdb_err_release(err);
		return 0;
	}
	return 1;
}

int testCreateDropContext(nowdb_scope_t *scope) {
	if (!openScope(scope)) return 0;
	if (!createContext(scope, "testctx")) return 0;
	if (!dropContext(scope, "testctx")) return 0;
	if (!closeScope(scope)) return 0;
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
	nowdb_scope_destroy(scope); free(scope); scope = NULL;

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
	if (!testCreateDropContext(scope)) {
		fprintf(stderr, "testCreateDropContext failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!openScope(scope)) {
		fprintf(stderr, "cannot open scope\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!createContext(scope, "CTX_ONE")) {
		fprintf(stderr, "cannot create context one\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!createContext(scope, "CTX_TWO")) {
		fprintf(stderr, "cannot create context two\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!closeScope(scope)) {
		fprintf(stderr, "cannot close scope\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!openScope(scope)) {
		fprintf(stderr, "cannot open scope\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!createContext(scope, "CTX_FOUR")) {
		fprintf(stderr, "cannot create context four\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!createContext(scope, "CTX_THREE")) {
		fprintf(stderr, "cannot create context three\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!dropContext(scope, "CTX_FOUR")) {
		fprintf(stderr, "cannot drop context\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!closeScope(scope)) {
		fprintf(stderr, "cannot close scope\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!openScope(scope)) {
		fprintf(stderr, "cannot open scope\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (createContext(scope, "CTX_TWO")) {
		fprintf(stderr, "duplicated context two\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!createContext(scope, "CTX_FIVE")) {
		fprintf(stderr, "cannot create context five\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!closeScope(scope)) {
		fprintf(stderr, "cannot close scope\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!openScope(scope)) {
		fprintf(stderr, "cannot open scope\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (scope->contexts.count != 4) {
		fprintf(stderr, "there should be 4 contexts, ");
		fprintf(stderr, "but there are %d\n",
		                   scope->contexts.count);
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
		NOWDB_IGNORE(nowdb_scope_close(scope));
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
