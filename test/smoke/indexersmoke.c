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

nowdb_index_keys_t *createKeys(char *ctx, uint16_t sz) {
	nowdb_err_t err;
	nowdb_index_keys_t *keys;

	if (ctx != NULL) {
		switch(sz) {
		case 1:
		err = nowdb_index_keys_create(&keys, 1,
		                       NOWDB_OFF_LABEL);
		break;
		case 2:
		err = nowdb_index_keys_create(&keys, 2,
		                        NOWDB_OFF_EDGE,
		                      NOWDB_OFF_DESTIN);
		break;
		case 3:
		err = nowdb_index_keys_create(&keys, 3,
		                        NOWDB_OFF_EDGE,
		                      NOWDB_OFF_ORIGIN,
		                      NOWDB_OFF_DESTIN);
		break;
		case 4:
		err = nowdb_index_keys_create(&keys, 4,
		                        NOWDB_OFF_EDGE,
		                      NOWDB_OFF_ORIGIN,
		                      NOWDB_OFF_DESTIN,
		                      NOWDB_OFF_LABEL);
		break;
		default: 
			fprintf(stderr, "too many offsets\n");
			return 0;
		}
	} else {
		err = nowdb_index_keys_create(&keys, 2,
		                      NOWDB_OFF_VERTEX,
		                       NOWDB_OFF_PROP);
	}
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	return keys;
}

int createIndex(nowdb_scope_t *scope,
                char        *idxname,
                char        *ctxname,
                uint16_t     sz) 
{
	nowdb_err_t err;
	nowdb_index_keys_t *keys;

	if (ctxname != NULL) {
		fprintf(stderr, "creating index %s.%s\n", ctxname, idxname);
	} else {
		fprintf(stderr, "creating index VERTEX.%s\n", idxname);
	}

	keys = createKeys(ctxname, sz);
	if (keys == NULL) return 0;
	err = nowdb_scope_createIndex(scope, idxname, ctxname, keys,
	                                    NOWDB_CONFIG_SIZE_TINY);
	if (err != NOWDB_OK) {
		nowdb_index_keys_destroy(keys);
		nowdb_err_print(err);
		nowdb_err_release(err);
		return 0;
	}
	nowdb_index_keys_destroy(keys);
	return 1;
}

int dropIndex(nowdb_scope_t *scope,
              char        *idxname) {
	nowdb_err_t err;

	err = nowdb_scope_dropIndex(scope, idxname);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return 0;
	}
	return 1;
}

int testGetIdx(nowdb_scope_t  *scope,
               char *name, char *ctx,
               uint16_t keys)
{
	nowdb_err_t       err;
	nowdb_index_t    *idx;
	nowdb_index_keys_t *k;

	k = createKeys(ctx, keys);
	if (k == NULL) return 0;

	err = nowdb_scope_getIndex(scope, ctx, k, &idx);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_index_keys_destroy(k);
		return 0;
	}
	nowdb_index_keys_destroy(k);

	err = nowdb_index_enduse(idx);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return 0;
	}

	err = nowdb_scope_getIndexByName(scope, name, &idx);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return 0;
	}

	err = nowdb_index_enduse(idx);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return 0;
	}
	return 1;
}

int main() {
	int rc = EXIT_SUCCESS;
	nowdb_scope_t *scope = NULL;

	if (!nowdb_init()) {
		fprintf(stderr, "cannot init environment\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

	/* destroy scope if exists */
	scope = mkScope("rsc/scope10");
	if (scope == NULL) {
		fprintf(stderr, "mkScope failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	dropScope(scope);
	nowdb_scope_destroy(scope); free(scope); scope=NULL;

	scope = createScope("rsc/scope10");
	if (scope == NULL) {
		fprintf(stderr, "cannot create scope\n");
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
	if (!createIndex(scope, "IDX_ONE", "CTX_ONE",1)) {
		fprintf(stderr, "createIndex failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!createIndex(scope, "IDX_TWO", "CTX_ONE",2)) {
		fprintf(stderr, "createIndex failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!createIndex(scope, "IDX_VIER", NULL,2)) {
		fprintf(stderr, "createIndex failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testGetIdx(scope, "IDX_ONE", "CTX_ONE", 1)) {
		fprintf(stderr, "testGetIdx failed for IDX_ONE\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	/*
	if (!testGetIdx(scope, "IDX_TWO", "CTX_ONE", 2)) {
		fprintf(stderr, "testGetIdx failed for IDX_TWO\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	*/
	if (!testGetIdx(scope, "IDX_VIER", NULL, 2)) {
		fprintf(stderr, "createIndex failed for IDX_VIER\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

cleanup:
	if (scope != NULL) {
		NOWDB_IGNORE(nowdb_scope_close(scope));
		nowdb_scope_destroy(scope); free(scope);
	}
	nowdb_close();
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}
