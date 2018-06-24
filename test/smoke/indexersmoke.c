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

#define ONE    16384
#define DELAY  10000000

nowdb_bool_t insertEdges(nowdb_store_t *store, uint32_t count) {
	nowdb_err_t err;
	nowdb_edge_t e;

	for(uint32_t i=0; i<count; i++) {

		memset(&e,0,64);

		do e.origin = rand()%100; while(e.origin == 0);
		do e.destin = rand()%100; while(e.destin == 0);
		do e.edge   = rand()%10; while(e.edge == 0);
		do e.label  = rand()%10; while(e.label == 0);
		err = nowdb_time_now(&e.timestamp);
		if (err != NOWDB_OK) {
			fprintf(stderr, "insert error\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
		e.weight = (uint64_t)i;
		e.weight2  = 0;
		e.wtype[0] = NOWDB_TYP_UINT;
		e.wtype[1] = NOWDB_TYP_NOTHING;

		err = nowdb_store_insert(store, &e);
		if (err != NOWDB_OK) {
			fprintf(stderr, "insert error\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
	}
	/*
	fprintf(stderr, "inserted %u (last: %lu)\n",
	                           count, e.weight);
	*/
	return TRUE;
}

nowdb_bool_t waitForSort(nowdb_store_t *store) {
	nowdb_err_t err;
	int max = 1000;
	int i;
	int len;

	for(i=0;i<max;i++) {
		err = nowdb_lock_read(&store->lock);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
		len = store->waiting.len;
		err = nowdb_unlock_read(&store->lock);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
		if (len == 0) break;
		err = nowdb_task_sleep(DELAY);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
	}
	if (i > max) {
		fprintf(stderr, "waited for too long\n");
		return FALSE;
	}
	return TRUE;
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

	cfg.allocsize = NOWDB_MEGA;
	cfg.largesize = NOWDB_MEGA;
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

nowdb_context_t *getContext(nowdb_scope_t *scope,
                            char        *ctxname) {
	nowdb_err_t      err;
	nowdb_context_t *ctx;

	err = nowdb_scope_getContext(scope, "CTX_ONE", &ctx);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	return ctx;
}

int main() {
	int rc = EXIT_SUCCESS;
	nowdb_scope_t *scope = NULL;
	nowdb_context_t *ctx;

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
	if (!testGetIdx(scope, "IDX_VIER", NULL, 2)) {
		fprintf(stderr, "createIndex failed for IDX_VIER\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	ctx = getContext(scope, "CTX_ONE");
	if (ctx == NULL) {
		fprintf(stderr, "cannot get context CTX_ONE\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!insertEdges(&ctx->store, ONE)) {
		fprintf(stderr, "cannot insert into CTX_ONE\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!waitForSort(&ctx->store)) {
		fprintf(stderr, "CTX_ONE does not get sorted\n");
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
