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

#define EDGE_OFF  25
#define LABEL_OFF 33
#define WEIGHT_OFF 41

/*
uint64_t origin;
uint64_t destin;
int64_t  timestamp;
char     byte; // control byte
uint64_t edge;
uint64_t label;
uint64_t weight;
*/

void setRandomValue(char *e, uint32_t off) {
	uint64_t x;
	do x = rand()%100; while(x == 0);
	memcpy(e+off, &x, 8);
}

nowdb_bool_t insertEdges(nowdb_store_t *store, uint32_t count) {
	nowdb_err_t err;
	char *e;
	uint32_t sz;
	int rc;

	fprintf(stderr, "inserting %u random edges\n", count);

	sz = nowdb_edge_recSize(1, 3);
	fprintf(stderr, "allocating %u bytes\n", sz);
	e = calloc(1, sz);
	if (e == NULL) {
		fprintf(stderr, "out-of-mem\n"); return 0;
	}
	for(uint32_t i=0; i<count; i++) {

		memset(e,0,sz);

		setRandomValue(e, NOWDB_OFF_ORIGIN);
		setRandomValue(e, NOWDB_OFF_DESTIN);
		setRandomValue(e, EDGE_OFF);
		setRandomValue(e, LABEL_OFF);
		rc = nowdb_time_now((nowdb_time_t*)(e+NOWDB_OFF_STAMP));
		if (rc != 0) {
			fprintf(stderr, "time error: %d\n", rc);
			return FALSE;
		}

		uint64_t w = (uint64_t)i;
		memcpy(e+WEIGHT_OFF, &w, sizeof(uint64_t));

		err = nowdb_store_insert(store, e);
		if (err != NOWDB_OK) {
			fprintf(stderr, "insert error\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
	}
	free(e);
	fprintf(stderr, "inserted...\n");
	return TRUE;
}

nowdb_bool_t waitForSort(nowdb_store_t *store) {
	nowdb_err_t err;
	int max = 1000;
	int i;
	int len;

	fprintf(stderr, "waiting...\n");
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

#define PROPSDESTROY() \
	nowdb_model_prop_t *p; \
	ts_algo_list_node_t *run; \
	for(run=props.head;run!=NULL;run=run->nxt){ \
		p=run->cont; \
		free(p->name); free(p); \
	} \
	ts_algo_list_destroy(&props);

int createType(nowdb_scope_t *scope, char *name) {
	nowdb_err_t err;
	ts_algo_list_t props;
	nowdb_model_prop_t *p;

	fprintf(stderr, "creating %s\n", name);

	ts_algo_list_init(&props);

	p = calloc(1, sizeof(nowdb_model_prop_t));
	if (p == NULL) return 0;

	p->name = strdup("id");
	p->pk = 1;
	p->value = NOWDB_TYP_UINT;

	if (p->name == NULL) {
		free(p);
		fprintf(stderr, "not enough memory\n");
		return 0;
	}
	if (ts_algo_list_append(&props, p) != TS_ALGO_OK) {
		fprintf(stderr, "cannot add to list\n");
		free(p);
		return 0;
	}

	p = calloc(1, sizeof(nowdb_model_prop_t));
	if (p == NULL) return 0;

	p->name = strdup("name");
	p->pk = 0;
	p->value = NOWDB_TYP_TEXT;

	if (p->name == NULL) {
		free(p);
		fprintf(stderr, "not enough memory\n");
		return 0;
	}
	if (ts_algo_list_append(&props, p) != TS_ALGO_OK) {
		fprintf(stderr, "cannot add to list\n");
		free(p); PROPSDESTROY();
		return 0;
	}
	err = nowdb_scope_createType(scope, name, &props);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot add type: ");
		nowdb_err_print(err); nowdb_err_release(err);
		ts_algo_list_destroy(&props);
		return 0;
	}
	ts_algo_list_destroy(&props);
	return 1;
}

#define PEDGEDESTROY() \
	nowdb_model_pedge_t *p; \
	ts_algo_list_node_t *run; \
	for(run=props.head;run!=NULL;run=run->nxt){ \
		p=run->cont; \
		free(p->name); free(p); \
	} \

int createEdge(nowdb_scope_t *scope, char *name) {
	nowdb_err_t err;
	nowdb_model_pedge_t *p;
	ts_algo_list_t props;

	if (!createType(scope, "client")) {
		fprintf(stderr, "create type failed\n");
		return 0;
	}
	if (!createType(scope, "product")) {
		fprintf(stderr, "create type failed\n");
		return 0;
	}

	fprintf(stderr, "creating edge %s\n", name);

	ts_algo_list_init(&props);

	p = calloc(1, sizeof(nowdb_model_pedge_t));
	if (p == NULL) {
		fprintf(stderr, "not enough memory\n");
		return 0;
	}
	p->name = strdup("edge");
	if (p->name == NULL) {
		free(p);
		fprintf(stderr, "not enough memory\n");
		return 0;
	}
	p->off = 25;
	p->value = NOWDB_TYP_UINT;

	err = nowdb_text_insert(scope->text,
		        p->name, &p->propid);
	if (err != NOWDB_OK) {
		free(p->name); free(p);
		fprintf(stderr, "cannot add text\n");
		nowdb_err_print(err); nowdb_err_release(err);
		return 0;
	}

	if (ts_algo_list_append(&props, p) != TS_ALGO_OK) {
		free(p->name); free(p);
		fprintf(stderr, "cannot append to list\n");
		return 0;
	}

	p = calloc(1, sizeof(nowdb_model_pedge_t));
	if (p == NULL) {
		PEDGEDESTROY();
		fprintf(stderr, "not enough memory\n");
		return 0;
	}
	p->name = strdup("label");
	if (p->name == NULL) {
		free(p->name); free(p); PEDGEDESTROY();
		fprintf(stderr, "not enough memory\n");
		return 0;
	}
	p->off = 33;
	p->value = NOWDB_TYP_UINT;

	err = nowdb_text_insert(scope->text,
		        p->name, &p->propid);
	if (err != NOWDB_OK) {
		free(p->name); free(p);
		fprintf(stderr, "cannot add text\n");
		nowdb_err_print(err); nowdb_err_release(err);
		return 0;
	}

	if (ts_algo_list_append(&props, p) != TS_ALGO_OK) {
		free(p->name); free(p); PEDGEDESTROY();
		fprintf(stderr, "cannot append to list\n");
		return 0;
	}

	p = calloc(1, sizeof(nowdb_model_pedge_t));
	if (p == NULL) {
		PEDGEDESTROY();
		fprintf(stderr, "not enough memory\n");
		return 0;
	}
	p->name = strdup("weight");
	if (p->name == NULL) {
		free(p->name); free(p); PEDGEDESTROY();
		fprintf(stderr, "not enough memory\n");
		return 0;
	}
	p->off = 41;
	p->value = NOWDB_TYP_UINT;

	err = nowdb_text_insert(scope->text,
		        p->name, &p->propid);
	if (err != NOWDB_OK) {
		free(p->name); free(p);
		fprintf(stderr, "cannot add text\n");
		nowdb_err_print(err); nowdb_err_release(err);
		return 0;
	}

	if (ts_algo_list_append(&props, p) != TS_ALGO_OK) {
		free(p->name); free(p); PEDGEDESTROY();
		fprintf(stderr, "cannot append to list\n");
		return 0;
	}

	err = nowdb_scope_createEdge(scope, name, "client", "product", &props);
	if (err != NOWDB_OK) {
		fprintf(stderr, "create edge failed\n");
		nowdb_err_print(err); nowdb_err_release(err);
		// ts_algo_list_destroy(&props);
		// PEDGEDESTROY();
		return 0;
	}
	ts_algo_list_destroy(&props);
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
		                            LABEL_OFF);
		break;
		case 2:
		err = nowdb_index_keys_create(&keys, 2,
		                              EDGE_OFF,
		                      NOWDB_OFF_DESTIN);
		break;
		case 3:
		err = nowdb_index_keys_create(&keys, 3,
		                              EDGE_OFF,
		                      NOWDB_OFF_ORIGIN,
		                      NOWDB_OFF_DESTIN);
		break;
		case 4:
		err = nowdb_index_keys_create(&keys, 4,
		                              EDGE_OFF,
		                      NOWDB_OFF_ORIGIN,
		                      NOWDB_OFF_DESTIN,
		                            LABEL_OFF);
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

	err = nowdb_scope_getContext(scope, "MYEDGE", &ctx);
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
	if (!createEdge(scope, "MYEDGE")) {
		fprintf(stderr, "cannot create edge one\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!createContext(scope, "MYEDGE")) {
		fprintf(stderr, "cannot create context one\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	/*
	if (!createIndex(scope, "IDX_ONE", "MYEDGE",1)) {
		fprintf(stderr, "createIndex failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!createIndex(scope, "IDX_TWO", "MYEDGE",2)) {
		fprintf(stderr, "createIndex failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!createIndex(scope, "IDX_VIER", NULL,2)) {
		fprintf(stderr, "createIndex failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testGetIdx(scope, "IDX_ONE", "MYEDGE", 1)) {
		fprintf(stderr, "testGetIdx failed for IDX_ONE\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testGetIdx(scope, "IDX_VIER", NULL, 2)) {
		fprintf(stderr, "createIndex failed for IDX_VIER\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	*/
	ctx = getContext(scope, "MYEDGE");
	if (ctx == NULL) {
		fprintf(stderr, "cannot get context MYEDGE\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!insertEdges(&ctx->store, ONE)) {
		fprintf(stderr, "cannot insert into MYEDGE\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!insertEdges(&ctx->store, 2*ONE)) {
		fprintf(stderr, "cannot insert into MYEDGE\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!waitForSort(&ctx->store)) {
		fprintf(stderr, "MYEDGE does not get sorted\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!closeScope(scope)) {
		fprintf(stderr, "cannot close scope\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!openScope(scope)) {
		fprintf(stderr, "cannot close scope\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	ctx = getContext(scope, "MYEDGE");
	if (ctx == NULL) {
		fprintf(stderr, "cannot get context MYEDGE\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!insertEdges(&ctx->store, 3*ONE)) {
		fprintf(stderr, "cannot insert into MYEDGE\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!waitForSort(&ctx->store)) {
		fprintf(stderr, "MYEDGE does not get sorted\n");
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
