/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for scope
 * ========================================================================
 */
#include <nowdb/io/file.h>
#include <nowdb/scope/scope.h>
#include <nowdb/model/model.h>

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

#define EDGE_OFF  25
#define LABEL_OFF 33
#define WEIGHT_OFF 41

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
	p->pos = 0;
	p->stamp = 0;
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
	p->pos = 1;
	p->stamp = 0;
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

	fprintf(stderr, "creating edge %s\n", name);

	ts_algo_list_init(&props);

	p = calloc(1, sizeof(nowdb_model_pedge_t));
	if (p == NULL) {
		fprintf(stderr, "not enough memory\n");
		return 0;
	}
	p->name = strdup("origin");
	if (p->name == NULL) {
		free(p);
		fprintf(stderr, "not enough memory\n");
		return 0;
	}
	p->origin = 1;
	p->pos = 0;
	p->off = 0;
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
		fprintf(stderr, "not enough memory\n");
		return 0;
	}
	p->name = strdup("destin");
	if (p->name == NULL) {
		free(p);
		fprintf(stderr, "not enough memory\n");
		return 0;
	}
	p->destin = 1;
	p->pos = 1;
	p->off = 8;
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
		fprintf(stderr, "not enough memory\n");
		return 0;
	}
	p->name = strdup("stamp");
	if (p->name == NULL) {
		free(p);
		fprintf(stderr, "not enough memory\n");
		return 0;
	}
	p->stamp = 1;
	p->pos = 2;
	p->off = 16;
	p->value = NOWDB_TYP_TIME;

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
		fprintf(stderr, "not enough memory\n");
		return 0;
	}
	p->name = strdup("edge");
	if (p->name == NULL) {
		free(p);
		fprintf(stderr, "not enough memory\n");
		return 0;
	}
	p->pos = 3;
	p->off = 24;
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
	p->pos = 4;
	p->off = 32;
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
	p->pos = 5;
	p->off = 40;
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

	fprintf(stderr, "CREATING EDGE %s\n", name);
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

int createContext(nowdb_scope_t *scope, char *name) {
	nowdb_err_t err;

	fprintf(stderr, "creating context %s\n", name);
	err = nowdb_scope_createContext(scope, name, NULL);
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
	if (!createEdge(scope, "testctx")) return 0;
	if (!createContext(scope, "testctx")) return 0;
	if (!dropContext(scope, "testctx")) return 0;
	// if (!dropEdge(scope, "testctx")) return 0;
	if (!closeScope(scope)) return 0;
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

	fprintf(stderr, "creating index %s.%s\n", ctxname, idxname);

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

int testCreateDropIndex(nowdb_scope_t *scope) {
	if (!createIndex(scope, "IDX_TEST0", "CTX_FIVE",2)) return 0;
	if (!dropIndex(scope, "IDX_TEST0")) return 0;
	return 1;
}

int main() {
	int rc = EXIT_SUCCESS;
	nowdb_scope_t *scope = NULL;

	if (!nowdb_init()) {
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

	fprintf(stderr, "trying to drop scope\n");
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
	if (!openScope(scope)) {
		fprintf(stderr, "cannot open scope\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!createType(scope, "client")) {
		fprintf(stderr, "create type failed\n");
		return 0;
	}
	if (!createType(scope, "product")) {
		fprintf(stderr, "create type failed\n");
		return 0;
	}
	if (!closeScope(scope)) {
		fprintf(stderr, "cannot close scope\n");
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
	if (!createEdge(scope, "CTX_ONE")) {
		fprintf(stderr, "cannot create context one\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!createContext(scope, "CTX_ONE")) {
		fprintf(stderr, "cannot create context one\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!createEdge(scope, "CTX_TWO")) {
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
	if (!createEdge(scope, "CTX_FOUR")) {
		fprintf(stderr, "cannot create edge four\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!createContext(scope, "CTX_FOUR")) {
		fprintf(stderr, "cannot create context four\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!createEdge(scope, "CTX_THREE")) {
		fprintf(stderr, "cannot create edge four\n");
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
	/*
	if (!dropEdge(scope, "CTX_FOUR")) {
		fprintf(stderr, "cannot drop edge\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	*/
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
	if (!createEdge(scope, "CTX_FIVE")) {
		fprintf(stderr, "cannot create context five\n");
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
	if (!testCreateDropIndex(scope)) {
		fprintf(stderr, "testCreateDropIndex failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!createIndex(scope, "IDX_ONE", "CTX_FIVE",1)) {
		fprintf(stderr, "createIndex failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!createIndex(scope, "IDX_TWO", "CTX_FIVE",2)) {
		fprintf(stderr, "createIndex failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!createIndex(scope, "IDX_THREE", "CTX_FIVE",3)) {
		fprintf(stderr, "createIndex failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!createIndex(scope, "IDX_VIER", "CTX_THREE",2)) {
		fprintf(stderr, "createIndex failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testGetIdx(scope, "IDX_ONE", "CTX_FIVE", 2)) {
		fprintf(stderr, "testGetIdx failed for IDX_ONE\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testGetIdx(scope, "IDX_TWO", "CTX_FIVE", 2)) {
		fprintf(stderr, "testGetIdx failed for IDX_TWO\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testGetIdx(scope, "IDX_THREE", "CTX_FIVE", 3)) {
		fprintf(stderr, "testGetIdx failed for IDX_THREE\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testGetIdx(scope, "IDX_VIER", "CTX_THREE", 2)) {
		fprintf(stderr, "createIndex failed for IDX_VIER\n");
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
	if (createIndex(scope, "IDX_THREE", "CTX_FIVE",4)) {
		fprintf(stderr, "createIndex succeeded for index name\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (createIndex(scope, "IDX_NEU", "CTX_FIVE",2)) {
		fprintf(stderr, "createIndex succeeded for keys\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testGetIdx(scope, "IDX_TWO", "CTX_FIVE", 2)) {
		fprintf(stderr, "testGetIdx failed for IDX_TWO\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testGetIdx(scope, "IDX_THREE", "CTX_FIVE", 3)) {
		fprintf(stderr, "testGetIdx failed for IDX_THREE\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testGetIdx(scope, "IDX_VIER", "CTX_THREE", 2)) {
		fprintf(stderr, "createIndex failed for IDX_VIER\n");
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
	nowdb_close();
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}
