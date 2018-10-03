/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Scope DML services
 * ========================================================================
 */
#include <nowdb/scope/dml.h>

static char *OBJECT = "dml";

#define INVALID(m) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, m);

#define NOMEM(m) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, m);

nowdb_err_t nowdb_dml_init(nowdb_dml_t   *dml, 
                           nowdb_scope_t *scope,
                           char       withCache) {
	nowdb_err_t err;

	if (dml ==NULL) INVALID("dml is NULL");
	if (scope==NULL) INVALID("scope is NULL");

	dml->trgname = NULL;
	dml->scope = scope;
	dml->store = NULL;
	dml->tlru  = NULL;
	dml->v = NULL;
	dml->p = NULL;
	dml->e = NULL;
	dml->o = NULL;
	dml->d = NULL;

	if (withCache) {
		dml->tlru = calloc(1,sizeof(nowdb_ptlru_t));
		if (dml->tlru == NULL) {
			NOMEM("allocating lru cache");
			return err;
		}
		err = nowdb_ptlru_init(dml->tlru, 1000000);
		if (err != NOWDB_OK) {
			free(dml->tlru); dml->tlru = NULL;
			return err;
		}
	}
	return NOWDB_OK;
}

void nowdb_dml_destroy(nowdb_dml_t *dml) {
	if (dml == NULL) return;

	if (dml->trgname != NULL) {
		free(dml->trgname); dml->trgname = NULL;
	}
	if (dml->tlru != NULL) {
		nowdb_ptlru_destroy(dml->tlru);
		free(dml->tlru ); dml->tlru = NULL;
	}
	if (dml->p != NULL) {
		free(dml->p); dml->p = NULL;
	}
}

static nowdb_err_t getContextOrVertex(nowdb_dml_t *dml,
                                      char    *trgname) {
	nowdb_err_t err1, err2;
	nowdb_context_t *ctx;

	err1 = nowdb_scope_getContext(dml->scope, trgname, &ctx);
	if (err1 == NOWDB_OK) {
		dml->target = NOWDB_TARGET_EDGE;
		dml->store = &ctx->store;
		return NOWDB_OK;
	}

	if (!nowdb_err_contains(err1, nowdb_err_key_not_found)) return err1;

	err2 = nowdb_model_getVertexByName(dml->scope->model,trgname,&dml->v);
	if (err2 != NOWDB_OK) {
		err2->cause = err1; return err2;
	}
	dml->target = NOWDB_TARGET_VERTEX;
	dml->store = &dml->scope->vertices;

	return NOWDB_OK;
}

nowdb_err_t nowdb_dml_setTarget(nowdb_dml_t *dml,
                                char    *trgname,
                          ts_algo_list_t *fields,
                          ts_algo_list_t *values) {
	nowdb_err_t err;

	if (dml == NULL) INVALID("no dml descriptor");
	if (dml->scope == NULL) INVALID("no scope in dml descriptor");
	if (trgname == NULL) INVALID("no target name");

	// check fields == values
	if (fields != NULL && values != NULL) {
		if (fields->len != values->len) {
			INVALID("field list and value list differ");
		}
	}

	dml->trgname = strdup(trgname);
	if (dml->trgname == NULL) {
		NOMEM("allocating target name");
		return err;
	}

	// get context or vertex
	err = getContextOrVertex(dml, trgname);
	if (err != NOWDB_OK) return err;

	return NOWDB_OK;

}

nowdb_err_t nowdb_dml_insertRow(nowdb_dml_t *dml,
                                char        *row,
                                uint32_t     sz);

static inline nowdb_err_t insertEdgeFields(nowdb_dml_t *dml,
                                     ts_algo_list_t *fields,
                                     ts_algo_list_t *values) 
{
	ts_algo_list_node_t *frun, *vrun;

	vrun = values->head;
	for(frun=fields->head; frun!=NULL; frun=frun->nxt) {
		// get offset from name

		// check and correct value type

		// write to edge (get edge model like in row)
		
		vrun = vrun->nxt;
	}
	// insert into store
	return NOWDB_OK;
}

static inline nowdb_err_t insertVertexFields(nowdb_dml_t *dml,
                                       ts_algo_list_t *fields,
                                       ts_algo_list_t *values) {
	return NOWDB_OK;
}

nowdb_err_t nowdb_dml_insertFields(nowdb_dml_t *dml,
                             ts_algo_list_t *fields,
                             ts_algo_list_t *values) 
{
	if (dml->target == NOWDB_TARGET_EDGE) {
		return insertEdgeFields(dml, fields, values);
	} else {
		return insertVertexFields(dml, fields, values);
	}
}
