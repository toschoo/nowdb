/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Statement Interface
 * ========================================================================
 */
#include <nowdb/query/stmt.h>
#include <nowdb/qplan/plan.h>
#include <nowdb/query/cursor.h>
#include <nowdb/index/index.h>
#include <nowdb/ifc/nowdb.h>
#include <nowdb/scope/dml.h>
#include <nowdb/nowproc.h>

/* -------------------------------------------------------------------------
 * Macro for the very common error "invalid ast"
 * -------------------------------------------------------------------------
 */
#define INVALIDAST(s) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, s)

/* -------------------------------------------------------------------------
 * Macro for the very common error "no mem"
 * -------------------------------------------------------------------------
 */
#define NOMEM(s) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, s)

/* -------------------------------------------------------------------------
 * Macro for the very common error "invalid argument"
 * -------------------------------------------------------------------------
 */
#define INVALID(s) \
	err = nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, s)

/* -------------------------------------------------------------------------
 * Macro for the very common error "python error"
 * -------------------------------------------------------------------------
 */
#define PYTHONERR(s) \
	err = nowdb_err_get(nowdb_err_python, FALSE, OBJECT, s)

/* -------------------------------------------------------------------------
 * Macro for the very common error "lua error"
 * -------------------------------------------------------------------------
 */
#define LUAERR(s) \
	err = nowdb_err_get(nowdb_err_lua, FALSE, OBJECT, s)

static char *OBJECT = "stmt";

/* -------------------------------------------------------------------------
 * Predeclaration
 * -------------------------------------------------------------------------
 */
void nowdb_dbresult_result(nowdb_dbresult_t p, nowdb_qry_result_t *q);

nowdb_err_t nowdb_dbresult_err(nowdb_dbresult_t res);

/* -------------------------------------------------------------------------
 * Create a scope
 * -------------------------------------------------------------------------
 */
static nowdb_err_t createScope(nowdb_ast_t  *op,
                               nowdb_path_t  base,
                               char         *name) {
	nowdb_ast_t *o;
	nowdb_path_t p;
	nowdb_scope_t *scope;
	nowdb_err_t err;

	p = nowdb_path_append(base, name);
	if (p == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                        FALSE, OBJECT, "path.append");

	o = nowdb_ast_option(op, NOWDB_AST_IFEXISTS);
	if (o != NULL) {
		if (nowdb_path_exists(p, NOWDB_DIR_TYPE_DIR)) {
			free(p); return NOWDB_OK;
		}
	}

	err = nowdb_scope_new(&scope, p, NOWDB_DB_VERSION); free(p);
	if (err != NOWDB_OK) return err;
	
	err = nowdb_scope_create(scope);
	if (err != NOWDB_OK) {
		nowdb_scope_destroy(scope); free(scope);
		return err;
	}
	nowdb_scope_destroy(scope); free(scope);
	return NOWDB_OK;
}

/* -------------------------------------------------------------------------
 * Drop a scope
 * -------------------------------------------------------------------------
 */
static nowdb_err_t dropScope(nowdb_ast_t  *op,
                             void         *rsc,
                             nowdb_path_t  base,
                             char         *name) {
	nowdb_ast_t *o;
	nowdb_path_t p=NULL;
	nowdb_scope_t *scope=NULL;
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;
	nowdb_t *lib = nowdb_proc_getLib(rsc);

	p = nowdb_path_append(base, name);
	if (p == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                        FALSE, OBJECT, "path.append");

	o = nowdb_ast_option(op, NOWDB_AST_IFEXISTS);
	if (o != NULL) {
		if (!nowdb_path_exists(p, NOWDB_DIR_TYPE_DIR)) {
			free(p); return NOWDB_OK;
		}
	}

	if (lib == NULL) {
		err = nowdb_scope_new(&scope, p, NOWDB_DB_VERSION);free(p);
		if (err != NOWDB_OK) return err;

		err = nowdb_scope_drop(scope);
		if (err != NOWDB_OK) {
			nowdb_scope_destroy(scope); free(scope);
			return err;
		}

		nowdb_scope_destroy(scope); free(scope);

		return NOWDB_OK;
	}

	err = nowdb_lock_write(lib->lock);
	if (err != NOWDB_OK) {
		fprintf(stderr, "ERROR ON LOCK: ");
		nowdb_err_print(err);
		return err;
	}

	err = nowdb_getScope(lib, name, &scope);
	if (err != NOWDB_OK) goto unlock;

	if (scope == NULL) {
		err = nowdb_scope_new(&scope, p, NOWDB_DB_VERSION);
		if (err != NOWDB_OK) goto unlock;

		err = nowdb_scope_drop(scope);
		if (err != NOWDB_OK) goto unlock;

		nowdb_scope_destroy(scope); free(scope);
		goto unlock;
	}

	err = nowdb_scope_close(scope);
	if (err != NOWDB_OK) goto unlock;
	
	err = nowdb_scope_drop(scope);
	if (err != NOWDB_OK) goto unlock;

	nowdb_dropScope(lib, name);

unlock:
	if (p != NULL) free(p);
	if (lib != NULL) {
		err2 = nowdb_unlock_write(lib->lock);
		if (err2 != NOWDB_OK) {
			err2->cause = err;
			return err2;
		}
	}
	return err;
}

/* -------------------------------------------------------------------------
 * Open a scope and deliver it back to caller
 * -------------------------------------------------------------------------
 */
static nowdb_err_t openScope(nowdb_path_t     path,
                             nowdb_scope_t **scope) {
	nowdb_err_t err;

	err = nowdb_scope_new(scope, path, NOWDB_DB_VERSION);
	if (err != NOWDB_OK) return err;

	err = nowdb_scope_open(*scope);
	if (err != NOWDB_OK) {
		nowdb_scope_destroy(*scope); free(*scope);
		return err;
	}
	return NOWDB_OK;
}

/* -------------------------------------------------------------------------
 * Check whether index exists or not
 * TODO:
 * - we should use "nosuch index" instead of "key not found".
 * -------------------------------------------------------------------------
 */
static inline nowdb_err_t checkIndexExists(nowdb_scope_t *scope,
                                           char          *name,
                                           nowdb_bool_t  *b) {
	nowdb_err_t    err;
	nowdb_index_t *idx=NULL;

	err = nowdb_scope_getIndexByName(scope, name, &idx);
	if (err == NOWDB_OK) {
		*b = TRUE;
		if (idx != NULL) return nowdb_index_enduse(idx);
	}
	if (nowdb_err_contains(err, nowdb_err_key_not_found)) {
		nowdb_err_release(err);
		*b = FALSE; return NOWDB_OK;
	}
	return err;
}

/* -------------------------------------------------------------------------
 * Allocate keys
 * -------------------------------------------------------------------------
 */
nowdb_index_keys_t *mkKeys(int sz) {
	nowdb_index_keys_t *k;
	k = calloc(1,sizeof(nowdb_index_keys_t));
	if (k == NULL) return NULL;
	k->off = calloc(sz,sizeof(uint16_t));
	if (k->off == NULL) {
		free(k); return NULL;
	}
	k->sz = sz;
	return k;
}

/* -------------------------------------------------------------------------
 * Generic getKeys (from create index statement)
 * -------------------------------------------------------------------------
 */
static nowdb_err_t getKeys(nowdb_ast_t *fld, char what,
                       int cnt, nowdb_index_keys_t **k) {
	nowdb_err_t err;
	nowdb_ast_t *nxt;
	int tmp;

	nxt = nowdb_ast_field(fld);
	if (nxt == NULL) {
		*k = mkKeys(cnt+1);
		if (*k == NULL) return nowdb_err_get(nowdb_err_no_mem,
		                     FALSE, OBJECT, "allocating keys");
		tmp = what==0?nowdb_vertex_offByName(fld->value):
		              nowdb_edge_offByName(fld->value);
		if (tmp < 0) {
			nowdb_index_keys_destroy(*k); *k = NULL;
			INVALIDAST("invalid field");
		}
		(*k)->off[cnt] = (uint16_t)tmp;
		return NOWDB_OK;
	}
	err = getKeys(nxt, what, cnt+1, k);
	if (err != NOWDB_OK) return err;
	tmp = what==0?nowdb_vertex_offByName(fld->value):
	              nowdb_edge_offByName(fld->value);
	if (tmp < 0) {
		nowdb_index_keys_destroy(*k); *k = NULL;
		INVALIDAST("invalid field");
	}
	(*k)->off[cnt] = (uint16_t)tmp;
	return NOWDB_OK;
}

/* -------------------------------------------------------------------------
 * getKeys for edge
 * -------------------------------------------------------------------------
 */
static nowdb_err_t getEdgeKeys(nowdb_ast_t *fld, int cnt,
                                  nowdb_index_keys_t **k) {
	return getKeys(fld, 1, cnt, k);
}

/* -------------------------------------------------------------------------
 * getKeys for vertex
 * -------------------------------------------------------------------------
 */
static nowdb_err_t getVertexKeys(nowdb_ast_t *fld, int cnt,
                                    nowdb_index_keys_t **k) {
	return getKeys(fld, 0, cnt, k);
}

/* -------------------------------------------------------------------------
 * Create Index
 * -------------------------------------------------------------------------
 */
static nowdb_err_t createIndex(nowdb_ast_t  *op,
                               char       *name,
                           nowdb_scope_t *scope) {
	nowdb_err_t err;
	nowdb_ast_t *o;
	nowdb_ast_t *on;
	nowdb_ast_t *flds;
	nowdb_index_keys_t *k;
	nowdb_bool_t x;
	uint64_t utmp;
	uint16_t sz = NOWDB_CONFIG_SIZE_SMALL;

	on = nowdb_ast_on(op);
	if (on == NULL) {
		INVALIDAST("no 'on' clause in AST");
	}

	o = nowdb_ast_option(op, NOWDB_AST_IFEXISTS);
	if (o != NULL) {
		err = checkIndexExists(scope, name, &x);
		if (err != NOWDB_OK) return err;
		if (x) return NOWDB_OK;
	}

	o = nowdb_ast_option(op, NOWDB_AST_SIZING);
	if (o != NULL) {
		if (nowdb_ast_getUInt(o, &utmp) != 0) {
			INVALIDAST("invalid ast: invalid size");
		}
		sz = (uint16_t)utmp;
	}

	flds = nowdb_ast_field(op);
	if (flds == NULL) INVALIDAST("no fields in AST");
	
	if (on->stype == NOWDB_AST_CONTEXT) {
		err = getEdgeKeys(flds,0,&k);
	} else {
		err = getVertexKeys(flds,0,&k);
	}
	if (err != NOWDB_OK) return err;
	if (on->stype == NOWDB_AST_CONTEXT) {
		err = nowdb_scope_createIndex(scope, name, on->value, k, sz);
	} else {
		err = nowdb_scope_createIndex(scope, name, NULL, k, sz);
	}
	nowdb_index_keys_destroy(k);
	return err;
}

/* -------------------------------------------------------------------------
 * Drop Index
 * -------------------------------------------------------------------------
 */
static nowdb_err_t dropIndex(nowdb_ast_t  *op,
                             char       *name,
                         nowdb_scope_t *scope) 
{
	nowdb_err_t err;
	nowdb_ast_t  *o;

	err = nowdb_scope_dropIndex(scope, name);
	if (err == NOWDB_OK) return NOWDB_OK;
	if (nowdb_err_contains(err, nowdb_err_key_not_found)) {
		o = nowdb_ast_option(op, NOWDB_AST_IFEXISTS);
		if (o != NULL) {
			nowdb_err_release(err);
			err = NOWDB_OK;
		}
	}
	return err;
}

/* -------------------------------------------------------------------------
 * Check whether the context exists or not
 * -------------------------------------------------------------------------
 */
static inline nowdb_err_t checkContextExists(nowdb_scope_t *scope,
                                             char          *name,
                                             nowdb_bool_t  *b) {
	nowdb_err_t err;
	nowdb_context_t *ctx;

	err = nowdb_scope_getContext(scope, name, &ctx);
	if (err == NOWDB_OK) {
		*b = TRUE; return NOWDB_OK;
	}
	if (nowdb_err_contains(err, nowdb_err_key_not_found)) {
		nowdb_err_release(err);
		*b = FALSE; return NOWDB_OK;
	}
	return err;
}

/* -------------------------------------------------------------------------
 * Create table
 * -------------------------------------------------------------------------
 */
static nowdb_err_t createContext(nowdb_ast_t  *op,
                                 char       *name,
                             nowdb_scope_t *scope) {
	nowdb_err_t err;
	nowdb_ast_t *opts, *o;
	nowdb_ast_t *strg;
	char *strgname = NULL;

	/* get options and, if 'ifexists' is given,
	 * check if the context exists */
	opts = nowdb_ast_option(op, 0);
	if (opts != NULL) {
		o = nowdb_ast_option(opts, NOWDB_AST_IFEXISTS);
		if (o != NULL) {
			nowdb_bool_t x = FALSE;
			err = checkContextExists(scope, name, &x);
			if (err != NOWDB_OK) return err;
			if (x) return NOWDB_OK;
		}
	}

	strg = nowdb_ast_storage(op);
	if (strg != NULL) strgname = strg->value;

	/* create the context */
	return nowdb_scope_createContext(scope, name, strgname);
}

/* -------------------------------------------------------------------------
 * Helper: destroy list of properties
 * -------------------------------------------------------------------------
 */
static void destroyProps(ts_algo_list_t *props) {
	ts_algo_list_node_t *runner, *tmp;
	nowdb_model_prop_t *p;

	runner=props->head;
	while(runner!=NULL) {
		p = runner->cont;
		free(p->name); free(p);
		tmp = runner->nxt;
		ts_algo_list_remove(props, runner);
		free(runner); runner = tmp;
	}
}

/* -------------------------------------------------------------------------
 * Helper: destroy list of edge properties
 * -------------------------------------------------------------------------
 */
static void destroyPedges(ts_algo_list_t *props) {
	ts_algo_list_node_t *runner, *tmp;
	nowdb_model_pedge_t *p;

	runner=props->head;
	while(runner!=NULL) {
		p = runner->cont;
		free(p->name); free(p);
		tmp = runner->nxt;
		ts_algo_list_remove(props, runner);
		free(runner); runner = tmp;
	}
}

/* -------------------------------------------------------------------------
 * Create Type
 * -------------------------------------------------------------------------
 */
static nowdb_err_t createType(nowdb_ast_t  *op,
                              char       *name,
                          nowdb_scope_t *scope)  {
	nowdb_err_t err;
	nowdb_ast_t  *o;
	nowdb_ast_t  *d;
	ts_algo_list_t props;
	nowdb_model_prop_t *p;
	char x=0; /* do we have props ? */
	uint32_t i=0;

	d = nowdb_ast_declare(op);
	ts_algo_list_init(&props);

	while (d != NULL) {
		x = 1;
		if (d->vtype != NOWDB_AST_V_STRING) {
			destroyProps(&props);
			INVALIDAST("missing property name");
		}
		p = calloc(1,sizeof(nowdb_model_prop_t));
		if (p == NULL) {
			destroyProps(&props);
			NOMEM("allocating property");
			return err;
		}

		p->name  = strdup(d->value);
		if (p->name == NULL) {
			destroyProps(&props); free(p);
			NOMEM("allocating property name");
			return err;
		}

		p->value = nowdb_ast_type(d->stype);
		p->propid = 0;
		p->roleid = 0;
		p->pos    = i; i++;
		p->pk = FALSE;
		p->stamp = FALSE;

		o = nowdb_ast_option(d, NOWDB_AST_PK);
		if (o != NULL) p->pk = TRUE;

		o = nowdb_ast_option(d, NOWDB_AST_STAMP);
		if (o != NULL) p->stamp = TRUE;

		if (ts_algo_list_append(&props, p) != TS_ALGO_OK) {
			destroyProps(&props); free(p);
			NOMEM("list.addpend");
			return err;
		}

		d = nowdb_ast_declare(d);
	}

	err = x?nowdb_scope_createType(scope, name, &props):
	        nowdb_scope_createType(scope, name, NULL);
	
	if (nowdb_err_contains(err, nowdb_err_dup_key)) {
		destroyProps(&props);
		/* if exists applies only for the type name */
		if (err->info != NULL &&
		    strcmp(err->info, name) == 0) {
			o = nowdb_ast_option(op, NOWDB_AST_IFEXISTS);
			if (o != NULL) {
				nowdb_err_release(err);
				err = NOWDB_OK;
			} else {
				return err;
			}
		}
	}
	if (err != NOWDB_OK) {
		destroyProps(&props);
		return err;
	}
	ts_algo_list_destroy(&props);
	err = createContext(op, name, scope);
	if (err != NOWDB_OK) {
		nowdb_err_t err2;
		err2 = nowdb_model_removeType(scope->model, name);
		if (err2 != NOWDB_OK) {
			nowdb_err_cascade(err2, err);
			return err2;
		}
	}
	return NOWDB_OK;
}

/* -------------------------------------------------------------------------
 * Drop Type
 * -------------------------------------------------------------------------
 */
static nowdb_err_t dropType(nowdb_ast_t  *op,
                            char       *name,
                        nowdb_scope_t *scope,
                            void        *rsc) {
	nowdb_err_t err;
	nowdb_ast_t  *o;

        // don't reuse this target on dml
	// this is not a solution!
	// the target may have been used in another session 
	// that session will not know that the target
	// has been dropped or altered!
	nowdb_dml_t *dml = nowdb_proc_getDML(rsc);
	if (dml == NULL) {
		err = nowdb_err_get(nowdb_err_no_rsc, FALSE,
			      OBJECT, "no scope in session");
		return err;
	}
	if (dml->trgname != NULL &&
	    strcasecmp(dml->trgname, name) == 0) {
		free(dml->trgname);
		dml->trgname = NULL;
	}

	err = nowdb_scope_dropType(scope, name);
	if (err == NOWDB_OK) {
		err = nowdb_scope_dropContext(scope, name);
	}
	if (nowdb_err_contains(err, nowdb_err_key_not_found)) {
		o = nowdb_ast_option(op, NOWDB_AST_IFEXISTS);
		if (o != NULL) {
			nowdb_err_release(err);
			err = NOWDB_OK;
		}
	}
	return err;
}

/* -------------------------------------------------------------------------
 * Apply create options on config
 * NOTE: configs shall be handled on the level of tablespace!
 * -------------------------------------------------------------------------
 */
static nowdb_err_t applyCreateOptions(nowdb_ast_t *opts,
                            nowdb_storage_config_t *cfg) {
	nowdb_ast_t *o;
	uint64_t cfgopts = 0;
	uint64_t utmp;

	/* with no options given: use defaults */
	if (opts == NULL) {
		cfgopts = NOWDB_CONFIG_SIZE_BIG        |
		          NOWDB_CONFIG_INSERT_CONSTANT | 
		          NOWDB_CONFIG_DISK_HDD;
		nowdb_storage_config(cfg, cfgopts);
		return NOWDB_OK;
	}

	/* get sizing (or use default) */
	o = nowdb_ast_option(opts, NOWDB_AST_SIZING);
	if (o == NULL) cfgopts |= NOWDB_CONFIG_SIZE_BIG;
	else {
		if (nowdb_ast_getUInt(o, &utmp) != 0)
			INVALIDAST("invalid ast: invalid sizing");
		cfgopts |= utmp;
	}

	/* get stress (or use default) */
	o = nowdb_ast_option(opts, NOWDB_AST_STRESS);
	if (o == NULL) cfgopts |= NOWDB_CONFIG_INSERT_CONSTANT;
	else {
		if (nowdb_ast_getUInt(o, &utmp) != 0)
			INVALIDAST("invalid ast: invalid throughput");
		cfgopts |= utmp;
	}

	/* get disk (or use default) */
	o = nowdb_ast_option(opts, NOWDB_AST_DISK);
	if (o == NULL) cfgopts |= NOWDB_CONFIG_DISK_HDD;
	else {
		if (nowdb_ast_getUInt(o, &utmp) != 0)
			INVALIDAST("invalid ast: invalid disk");
		cfgopts |= utmp;
	}

	/* get nocomp */
	o = nowdb_ast_option(opts, NOWDB_AST_COMP);
	if (o != NULL) cfgopts |= NOWDB_CONFIG_NOCOMP;

	/* get nosort */
	o = nowdb_ast_option(opts, NOWDB_AST_SORT);
	if (o != NULL) cfgopts |= NOWDB_CONFIG_NOSORT;

	/* apply the options */
	nowdb_storage_config(cfg, cfgopts);

	return NOWDB_OK;
}

/* -------------------------------------------------------------------------
 * Apply explicitly stated options on config
 * -------------------------------------------------------------------------
 */
static nowdb_err_t applyGenericOptions(nowdb_ast_t *opts,
                             nowdb_storage_config_t *cfg) {

	nowdb_ast_t *o;
	uint64_t utmp;

	if (opts == NULL) return NOWDB_OK;

	o = nowdb_ast_option(opts, NOWDB_AST_SORTERS);
	if (o != NULL) {
		if (nowdb_ast_getUInt(o, &utmp) != 0)
			INVALIDAST("invalid ast: invalid sorters");
		cfg->sorters = (uint32_t)utmp;
	}
	o = nowdb_ast_option(opts, NOWDB_AST_ALLOCSZ);
	if (o != NULL) {
		if (nowdb_ast_getUInt(o, &utmp) != 0)
			INVALIDAST("invalid ast: invalid alloc size");
		cfg->filesize = (uint32_t)utmp;
	}
	o = nowdb_ast_option(opts, NOWDB_AST_LARGESZ);
	if (o != NULL) {
		if (nowdb_ast_getUInt(o, &utmp) != 0)
			INVALIDAST("invalid ast: invalid large size");
		cfg->largesize = (uint32_t)utmp;
	}
	return NOWDB_OK;
}

/* -------------------------------------------------------------------------
 * Check whether the storage exists or not
 * -------------------------------------------------------------------------
 */
static inline nowdb_err_t checkStorageExists(nowdb_scope_t *scope,
                                             char          *name,
                                             nowdb_bool_t  *b) {
	nowdb_err_t err;
	nowdb_storage_t *strg;

	err = nowdb_scope_getStorage(scope, name, &strg);
	if (err == NOWDB_OK) {
		*b = TRUE; return NOWDB_OK;
	}
	if (nowdb_err_contains(err, nowdb_err_key_not_found)) {
		nowdb_err_release(err);
		*b = FALSE; return NOWDB_OK;
	}
	return err;
}

/* -------------------------------------------------------------------------
 * Create storage
 * -------------------------------------------------------------------------
 */
static nowdb_err_t createStorage(nowdb_ast_t  *op,
                                 char       *name,
                             nowdb_scope_t *scope) {
	nowdb_err_t err;
	nowdb_ast_t *opts, *o;
	nowdb_storage_config_t cfg;

	/* get options and, if 'ifexists' is given,
	 * check if the context exists */
	opts = nowdb_ast_option(op, 0);
	if (opts != NULL) {
		o = nowdb_ast_option(opts, NOWDB_AST_IFEXISTS);
		if (o != NULL) {
			nowdb_bool_t x = FALSE;
			err = checkStorageExists(scope, name, &x);
			if (err != NOWDB_OK) return err;
			if (x) return NOWDB_OK;
		}
	}

	/* apply implicit options */
	err = applyCreateOptions(opts, &cfg);
	if (err != NOWDB_OK) return err;

	/* apply explicit options */
	err = applyGenericOptions(opts, &cfg);
	if (err != NOWDB_OK) return err;

	/* create the context */
	return nowdb_scope_createStorage(scope, name, &cfg);
}

/* -------------------------------------------------------------------------
 * Create Edge
 * -------------------------------------------------------------------------
 */
static nowdb_err_t createEdge(nowdb_ast_t  *op,
                              char       *name,
                          nowdb_scope_t *scope)  {
	nowdb_err_t err;
	nowdb_ast_t  *o;
	nowdb_ast_t  *f;
	nowdb_ast_t  *d;
	char *origin=NULL, *destin=NULL;
	nowdb_model_pedge_t *p;
	ts_algo_list_t props;

	d = nowdb_ast_declare(op);
	if (d == NULL) INVALIDAST("no declarations in AST");
	ts_algo_list_init(&props);

	while(d != NULL) {
		if (d->stype == NOWDB_AST_TYPE) {
			f = nowdb_ast_off(d);
			if (f == NULL) {
				destroyPedges(&props);
				INVALIDAST("no offset in decl");
			}
			if (f->stype == NOWDB_OFF_ORIGIN) {
				// fprintf(stderr, "ORIGIN\n");
				origin = d->value;
			} else {
				// fprintf(stderr, "DESTIN\n");
				destin = d->value;
			}
		} else {
			if (d->vtype != NOWDB_AST_V_STRING) {
				destroyPedges(&props);
				INVALIDAST("missing property name");
			}
			p = calloc(1,sizeof(nowdb_model_pedge_t));
			if (p == NULL) {
				destroyPedges(&props);
				NOMEM("allocating property");
				return err;
			}
	
			p->name  = strdup(d->value);
			if (p->name == NULL) {
				destroyPedges(&props); free(p);
				NOMEM("allocating property name");
				return err;
			}
			// fprintf(stderr, "%s\n", p->name);
	
			p->value = nowdb_ast_type(d->stype);
			p->edgeid = 0;
			p->off = 0;
			
			err = nowdb_text_insert(scope->text,
			               p->name, &p->propid);
			if (err != NOWDB_OK) {
				destroyPedges(&props);
				free(p->name); free(p);
				return err;
			}
	
			if (ts_algo_list_append(&props, p) != TS_ALGO_OK) {
				destroyPedges(&props);
				free(p->name); free(p);
				NOMEM("list.append");
				return err;
			}
		}
		d = nowdb_ast_declare(d);
	}

	if (origin == NULL && destin == NULL) {
		destroyPedges(&props);
		INVALIDAST("no vertex in edge");
	}
	err = nowdb_scope_createEdge(scope, name, origin, destin, &props);
	if (err != NOWDB_OK) {
		destroyPedges(&props);
		if (nowdb_err_contains(err, nowdb_err_dup_key)) {
			o = nowdb_ast_option(op, NOWDB_AST_IFEXISTS);
			if (o != NULL) {
				nowdb_err_release(err);
				return NOWDB_OK;
			}
		}
		return err;
	}
	ts_algo_list_destroy(&props);
	err = createContext(op, name, scope);
	if (err != NOWDB_OK) {
		nowdb_err_t err2;
		err2 = nowdb_model_removeEdgeType(scope->model, name);
		if (err2 != NOWDB_OK) {
			nowdb_err_cascade(err2, err);
			return err2;
		}
	}
	return err;
}

/* -------------------------------------------------------------------------
 * Drop Edge
 * -------------------------------------------------------------------------
 */
static nowdb_err_t dropEdge(nowdb_ast_t  *op,
                            char       *name,
                        nowdb_scope_t *scope)  {
	nowdb_err_t err;
	nowdb_ast_t  *o;

	err = nowdb_scope_dropEdge(scope, name);
	if (err == NOWDB_OK) {
		err = nowdb_scope_dropContext(scope, name);
	}
	if (nowdb_err_contains(err, nowdb_err_key_not_found)) {
		o = nowdb_ast_option(op, NOWDB_AST_IFEXISTS);
		if (o != NULL) {
			nowdb_err_release(err);
			err = NOWDB_OK;
		}
	}
	return err;
}

/* -------------------------------------------------------------------------
 * Create Procedure
 * -------------------------------------------------------------------------
 */
static nowdb_err_t createProc(nowdb_ast_t  *op,
                              nowdb_ast_t *trg,
                          nowdb_scope_t *scope)  {
	nowdb_err_t err;
	nowdb_proc_desc_t *pd;
	nowdb_ast_t  *l;
	nowdb_ast_t  *m;
	nowdb_ast_t  *o;
	nowdb_ast_t  *p;
	nowdb_ast_t  *r;
	char *module;
	char *name;
	char *lang;
	char lx, rx;
	int i=0;

	// name and module
	name = trg->value;
	if (name == NULL) INVALIDAST("no target name in AST");

	m = nowdb_ast_target(trg);
	if (m == NULL) INVALIDAST("no module in AST 'target'");

	module = m->value;
	if (module == NULL) INVALIDAST("no module name in AST");

	// language
	l = nowdb_ast_option(op, NOWDB_AST_LANG);
	if (l == NULL) INVALIDAST("no language AST");

	lang = l->value;
	if (lang == NULL) INVALIDAST("no name in language option");

	if (strcasecmp(lang, "lua") == 0) lx = NOWDB_STORED_LUA;
	else if (strcasecmp(lang, "python") == 0) lx = NOWDB_STORED_PYTHON;
	else INVALIDAST("unknown language in AST");

	// return type
	r = nowdb_ast_option(op, NOWDB_AST_TYPE);
	if (r == NULL) rx = 0;
	else {
		rx = nowdb_ast_type((uint16_t)(uint64_t)r->value);
	}

	// make proc descriptor
	pd = calloc(1, sizeof(nowdb_proc_desc_t));
	if (pd == NULL) {
		NOMEM("allocating proc descriptor");
		return err;
	}

	pd->name = strdup(name);
	if (pd->name == NULL) {
		free(pd);
		NOMEM("allocating procedure name");
		return err;
	}

	pd->module = strdup(module);
	if (pd->module == NULL) {
		free(pd->name); free(pd);
		NOMEM("allocating module name");
		return err;
	}

	pd->lang = lx;
	pd->rtype = rx;
	pd->type = rx==0?NOWDB_STORED_PROC:NOWDB_STORED_FUN;
	
	// arguments
	pd->argn = 0;
	pd->args = NULL;

	p = nowdb_ast_declare(op);
	while(p!=NULL) {
		pd->argn++;
		p = nowdb_ast_declare(p);
	}

	// fprintf(stderr, "parameters: %u\n", pd->argn);

	if (pd->argn > 0) {
		pd->args = calloc(pd->argn, sizeof(nowdb_proc_arg_t));
		if (pd->args == NULL) {
			nowdb_proc_desc_destroy(pd); free(pd);
			NOMEM("allocating procedure parameters");
			return err;
		}
	
		p = nowdb_ast_declare(op); i=0;
		while(p!=NULL) {
			/*
			fprintf(stderr, "parameter %s (%d)\n",
			            (char*)p->value, p->stype);
			*/
	
			pd->args[i].name = strdup(p->value);
			if (pd->args[i].name == NULL) {
				nowdb_proc_desc_destroy(pd); free(pd);
				NOMEM("allocating parameter name");
				return err;
			}
			pd->args[i].typ = nowdb_ast_type(p->stype);
			pd->args[i].pos = i; // caution!
	
			p = nowdb_ast_declare(p); i++;
		}
	}
	
	err = nowdb_scope_createProcedure(scope, pd);
	if (err != NOWDB_OK) {
		nowdb_proc_desc_destroy(pd); free(pd);
	}
	if (nowdb_err_contains(err, nowdb_err_dup_key)) {
		o = nowdb_ast_option(op, NOWDB_AST_IFEXISTS);
		if (o != NULL) {
			nowdb_err_release(err);
			err = NOWDB_OK;
		}
	}
	return err;
}

/* -------------------------------------------------------------------------
 * Drop Proc
 * -------------------------------------------------------------------------
 */
static nowdb_err_t dropProc(nowdb_ast_t  *op,
                            char       *name,
                        nowdb_scope_t *scope)  {
	nowdb_err_t err;
	nowdb_ast_t  *o;

	err = nowdb_scope_dropProcedure(scope, name);
	if (err == NOWDB_OK) return NOWDB_OK;
	if (nowdb_err_contains(err, nowdb_err_key_not_found)) {
		o = nowdb_ast_option(op, NOWDB_AST_IFEXISTS);
		if (o != NULL) {
			nowdb_err_release(err);
			err = NOWDB_OK;
		}
	}
	return err;
}

/* -------------------------------------------------------------------------
 * Helper: create lock
 * -------------------------------------------------------------------------
 */
static nowdb_err_t createLock(nowdb_ast_t  *op,
                              nowdb_ast_t *trg,
                          nowdb_scope_t *scope)  {
	nowdb_err_t err;
	err = nowdb_scope_createLock(scope, trg->value);
	if (err == NOWDB_OK) return NOWDB_OK;
	if (err->errcode == nowdb_err_dup_key) {
		nowdb_ast_t *o = nowdb_ast_option(op, NOWDB_AST_IFEXISTS);
		if (o != NULL) {
			nowdb_err_release(err);
			return NOWDB_OK;
		}
	}
	return err;
}

/* -------------------------------------------------------------------------
 * Helper: drop lock
 * -------------------------------------------------------------------------
 */
static nowdb_err_t dropLock(nowdb_ast_t  *op,
                                  char *name,
                        nowdb_scope_t *scope)  {
	nowdb_err_t err;
	err = nowdb_scope_dropLock(scope, name);
	if (err == NOWDB_OK) return NOWDB_OK;
	if (err->errcode == nowdb_err_key_not_found) {
		nowdb_ast_t *o = nowdb_ast_option(op, NOWDB_AST_IFEXISTS);
		if (o != NULL) {
			nowdb_err_release(err);
			return NOWDB_OK;
		}
	}
	return err;
}

/* -------------------------------------------------------------------------
 * Helper: copy value
 * -------------------------------------------------------------------------
 */
static inline nowdb_err_t copyval(void *src, nowdb_type_t t,
                                  nowdb_simple_value_t *val) {
	nowdb_err_t err;

	if (t == NOWDB_TYP_NOTHING) {
		val->value = NULL;

	} else if (t == NOWDB_TYP_TEXT) {
		val->value = strdup(src);
		if (val->value == NULL) {
			NOMEM("strdup");
			return err;
		}
	} else {
		val->value = malloc(sizeof(nowdb_key_t));
		if (val->value == NULL) {
			NOMEM("allocating value");
			return err;
		}
		memcpy(val->value, src, sizeof(nowdb_key_t));
	}
	return NOWDB_OK;
}

/* -------------------------------------------------------------------------
 * Helper: expression to simplified value
 * -------------------------------------------------------------------------
 */
static inline nowdb_err_t expr2simplev(nowdb_scope_t *scope,
                                       nowdb_ast_t   *trg,
                                       nowdb_ast_t   *v,
                                       nowdb_simple_value_t **val) {
	nowdb_err_t err=NOWDB_OK;
	nowdb_expr_t expr;
	uint32_t limits;
	void *tmp;
	char agg=0;

	NOWDB_PLAN_OK_ALL(limits);
	NOWDB_PLAN_OK_REMOVE(limits,NOWDB_PLAN_OK_FIELD);
	NOWDB_PLAN_OK_REMOVE(limits,NOWDB_PLAN_OK_AGG);

	err = nowdb_plan_getExpr(scope, NULL, NULL,
                       trg, v, limits, &expr, &agg);
	if (err != NOWDB_OK) return err;

	*val = calloc(1,sizeof(nowdb_simple_value_t));
	if (*val == NULL) {
		NOMEM("allocating simplified value");
		nowdb_expr_destroy(expr); free(expr);
		return err;
	}
	err = nowdb_expr_eval(expr, NULL, NULL,
	                    &(*val)->type, &tmp);
	if (err != NOWDB_OK) {
		nowdb_expr_destroy(expr); free(expr);
		free(*val); *val = NULL;
		return err;
	}
	err = copyval(tmp, (*val)->type, *val);
	nowdb_expr_destroy(expr); free(expr);
	if (err != NOWDB_OK) {
		free(*val); *val = NULL;
		return err;
	}
	return NOWDB_OK;
}

/* -------------------------------------------------------------------------
 * Load a file
 * TODO:
 * - Should be more versatile,
 *   currently only one format (csv) is supported;
 *   the function should be extended to provide more formats:
 *   + native binary
 *   + generic binary (avro)
 *   + json(?)
 * -------------------------------------------------------------------------
 */
static nowdb_err_t load(nowdb_scope_t    *scope,
                        nowdb_path_t       path,
                        nowdb_path_t      epath,
                        nowdb_ast_t        *trg,
                        char              *type,
                        nowdb_bitmap32_t    flg,
                        nowdb_qry_result_t *res) {
	FILE *stream, *estream=stderr;
	nowdb_err_t err=NOWDB_OK;
	nowdb_context_t *ctx;
	nowdb_loader_t   ldr;
	nowdb_qry_report_t *rep;

	if (trg->value == NULL) {
		INVALIDAST("no target name in AST");
	}

	/* open stream from path */
	stream = fopen(path, "rb");
	if (stream == NULL) return nowdb_err_get(nowdb_err_open,
		                            TRUE, OBJECT, path);

	err = nowdb_scope_getContext(scope, trg->value, &ctx);
	if (err != NOWDB_OK) {
		fclose(stream);return err;
	}

	/* open error stream from epath */
	if (epath != NULL) {
		estream = fopen(epath, "wb");
		if (estream == NULL) return nowdb_err_get(nowdb_err_open,
			                            TRUE, OBJECT, epath);
	}

	switch(trg->stype) {

	/* create vertex loader */
	case NOWDB_AST_VERTEX:
	case NOWDB_AST_TYPE:
		/*
		fprintf(stderr, "loading '%s' into vertex as type '%s'\n",
		                path, type);
		*/

		flg |= NOWDB_CSV_VERTEX;
		err = nowdb_loader_init(&ldr, stream, estream,
		                        scope, ctx,
		                        scope->model,
		                        scope->text,
		                        type,  flg);
		if (err != NOWDB_OK) {
			if (epath != NULL) fclose(estream);
			fclose(stream); return err;
		}
		break;

	/* create edge loader */
	case NOWDB_AST_CONTEXT:

		if (type != NULL) {
			fprintf(stderr, "loading '%s' into '%s' as edge\n",
			                                   path, ctx->name);
		} else {
			fprintf(stderr, "loading '%s' into '%s'\n",
			                           path, ctx->name);
		}

		err = nowdb_loader_init(&ldr, stream, estream,
		                        scope, ctx,
		                        scope->model,
		                        scope->text,
		                        type,  flg);
		if (err != NOWDB_OK) {
			if (epath != NULL) fclose(estream);
			fclose(stream); return err;
		}
		break;
	
	default:
		if (epath != NULL) fclose(estream);
		fclose(stream); 
		fprintf(stderr, "target: %d\n", trg->stype);
		INVALIDAST("invalid target for load");
	}

	/* run the loader */
	err = nowdb_loader_run(&ldr);
	fclose(stream); 
	if (epath != NULL) fclose(estream);

	if (err == NOWDB_OK) {
		/* create a report */
		rep = calloc(1, sizeof(nowdb_qry_report_t));
		if (rep == NULL) {
			err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
			                               "allocating report");
		} else {
			rep->affected = ldr.loaded;
			rep->errors = ldr.errors;
			rep->runtime = ldr.runtime;
			res->result = rep;
		}
	}
	nowdb_loader_destroy(&ldr);
	return err;
}

/* -------------------------------------------------------------------------
 * Drop Storage
 * -------------------------------------------------------------------------
 */
static nowdb_err_t dropStorage(nowdb_ast_t  *op,
                              char          *name,
                              nowdb_scope_t *scope) {
	nowdb_ast_t *o;
	nowdb_err_t err;

	err = nowdb_scope_dropStorage(scope, name);
	if (err == NOWDB_OK) return NOWDB_OK;
	if (nowdb_err_contains(err, nowdb_err_key_not_found)) {
		o = nowdb_ast_option(op, NOWDB_AST_IFEXISTS);
		if (o != NULL) {
			nowdb_err_release(err);
			return NOWDB_OK;
		}
	}
	return err;
}

/* -------------------------------------------------------------------------
 * Find scope in list of open scopes
 * -------------------------------------------------------------------------
 */
static inline nowdb_err_t findScope(nowdb_t        *lib,
                                    char           *name,
                                    nowdb_scope_t **scope) {
	if (lib == NULL) return NOWDB_OK;
	return nowdb_getScope(lib, name, scope);
}

/* -------------------------------------------------------------------------
 * Add tist list of open scopes
 * -------------------------------------------------------------------------
 */
static inline nowdb_err_t addScope(nowdb_t       *lib,
                                   char          *name,
                                   nowdb_scope_t *scope) {
	if (lib == NULL) return NOWDB_OK;
	return nowdb_addScope(lib, name, scope);
}

/* -------------------------------------------------------------------------
 * Handle use statement
 * -------------------------------------------------------------------------
 */
static nowdb_err_t handleUse(nowdb_ast_t        *ast,
                             void               *rsc,
                             nowdb_path_t       base,
                             nowdb_qry_result_t *res) {
	nowdb_path_t p;
	nowdb_scope_t *scope = NULL;
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;
	nowdb_t *lib = nowdb_proc_getLib(rsc);
	char *name;

	name = nowdb_ast_getString(ast);
	if (name == NULL) INVALIDAST("no name in AST");

	if (lib != NULL) {
		err = nowdb_lock_write(lib->lock);
		if (err != NOWDB_OK) {
			fprintf(stderr, "ERROR ON LOCK: ");
			nowdb_err_print(err);
			return err;
		}
	}

	err = findScope(lib, name, &scope);
	if (err != NOWDB_OK) goto unlock;

	if (scope != NULL) {
		res->result = scope;
		goto unlock;
	}

	p = nowdb_path_append(base, name);
	if (p == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem,
	                FALSE, OBJECT, "path.append");
		goto unlock;
	}

	err = openScope(p, &scope); free(p);
	if (err != NOWDB_OK) goto unlock;

	err = addScope(lib, name, scope);
	if (err != NOWDB_OK) goto unlock;

	res->result = scope;

unlock:
	if (lib != NULL) {
		err2 = nowdb_unlock_write(lib->lock);
		if (err2 != NOWDB_OK) {
			err2->cause = err;
			return err2;
		}
	}
	return err;
}

/* -------------------------------------------------------------------------
 * Helper: handle single select statement
 * -------------------------------------------------------------------------
 */
static nowdb_err_t handleSelect(nowdb_scope_t    *scope,
                                nowdb_ast_t         *op,
                                nowdb_qry_result_t *res) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_qry_row_t *r;
	char *row=NULL, *tmp;
	uint32_t sz=0;
	nowdb_ast_t     *v;
	nowdb_simple_value_t *val;

	v = nowdb_ast_field(op);
	while(v != NULL) {
		err = expr2simplev(scope, NULL, v, &val);
		if (err != NOWDB_OK) {
			if (row != NULL) free(row);
			return err;
		}
		if (row == NULL) {
			row = nowdb_row_fromValue(val->type,
			                          val->value, &sz);
			if (row == NULL) {
				if (val->value != NULL) {
					free(val->value);
				}
				free(val); val = NULL;
				NOMEM("allocating row");
				break;
			}
		} else {
			tmp = nowdb_row_addValue(row, val->type,
			                              val->value, &sz);
			if (tmp == NULL) {
				if (val->value != NULL) {
					free(val->value);
				}
				free(val); val = NULL;
				free(row); row = NULL;
				NOMEM("allocating row");
				break;
			}
			row = tmp;
		}
		if (val != NULL) {
			if (val->value != NULL) {
				free(val->value);
			}
			free(val); val = NULL;
		}
		v = nowdb_ast_field(v);
	}
	if (err != NOWDB_OK) return err;
	if (row != NULL) {
		nowdb_row_addEOR(row, &sz);
		r = calloc(1, sizeof(nowdb_qry_row_t));
		if (r == NULL) {
			free(row);
			NOMEM("allocating qryrow");
			return err;
		}
		r->sz = sz;
		r->row = row;
		res->resType = NOWDB_QRY_RESULT_ROW;
		res->result = r;
	}
	return err;
}

/* -------------------------------------------------------------------------
 * Helper: handle lock/unlock
 * -------------------------------------------------------------------------
 */
static inline nowdb_err_t handleLock(nowdb_scope_t   *scope,
                                     nowdb_ast_t        *op,
                                     void              *proc,
                                     nowdb_qry_result_t *res) {
	nowdb_err_t  err;
	nowdb_ast_t *o;
	char *name;
	char mode = NOWDB_IPC_WLOCK;
	int64_t tmo = -1;

	if (proc == NULL) {
		INVALID("no session"); return err;
	}

	if (op->value == NULL) {
		INVALIDAST("no target name in ast");
	}

	name = op->value;
	if (strnlen(name, NOWDB_MAX_NAME+1) >= NOWDB_MAX_NAME) {
		INVALIDAST("lock name too long");
	}

	char holds = nowdb_proc_holdsLock(proc, name);

	if (op->ntype == NOWDB_AST_LOCK) {
		if (holds) return nowdb_err_get(nowdb_err_selflock,
		                               FALSE, OBJECT, name);
		o = nowdb_ast_option(op, NOWDB_AST_MODE);
		if (o != NULL) {
			if (strcasecmp(o->value, "READING") == 0)
				mode = NOWDB_IPC_RLOCK;
		}
		o = nowdb_ast_option(op, NOWDB_AST_TIMEOUT);
		if (o != NULL) {
			if (nowdb_ast_getInt(o, &tmo) != 0) {
				INVALIDAST("timeout is not a valid number");
			}
		}
		err = nowdb_ipc_lock(scope->ipc, name, mode, tmo);
		if (err == NOWDB_OK) {
			err = nowdb_proc_addLock(proc, name);
		}
	} else {
		if (!holds) return nowdb_err_get(nowdb_err_doesnothold,
		                                   FALSE, OBJECT, name);
		err = nowdb_ipc_unlock(scope->ipc, name);
		if (err == NOWDB_OK) {
			nowdb_proc_removeLock(proc, name);
		}
	}
	return err;
}

#define LUADESTROYARGS(n,args) \
	for(int z=0; z<n; z++) { \
		if (args[z] != NULL) { \
			if (args[z]->value != NULL) { \
				free(args[z]->value); \
			} \
			free(args[z]); args[z] = NULL; \
		} \
	} \
	free(args);

/* -------------------------------------------------------------------------
 * Helper: get lua arguments from ast
 * -------------------------------------------------------------------------
 */
static inline nowdb_err_t getLuaArgs(nowdb_scope_t        *scope,
                                     nowdb_proc_desc_t       *pd,
                                     nowdb_ast_t         *params,
                                     nowdb_simple_value_t **args) {
	nowdb_err_t err;
	nowdb_ast_t  *p;

	if (pd->argn == 0) return NOWDB_OK;
	if (params == NULL) {
		LUAERR("not enough parameters");
		return err;
	}
	p = params;
	for(int i=0; i<pd->argn; i++) {
		err = expr2simplev(scope, NULL, p, args+i);
		if (err != NOWDB_OK) {
			LUADESTROYARGS(i,args);
			return err;
		}
		p = nowdb_ast_nextParam(p);
	}
	return NOWDB_OK;
}

/* -------------------------------------------------------------------------
 * Helper: execute Lua function 
 * -------------------------------------------------------------------------
 */
static inline nowdb_err_t execLua(nowdb_scope_t    *scope,
                                  nowdb_ast_t        *ast,
                                  nowdb_proc_t      *proc,
                                  nowdb_proc_desc_t   *pd,
                                  void                 *f,
                                  void                 *c,
                                  nowdb_qry_result_t *res) {
	nowdb_err_t err;
	nowdb_simple_value_t **args=NULL;
	nowdb_dbresult_t p=NULL;

	res->resType = NOWDB_QRY_RESULT_NOTHING;
	res->result  = NULL;

	if (pd->argn > 0) {
		args = calloc(pd->argn, sizeof(nowdb_simple_value_t*));
		if (args == NULL) {
			NOMEM("allocating argument buffer");
			return err;
		}
		err = getLuaArgs(scope, pd, nowdb_ast_param(ast), args);
		if (err != NOWDB_OK) return err;
	}

	p = nowdb_proc_call(proc, c, f, args);
	if (args != NULL) {
		LUADESTROYARGS(pd->argn, args);
	}
	if (p == NULL) {
		LUAERR("Call failed: result is NULL");
		return err;
	}
	if (!nowdb_dbresult_status(p)) {
		err = nowdb_dbresult_err(p); free(p);
		return err;
	}
	nowdb_dbresult_result(p, res); free(p);
	return NOWDB_OK;
}

/* -------------------------------------------------------------------------
 * Helper: convert nowdb type to python type
 * -------------------------------------------------------------------------
 */
#ifdef _NOWDB_WITH_PYTHON
static inline nowdb_err_t buildPyType(nowdb_simple_value_t *val,
                                      uint16_t              pos,
                                      PyObject            *args) {
	nowdb_err_t err;
	PyObject *x;
	double d;
	uint64_t u;
	int64_t l;

	switch(val->type) {
	case NOWDB_TYP_TEXT:
		x = Py_BuildValue("s",val->value); break;
 
	case NOWDB_TYP_FLOAT: 
		memcpy(&d, val->value, sizeof(double));
		x = Py_BuildValue("d",d); break;

	case NOWDB_TYP_UINT: 
		memcpy(&u, val->value, sizeof(uint64_t));
		x = Py_BuildValue("K",u); break;

	case NOWDB_TYP_INT: 
	case NOWDB_TYP_DATE: 
	case NOWDB_TYP_TIME: 
		memcpy(&l, val->value, sizeof(int64_t));
		x = Py_BuildValue("L",l); break;

	case NOWDB_TYP_BOOL:
		x = PyBool_FromLong((long int)(*(int64_t*)val->value));
		break;

	// NOTHING

	default:
		INVALID("invalid parameter type");
		return err;
	}
	if (PyTuple_SetItem(args,(Py_ssize_t)pos,x) != 0) {
		PYTHONERR("cannot set item to tuple");
		return err;
	}
	return NOWDB_OK;
}
#endif

/* -------------------------------------------------------------------------
 * Helper: load arguments for python function
 * -------------------------------------------------------------------------
 */
#ifdef _NOWDB_WITH_PYTHON
static nowdb_err_t loadPyArgs(nowdb_scope_t *scope,
                              nowdb_proc_desc_t *pd,
                              nowdb_ast_t   *params,
                              PyObject       **args) {
	nowdb_err_t err;
	nowdb_simple_value_t *val;
	nowdb_ast_t *p;

	if (pd->argn == 0) {
		if (params == NULL) return NOWDB_OK;
		PYTHONERR("too many arguments");
		return err;
	}
	if (params == NULL) {
		PYTHONERR("not enough parameters");
		return err;
	}

	*args = PyTuple_New((Py_ssize_t)pd->argn);
	if (*args == NULL) {
		NOMEM("allocating Python tuple");
		return err;
	}

	p = params;
	for(uint16_t i=0; i<pd->argn; i++) {
		if (p == NULL) {
			Py_DECREF(*args);
			PYTHONERR("not enough parameters");
			return err;
		}

		err = expr2simplev(scope, NULL, p, &val);
		if (err != NOWDB_OK) {
			Py_DECREF(*args);
			return err;
		}

		err = buildPyType(val, pd->args[i].pos, *args);
		if (err != NOWDB_OK) {
			Py_DECREF(*args);
			if (val->value != NULL) free(val->value);
			free(val);
			return err;
		}
		if (val->value != NULL) free(val->value);
		free(val); val = NULL;

		p = nowdb_ast_nextParam(p);
	}
	if (p != NULL) {
		Py_DECREF(*args);
		PYTHONERR("too many arguments");
		return err;
	}
	return NOWDB_OK;
}
#endif

/* -------------------------------------------------------------------------
 * Helper: execute a python function
 * -------------------------------------------------------------------------
 */
#ifdef _NOWDB_WITH_PYTHON
static inline nowdb_err_t execPython(nowdb_scope_t    *scope,
                                     nowdb_ast_t        *ast,
                                     nowdb_proc_t      *proc,
                                     nowdb_proc_desc_t   *pd,
                                     void                 *f,
                                     void                 *c,
                                     nowdb_qry_result_t *res)
{
	nowdb_err_t err;
	PyThreadState *ts;
	PyObject *r;
	PyObject *args=NULL;
	nowdb_dbresult_t p;

	res->resType = NOWDB_QRY_RESULT_NOTHING;
	res->result  = NULL;

	ts = nowdb_proc_getInterpreter(proc);
	if (ts == NULL) {
		INVALID("no interpreter");
		return err;
	}

	PyEval_RestoreThread(ts);
	
	// load arguments
	err = loadPyArgs(scope, pd, nowdb_ast_param(ast), &args);
	if (err != NOWDB_OK) {
		fprintf(stderr, "PY ARGS NOT LOADED\n");
		nowdb_err_print(err);
		nowdb_proc_updateInterpreter(proc);
		return err;
	}

	// fprintf(stderr, "calling f (%s) as %p | %p\n", pd->name, f, pd);

	r = nowdb_proc_call(proc, c, f, args);
	if (args != NULL) Py_DECREF(args);

	if (r == NULL) {
		PYTHONERR("Call failed: result is NULL");
		nowdb_proc_updateInterpreter(proc);
		return err;
	}
	p = PyLong_AsVoidPtr(r); Py_DECREF(r);
	if (p == NULL) {
		nowdb_proc_updateInterpreter(proc);
		return NOWDB_OK;
	}
	if (!nowdb_dbresult_status(p)) {
		err = nowdb_dbresult_err(p); free(p);
		nowdb_proc_updateInterpreter(proc);
		return err;
	}
	nowdb_dbresult_result(p, res); free(p);

	nowdb_proc_updateInterpreter(proc);

	return NOWDB_OK;
}
#endif

/* -------------------------------------------------------------------------
 * Handle execute statement
 * -------------------------------------------------------------------------
 */
static nowdb_err_t handleExec(nowdb_scope_t    *scope,
                              nowdb_ast_t        *ast,
                              void               *rsc,
                              nowdb_qry_result_t *res) {
	nowdb_err_t err;
	char *pname;
	nowdb_proc_desc_t *pd;
	nowdb_t *lib;
	void  *f, *c;

	lib = nowdb_proc_getLib(rsc);
	if (lib == NULL) {
		INVALID("no library in session");
		return err;
	}

	pname = ast->value;
	if (pname == NULL) INVALIDAST("procedure without name");

	err = nowdb_proc_loadFun(rsc, pname, &pd, &f, &c);
	if (err != NOWDB_OK) return err;

	switch(pd->lang) {
	case NOWDB_STORED_LUA:
 		if (!lib->luaEnabled) {
			INVALID("feature not enabled: Lua");
			return err;
		}

		return execLua(scope, ast, rsc, pd, f, c, res);

	case NOWDB_STORED_PYTHON:

 		if (!lib->pyEnabled) {
			INVALID("feature not enabled: Python");
			return err;
		}

#ifdef _NOWDB_WITH_PYTHON
		return execPython(scope, ast, rsc, pd, f, c, res);
#else
		return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT,
	                                        "python not supported");
#endif

	default:
		INVALID("unknown language");
		return err;
	}
	return NOWDB_OK;
}

/* -------------------------------------------------------------------------
 * Show edges
 * -------------------------------------------------------------------------
 */
static nowdb_err_t showThings(nowdb_scope_t    *scope,
                              nowdb_ast_t        *ast,
                              nowdb_qry_result_t *res) {
	nowdb_err_t err=NOWDB_OK;
	ts_algo_list_t *list;
	ts_algo_list_node_t *run;
	uint32_t sz = 0;
	char *row=NULL;
	char *tmp;
	nowdb_qry_row_t *r;
	char what;
	char *name;

	if (ast->value == NULL) INVALIDAST("no target in ast");
	if (strcasecmp(ast->value, "edges") == 0) what=0;
	else if (strcasecmp(ast->value, "vertices") == 0 ||
	         strcasecmp(ast->value, "types") == 0) what=1;
	else if (strcasecmp(ast->value, "procedures") == 0 ||
	         strcasecmp(ast->value, "procs") == 0) what=2;
	else if (strcasecmp(ast->value, "stores") == 0) what=3;
	else if (strcasecmp(ast->value, "locks") == 0) what=4;
	else INVALIDAST("unknown target in ast");

	switch(what) {
	case 0:
		err = nowdb_model_getEdges(scope->model, &list); break;
	case 1:
		err = nowdb_model_getVertices(scope->model, &list); break;
	case 2:
		err = nowdb_procman_all(scope->pman, &list); break;
	case 3:
		err = nowdb_scope_allStorage(scope, &list); break;
	case 4:
		err = nowdb_ipc_locks(scope->ipc, &list); break;
	}
	if (err != NOWDB_OK) return err;
	if (list == NULL) {
		return nowdb_err_get(nowdb_err_eof, FALSE, OBJECT, NULL);
	}
	for(run=list->head; run!=NULL; run=run->nxt) {
		switch(what) {
		case 0: name = ((nowdb_model_edge_t*)run->cont)->name; break;
		case 1: name = ((nowdb_model_vertex_t*)run->cont)->name; break;
		case 2: name = ((nowdb_proc_desc_t*)run->cont)->name; break;
		case 3: name = ((nowdb_storage_t*)run->cont)->name; break;
		case 4: name = run->cont; break;
		default: name = "unknown";
		}
		if (row == NULL) {
			row = nowdb_row_fromValue(NOWDB_TYP_TEXT,
			                              name, &sz);
			if (row == NULL) {
				NOMEM("allocating row");
				break;
			}
		} else {
			tmp = nowdb_row_addValue(row, NOWDB_TYP_TEXT,
			                                  name, &sz);
			if (tmp == NULL) {
				free(row);
				NOMEM("allocating row");
				break;
			}
			row = tmp;
		}
		nowdb_row_addEOR(row, &sz);
	}
	ts_algo_list_destroy(list); free(list);
	if (err == NOWDB_OK) {
		r = calloc(1, sizeof(nowdb_qry_row_t));
		if (r == NULL) {
			NOMEM("allocating qryrow");
			return err;
		}
		r->sz = sz;
		r->row = row;
		res->resType = NOWDB_QRY_RESULT_ROW;
		res->result = r;
	}
	return err;
}

/* -------------------------------------------------------------------------
 * Helper: get vertex properties
 * -------------------------------------------------------------------------
 */
static nowdb_err_t getProps(nowdb_scope_t  *scope,
                            char           *name,
                            ts_algo_list_t *list) {
	nowdb_err_t err;
	nowdb_model_vertex_t *v;

	err = nowdb_model_getVertexByName(scope->model, name, &v);
	if (err != NOWDB_OK) return err;

	err = nowdb_model_getProperties(scope->model, v->roleid, list);
	if (err != NOWDB_OK) return err;

	return NOWDB_OK;
}

/* -------------------------------------------------------------------------
 * Helper: get edge properties
 * -------------------------------------------------------------------------
 */
static nowdb_err_t getPedges(nowdb_scope_t  *scope,
                             char           *name,
                             ts_algo_list_t *list) {
	nowdb_err_t err;
	nowdb_model_edge_t *e;

	err = nowdb_model_getEdgeByName(scope->model, name, &e);
	if (err != NOWDB_OK) return err;

	err = nowdb_model_getPedges(scope->model, e->edgeid, list);
	if (err != NOWDB_OK) return err;

	return NOWDB_OK;
}

/* -------------------------------------------------------------------------
 * Describe non-model thing
 * -------------------------------------------------------------------------
 */
static nowdb_err_t describeNonModel(nowdb_scope_t      *scope,
                                    char               *name,
                                    nowdb_qry_result_t *res) {
	nowdb_err_t err=NOWDB_OK;
	nowdb_proc_desc_t    *pd;
	nowdb_proc_arg_t   *args;
	nowdb_qry_row_t *r;
	uint32_t sz=0;
	char *row=NULL;
	char *tmp=NULL;

	res->resType = NOWDB_QRY_RESULT_NOTHING;
	res->result = NULL;

	err = nowdb_procman_get(scope->pman, name, &pd);
	if (err != NOWDB_OK) return err;

	args = pd->args;
	for(int i=0; i<pd->argn; i++) {
		if (row == NULL) {
			row = nowdb_row_fromValue(NOWDB_TYP_TEXT,
			                      args[i].name, &sz);
			if (row == NULL) {
				NOMEM("allocating row");
				break;
			}
		} else {
			tmp = nowdb_row_addValue(row, NOWDB_TYP_TEXT,
			                           args[i].name, &sz);
			if (tmp == NULL) {
				free(row);
				NOMEM("allocating row");
				break;
			}
			row = tmp;
		}
		tmp = nowdb_row_addValue(row, NOWDB_TYP_TEXT,
		            nowdb_typename(args[i].typ), &sz);
		if (tmp == NULL) {
			free(row);
			NOMEM("allocating row");
			break;
		}
		row = tmp;
		nowdb_row_addEOR(row, &sz);
	}
	nowdb_proc_desc_destroy(pd); free(pd);
	if (err != NOWDB_OK) return err;
	if (row != NULL) {
		r = calloc(1, sizeof(nowdb_qry_row_t));
		if (r == NULL) {
			NOMEM("allocating qryrow");
			free(row);
			return err;
		}
		r->sz = sz;
		r->row = row;
		res->resType = NOWDB_QRY_RESULT_ROW;
		res->result = r;
	}
	return NOWDB_OK;
}

/* -------------------------------------------------------------------------
 * Describe
 * -------------------------------------------------------------------------
 */
static nowdb_err_t describeThing(nowdb_scope_t      *scope,
                                 nowdb_ast_t        *ast,
                                 nowdb_qry_result_t *res) {
	nowdb_err_t err=NOWDB_OK;
	ts_algo_list_t list;
	ts_algo_list_node_t  *run;
	uint32_t sz = 0;
	uint32_t typ = 0;
	char *row=NULL;
	char *tmp;
	nowdb_qry_row_t *r;
	nowdb_content_t what;
	char *name;

	if (scope == NULL) INVALIDAST("no scope");
	if (ast->value == NULL) INVALIDAST("no target in ast");
	err = nowdb_model_whatIs(scope->model, ast->value, &what);
	if (err != NOWDB_OK) {
		if (nowdb_err_contains(err, nowdb_err_key_not_found)) {
			nowdb_err_release(err);
			return describeNonModel(scope, ast->value, res);
		}
		return err;
	}

	ts_algo_list_init(&list);
	if (what == NOWDB_CONT_EDGE) {
		err = getPedges(scope, ast->value, &list);
	} else if (what == NOWDB_CONT_VERTEX) {
		err = getProps(scope, ast->value, &list);
	} else INVALIDAST("unknown target in ast");

	for(run=list.head; run!=NULL; run=run->nxt) {
		if (what == NOWDB_CONT_EDGE) {
			name = ((nowdb_model_pedge_t*)run->cont)->name;
			typ  = ((nowdb_model_pedge_t*)run->cont)->value;
		} else {
			name = ((nowdb_model_prop_t*)run->cont)->name;
			typ  = ((nowdb_model_prop_t*)run->cont)->value;
		}
		if (row == NULL) {
			row = nowdb_row_fromValue(NOWDB_TYP_TEXT,
			                              name, &sz);
			if (row == NULL) {
				NOMEM("allocating row");
				break;
			}
		} else {
			tmp = nowdb_row_addValue(row, NOWDB_TYP_TEXT,
			                                  name, &sz);
			if (tmp == NULL) {
				free(row);
				NOMEM("allocating row");
				break;
			}
			row = tmp;
		}
		tmp = nowdb_row_addValue(row, NOWDB_TYP_TEXT,
		                   nowdb_typename(typ), &sz);
		if (tmp == NULL) {
			free(row);
			NOMEM("allocating row");
			break;
		}
		row = tmp;
		nowdb_row_addEOR(row, &sz);
	}
	ts_algo_list_destroy(&list);
	if (err == NOWDB_OK) {
		r = calloc(1, sizeof(nowdb_qry_row_t));
		if (r == NULL) {
			NOMEM("allocating qryrow");
			return err;
		}
		r->sz = sz;
		r->row = row;
		res->resType = NOWDB_QRY_RESULT_ROW;
		res->result = r;
	}
	return err;
}

/* -------------------------------------------------------------------------
 * Handle DDL statement
 * -------------------------------------------------------------------------
 */
static nowdb_err_t handleDDL(nowdb_ast_t *ast,
                         nowdb_scope_t *scope,
                                   void  *rsc,
                         nowdb_path_t    base,
                      nowdb_qry_result_t *res) 
{
	nowdb_ast_t *op;
	nowdb_ast_t *trg;
	
	res->resType = NOWDB_QRY_RESULT_NOTHING;
	res->result = NULL;
	
	op = nowdb_ast_operation(ast);
	if (op == NULL) INVALIDAST("no operation in AST");

	
	trg = nowdb_ast_target(ast);
	if ((trg == NULL ||
	     trg->stype != NOWDB_AST_SCOPE) && scope == NULL) {
		INVALIDAST("no scope");
	}

	if (op->ntype == NOWDB_AST_SHOW) {
		return showThings(scope, op, res);
	}
	if (op->ntype == NOWDB_AST_DESC) {
		return describeThing(scope, op, res);
	}

	if (trg == NULL) INVALIDAST("no target in AST");
	if (trg->value == NULL) INVALIDAST("no target name in AST");

	/* create */
	if (op->ntype == NOWDB_AST_CREATE) {
		switch(trg->stype) {
		case NOWDB_AST_SCOPE:
			return createScope(op, base, trg->value);
		case NOWDB_AST_STORAGE:
			return createStorage(op, trg->value, scope);
		case NOWDB_AST_INDEX:
			return createIndex(op, trg->value, scope);
		case NOWDB_AST_TYPE:
			return createType(op, trg->value, scope);
		case NOWDB_AST_EDGE:
			return createEdge(op, trg->value, scope);
		case NOWDB_AST_PROC:
			return createProc(op, trg, scope);
		case NOWDB_AST_MUTEX:
			return createLock(op, trg, scope);
		default: INVALIDAST("invalid target in AST");
		}
	}
	// alter
	/* drop */
	else if (op->ntype == NOWDB_AST_DROP) {
		switch(trg->stype) {
		case NOWDB_AST_SCOPE:
			return dropScope(op, rsc, base, trg->value);
		case NOWDB_AST_STORAGE:
			return dropStorage(op, trg->value, scope);
		case NOWDB_AST_INDEX:
			return dropIndex(op, trg->value, scope);
		case NOWDB_AST_TYPE:
			return dropType(op, trg->value, scope, rsc);
		case NOWDB_AST_EDGE:
			return dropEdge(op, trg->value, scope);
		case NOWDB_AST_PROC:
			return dropProc(op, trg->value, scope);
		case NOWDB_AST_MUTEX:
			return dropLock(op, trg->value, scope);
		default: INVALIDAST("invalid target in AST");
		}
	}
	return nowdb_err_get(nowdb_err_not_supp,
	            FALSE, OBJECT, "handleDDL");
}

/* -------------------------------------------------------------------------
 * Helper: get string from ast string value
 * TODO: this should be done in the parser!
 * -------------------------------------------------------------------------
 */
static nowdb_err_t getString(nowdb_ast_t *ast, char **str) {
	nowdb_err_t err;
	char *tmp;
	size_t  s;

	if (ast == NULL) INVALIDAST("option is NULL");
	if (ast->value == NULL) INVALIDAST("value is NULL");

	tmp = ast->value; s = strlen(tmp);
	if (s < 1) INVALIDAST("incomplete string value");

	*str = malloc(s+1);
	if (*str == NULL) {
		NOMEM("allocating path");
		return err;
	}
	if (tmp[0] == '\'') {
		strcpy(*str, tmp+1); (*str)[s-2] = 0;
	} else {
		strcpy(*str, tmp);
	}
	return NOWDB_OK;
}

/* -------------------------------------------------------------------------
 * Handle load statement
 * -------------------------------------------------------------------------
 */
static nowdb_err_t handleLoad(nowdb_ast_t *op,
                              nowdb_ast_t *trg,
                              nowdb_scope_t *scope,
                              nowdb_qry_result_t *res) {
	nowdb_err_t  err;
	nowdb_path_t path=NULL;
	nowdb_path_t epath=NULL;
	nowdb_ast_t *opts, *o;
	nowdb_ast_t *m;
	nowdb_bitmap32_t flgs = 0;
	char *type = NULL;

	/* get options */
	opts = nowdb_ast_option(op, 0);
	if (opts != NULL) {
		/* ignore header */
		o = nowdb_ast_option(opts, NOWDB_AST_IGNORE);
		if (o != NULL) flgs = NOWDB_CSV_HAS_HEADER;

		o = nowdb_ast_option(opts, NOWDB_AST_USE);
		if (o != NULL) flgs = NOWDB_CSV_HAS_HEADER |
		                      NOWDB_CSV_USE_HEADER;

		o = nowdb_ast_option(opts, NOWDB_AST_ERRORS);
		if (o != NULL) {
			err = getString(o, &epath);
			if (err != NOWDB_OK) return err;
		}
	}
	if (op->value == NULL) {
		if (epath != NULL) free(epath);
		INVALIDAST("no path in load operation");
	}

	err = getString(op, &path);
	if (err != NOWDB_OK) {
		if (epath != NULL) free(epath);
		return err;
	}

	/* get model type if any */
	if (trg->value == NULL) {
		m = nowdb_ast_option(op, NOWDB_AST_TYPE);
		if (m != NULL) {
			type = m->value;
			flgs |= NOWDB_CSV_MODEL;
		}
	} else {
		type = trg->value;
		flgs |= NOWDB_CSV_MODEL;
	}

	/* load and cleanup */
	err = load(scope, path, epath, trg, type, flgs, res);
	free(path); if (epath != NULL) free(epath);
	return err;
}

/* -----------------------------------------------------------------------
 * Adjust target to what it is according to model
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t adjustTarget(nowdb_scope_t *scope,
                                       nowdb_ast_t   *trg) {
	nowdb_err_t   err;
	nowdb_content_t t;

	if (trg->value == NULL) return NOWDB_OK;

	err = nowdb_model_whatIs(scope->model, trg->value, &t);
	if (err != NOWDB_OK) {
		if (err->errcode == nowdb_err_key_not_found) {
			nowdb_err_release(err);
			trg->stype = NOWDB_AST_CONTEXT;
			return NOWDB_OK;
		}
		return err;
	}

	trg->stype = t==NOWDB_CONT_VERTEX?NOWDB_AST_TYPE:
	                               NOWDB_AST_CONTEXT;
	return NOWDB_OK;
}

/* -------------------------------------------------------------------------
 * Handle DLL statement
 * -------------------------------------------------------------------------
 */
static nowdb_err_t handleDLL(nowdb_ast_t *ast,
                         nowdb_scope_t *scope,
                      nowdb_qry_result_t *res) {
	nowdb_err_t err;
	nowdb_ast_t *op;
	nowdb_ast_t *trg;

	if (scope == NULL) INVALIDAST("no scope");
	
	res->resType = NOWDB_QRY_RESULT_REPORT;
	res->result = NULL;

	op = nowdb_ast_operation(ast);
	if (op == NULL) INVALIDAST("no operation in AST");
	
	trg = nowdb_ast_target(ast);
	if (trg == NULL) INVALIDAST("no target in AST");

	err = adjustTarget(scope, trg);
	if (err != NOWDB_OK) return err;

	if (op->ntype != NOWDB_AST_LOAD) INVALIDAST("invalid operation");
	
	return handleLoad(op, trg, scope, res);
}

/* -------------------------------------------------------------------------
 * Helper: destroy list of values
 * -------------------------------------------------------------------------
 */
static inline void destroyValues(ts_algo_list_t *values) {
	ts_algo_list_node_t *runner, *tmp;
	nowdb_simple_value_t *v;

	runner=values->head;
	while(runner!=NULL) {
		tmp = runner->nxt;
		ts_algo_list_remove(values, runner);
		v = runner->cont;
		if (v->value != NULL) free(v->value);
		free(runner->cont);
		free(runner);
		runner=tmp;
	}
}

/* -------------------------------------------------------------------------
 * Handle insert statement
 * -------------------------------------------------------------------------
 */
static nowdb_err_t handleInsert(nowdb_ast_t      *op,
                                nowdb_ast_t      *trg,
                                nowdb_scope_t    *scope,
                                nowdb_proc_t     *proc,
                                nowdb_qry_result_t *res) {
	nowdb_err_t err=NOWDB_OK;
	ts_algo_list_t fields, *fptr=NULL;
	ts_algo_list_t values;
	nowdb_simple_value_t *val;
	nowdb_ast_t *f, *v;
	nowdb_dml_t *dml;
	nowdb_qry_report_t *rep;
	struct timespec t1, t2;

	nowdb_timestamp(&t1);

	ts_algo_list_init(&fields);
	ts_algo_list_init(&values);

	f = nowdb_ast_field(op);
	if (f != NULL) fptr = &fields;
	while (f != NULL) {
		if (ts_algo_list_append(&fields, f->value) != TS_ALGO_OK) {
			ts_algo_list_destroy(&fields);
			NOMEM("fields.append");
			return err;
		}
		f = nowdb_ast_field(f);
	}

	v = nowdb_ast_value(op);
	while(v != NULL) {
		err = expr2simplev(scope, trg, v, &val);
		if (err != NOWDB_OK) break;

		if (ts_algo_list_append(&values, val) != TS_ALGO_OK) {
			NOMEM("values.append");
			break;
		}
		v = nowdb_ast_field(v);
	}
	
	if (err != NOWDB_OK) goto cleanup;

	dml = nowdb_proc_getDML(proc);
	if (dml == NULL) {
		err = nowdb_err_get(nowdb_err_no_rsc, FALSE,
			      OBJECT, "no scope in session");
		goto cleanup;
	}
	err = nowdb_dml_setTarget(dml, trg->value, fptr, &values);
	if (err != NOWDB_OK) goto cleanup;

	err = nowdb_dml_insertFields(dml, fptr, &values);
	if (err != NOWDB_OK) goto cleanup;

	nowdb_timestamp(&t2);

	/* create a report */
	rep = calloc(1, sizeof(nowdb_qry_report_t));
	if (rep == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                               "allocating report");
	} else {
		rep->affected = 1;
		rep->errors = 0;
		rep->runtime = nowdb_time_minus(&t2, &t1)/1000;
		res->result = rep;
	}

cleanup:
	ts_algo_list_destroy(&fields);
	destroyValues(&values);

	return err;
}

/* -------------------------------------------------------------------------
 * Handle update statement
 * -------------------------------------------------------------------------
 */
static nowdb_err_t handleUpdate(nowdb_ast_t      *op,
                                nowdb_ast_t      *trg,
                                nowdb_scope_t    *scope,
                                nowdb_qry_result_t *res) {
	return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT,
	                          "update not yet implemented");
}

/* -------------------------------------------------------------------------
 * Handle delete statement
 * -------------------------------------------------------------------------
 */
static nowdb_err_t handleDelete(nowdb_ast_t      *op,
                                nowdb_ast_t      *trg,
                                nowdb_scope_t    *scope,
                                nowdb_qry_result_t *res) {
	return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT,
	                          "delete not yet implemented");
}

/* -------------------------------------------------------------------------
 * Handle DML statement
 * TODO
 * -------------------------------------------------------------------------
 */
static nowdb_err_t handleDML(nowdb_ast_t *ast,
                         nowdb_scope_t *scope,
                             void        *rsc,
                      nowdb_qry_result_t *res) {
	nowdb_ast_t *op;
	nowdb_ast_t *trg;

	if (scope == NULL) INVALIDAST("no scope");

	/* result is a report */
	res->resType = NOWDB_QRY_RESULT_REPORT;
	res->result = NULL;

	op = nowdb_ast_operation(ast);
	if (op == NULL) INVALIDAST("no operation in AST");
	
	trg = nowdb_ast_target(op);
	if (trg == NULL) INVALIDAST("no target in AST");

	switch(op->ntype) {
	case NOWDB_AST_INSERT:
		return handleInsert(op, trg, scope, rsc, res);
	case NOWDB_AST_UPDATE:
		return handleUpdate(op, trg, scope, res);
	case NOWDB_AST_DELETE: 
		return handleDelete(op, trg, scope, res);
	default: INVALIDAST("invalid operation");
	}
}

/* -------------------------------------------------------------------------
 * Handle DQL statement
 * -------------------------------------------------------------------------
 */
static nowdb_err_t handleDQL(nowdb_ast_t *ast,
                         nowdb_scope_t *scope,
                      nowdb_qry_result_t *res) {
	nowdb_err_t err;
	nowdb_cursor_t *cur;
	ts_algo_list_t plan;

	if (scope == NULL) INVALIDAST("no scope");

	/* result is a cursor */
	res->resType = NOWDB_QRY_RESULT_CURSOR;
	res->result = NULL;

	/* transform ast -> plan, i.e.
	 * create and optimise plan */
	ts_algo_list_init(&plan);
	err = nowdb_plan_fromAst(scope, ast, &plan);
	if (err != NOWDB_OK) return err;

	/* create cursor */
	err = nowdb_cursor_new(scope, &plan, &cur);
	if (err != NOWDB_OK) {
		nowdb_plan_destroy(&plan, TRUE);
		return err;
	}

	/* cleanup and terminate */
	nowdb_plan_destroy(&plan, FALSE);
	res->result = cur;
	return NOWDB_OK;
}

/* -------------------------------------------------------------------------
 * Handle Misc statement
 * -------------------------------------------------------------------------
 */
static nowdb_err_t handleMisc(nowdb_ast_t *ast,
                          nowdb_scope_t *scope,
                          void            *rsc,
                          nowdb_path_t    base,
                       nowdb_qry_result_t *res) {
	nowdb_ast_t *op;

	op = nowdb_ast_operation(ast);
	if (op == NULL) INVALIDAST("no operation in AST");
	switch(op->ntype) {

	case NOWDB_AST_USE: 
		res->resType = NOWDB_QRY_RESULT_SCOPE;
		res->result = NULL;
		return handleUse(op, rsc, base, res);

	case NOWDB_AST_EXEC:
		return handleExec(scope, op, rsc, res);

	case NOWDB_AST_FETCH: 
	case NOWDB_AST_CLOSE: 

		if (scope == NULL) INVALIDAST("no scope");

		res->resType = NOWDB_QRY_RESULT_OP;
		res->result = op;
		return NOWDB_OK;

	case NOWDB_AST_SELECT: 
		res->resType = NOWDB_QRY_RESULT_NOTHING;
		res->result = NULL;
		return handleSelect(scope, op, res);

	case NOWDB_AST_LOCK:
	case NOWDB_AST_UNLOCK:
		res->resType = NOWDB_QRY_RESULT_NOTHING;
		res->result = NULL;
		return handleLock(scope, op, rsc, res);

	default: INVALIDAST("invalid operation in AST");
	}
}

/* -------------------------------------------------------------------------
 * Handle generic statement
 * -------------------------------------------------------------------------
 */
nowdb_err_t nowdb_stmt_handle(nowdb_ast_t *ast,
                          nowdb_scope_t *scope,
                          void            *rsc,
                          nowdb_path_t    base,
                       nowdb_qry_result_t *res) {
	if (ast == NULL) return nowdb_err_get(nowdb_err_invalid,
	                    FALSE, OBJECT, "ast objet is NULL");
	if (res == NULL) return nowdb_err_get(nowdb_err_invalid,
	                 FALSE, OBJECT, "result objet is NULL");
	switch(ast->ntype) {
	case NOWDB_AST_DDL: return handleDDL(ast, scope, rsc, base, res);
	case NOWDB_AST_DLL: return handleDLL(ast, scope, res);
	case NOWDB_AST_DML: return handleDML(ast, scope, rsc, res);
	case NOWDB_AST_DQL: return handleDQL(ast, scope, res);
	case NOWDB_AST_MISC: return handleMisc(ast, scope, rsc, base, res);
	default: return nowdb_err_get(nowdb_err_invalid,
	                  FALSE, OBJECT, "invalid ast");
	}
}
