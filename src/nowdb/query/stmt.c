/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Statement Interface
 * ========================================================================
 */
#include <nowdb/query/stmt.h>
#include <nowdb/query/plan.h>
#include <nowdb/query/cursor.h>
#include <nowdb/index/index.h>
#include <nowdb/ifc/nowdb.h>
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
	return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, s)

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

	err = nowdb_scope_new(&scope, p, NOWDB_VERSION); free(p);
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
		err = nowdb_scope_new(&scope, p, NOWDB_VERSION);free(p);
		if (err != NOWDB_OK) return err;

		err = nowdb_scope_drop(scope);
		if (err != NOWDB_OK) return err;

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
		err = nowdb_scope_new(&scope, p, NOWDB_VERSION);
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

	err = nowdb_scope_new(scope, path, NOWDB_VERSION);
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
	k->off = calloc(sz,sizeof(nowdb_index_keys_t));
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
		}

		p->name  = strdup(d->value);
		if (p->name == NULL) {
			destroyProps(&props);
			NOMEM("allocating property name");
		}

		p->value = nowdb_ast_type(d->stype);
		p->propid = 0;
		p->roleid = 0;
		p->pk = FALSE;

		o = nowdb_ast_option(d, NOWDB_AST_PK);
		if (o != NULL) p->pk = TRUE;

		if (ts_algo_list_append(&props, p) != TS_ALGO_OK) {
			destroyProps(&props);
			NOMEM("list.addpend");
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
	
	return NOWDB_OK;
}

/* -------------------------------------------------------------------------
 * Drop Type
 * -------------------------------------------------------------------------
 */
static nowdb_err_t dropType(nowdb_ast_t  *op,
                            char       *name,
                        nowdb_scope_t *scope)  {
	nowdb_err_t err;
	nowdb_ast_t  *o;

	err = nowdb_scope_dropType(scope, name);
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
	uint32_t weight = NOWDB_TYP_NOTHING;
	uint32_t weight2 = NOWDB_TYP_NOTHING;
	uint32_t label = NOWDB_TYP_NOTHING;

	d = nowdb_ast_declare(op);
	if (d == NULL) INVALIDAST("no declarations in AST");

	while(d != NULL) {
		if (d->stype == NOWDB_AST_TYPE) {
			f = nowdb_ast_off(d);
			if (f == NULL) INVALIDAST("no offset in decl");
			if (f->stype == NOWDB_OFF_ORIGIN) {
				origin = d->value;
			} else {
				destin = d->value;
			}
		} else {
			switch((uint32_t)(uint64_t)d->value) {
			case NOWDB_OFF_LABEL:
				label = nowdb_ast_type(d->stype); break;
			case NOWDB_OFF_WEIGHT:
				weight = nowdb_ast_type(d->stype); break;
			case NOWDB_OFF_WEIGHT2:
				weight2 = nowdb_ast_type(d->stype); break;
			default:
				INVALIDAST("unknown field in edge");
			}
		}
		d = nowdb_ast_declare(d);
	}
	if (origin == NULL && destin == NULL) {
		INVALIDAST("no vertex in edge");
	}
	err = nowdb_scope_createEdge(scope, name, origin, destin,
	                                  label, weight, weight2);
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
 * Drop Edge
 * -------------------------------------------------------------------------
 */
static nowdb_err_t dropEdge(nowdb_ast_t  *op,
                            char       *name,
                        nowdb_scope_t *scope)  {
	nowdb_err_t err;
	nowdb_ast_t  *o;

	err = nowdb_scope_dropEdge(scope, name);
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

	if (strcasecmp(lang, "python") == 0) lx = NOWDB_STORED_PYTHON;
	else INVALIDAST("unknown language in AST");

	// return type
	r = nowdb_ast_option(op, NOWDB_AST_TYPE);
	if (r == NULL) rx = 0;
	else {
		rx = nowdb_ast_type((uint16_t)(uint64_t)r->value);
	}

	// make proc descriptor
	pd = calloc(1, sizeof(nowdb_proc_desc_t));
	if (pd == NULL) NOMEM("allocating proc descriptor");

	pd->name = strdup(name);
	if (pd->name == NULL) {
		free(pd);
		NOMEM("allocating procedure name");
	}

	pd->module = strdup(module);
	if (pd->module == NULL) {
		free(pd->name); free(pd);
		NOMEM("allocating module name");
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

	fprintf(stderr, "parameters: %u\n", pd->argn);
	pd->args = calloc(pd->argn, sizeof(nowdb_proc_arg_t));
	if (pd->args == NULL) {
		nowdb_proc_desc_destroy(pd); free(pd);
		NOMEM("allocating procedure parameters");
	}

	p = nowdb_ast_declare(op); i=0;
	while(p!=NULL) {
		fprintf(stderr, "parameter %s (%d)\n",
		            (char*)p->value, p->stype);

		pd->args[i].name = strdup(p->value);
		if (pd->args[i].name == NULL) {
			nowdb_proc_desc_destroy(pd); free(pd);
			NOMEM("allocating parameter name");
		}
		pd->args[i].typ = nowdb_ast_type(p->stype);
		pd->args[i].pos = i; // caution!

		p = nowdb_ast_declare(p); i++;
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
                        nowdb_ast_t        *trg,
                        char              *type,
                        nowdb_bitmap32_t    flg,
                        nowdb_qry_result_t *res) {
	FILE *stream;
	nowdb_err_t err=NOWDB_OK;
	nowdb_context_t *ctx;
	nowdb_loader_t   ldr;
	nowdb_qry_report_t *rep;

	/* open stream from path */
	stream = fopen(path, "rb");
	if (stream == NULL) return nowdb_err_get(nowdb_err_open,
		                            TRUE, OBJECT, path);

	switch(trg->stype) {

	/* create vertex loader */
	case NOWDB_AST_VERTEX:
		fprintf(stderr, "loading '%s' into vertex as type '%s'\n",
		                path, type);

		flg |= NOWDB_CSV_VERTEX;
		err = nowdb_loader_init(&ldr, stream, stderr,
		                        &scope->vertices,
		                        scope->model,
		                        scope->text,
		                        type,  flg);
		if (err != NOWDB_OK) {
			fclose(stream); return err;
		}
		break;

	/* create context loader */
	case NOWDB_AST_CONTEXT:
		if (trg->value == NULL) INVALIDAST("no target name in AST");
		err = nowdb_scope_getContext(scope, trg->value, &ctx);
		if (err != NOWDB_OK) {
			fclose(stream); return err;
		}

		if (type != NULL) {
			fprintf(stderr, "loading '%s' into '%s' as edge\n",
			                                   path, ctx->name);
		} else {
			fprintf(stderr, "loading '%s' into '%s'\n",
			                           path, ctx->name);
		}

		err = nowdb_loader_init(&ldr, stream, stderr,
		                        &ctx->store,
		                        scope->model,
		                        scope->text,
		                        type,  flg);
		if (err != NOWDB_OK) {
			fclose(stream); return err;
		}
		break;
	
	default:
		fclose(stream); 
		INVALIDAST("invalid target for load");
	}

	/* run the loader */
	err = nowdb_loader_run(&ldr);
	fclose(stream); 

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
	nowdb_loader_destroy(&ldr);
	return err;
}

/* -------------------------------------------------------------------------
 * Apply create options on config
 * -------------------------------------------------------------------------
 */
static nowdb_err_t applyCreateOptions(nowdb_ast_t *opts,
                                nowdb_ctx_config_t *cfg) {
	nowdb_ast_t *o;
	uint64_t cfgopts = 0;
	uint64_t utmp;

	/* with no options given: use defaults */
	if (opts == NULL) {
		cfgopts = NOWDB_CONFIG_SIZE_BIG        |
		          NOWDB_CONFIG_INSERT_CONSTANT | 
		          NOWDB_CONFIG_DISK_HDD;
		nowdb_ctx_config(cfg, cfgopts);
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
	nowdb_ctx_config(cfg, cfgopts);

	return NOWDB_OK;
}

/* -------------------------------------------------------------------------
 * Apply explicitly stated options on config
 * -------------------------------------------------------------------------
 */
static nowdb_err_t applyGenericOptions(nowdb_ast_t *opts,
                                 nowdb_ctx_config_t *cfg) {

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
		cfg->allocsize = (uint32_t)utmp;
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
 * Check whether the context exists or not
 * TODO:
 * - we should use "nosuch context" instead of "key not found".
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
 * Create context
 * -------------------------------------------------------------------------
 */
static nowdb_err_t createContext(nowdb_ast_t  *op,
                                 char       *name,
                             nowdb_scope_t *scope) {
	nowdb_err_t err;
	nowdb_ast_t *opts, *o;
	nowdb_ctx_config_t cfg;

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

	/* apply implicit options */
	err = applyCreateOptions(opts, &cfg);
	if (err != NOWDB_OK) return err;

	/* apply explicit options */
	err = applyGenericOptions(opts, &cfg);
	if (err != NOWDB_OK) return err;

	/* create the context */
	return nowdb_scope_createContext(scope, name, &cfg);
}

/* -------------------------------------------------------------------------
 * Drop context
 * -------------------------------------------------------------------------
 */
static nowdb_err_t dropContext(nowdb_ast_t  *op,
                              char          *name,
                              nowdb_scope_t *scope) {
	nowdb_ast_t *o;
	nowdb_err_t err;

	err = nowdb_scope_dropContext(scope, name);
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
 * Helper: load arguments for python function
 * -------------------------------------------------------------------------
 */
#ifdef _NOWDB_WITH_PYTHON
static nowdb_err_t loadPyArgs(nowdb_proc_desc_t *pd,
                              nowdb_ast_t   *params,
                              PyObject       **args) {
	nowdb_err_t err;
	nowdb_ast_t *p;
	nowdb_value_t v;
	int x;
	double d;
	uint64_t u;
	int64_t l;

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
	if (*args == NULL) NOMEM("allocating Python tuple");

	p = params;
	for(uint16_t i=0; i<pd->argn; i++) {
		if (p == NULL) {
			Py_DECREF(*args);
			PYTHONERR("not enough parameters");
			return err;
		}

		/*
		fprintf(stderr, "parameter %hu (%hu): %s\n",
		     pd->args[i].pos,  pd->args[i].typ, (char*)p->value);
		*/

		if (pd->args[i].typ != NOWDB_TYP_TEXT) {
			if (nowdb_strtoval(p->value,
			            pd->args[i].typ,
                                           &v) != 0)
			{
				Py_DECREF(*args);
				INVALID("conversion error");
				return err;
			}
		}

		switch(pd->args[i].typ) {

		case NOWDB_TYP_TEXT:
			if (PyTuple_SetItem(*args,
			   (Py_ssize_t)pd->args[i].pos,
			    Py_BuildValue("s", p->value)) != 0) 
			{
				Py_DECREF(*args);
				PYTHONERR("cannot set item to tuple");
				return err;
			}
			break;
 
		case NOWDB_TYP_FLOAT: 
			memcpy(&d, &v, 8);
			if (PyTuple_SetItem(*args,
			   (Py_ssize_t)pd->args[i].pos,
			    Py_BuildValue("d", d)) != 0) 
			{
				Py_DECREF(*args);
				PYTHONERR("cannot set item to tuple");
				return err;
			}
			break;

		case NOWDB_TYP_UINT: 
			memcpy(&u, &v, 8);
			if (PyTuple_SetItem(*args,
			   (Py_ssize_t)pd->args[i].pos,
			    Py_BuildValue("K", u)) != 0) 
			{
				Py_DECREF(*args);
				PYTHONERR("cannot set item to tuple");
				return err;
			}
			break;

		case NOWDB_TYP_INT: 
		case NOWDB_TYP_DATE: 
		case NOWDB_TYP_TIME: 
			memcpy(&l, &v, 8);
			if (PyTuple_SetItem(*args,
			   (Py_ssize_t)pd->args[i].pos,
			    Py_BuildValue("L", l)) != 0) 
			{
				Py_DECREF(*args);
				PYTHONERR("cannot set item to tuple");
				return err;
			}
			break;

		case NOWDB_TYP_BOOL:
			if (v) {
				x = PyTuple_SetItem(*args,
				    (Py_ssize_t)pd->args[i].pos,
				     Py_True);
			} else {
				x = PyTuple_SetItem(*args,
				    (Py_ssize_t)pd->args[i].pos,
				     Py_False);
			}
			if (x != 0) {
				Py_DECREF(*args);
				PYTHONERR("cannot set item to tuple");
				return err;
			}
			break;

		default:
			fprintf(stderr, "INVALID TYPE (%hu): %hu\n",
			        pd->args[i].pos, pd->args[i].typ);
			Py_DECREF(*args);
			INVALID("invalid parameter type");
			return err;
		}
		p = nowdb_ast_param(p);
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
 * Helper: load arguments for python function
 * -------------------------------------------------------------------------
 */
#ifdef _NOWDB_WITH_PYTHON
static inline nowdb_err_t execPython(nowdb_ast_t        *ast,
                                     nowdb_proc_t      *proc,
                                     nowdb_proc_desc_t   *pd,
                                     PyObject             *f,
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
	err = loadPyArgs(pd, nowdb_ast_param(ast), &args);
	if (err != NOWDB_OK) {
		fprintf(stderr, "PY ARGS NOT LOADED\n");
		nowdb_err_print(err);
		nowdb_proc_updateInterpreter(proc);
		return err;
	}

	r = PyObject_CallObject(f, args);
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
static nowdb_err_t handleExec(nowdb_ast_t        *ast,
                              void               *rsc,
                              nowdb_qry_result_t *res) {
	nowdb_err_t err;
	char *pname;
	nowdb_proc_desc_t *pd;
	nowdb_t *lib;
	void    *f;

	lib = nowdb_proc_getLib(rsc);
	if (lib == NULL) {
		INVALID("no library in session");
		return err;
	}

	pname = ast->value;
	if (pname == NULL) INVALIDAST("procedure without name");

	err = nowdb_proc_loadFun(rsc, pname, &pd, &f);
	if (err != NOWDB_OK) return err;

	switch(pd->lang) {
	case NOWDB_STORED_PYTHON:

 		if (!lib->pyEnabled) {
			INVALID("feature not enabled: Python");
			return err;
		}

#ifdef _NOWDB_WITH_PYTHON
		return execPython(ast, rsc, pd, f, res);
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
	if (trg == NULL) INVALIDAST("no target in AST");

	if (trg->value == NULL) INVALIDAST("no target name in AST");

	if (trg->stype != NOWDB_AST_SCOPE && scope == NULL) {
		INVALIDAST("no scope");
	}

	/* create */
	if (op->ntype == NOWDB_AST_CREATE) {
		switch(trg->stype) {
		case NOWDB_AST_SCOPE:
			return createScope(op, base, trg->value);
		case NOWDB_AST_CONTEXT:
			return createContext(op, trg->value, scope);
		case NOWDB_AST_INDEX:
			return createIndex(op, trg->value, scope);
		case NOWDB_AST_TYPE:
			return createType(op, trg->value, scope);
		case NOWDB_AST_EDGE:
			return createEdge(op, trg->value, scope);
		case NOWDB_AST_PROC:
			return createProc(op, trg, scope);
		default: INVALIDAST("invalid target in AST");
		}
	}
	// alter
	/* drop */
	else if (op->ntype == NOWDB_AST_DROP) {
		switch(trg->stype) {
		case NOWDB_AST_SCOPE:
			return dropScope(op, rsc, base, trg->value);
		case NOWDB_AST_CONTEXT:
			return dropContext(op, trg->value, scope);
		case NOWDB_AST_INDEX:
			return dropIndex(op, trg->value, scope);
		case NOWDB_AST_TYPE:
			return dropType(op, trg->value, scope);
		case NOWDB_AST_EDGE:
			return dropEdge(op, trg->value, scope);
		case NOWDB_AST_PROC:
			return dropProc(op, trg->value, scope);
		default: INVALIDAST("invalid target in AST");
		}
	}
	return nowdb_err_get(nowdb_err_not_supp,
	            FALSE, OBJECT, "handleDDL");
}

/* -------------------------------------------------------------------------
 * Handle load statement
 * -------------------------------------------------------------------------
 */
static nowdb_err_t handleLoad(nowdb_ast_t *op,
                              nowdb_ast_t *trg,
                              nowdb_scope_t *scope,
                              nowdb_qry_result_t *res) {
	size_t s;
	nowdb_err_t  err;
	nowdb_path_t p, tmp;
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
	}
	if (op->value == NULL) INVALIDAST("no path in load operation");

	tmp = op->value; s = strlen(tmp);
	if (s < 1) INVALIDAST("incomplete path value");

	/* remove quotes from value:
	   TODO: this should be done in the sql parser */
	p = malloc(s+1);
	if (p == NULL) return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
	                                                 "allocating path");
	if (tmp[0] == '\'') {
		strcpy(p, tmp+1); p[s-2] = 0;
	} else {
		strcpy(p, tmp);
	}

	/* get model type if any */
	m = nowdb_ast_option(op, NOWDB_AST_TYPE);
	if (m != NULL) {
		type = m->value;
		flgs |= NOWDB_CSV_MODEL;
	}

	/* load and cleanup */
	err = load(scope, p, trg, type, flgs, res); free(p);
	return err;
}

/* -------------------------------------------------------------------------
 * Handle DLL statement
 * -------------------------------------------------------------------------
 */
static nowdb_err_t handleDLL(nowdb_ast_t *ast,
                         nowdb_scope_t *scope,
                      nowdb_qry_result_t *res) {
	nowdb_ast_t *op;
	nowdb_ast_t *trg;

	if (scope == NULL) INVALIDAST("no scope");
	
	res->resType = NOWDB_QRY_RESULT_REPORT;
	res->result = NULL;

	op = nowdb_ast_operation(ast);
	if (op == NULL) INVALIDAST("no operation in AST");
	
	trg = nowdb_ast_target(ast);
	if (trg == NULL) INVALIDAST("no target in AST");

	if (op->ntype != NOWDB_AST_LOAD) INVALIDAST("invalid operation");
	
	return handleLoad(op, trg, scope, res);
}

/* -------------------------------------------------------------------------
 * Handle insert statement
 * -------------------------------------------------------------------------
 */
static nowdb_err_t handleInsert(nowdb_ast_t      *op,
                                nowdb_ast_t      *trg,
                                nowdb_scope_t    *scope,
                                nowdb_qry_result_t *res) {

	// check whether target is context or vertex
	// get model for that context/vertex
	// check that we have a field list
	// check that values and fields correspond
	// insert
	return NOWDB_OK;
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
                      nowdb_qry_result_t *res) {
	nowdb_err_t err;
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
	case NOWDB_AST_INSERT: return handleInsert(op, trg, scope, res);
	case NOWDB_AST_UPDATE: return handleUpdate(op, trg, scope, res);
	case NOWDB_AST_DELETE: return handleDelete(op, trg, scope, res);
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
		return handleExec(op, rsc, res);

	case NOWDB_AST_FETCH: 
	case NOWDB_AST_CLOSE: 

		if (scope == NULL) INVALIDAST("no scope");

		res->resType = NOWDB_QRY_RESULT_OP;
		res->result = op;
		return NOWDB_OK;

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
	case NOWDB_AST_DML: return handleDML(ast, scope, res);
	case NOWDB_AST_DQL: return handleDQL(ast, scope, res);
	case NOWDB_AST_MISC: return handleMisc(ast, scope, rsc, base, res);
	default: return nowdb_err_get(nowdb_err_invalid,
	                  FALSE, OBJECT, "invalid ast");
	}
}
