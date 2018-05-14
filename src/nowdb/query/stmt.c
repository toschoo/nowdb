#include <nowdb/query/stmt.h>

#define INVALIDAST(s) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, s)

static char *OBJECT = "stmt";

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

static nowdb_err_t openScope(nowdb_path_t     base,
                             char            *name,
                             nowdb_scope_t **scope) {
	nowdb_path_t p;
	nowdb_err_t err;

	p = nowdb_path_append(base, name);
	if (p == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                        FALSE, OBJECT, "path.append");

	err = nowdb_scope_new(scope, p, NOWDB_VERSION); free(p);
	if (err != NOWDB_OK) return err;

	err = nowdb_scope_open(*scope);
	if (err != NOWDB_OK) {
		nowdb_scope_destroy(*scope); free(*scope);
		return err;
	}
	return NOWDB_OK;
}

static nowdb_err_t createIndex(nowdb_ast_t  *op,
                               char       *name,
                           nowdb_scope_t *scope) {
	return nowdb_err_get(nowdb_err_not_supp,
	           FALSE, OBJECT, "createIndex");
}

static nowdb_err_t load(nowdb_scope_t    *scope,
                        nowdb_path_t       path,
                        nowdb_ast_t        *trg,
                        nowdb_bool_t        ign,
                        nowdb_qry_result_t *res) {
	FILE *stream;
	nowdb_err_t err=NOWDB_OK;
	nowdb_context_t *ctx;
	nowdb_loader_t   ldr;
	nowdb_bitmap32_t flg=0;
	nowdb_qry_report_t *rep;

	if (ign) flg |= NOWDB_CSV_HAS_HEADER;

	stream = fopen(path, "rb");
	if (stream == NULL) return nowdb_err_get(nowdb_err_open,
		                            TRUE, OBJECT, path);

	switch(trg->stype) {
	case NOWDB_AST_VERTEX:
		fprintf(stderr, "loading '%s' into vertex\n", path);

		flg |= NOWDB_CSV_VERTEX;
		err = nowdb_loader_init(&ldr, stream, stderr,
		                      &scope->vertices, flg);
		if (err != NOWDB_OK) {
			fclose(stream); return err;
		}
		break;

	case NOWDB_AST_CONTEXT:
		if (trg->value == NULL) INVALIDAST("no target name in AST");
		err = nowdb_scope_getContext(scope, trg->value, &ctx);
		if (err != NOWDB_OK) return err;

		fprintf(stderr, "loading '%s' into '%s'\n", path, ctx->name);

		err = nowdb_loader_init(&ldr, stream, stderr,
		                           &ctx->store, flg);
		if (err != NOWDB_OK) {
			fclose(stream); return err;
		}
		break;
	
	default:
		fclose(stream); 
		INVALIDAST("invalid target for load");
	}

	err = nowdb_loader_run(&ldr);
	fclose(stream); 

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
	nowdb_loader_destroy(&ldr); // <- will destroy the error!
	return err;
}

static nowdb_err_t applyCreateOptions(nowdb_ast_t *opts,
                                nowdb_ctx_config_t *cfg) {
	nowdb_ast_t *o;
	uint64_t cfgopts = 0;
	uint64_t utmp;

	if (opts == NULL) {
		cfgopts = NOWDB_CONFIG_SIZE_BIG        |
		          NOWDB_CONFIG_INSERT_CONSTANT | 
		          NOWDB_CONFIG_DISK_HDD;
		nowdb_ctx_config(cfg, cfgopts);
		return NOWDB_OK;
	}

	o = nowdb_ast_option(opts, NOWDB_AST_SIZING);
	if (o == NULL) cfgopts |= NOWDB_CONFIG_SIZE_BIG;
	else {
		if (nowdb_ast_getUInt(o, &utmp) != 0)
			INVALIDAST("invalid ast: invalid sizing");
		cfgopts |= utmp;
	}

	o = nowdb_ast_option(opts, NOWDB_AST_STRESS);
	if (o == NULL) cfgopts |= NOWDB_CONFIG_INSERT_CONSTANT;
	else {
		if (nowdb_ast_getUInt(o, &utmp) != 0)
			INVALIDAST("invalid ast: invalid throughput");
		cfgopts |= utmp;
	}

	o = nowdb_ast_option(opts, NOWDB_AST_DISK);
	if (o == NULL) cfgopts |= NOWDB_CONFIG_DISK_HDD;
	else {
		if (nowdb_ast_getUInt(o, &utmp) != 0)
			INVALIDAST("invalid ast: invalid disk");
		cfgopts |= utmp;
	}

	o = nowdb_ast_option(opts, NOWDB_AST_COMP);
	if (o != NULL) cfgopts |= NOWDB_CONFIG_NOCOMP;

	o = nowdb_ast_option(opts, NOWDB_AST_SORT);
	if (o != NULL) cfgopts |= NOWDB_CONFIG_NOSORT;

	nowdb_ctx_config(cfg, cfgopts);

	return NOWDB_OK;
}

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

static nowdb_err_t createContext(nowdb_ast_t  *op,
                                 char       *name,
                             nowdb_scope_t *scope) {
	nowdb_err_t err;
	nowdb_ast_t *opts, *o;
	nowdb_ctx_config_t cfg;

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

	err = applyCreateOptions(opts, &cfg);
	if (err != NOWDB_OK) return err;

	err = applyGenericOptions(opts, &cfg);
	if (err != NOWDB_OK) return err;

	return nowdb_scope_createContext(scope, name, &cfg);
}

static nowdb_err_t handleUse(nowdb_ast_t        *ast,
                             nowdb_path_t       base,
                             nowdb_qry_result_t *res) {
	nowdb_scope_t *scope;
	nowdb_err_t err;
	char *name;

	name = nowdb_ast_getString(ast);
	if (name == NULL) INVALIDAST("no name in AST");

	err = openScope(base, name, &scope);
	if (err != NOWDB_OK) return err;

	res->result = scope;

	return NOWDB_OK;
}

static nowdb_err_t handleDDL(nowdb_ast_t *ast,
                         nowdb_scope_t *scope,
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

	if (op->ntype == NOWDB_AST_CREATE) {
		switch(trg->stype) {
		case NOWDB_AST_SCOPE:
			return createScope(op, base, trg->value);
		case NOWDB_AST_CONTEXT:
			return createContext(op, trg->value, scope);
		case NOWDB_AST_INDEX:
			return createIndex(op, trg->value, scope);
		default: INVALIDAST("invalid target in AST");
		}
	}
	// alter
	// drop
	return nowdb_err_get(nowdb_err_not_supp,
	            FALSE, OBJECT, "handleDDL");
}

static nowdb_err_t handleLoad(nowdb_ast_t *op,
                              nowdb_ast_t *trg,
                              nowdb_scope_t *scope,
                              nowdb_qry_result_t *res) {
	size_t s;
	nowdb_err_t  err;
	nowdb_bool_t ign = FALSE;
	nowdb_path_t p, tmp;
	nowdb_ast_t *opts, *o;

	opts = nowdb_ast_option(op, 0);
	if (opts != NULL) {
		o = nowdb_ast_option(opts, NOWDB_AST_IGNORE);
		if (o != NULL) ign = TRUE;
	}
	if (op->value == NULL) INVALIDAST("no path in load operation");

	tmp = op->value; s = strlen(tmp);
	if (s < 1) INVALIDAST("incomplete path value");

	p = malloc(s);
	if (p == NULL) return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
	                                                 "allocating path");
	if (tmp[0] == '\'') {
		strcpy(p, tmp+1); p[s-2] = 0;
	} else {
		strcpy(p, tmp);
	}
	err = load(scope, p, trg, ign, res); free(p);
	return err;
}

static nowdb_err_t handleDLL(nowdb_ast_t *ast,
                         nowdb_scope_t *scope,
                      nowdb_qry_result_t *res) {
	nowdb_ast_t *op;
	nowdb_ast_t *trg;
	
	res->resType = NOWDB_QRY_RESULT_REPORT;
	res->result = NULL;
	
	op = nowdb_ast_operation(ast);
	if (op == NULL) INVALIDAST("no operation in AST");
	
	trg = nowdb_ast_target(ast);
	if (trg == NULL) INVALIDAST("no target in AST");

	if (op->ntype != NOWDB_AST_LOAD) INVALIDAST("invalid operation");
	
	return handleLoad(op, trg, scope, res);
}

static nowdb_err_t handleDML(nowdb_ast_t *ast,
                         nowdb_scope_t *scope,
                      nowdb_qry_result_t *res) {
	return nowdb_err_get(nowdb_err_not_supp,
	            FALSE, OBJECT, "handleDML");
}

static nowdb_err_t handleDQL(nowdb_ast_t *ast,
                         nowdb_scope_t *scope,
                      nowdb_qry_result_t *res) {
	return nowdb_err_get(nowdb_err_not_supp,
	            FALSE, OBJECT, "handleDQL");
}

static nowdb_err_t handleMisc(nowdb_ast_t *ast,
                          nowdb_scope_t *scope,
                          nowdb_path_t    base,
                       nowdb_qry_result_t *res) {
	nowdb_ast_t *op;

	op = nowdb_ast_operation(ast);
	if (op == NULL) INVALIDAST("no operation in AST");
	switch(op->ntype) {

	case NOWDB_AST_USE: 
		res->resType = NOWDB_QRY_RESULT_SCOPE;
		res->result = NULL;
		return handleUse(op, base, res);

	default: INVALIDAST("invalid operation in AST");
	}
}

nowdb_err_t nowdb_stmt_handle(nowdb_ast_t *ast,
                          nowdb_scope_t *scope,
                          nowdb_path_t    base,
                       nowdb_qry_result_t *res) {
	if (ast == NULL) return nowdb_err_get(nowdb_err_invalid,
	                    FALSE, OBJECT, "ast objet is NULL");
	if (res == NULL) return nowdb_err_get(nowdb_err_invalid,
	                 FALSE, OBJECT, "result objet is NULL");
	switch(ast->ntype) {
	case NOWDB_AST_DDL: return handleDDL(ast, scope, base, res);
	case NOWDB_AST_DLL: return handleDLL(ast, scope, res);
	case NOWDB_AST_DML: return handleDML(ast, scope, res);
	case NOWDB_AST_DQL: return handleDQL(ast, scope, res);
	case NOWDB_AST_MISC: return handleMisc(ast, scope, base, res);
	default: return nowdb_err_get(nowdb_err_invalid,
	                  FALSE, OBJECT, "invalid ast");
	}
}

