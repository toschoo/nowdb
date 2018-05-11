#include <nowdb/query/stmt.h>

static char *OBJECT = "statement";

static nowdb_err_t createScope(char *name) {
	return nowdb_err_get(nowdb_err_not_supp,
	           FALSE, OBJECT, "createScope");
}

static nowdb_err_t createIndex(nowdb_ast_t  *op,
                               char       *name,
                           nowdb_scope_t *scope) {
	return nowdb_err_get(nowdb_err_not_supp,
	           FALSE, OBJECT, "createIndex");
}

/* store context options in AST, not SQL options!!! */
static nowdb_err_t createContext(nowdb_ast_t  *op,
                                 char       *name,
                             nowdb_scope_t *scope) {
	nowdb_ast_t *opts, *o;
	nowdb_ctx_config_t cfg;
	uint64_t cfgopts = 0;

	opts = nowdb_ast_option(op, 0);
	if (opts == NULL) {
		cfgopts = NOWDB_CONFIG_SIZE_BIG      |
		          NOWDB_CONFIG_INSERT_STRESS | 
		          NOWDB_CONFIG_DISK_HDD;
	} else {
		o = nowdb_ast_option(opts, NOWDB_AST_SIZING);
		if (o == NULL) cfgopts |= NOWDB_CONFIG_SIZE_BIG;
		else cfgopts |= (uint64_t)o->value;

		o = nowdb_ast_option(opts, NOWDB_AST_THROUGHP);
		if (o == NULL) cfgopts |= NOWDB_CONFIG_INSERT_STRESS;
		else cfgopts |= (uint64_t)o->value;

		o = nowdb_ast_option(opts, NOWDB_AST_DISK);
		if (o == NULL) cfgopts |= NOWDB_CONFIG_DISK_HDD;
		else cfgopts |= (uint64_t)o->value;

		o = nowdb_ast_option(opts, NOWDB_AST_COMP);
		if (o != NULL) cfgopts |= NOWDB_CONFIG_NOCOMP;

		o = nowdb_ast_option(opts, NOWDB_AST_SORT);
		if (o != NULL) cfgopts |= NOWDB_CONFIG_NOSORT;
	}

	nowdb_ctx_config(&cfg, cfgopts);

	if (opts != NULL) {
		// set explicit options
		o = nowdb_ast_option(opts, NOWDB_AST_SORTERS);
		o = nowdb_ast_option(opts, NOWDB_AST_ALLOCSZ);
		o = nowdb_ast_option(opts, NOWDB_AST_LARGESZ);
	}

	return nowdb_scope_createContext(scope, name, &cfg);
}

static nowdb_err_t handleDDL(nowdb_ast_t *ast,
                         nowdb_scope_t *scope,
                      nowdb_qry_result_t *res) 
{
	nowdb_ast_t *op;
	nowdb_ast_t *trg;
	
	res->resType = NOWDB_QRY_RESULT_NOTHING;
	res->result = NULL;
	
	op = nowdb_ast_operation(ast);
	if (op == NULL) return nowdb_err_get(nowdb_err_invalid,
	                 FALSE, OBJECT, "no operation in AST");
	
	trg = nowdb_ast_target(ast);
	if (trg == NULL) return nowdb_err_get(nowdb_err_invalid,
	                     FALSE, OBJECT, "no target in AST");

	if (trg->value == NULL) return nowdb_err_get(nowdb_err_invalid,
	                       FALSE, OBJECT, "no target name in AST");

	if (op->ntype == NOWDB_AST_CREATE) {
		switch(trg->stype) {
		case NOWDB_AST_SCOPE: return createScope(trg->value);
		case NOWDB_AST_CONTEXT:
			return createContext(op, trg->value, scope);
		case NOWDB_AST_INDEX:
			return createIndex(op, trg->value, scope);
		default: return nowdb_err_get(nowdb_err_invalid,
		         FALSE, OBJECT, "invalid target in AST");
		}
	}
	// alter
	// drop
	return nowdb_err_get(nowdb_err_not_supp,
	            FALSE, OBJECT, "handleDDL");
}

static nowdb_err_t handleDLL(nowdb_ast_t *ast,
                         nowdb_scope_t *scope,
                      nowdb_qry_result_t *res) {
	return nowdb_err_get(nowdb_err_not_supp,
	            FALSE, OBJECT, "handleDLL");
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

nowdb_err_t nowdb_stmt_handle(nowdb_ast_t *ast,
                          nowdb_scope_t *scope,
                       nowdb_qry_result_t *res) {
	if (ast == NULL) return nowdb_err_get(nowdb_err_invalid,
	                    FALSE, OBJECT, "ast objet is NULL");
	if (res == NULL) return nowdb_err_get(nowdb_err_invalid,
	                 FALSE, OBJECT, "result objet is NULL");
	switch(ast->ntype) {
	case NOWDB_AST_DDL: return handleDDL(ast, scope, res);
	case NOWDB_AST_DLL: return handleDLL(ast, scope, res);
	case NOWDB_AST_DML: return handleDML(ast, scope, res);
	case NOWDB_AST_DQL: return handleDQL(ast, scope, res);
	default: return nowdb_err_get(nowdb_err_invalid,
	                  FALSE, OBJECT, "invalid ast");
	}
}

