/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Execution Plan
 * --------------
 * quite simple now, but will be the most complex thing
 * ========================================================================
 */
#include <nowdb/query/plan.h>
#include <nowdb/reader/filter.h>

#include <string.h>

static char *OBJECT = "plan";

#define INVALIDAST(s) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, s)

static inline nowdb_err_t getField(char    *name,
                                   uint32_t *off,
                                   uint32_t  *sz,
                                   int     *type) {
	if (strcasecmp(name, "VID") == 0) {
		*off = NOWDB_OFF_VERTEX; *sz = 8;
		*type = NOWDB_TYP_UINT; /* may be string! */
		return NOWDB_OK;
	}
	if (strcasecmp(name, "PROPERTY") == 0) {
		*off = NOWDB_OFF_PROP; *sz = 8;
		*type = NOWDB_TYP_UINT; /* may be string! */
		return NOWDB_OK;
	}
	if (strcasecmp(name, "VALUE") == 0) {
		*off = NOWDB_OFF_VALUE; *sz = 8;
		*type = 0; /* we don't know what it is */
		return NOWDB_OK;
	}
	if (strcasecmp(name, "VTYPE") == 0) {
		*off = NOWDB_OFF_VTYPE; *sz = 4;
		*type = NOWDB_TYP_INT;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "ROLE") == 0) {
		*off = NOWDB_OFF_ROLE; *sz = 4;
		*type = NOWDB_TYP_INT;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "EDGE") == 0) {
		*off = NOWDB_OFF_EDGE; *sz = 8;
		*type = NOWDB_TYP_UINT; /* may be string! */
		return NOWDB_OK;
	}
	if (strcasecmp(name, "ORIGIN") == 0) {
		*off = NOWDB_OFF_ORIGIN; *sz = 8;
		*type = NOWDB_TYP_UINT; /* may be string! */
		return NOWDB_OK;
	}
	if (strcasecmp(name, "DESTIN") == 0) {
		*off = NOWDB_OFF_DESTIN; *sz = 8;
		*type = NOWDB_TYP_UINT; /* may be string! */
		return NOWDB_OK;
	}
	if (strcasecmp(name, "LABEL") == 0) {
		*off = NOWDB_OFF_LABEL; *sz = 8;
		*type = NOWDB_TYP_UINT; /* may be string! */
		return NOWDB_OK;
	}
	if (strcasecmp(name, "TIMESTAMP") == 0) {
		*off = NOWDB_OFF_TMSTMP; *sz = 8; 
		*type = NOWDB_TYP_TIME;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "WEIGHT") == 0) {
		*off = NOWDB_OFF_WEIGHT; *sz = 8; 
		*type = 0; /* we don't know what it is */
		return NOWDB_OK;
	}
	if (strcasecmp(name, "WEIGHT2") == 0) {
		*off = NOWDB_OFF_WEIGHT; *sz = 8; 
		*type = 0; /* we don't know what it is */
		return NOWDB_OK;
	}
	if (strcasecmp(name, "WTYPE") == 0) {
		*off = NOWDB_OFF_WTYPE; *sz = 4; 
		*type = NOWDB_TYP_INT;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "WTYPE2") == 0) {
		*off = NOWDB_OFF_WTYPE2; *sz = 4; 
		*type = NOWDB_TYP_INT;
		return NOWDB_OK;
	}
	*off = 0; *sz = 0; *type = 0;
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                     "unknown field");
}

static inline nowdb_err_t getValue(char    *str,
                                   uint32_t  sz,
                                   int     *typ,
                                   int    stype,
                                   void **value) {
	char *tmp;

	*value = malloc(sz);
	if (value == NULL) return nowdb_err_get(nowdb_err_no_mem,
		             FALSE, OBJECT, "allocating buffer");
	
	if (*typ == 0) {
		switch(stype) {
		case NOWDB_AST_FLOAT: *typ = NOWDB_TYP_FLOAT; break;
		case NOWDB_AST_UINT: *typ = NOWDB_TYP_UINT; break;
		case NOWDB_AST_INT: *typ = NOWDB_TYP_INT; break;
		case NOWDB_AST_TEXT:
		default: *value = NULL; return NOWDB_OK;
		}
	}

	switch(*typ) {
	case NOWDB_TYP_FLOAT:
		**(double**)value = (double)strtod(str, &tmp);
		break;

	case NOWDB_TYP_UINT:
		if (sz == 4) 
			**(uint32_t**)value = (uint32_t)strtoul(str, &tmp, 10);
		else
			**(uint64_t**)value = (uint64_t)strtoul(str, &tmp, 10);
		break;

	case NOWDB_TYP_TIME:
	case NOWDB_TYP_INT:
		if (sz == 4) 
			**(int32_t**)value = (int32_t)strtol(str, &tmp, 10);
		else 
			**(int64_t**)value = (int64_t)strtol(str, &tmp, 10);
		break;

	default: return nowdb_err_get(nowdb_err_panic, FALSE, OBJECT,
	                                          "unexpected type");
	}
	if (*tmp != 0) return nowdb_err_get(nowdb_err_invalid,
	                  FALSE, OBJECT, "conversion failed");
	return NOWDB_OK;
}

static inline nowdb_err_t getCompare(nowdb_filter_t **comp, nowdb_ast_t *ast) {
	nowdb_err_t err;
	nowdb_ast_t *op1, *op2;
	uint32_t off, sz;
	int typ;

	op1 = nowdb_ast_operand(ast, 1);
	if (op1 == NULL) INVALIDAST("no first operand in compare");

	op2 = nowdb_ast_operand(ast, 2);
	if (op2 == NULL) INVALIDAST("no second operand in compare");

	if (op1->value == NULL) INVALIDAST("first operand in compare is NULL");
	if (op2->value == NULL) INVALIDAST("second operand in compare is NULL");

	/* check whether op1 is field or value */
	if (op1->ntype == NOWDB_AST_FIELD) {
		err = getField(op1->value, &off, &sz, &typ);
		if (err != NOWDB_OK) return err;
		err = getValue(op2->value, sz, &typ, op2->stype, &ast->conv);
	} else {
		err = getField(op2->value, &off, &sz, &typ);
		if (err != NOWDB_OK) return err;
		err = getValue(op1->value, sz, &typ, op1->stype, &ast->conv);
	}
	if (err != NOWDB_OK) return err;

	err = nowdb_filter_newCompare(comp, ast->stype,
	                      off, sz, typ, ast->conv);
	if (err != NOWDB_OK) return err;
	
	return NOWDB_OK;
}

static inline nowdb_err_t getCondition(nowdb_filter_t **b, nowdb_ast_t *ast) {
	int op;
	nowdb_err_t err;

	switch(ast->ntype) {
	case NOWDB_AST_COMPARE: return getCompare(b, ast);
	case NOWDB_AST_JUST: return getCondition(b, nowdb_ast_operand(ast,1));

	case NOWDB_AST_NOT:
		err = nowdb_filter_newBool(b, NOWDB_FILTER_NOT);
		if (err != NOWDB_OK) return err;
		return getCondition(&(*b)->left, nowdb_ast_operand(ast, 1));

	case NOWDB_AST_AND:
	case NOWDB_AST_OR:
		op = ast->ntype==NOWDB_AST_AND?NOWDB_FILTER_AND:
		                               NOWDB_FILTER_OR;
		err = nowdb_filter_newBool(b,op);
		if (err != NOWDB_OK) return err;
		err = getCondition(&(*b)->left, nowdb_ast_operand(ast,1));
		if (err != NOWDB_OK) return err;
		return getCondition(&(*b)->right, nowdb_ast_operand(ast,2));

	default:
		fprintf(stderr, "unknown: %d\n", ast->ntype); 
		INVALIDAST("unknown condition type in ast");
	}
}

/* this is way to simple. we need to get the where *per target* and
 * we have to consider aliases. Also, the target may be derived, so
 * we must consider fields that we do not know beforehand */
static inline nowdb_err_t addWhere(nowdb_ast_t *ast, ts_algo_list_t *plan) {
	nowdb_err_t   err;
	nowdb_ast_t  *cond;
	nowdb_plan_t *stp;
	nowdb_filter_t *b=NULL;

	if (ast == NULL) return NOWDB_OK;

	cond = nowdb_ast_operand(ast,1);
	if (cond == NULL) INVALIDAST("no first operand in where");

	// fprintf(stderr, "where: %d->%d\n", ast->ntype, cond->ntype);

	err = getCondition(&b, cond);
	if (err != NOWDB_OK) return err;

	stp = malloc(sizeof(nowdb_plan_t));
	if (stp == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                     FALSE, OBJECT, "allocating plan");
	stp->ntype = NOWDB_PLAN_FILTER;
	stp->stype = 0;
	stp->target = 0;
	stp->name = NULL;
	stp->load = b;

	if (ts_algo_list_append(plan, stp) != TS_ALGO_OK) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                     "list.append");
		free(stp); return err;
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Over-simplistic to get it going:
 * - we assume an ast with a simple target object
 * - we create 1 reader fullscan+ on either vertex or context
 * - that's it
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_plan_fromAst(nowdb_ast_t *ast, ts_algo_list_t *plan) {
	nowdb_err_t   err;
	nowdb_ast_t  *trg, *from;
	nowdb_plan_t *stp;

	from = nowdb_ast_from(ast);
	if (from == NULL) INVALIDAST("no 'from' in DQL");

	trg = nowdb_ast_target(from);
	if (trg == NULL) INVALIDAST("no target in from");

	stp = malloc(sizeof(nowdb_plan_t));
	if (stp == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                      FALSE, OBJECT, "allocating plan");
	stp->ntype = NOWDB_PLAN_SUMMARY;
	stp->stype = 0;
	stp->target = 1;
	stp->name = NULL;
	stp->load = NULL;

	if (ts_algo_list_append(plan, stp) != TS_ALGO_OK) {
		return nowdb_err_get(nowdb_err_no_mem,
	                FALSE, OBJECT, "list append");
	}
	stp = malloc(sizeof(nowdb_plan_t));
	if (stp == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
	                                         "allocating plan");
		nowdb_plan_destroy(plan); return err;
	}
	stp->ntype = NOWDB_PLAN_READER;
	stp->stype = NOWDB_READER_FS_;
	stp->target = trg->stype;
	stp->name = trg->value;
	stp->load = NULL;

	if (ts_algo_list_append(plan, stp) != TS_ALGO_OK) {
		err = nowdb_err_get(nowdb_err_no_mem,
	               FALSE, OBJECT, "list append");
		nowdb_plan_destroy(plan); return err;
	}

	err = addWhere(nowdb_ast_where(ast), plan);
	if (err != NOWDB_OK) {
		nowdb_plan_destroy(plan); return err;
	}
	return NOWDB_OK;
}

void nowdb_plan_destroy(ts_algo_list_t *plan) {
	ts_algo_list_node_t *runner, *tmp;

	if (plan == NULL) return;
	runner = plan->head;
	while(runner!=NULL) {
		free(runner->cont); tmp = runner->nxt;
		ts_algo_list_remove(plan, runner);
		free(runner); runner = tmp;
	}
}

