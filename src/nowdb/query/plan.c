/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Execution Plan
 * ========================================================================
 */
#include <nowdb/query/plan.h>
#include <nowdb/reader/filter.h>

#include <string.h>

static char *OBJECT = "plan";

/* ------------------------------------------------------------------------
 * Macro to wrap the frequent error "invalid ast"
 * ------------------------------------------------------------------------
 */
#define INVALIDAST(s) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, s)

/* ------------------------------------------------------------------------
 * Get field:
 * ----------
 * - sets the offset into the structure
 * - the size of the field
 * - the type of the field (if known!)
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getField(char            *name,
                                   nowdb_ast_t      *trg,
                                   uint32_t         *off,
                                   uint32_t          *sz,
                                   char            isstr,
                                   nowdb_type_t    *type) {
	if (strcasecmp(name, "VID") == 0) {
		*off = NOWDB_OFF_VERTEX; *sz = 8;
		*type = isstr?NOWDB_TYP_TEXT:NOWDB_TYP_UINT;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "PROPERTY") == 0) {
		*off = NOWDB_OFF_PROP; *sz = 8;
		*type = isstr?NOWDB_TYP_TEXT:NOWDB_TYP_UINT;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "VALUE") == 0) {
		*off = NOWDB_OFF_VALUE; *sz = 8;
		*type = isstr?NOWDB_TYP_TEXT:NOWDB_TYP_UINT;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "VTYPE") == 0) {
		*off = NOWDB_OFF_VTYPE; *sz = 4;
		*type = NOWDB_TYP_INT;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "ROLE") == 0) {
		*off = NOWDB_OFF_ROLE; *sz = 4;
		*type = isstr?NOWDB_TYP_TEXT:NOWDB_TYP_UINT;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "EDGE") == 0) {
		*off = NOWDB_OFF_EDGE; *sz = 8;
		*type = isstr?NOWDB_TYP_TEXT:NOWDB_TYP_UINT;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "ORIGIN") == 0) {
		*off = NOWDB_OFF_ORIGIN; *sz = 8;
		*type = isstr?NOWDB_TYP_TEXT:NOWDB_TYP_UINT;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "DESTIN") == 0) {
		*off = NOWDB_OFF_DESTIN; *sz = 8;
		*type = isstr?NOWDB_TYP_TEXT:NOWDB_TYP_UINT;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "LABEL") == 0) {
		*off = NOWDB_OFF_LABEL; *sz = 8;
		*type = isstr?NOWDB_TYP_TEXT:NOWDB_TYP_UINT;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "TIMESTAMP") == 0) {
		*off = NOWDB_OFF_TMSTMP; *sz = 8; 
		*type = isstr?NOWDB_TYP_TEXT:NOWDB_TYP_TIME;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "WEIGHT") == 0) {
		*off = NOWDB_OFF_WEIGHT; *sz = 8; 
		*type = isstr?NOWDB_TYP_TEXT:0;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "WEIGHT2") == 0) {
		*off = NOWDB_OFF_WEIGHT; *sz = 8; 
		*type = isstr?NOWDB_TYP_TEXT:0;
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

/* ------------------------------------------------------------------------
 * Get edge field:
 * ---------------
 * - sets the offset into the structure
 * - the size of the field
 * - the type of the field (if known!)
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getEdgeField(char              *name,
                                       nowdb_model_edge_t   *e,
                                       nowdb_model_vertex_t *o,
                                       nowdb_model_vertex_t *d,
                                       nowdb_ast_t        *trg,
                                       uint32_t           *off,
                                       uint32_t            *sz,
                                       nowdb_type_t      *type) {
	if (strcasecmp(name, "EDGE") == 0) {
		*off = NOWDB_OFF_EDGE; *sz = 8;
		*type = NOWDB_TYP_TEXT;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "ORIGIN") == 0) {
		*off = NOWDB_OFF_ORIGIN; *sz = 8;
		if (o != NULL && o->vid == NOWDB_MODEL_TEXT) {
			*type = NOWDB_TYP_TEXT;
		} else {
			*type = NOWDB_TYP_UINT;
		}
		return NOWDB_OK;
	}
	if (strcasecmp(name, "DESTIN") == 0) {
		*off = NOWDB_OFF_DESTIN; *sz = 8;
		if (d != NULL && d->vid == NOWDB_MODEL_TEXT) {
			*type = NOWDB_TYP_TEXT;
		} else {
			*type = NOWDB_TYP_UINT;
		}
		return NOWDB_OK;
	}
	if (strcasecmp(name, "LABEL") == 0) {
		*off = NOWDB_OFF_LABEL; *sz = 8;
		if (e != NULL && e->edge == NOWDB_MODEL_TEXT) {
			*type = NOWDB_TYP_TEXT;
		} else {
			*type = NOWDB_TYP_UINT;
		}
		return NOWDB_OK;
	}
	if (strcasecmp(name, "TIMESTAMP") == 0) {
		*off = NOWDB_OFF_TMSTMP; *sz = 8; 
		*type = NOWDB_TYP_TIME;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "WEIGHT") == 0) {
		*off = NOWDB_OFF_WEIGHT; *sz = 8; 
		*type = e!=NULL?e->weight:0;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "WEIGHT2") == 0) {
		*off = NOWDB_OFF_WEIGHT; *sz = 8; 
		*type = e!=NULL?e->weight2:0;
		return NOWDB_OK;
	}
	*off = 0; *sz = 0; *type = 0;
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                     "unknown field");
}

/* ------------------------------------------------------------------------
 * Get Value:
 * ----------
 * The type of the value is inferred either from
 * - the type of the field (then *typ is set) or
 * - from the explicit type coming from the ast
 * TODO:
 * - check types
 * - use the model to determine types
 * - handle strings!!!
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getValue(nowdb_scope_t *scope,
                                   char            *str,
				   uint32_t         off,
                                   uint32_t          sz,
                                   nowdb_type_t    *typ,
                                   int            stype,
                                   void         **value) {
	char *tmp;
	nowdb_err_t err;

	*value = malloc(sz);
	if (value == NULL) return nowdb_err_get(nowdb_err_no_mem,
		             FALSE, OBJECT, "allocating buffer");
	
	if (*typ == 0) {
		switch(stype) {
		case NOWDB_AST_FLOAT: *typ = NOWDB_TYP_FLOAT; break;
		case NOWDB_AST_UINT: *typ = NOWDB_TYP_UINT; break;
		case NOWDB_AST_INT: *typ = NOWDB_TYP_INT; break;
		case NOWDB_AST_TEXT: *typ = NOWDB_TYP_TEXT; break;
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

	case NOWDB_TYP_TEXT:
		if (off == NOWDB_OFF_TMSTMP) {
			err = nowdb_time_fromString(str,
			      NOWDB_TIME_FORMAT, *value);
			fprintf(stderr, "%ld\n", **(int64_t**)value);
		} else {
			err = nowdb_text_getKey(scope->text, str, *value);
		}
		if (err != NOWDB_OK) {
			free(*value); *value = NULL;
		}
		return err;

	default: return nowdb_err_get(nowdb_err_panic, FALSE, OBJECT,
	                                          "unexpected type");
	}
	if (*tmp != 0) return nowdb_err_get(nowdb_err_invalid,
	                  FALSE, OBJECT, "conversion failed");
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Get typed Value:
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getTypedValue(nowdb_scope_t     *scope,
                                        char                *str,
                                        nowdb_model_prop_t *prop,
                                        void             **value) {
	nowdb_err_t err;
	char *tmp;

	*value = malloc(sizeof(nowdb_key_t));
	if (value == NULL) return nowdb_err_get(nowdb_err_no_mem,
		             FALSE, OBJECT, "allocating buffer");
	switch(prop->value) {
	case NOWDB_TYP_FLOAT:
		**(double**)value = (double)strtod(str, &tmp);
		break;

	case NOWDB_TYP_UINT:
		**(uint64_t**)value = (uint64_t)strtoul(str, &tmp, 10);
		break;

	case NOWDB_TYP_TIME:
	case NOWDB_TYP_INT:
		**(int64_t**)value = (int64_t)strtol(str, &tmp, 10);
		break;

	case NOWDB_TYP_TEXT:
		err = nowdb_text_getKey(scope->text, str, *value);
		if (err != NOWDB_OK) {
			free(*value); *value = NULL;
		}
		return err;

	default: return nowdb_err_get(nowdb_err_panic, FALSE, OBJECT,
	                                          "unexpected type");
	}
	if (*tmp != 0) {
		free(*value); *value = NULL;
		return nowdb_err_get(nowdb_err_invalid,
	           FALSE, OBJECT, "conversion failed");
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Create an edge filter comparison node from an ast comparison node 
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getEdgeCompare(nowdb_scope_t   *scope,
                                          nowdb_ast_t      *trg,
                                          nowdb_model_edge_t *e,
                                          int          operator,
                                          nowdb_ast_t      *op1,
                                          nowdb_ast_t      *op2,
                                          nowdb_filter_t **comp) {
	nowdb_err_t err;
	nowdb_model_vertex_t *o;
	nowdb_model_vertex_t *d;
	nowdb_ast_t *op;
	void *value;
	uint32_t off,sz;
	nowdb_type_t typ;

	err = nowdb_model_getVertexById(scope->model, e->origin, &o);
	if (err != NOWDB_OK) return err;

	err = nowdb_model_getVertexById(scope->model, e->destin, &d);
	if (err != NOWDB_OK) return err;

	op = op1->ntype == NOWDB_AST_FIELD?op1:op2;

	err = getEdgeField(op->value, e, o, d, trg, &off, &sz, &typ);
	if (err != NOWDB_OK) return err;

	op = op1->ntype == NOWDB_AST_FIELD?op2:op1;

	err = getValue(scope, op->value, off, sz, &typ, 0, &value);
	if (err != NOWDB_OK) return err;

	err = nowdb_filter_newCompare(comp, operator, off, sz, typ, value);
	if (err != NOWDB_OK) {
		free(value); return err;
	}
	nowdb_filter_own(*comp);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Create a typed filter comparison node from an ast comparison node 
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getTypedCompare(nowdb_scope_t    *scope,
                                          nowdb_ast_t        *trg,
                                          nowdb_model_vertex_t *v,
                                          int            operator,
                                          nowdb_ast_t        *op1,
                                          nowdb_ast_t        *op2,
                                          nowdb_filter_t   **comp) {
	nowdb_err_t err;
	nowdb_filter_t *pid=NULL;
	nowdb_filter_t *val=NULL;
	nowdb_filter_t *and;
	nowdb_model_prop_t *p;
	nowdb_ast_t *op;
	void *value;
	uint32_t off=0;

	op = op1->ntype == NOWDB_AST_FIELD?op1:op2;

	// get propid
	err = nowdb_model_getPropByName(scope->model,
	                   v->roleid, op->value, &p);
	if (err != NOWDB_OK) return err;

	// make comp propid == 'propid'
	if (!p->pk) {
		err = nowdb_filter_newCompare(&pid, NOWDB_FILTER_EQ,
		                                     NOWDB_OFF_PROP,
		                                sizeof(nowdb_key_t),
		                                     NOWDB_TYP_UINT,
	                                     	        &p->propid);
		if (err != NOWDB_OK) return err;
	}

	// if pk, it is just vid!
	off = p->pk?NOWDB_OFF_VERTEX:NOWDB_OFF_VALUE;

	op = op1->ntype == NOWDB_AST_FIELD?op2:op1;

	err = getValue(scope, op->value, off, sizeof(nowdb_key_t),
	                                    &p->value, 0, &value);
	if (err != NOWDB_OK) {
		nowdb_filter_destroy(pid);
		free(pid); return err;
	}

	err = nowdb_filter_newCompare(&val, operator, off,
		                      sizeof(nowdb_key_t),
		                                 p->value,
		                                    value);
	if (err != NOWDB_OK) {
		nowdb_filter_destroy(pid); free(pid); 
		free(value); return err;
	}
	nowdb_filter_own(val);

	if (p->pk) {
		*comp = val; return NOWDB_OK;
	}

	err = nowdb_filter_newBool(&and, NOWDB_FILTER_AND);
	if (err != NOWDB_OK) {
		nowdb_filter_destroy(pid); free(pid); 
		nowdb_filter_destroy(val); free(val); 
		free(value); return err;
	}
	and->left = pid;
	and->right = val;
	*comp = and;

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Create a filter comparison node from an ast comparison node 
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getCompare(nowdb_scope_t    *scope,
                                     nowdb_ast_t        *trg,
                                     nowdb_model_edge_t   *e,
                                     nowdb_model_vertex_t *v,
                                     nowdb_filter_t   **comp,
                                     nowdb_ast_t        *ast) {
	nowdb_err_t err;
	nowdb_ast_t *op1, *op2;
	uint32_t off, sz;
	void *conv;
	nowdb_type_t typ;

	op1 = nowdb_ast_operand(ast, 1);
	if (op1 == NULL) INVALIDAST("no first operand in compare");

	op2 = nowdb_ast_operand(ast, 2);
	if (op2 == NULL) INVALIDAST("no second operand in compare");

	if (op1->value == NULL) INVALIDAST("first operand in compare is NULL");
	if (op2->value == NULL) INVALIDAST("second operand in compare is NULL");

	if (trg->stype == NOWDB_AST_TYPE) {
		return getTypedCompare(scope, trg, v,
		          ast->stype, op1, op2, comp);
	} else if (e != NULL) {
		return getEdgeCompare(scope, trg, e,
		         ast->stype, op1, op2, comp);
	}

	/* check whether op1 is field or value */
	if (op1->ntype == NOWDB_AST_FIELD) {
		err = getField(op1->value, trg, &off,
		               &sz, op2->isstr, &typ);
		if (err != NOWDB_OK) return err;
		err = getValue(scope, op2->value, off, sz,
		                 &typ, op2->stype, &conv);
	} else {
		err = getField(op2->value, trg, &off,
		               &sz, op1->isstr, &typ);
		if (err != NOWDB_OK) return err;
		err = getValue(scope, op1->value, off, sz,
		                 &typ, op1->stype, &conv);
	}
	if (err != NOWDB_OK) return err;

	err = nowdb_filter_newCompare(comp, ast->stype,
	                           off, sz, typ, conv);
	if (err != NOWDB_OK) return err;
	nowdb_filter_own(*comp);
	
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Get filter condition (boolean or comparison) from ast condition node
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getCondition(nowdb_scope_t    *scope,
                                       nowdb_ast_t        *trg,
                                       nowdb_model_edge_t   *e,
                                       nowdb_model_vertex_t *v,
                                       nowdb_filter_t      **b,
                                       nowdb_ast_t        *ast) {
	int op;
	nowdb_err_t err;

	switch(ast->ntype) {
	case NOWDB_AST_COMPARE: return getCompare(scope, trg, e, v, b, ast);
	case NOWDB_AST_JUST: return getCondition(scope, trg, e, v, b,
	                                   nowdb_ast_operand(ast,1));

	case NOWDB_AST_NOT:
		err = nowdb_filter_newBool(b, NOWDB_FILTER_NOT);
		if (err != NOWDB_OK) return err;
		return getCondition(scope, trg, e, v, &(*b)->left,
		                       nowdb_ast_operand(ast, 1));

	case NOWDB_AST_AND:
	case NOWDB_AST_OR:
		op = ast->ntype==NOWDB_AST_AND?NOWDB_FILTER_AND:
		                               NOWDB_FILTER_OR;
		err = nowdb_filter_newBool(b,op);
		if (err != NOWDB_OK) return err;
		err = getCondition(scope, trg, e, v, &(*b)->left,
		                       nowdb_ast_operand(ast,1));
		if (err != NOWDB_OK) return err;
		return getCondition(scope, trg, e, v, &(*b)->right,
		                         nowdb_ast_operand(ast,2));

	default:
		INVALIDAST("unknown condition type in ast");
	}
}

/* ------------------------------------------------------------------------
 * Identify index candidates in filter
 * ------------------------------------------------------------------------
 */
static nowdb_err_t idxFromFilter(nowdb_filter_t *filter,
                                 ts_algo_list_t *cands) {
	nowdb_err_t err;
	int x;

	if (filter == NULL) return NOWDB_OK;

	if (filter->ntype == NOWDB_FILTER_COMPARE) {
		if (filter->op == NOWDB_FILTER_EQ) {
			if (ts_algo_list_append(cands,
			        filter) != TS_ALGO_OK) {
				return nowdb_err_get(nowdb_err_no_mem,
				        FALSE, OBJECT, "list.append");
			}
			return NOWDB_OK;
		}
	}
	if (filter->ntype == NOWDB_FILTER_BOOL) {
		if (filter->op == NOWDB_FILTER_AND) {
			x = cands->len;	
			err = idxFromFilter(filter->left, cands);
			if (err != NOWDB_OK) return err;
			if (x == cands->len) return NOWDB_OK;
			err = idxFromFilter(filter->right, cands);
			if (err != NOWDB_OK) return err;
		}
		/* or is multi-index */
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Check whether candidates cover keys
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t cover(ts_algo_list_t     *cands,
                                nowdb_index_t      *idx,
                                nowdb_index_keys_t *keys,
                                ts_algo_list_t     *res,
                                char               *ok) {
	nowdb_filter_t *node;
	ts_algo_list_node_t *runner;

	/* we need a result idx/key */
	/* we need to find it by searching for all keys */

	*ok = 0;
	for(int i=0;i<keys->sz;i++) {
		for(runner=cands->head;runner!=NULL;runner=runner->nxt) {
			node = runner->cont;
			if (keys->off[i] == node->off) {
				if (ts_algo_list_append(res,node)
				                   != TS_ALGO_OK) {
					return nowdb_err_get(nowdb_err_no_mem,
				                FALSE, OBJECT, "list.append");
				}
			}
		}
	}
	if (res->len == keys->sz) *ok = 1;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Compare indexes (descending)
 * ------------------------------------------------------------------------
 */
#define KEY(x) \
	((nowdb_index_keys_t*)x)

static ts_algo_cmp_t comparekeysz(void *one, void *two) {
	if (KEY(one)->sz < KEY(two)->sz) return ts_algo_cmp_greater;
	if (KEY(one)->sz > KEY(two)->sz) return ts_algo_cmp_less;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * make idx+keys
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t makeIndexAndKeys(nowdb_index_t  *idx,
                                           ts_algo_list_t *nodes,
                                           ts_algo_list_t *res) {
	nowdb_plan_idx_t *pidx;
	ts_algo_list_node_t *runner;
	nowdb_filter_t    *node;
	int i=0;

	pidx = calloc(1, sizeof(nowdb_plan_idx_t));
	if (pidx == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                  FALSE, OBJECT, "allocating plan idx");
	pidx->idx = idx;
	pidx->keys = malloc(nodes->len*8);
	if (pidx->keys == NULL) {
		free(pidx);
		return nowdb_err_get(nowdb_err_no_mem,
	            FALSE, OBJECT, "allocating keys");
	}
	for(runner=nodes->head;runner!=NULL;runner=runner->nxt) {
		node = runner->cont;
		memcpy(pidx->keys+i, node->val, node->size);
		i+=node->size;
	}
	if (ts_algo_list_append(res, pidx) != TS_ALGO_OK) {
		free(pidx->keys); free(pidx);
		return nowdb_err_get(nowdb_err_no_mem,
	                FALSE, OBJECT, "list.append");
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Intersect candidates and indices
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t intersect(ts_algo_list_t *cands,
                                    ts_algo_list_t *idxes,
                                    ts_algo_list_t *res) {
	nowdb_err_t err = NOWDB_OK;
	ts_algo_list_node_t *runner;
	nowdb_index_t *idx;
	nowdb_index_keys_t *keys;
	ts_algo_list_t *xes;
	ts_algo_list_t  nodes;
	char x;

	/* sort idxes by keysize */
	xes = ts_algo_list_sort(idxes, &comparekeysz);
	if (xes == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                           FALSE, OBJECT, "list.sort");

	ts_algo_list_init(&nodes);
	for(runner=xes->head;runner!=NULL;runner=runner->nxt) {
		idx = ((nowdb_index_desc_t*)runner->cont)->idx;
		keys = nowdb_index_getResource(idx);
		err = cover(cands, idx, keys, &nodes, &x);
		if (err != NOWDB_OK) break;
		if (x) {
			err = makeIndexAndKeys(idx, &nodes, res);
			break;
		}
		ts_algo_list_destroy(&nodes);
		ts_algo_list_init(&nodes);
	}
	ts_algo_list_destroy(&nodes);
	ts_algo_list_destroy(xes); free(xes);
	return err;
}

/* ------------------------------------------------------------------------
 * Find indices
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getIndices(nowdb_scope_t  *scope,
                                     int             stype,
                                     char           *context,
                                     nowdb_filter_t *filter, 
                                     ts_algo_list_t *res) {
	ts_algo_list_t cands;
	ts_algo_list_t idxes;
	nowdb_err_t err;
	nowdb_context_t *ctx=NULL;

	if (filter == NULL) return NOWDB_OK;

	if (context != NULL && stype == NOWDB_AST_CONTEXT) {
		err = nowdb_scope_getContext(scope, context, &ctx);
		if (err != NOWDB_OK) return err;
	}

	ts_algo_list_init(&cands);
	ts_algo_list_init(&idxes);

	err = nowdb_index_man_getAllOf(scope->iman, ctx, &idxes);
	if (err != NOWDB_OK || idxes.len == 0) {
		ts_algo_list_destroy(&cands);
		ts_algo_list_destroy(&idxes);
		return err;
	}
	
	err = idxFromFilter(filter, &cands);
	if (err != NOWDB_OK) {
		ts_algo_list_destroy(&cands);
		ts_algo_list_destroy(&idxes);
		return err;
	}

	err = intersect(&cands, &idxes, res);

	ts_algo_list_destroy(&cands);
	ts_algo_list_destroy(&idxes);

	return err;
}

/* ------------------------------------------------------------------------
 * Add vertex type to filter
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getType(nowdb_scope_t    *scope,
                                  nowdb_ast_t      *trg,
                                  nowdb_filter_t  **filter,
                                  nowdb_model_vertex_t **v) {
	nowdb_err_t err;

	err = nowdb_model_getVertexByName(scope->model, trg->value, v);
	if (err != NOWDB_OK) return err;

	err = nowdb_filter_newCompare(filter, NOWDB_FILTER_EQ,
	               NOWDB_OFF_ROLE, sizeof(nowdb_roleid_t),
	                       NOWDB_TYP_UINT, &(*v)->roleid);
	if (err != NOWDB_OK) return err;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Get filter
 * ----------
 * this is way to simple. we need to get the where *per target* and
 * we have to consider aliases. Also, the target may be derived, so
 * we must consider fields that we do not know beforehand.
 * Furthermore, not every where condition goes into a filter condition.
 * A filter condition is always of the form: 
 * field = value.
 * However, a where condition may assume the forms:
 * field = field (join!)
 * value = value (constant)
 * TRUE, FALSE
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getFilter(nowdb_scope_t   *scope,
                                    nowdb_ast_t     *trg,
                                    nowdb_ast_t     *ast,
                                    nowdb_filter_t **filter) {
	nowdb_err_t     err;
	nowdb_ast_t    *cond;
	nowdb_filter_t *w = NULL;
	nowdb_filter_t *t = NULL;
	nowdb_filter_t *and;
	nowdb_model_vertex_t *v=NULL;
	nowdb_model_edge_t   *e=NULL;

	*filter = NULL;

	if (trg->stype == NOWDB_AST_TYPE) {
		err = getType(scope, trg, &t, &v);
		if (err != NOWDB_OK) return err;
	}
	if (ast == NULL) return NOWDB_OK;

	cond = nowdb_ast_operand(ast,1);
	if (cond == NULL) INVALIDAST("no first operand in where");

	/*
	fprintf(stderr, "where: %d->%d\n", ast->ntype, cond->ntype);
	*/

	/* get condition creates a filter */
	err = getCondition(scope, trg, e, v, &w, cond);
	if (err != NOWDB_OK) {
		if (*filter != NULL) {
			nowdb_filter_destroy(*filter); 
			free(*filter); *filter = NULL;
			return err;
		}
		return err;
	}
	if (t != NULL && w != NULL) {
		err = nowdb_filter_newBool(&and, NOWDB_FILTER_AND);
		if (err != NOWDB_OK) {
			nowdb_filter_destroy(t);
			nowdb_filter_destroy(w);
			free(t); t = NULL;
			free(w); w = NULL;
			return err;
		}
		and->left = t;
		and->right = w;
		*filter = and;

	} else if (t != NULL) *filter = t; else *filter = w;

	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Over-simplistic to get it going:
 * - we assume an ast with a simple target object
 * - we create 1 reader fullscan+ or indexsearch 
 *   on either vertex or context
 * - that's it
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_plan_fromAst(nowdb_scope_t  *scope,
                               nowdb_ast_t    *ast,
                               ts_algo_list_t *plan) {
	nowdb_filter_t *filter = NULL;
	ts_algo_list_t idxes;
	nowdb_err_t   err;
	nowdb_ast_t  *trg, *from;
	nowdb_plan_t *stp;

	from = nowdb_ast_from(ast);
	if (from == NULL) INVALIDAST("no 'from' in DQL");

	trg = nowdb_ast_target(from);
	if (trg == NULL) INVALIDAST("no target in from");

	/* create summary node */
	stp = malloc(sizeof(nowdb_plan_t));
	if (stp == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                      FALSE, OBJECT, "allocating plan");
	stp->ntype = NOWDB_PLAN_SUMMARY;
	stp->stype = 0;
	stp->helper = 1; /* number of targets */
	stp->name = NULL;
	stp->load = NULL;

	/* add summary node */
	if (ts_algo_list_append(plan, stp) != TS_ALGO_OK) {
		return nowdb_err_get(nowdb_err_no_mem,
	                FALSE, OBJECT, "list append");
	}

	/* create filter from where */
	err = getFilter(scope, trg, nowdb_ast_where(ast), &filter);
	if (err != NOWDB_OK) {
		nowdb_plan_destroy(plan, FALSE); return err;
	}

	/* find indices for filter */
	ts_algo_list_init(&idxes);
	err = getIndices(scope, trg->stype, trg->value, filter, &idxes);
	if (err != NOWDB_OK) {
		if (filter != NULL) {
			nowdb_filter_destroy(filter); free(filter);
		}
		nowdb_plan_destroy(plan, FALSE); return err;
	}

	/* create reader from target or index */
	stp = malloc(sizeof(nowdb_plan_t));
	if (stp == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
	                                         "allocating plan");
		if (filter != NULL) {
			nowdb_filter_destroy(filter); free(filter);
		}
		ts_algo_list_destroy(&idxes);
		nowdb_plan_destroy(plan, FALSE); return err;
	}

	stp->ntype = NOWDB_PLAN_READER;
	if (idxes.len == 1) {
		stp->stype = NOWDB_READER_SEARCH_;
		stp->helper = trg->stype;
		stp->name = trg->value;
		stp->load = idxes.head->cont;
		/* keys? */
	} else {
		stp->stype = NOWDB_READER_FS_; /* default is fullscan+ */
		stp->helper = trg->stype;
		stp->name = trg->value;
		stp->load = NULL;
	}

	/* we don't need this anymore */
	ts_algo_list_destroy(&idxes);

	/* add target node */
	if (ts_algo_list_append(plan, stp) != TS_ALGO_OK) {
		err = nowdb_err_get(nowdb_err_no_mem,
	               FALSE, OBJECT, "list append");
		if (filter != NULL) {
			nowdb_filter_destroy(filter); free(filter);
		}
		nowdb_plan_destroy(plan, FALSE); return err;
	}

	/* add filter */
	if (filter != NULL) {
		stp = malloc(sizeof(nowdb_plan_t));
		if (stp == NULL) {
			err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
			                                 "allocating plan");
			if (filter != NULL) {
				nowdb_filter_destroy(filter); free(filter);
			}
			nowdb_plan_destroy(plan, FALSE); return err;
		}

		stp->ntype = NOWDB_PLAN_FILTER;
		stp->stype = 0;
		stp->helper = 0;
		stp->name = NULL;
		stp->load = filter; /* the filter is stored directly here */

		if (ts_algo_list_append(plan, stp) != TS_ALGO_OK) {
			err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                             "list.append");
			if (filter != NULL) {
				nowdb_filter_destroy(filter); free(filter);
			}
			free(stp); return err;
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Destroy plan
 * ------------------------------------------------------------------------
 */
void nowdb_plan_destroy(ts_algo_list_t *plan, char cont) {
	ts_algo_list_node_t *runner, *tmp;
	nowdb_plan_t *node;

	if (plan == NULL) return;
	runner = plan->head;
	while(runner!=NULL) {
		node = runner->cont;
		if (cont && node->ntype == NOWDB_PLAN_FILTER) {
			nowdb_filter_destroy(node->load); free(node->load);
		}
		free(node); tmp = runner->nxt;
		ts_algo_list_remove(plan, runner);
		free(runner); runner = tmp;
	}
}
