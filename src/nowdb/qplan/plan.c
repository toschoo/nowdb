/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Execution Plan
 * ========================================================================
 */
#include <nowdb/qplan/plan.h>
#include <nowdb/query/row.h>
#include <nowdb/fun/fun.h>

#include <string.h>

static char *OBJECT = "plan";

/* ------------------------------------------------------------------------
 * Macro to wrap the frequent error "invalid ast"
 * ------------------------------------------------------------------------
 */
#define INVALIDAST(s) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, s)

/* ------------------------------------------------------------------------
 * Macro to wrap the frequent error "no memory"
 * ------------------------------------------------------------------------
 */
#define NOMEM(s) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, s)

/* ------------------------------------------------------------------------
 * Macro to wrap the frequent error "panic"
 * ------------------------------------------------------------------------
 */
#define PANIC(s) \
	return nowdb_err_get(nowdb_err_panic, FALSE, OBJECT, s)

/* ------------------------------------------------------------------------
 * Some helper macros
 * ------------------------------------------------------------------------
 */
#define OP(x) \
	NOWDB_EXPR_TOOP(x)

#define FIELD(x) \
	NOWDB_EXPR_TOFIELD(x)

#define CONST(x) \
	NOWDB_EXPR_TOCONST(x)

#define ARG(x,i) \
	OP(x)->argv[i]

/* -----------------------------------------------------------------------
 * Predeclaration for recursive call
 * -----------------------------------------------------------------------
 */
static nowdb_err_t getExpr(nowdb_scope_t    *scope,
                           nowdb_model_vertex_t *v,
                           nowdb_model_edge_t   *e,
                           char            needtxt,
                           nowdb_ast_t        *trg,
                           nowdb_ast_t      *field,
                           nowdb_expr_t      *expr,
                           char               *agg);

/* ------------------------------------------------------------------------
 * Identify index candidates in filter
 * ------------------------------------------------------------------------
 */
static nowdb_err_t idxFromFilter(nowdb_expr_t   filter,
                                 ts_algo_list_t *cands) {
	nowdb_err_t err;

	if (filter == NULL) return NOWDB_OK;

	if (nowdb_expr_type(filter) == NOWDB_EXPR_OP) {

		switch(OP(filter)->fun) {
		case NOWDB_EXPR_OP_EQ:
		case NOWDB_EXPR_OP_IN:
			if (!nowdb_expr_family3(filter)) return NOWDB_OK;
			if (ts_algo_list_append(cands,
			        filter) != TS_ALGO_OK) {
				return nowdb_err_get(nowdb_err_no_mem,
				        FALSE, OBJECT, "list.append");
			}
			return NOWDB_OK;

		case NOWDB_EXPR_OP_AND:
			err = idxFromFilter(NOWDB_EXPR_TOOP(
			            filter)->argv[0], cands);
			if (err != NOWDB_OK) return err;
			err = idxFromFilter(NOWDB_EXPR_TOOP(
			            filter)->argv[1], cands);
			if (err != NOWDB_OK) return err;
			return NOWDB_OK;

		// or!

		case NOWDB_EXPR_OP_JUST:
			err = idxFromFilter(NOWDB_EXPR_TOOP(
			            filter)->argv[0], cands);
			if (err != NOWDB_OK) return err;
			return NOWDB_OK;

		/* or is multi-index */
		default: return NOWDB_OK;
		}
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
	nowdb_expr_t *node;
	ts_algo_list_node_t *runner;

	/* we need a result idx/key */
	/* we need to find it by searching for all keys */
	// we have
	// EQ(a,b)
	// we need off from a or b
	// and value from the other
	*ok = 0;
	for(int i=0;i<keys->sz;i++) {
		for(runner=cands->head;runner!=NULL;runner=runner->nxt) {
			node = runner->cont;
			// we should check both sides explicitly
			int k = nowdb_expr_type(
			            ARG(node,0))==NOWDB_EXPR_FIELD?0:1;
			/*
			fprintf(stderr, "comparing %hu : %d\n",
			        keys->off[i], FIELD(ARG(node,k))->off);
			*/
			if (keys->off[i] == FIELD(ARG(node,k))->off) {
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
 * destroy pidx
 * ------------------------------------------------------------------------
 */
static inline void destroyPlanIdx(nowdb_plan_idx_t *pidx) {
	if (pidx == NULL) return;
	if (pidx->maps != NULL) {
		free(pidx->maps); pidx->maps = NULL;
	}
	if (pidx->keys != NULL) {
		free(pidx->keys); pidx->keys = NULL;
	}
	free(pidx);
}

/* ------------------------------------------------------------------------
 * make idx+keys
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t makeIndexAndKeys(nowdb_scope_t  *scope,
                                           nowdb_index_t  *idx,
                                           ts_algo_list_t *nodes,
                                           ts_algo_list_t *res) {
	nowdb_plan_idx_t *pidx;
	nowdb_err_t err;
	ts_algo_list_node_t *runner;
	nowdb_expr_t      node;
	int i=0, off=0;
	char usedmaps = 0;

	if (nodes == NULL || nodes->len == 0) {
		INVALIDAST("no nodes");
	}
	pidx = calloc(1, sizeof(nowdb_plan_idx_t));
	if (pidx == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                  FALSE, OBJECT, "allocating plan idx");
	pidx->idx = idx;
	pidx->keys = malloc(nodes->len*8);
	if (pidx->keys == NULL) {
		free(pidx);
		NOMEM("allocating keys");
		return err;
	}
	pidx->maps = malloc(nodes->len*sizeof(ts_algo_tree_t*));
	if (pidx->maps == NULL) {
		destroyPlanIdx(pidx);
		NOMEM("allocating maps");
		return err;
	}
	for(runner=nodes->head;runner!=NULL;runner=runner->nxt) {
		node = runner->cont;
		nowdb_expr_t f, c;
		if (!nowdb_expr_getFieldAndConst(node, &f, &c)) continue;
		int sz = CONST(c)->type==NOWDB_TYP_SHORT?4:8;

		if (CONST(c)->tree != NULL) {
			usedmaps = 1;
			pidx->maps[i] = CONST(c)->tree;
		} else {
			pidx->maps[i] = NULL;
			memcpy(pidx->keys+off, CONST(c)->value, sz);
		}
		off+=sz;i++;
	}
	if (!usedmaps) {
		free(pidx->maps); pidx->maps = NULL;
	}
	if (ts_algo_list_append(res, pidx) != TS_ALGO_OK) {
		destroyPlanIdx(pidx);
		return nowdb_err_get(nowdb_err_no_mem,
	                FALSE, OBJECT, "list.append");
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Intersect candidates and indices
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t intersect(nowdb_scope_t  *scope,
                                    ts_algo_list_t *cands,
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
			err = makeIndexAndKeys(scope, idx, &nodes, res);
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
 * get pedge or vertex prop
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getPedgeOrProp(nowdb_scope_t        *scope,
                                         char                *target,
                                         char                 *field,
			                 nowdb_model_edge_t      **e,
			                 nowdb_model_pedge_t **pedge) {
	nowdb_err_t err;

	if (*e == NULL) {
		err = nowdb_model_getEdgeByName(scope->model,
			                           target, e);
		if (err != NOWDB_OK) return err;
	}
	err = nowdb_model_getPedgeByName(scope->model,
                                         (*e)->edgeid,
                                         field, pedge);
	if (err != NOWDB_OK) return err;

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * fields to keys
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t fields2keys(nowdb_scope_t  *scope,
                                      char           *target,
                                      ts_algo_list_t *fields,
                                      int                trg,
                                      nowdb_index_keys_t **keys) {
	nowdb_err_t err;
	nowdb_field_t *f;
	ts_algo_list_node_t  *runner;
	nowdb_model_pedge_t  *pedge;
	nowdb_model_edge_t   *e=NULL;
	int i=0;

	if (fields->len == 0) return NOWDB_OK;

	*keys = calloc(1,sizeof(nowdb_index_keys_t));
	if (*keys == NULL) {
		NOMEM("allocating keys");
		return err;
	}
	(*keys)->sz = fields->len;
	(*keys)->off = calloc(fields->len, sizeof(uint16_t));
	if ((*keys)->off == NULL) {
		NOMEM("allocating key offsets");
		free(*keys);
		return err;
	}
	for(runner=fields->head; runner!=NULL; runner=runner->nxt) {
		f = runner->cont;
		if (f->name != NULL) {
			err = getPedgeOrProp(scope, target, f->name,
			                                &e, &pedge);
			if (err != NOWDB_OK) {
				free((*keys)->off); free(*keys);
				return err;
			}
			(*keys)->off[i] = (uint16_t)pedge->off;
		} else {
			(*keys)->off[i] = (uint16_t)f->off;
		}
		i++;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Find indices for group
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getGroupOrderIndex(nowdb_scope_t  *scope,
                                             int             stype,
                                             char           *context,
                                             ts_algo_list_t *fields, 
                                             ts_algo_list_t *res) {
	nowdb_index_keys_t *keys=NULL;
	nowdb_index_desc_t *desc=NULL;
	nowdb_context_t    *ctx=NULL;
	nowdb_plan_idx_t   *idx=NULL;
	nowdb_err_t err;

	// we should use expressions here!
	err = fields2keys(scope, context, fields, stype, &keys);
	if (err != NOWDB_OK) return err;
	if (keys == NULL) return NOWDB_OK;

	if (context != NULL && stype == NOWDB_AST_CONTEXT) {
		err = nowdb_scope_getContext(scope, context, &ctx);
		if (err != NOWDB_OK) {
			free(keys->off); free(keys);
			return err;
		}
	}

	err = nowdb_index_man_getByKeys(scope->iman, ctx, keys, &desc);
	if (err != NOWDB_OK) {
		free(keys->off); free(keys);
		return err;
	}

	idx = calloc(1,sizeof(nowdb_plan_idx_t));
	if (idx == NULL) {
		free(keys->off); free(keys);
		NOMEM("allocating plan index");
		return err;
	}

	idx->idx = desc->idx;
	idx->keys = NULL;

	free(keys->off); free(keys);

	if (ts_algo_list_append(res, idx) != TS_ALGO_OK) {
		NOMEM("list.append");
		free(idx); return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Find indices for filter
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getIndices(nowdb_scope_t  *scope,
                                     int             stype,
                                     char           *context,
                                     nowdb_expr_t    filter, 
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
	if (err != NOWDB_OK || cands.len == 0) {
		ts_algo_list_destroy(&cands);
		ts_algo_list_destroy(&idxes);
		return err;
	}

	err = intersect(scope, &cands, &idxes, res);

	ts_algo_list_destroy(&cands);
	ts_algo_list_destroy(&idxes);

	return err;
}

/* ------------------------------------------------------------------------
 * Add vertex type to filter
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getType(nowdb_scope_t     *scope,
                                  nowdb_ast_t         *trg,
                                  nowdb_expr_t          *t,
                                  nowdb_model_vertex_t **v) {
	nowdb_err_t err;
	nowdb_expr_t  c;
	nowdb_expr_t  f;

	err = nowdb_model_getVertexByName(scope->model, trg->value, v);
	if (err != NOWDB_OK) return err;

	err = nowdb_expr_newVertexOffField(&f, NOWDB_OFF_ROLE);
	if (err != NOWDB_OK) return err;

	err = nowdb_expr_newConstant(&c, &(*v)->roleid, NOWDB_TYP_SHORT);
	if (err != NOWDB_OK) {
		nowdb_expr_destroy(f); free(f);
		return err;
	}
	err = nowdb_expr_newOp(t, NOWDB_EXPR_OP_EQ, f, c);
	if (err != NOWDB_OK) {
		nowdb_expr_destroy(f); free(f);
		nowdb_expr_destroy(c); free(c);
		return err;
	}
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
static inline nowdb_err_t getFilter(nowdb_scope_t *scope,
                                    nowdb_ast_t   *trg,
                                    nowdb_ast_t   *ast,
                                    nowdb_expr_t  *filter) {
	nowdb_err_t  err;
	nowdb_ast_t  *op;
	nowdb_expr_t w = NULL;
	nowdb_expr_t t = NULL;
	nowdb_expr_t and=NULL;
	nowdb_model_vertex_t *v=NULL;
	nowdb_model_edge_t   *e=NULL;
	char x=0;

	*filter = NULL;

	if (trg->stype == NOWDB_AST_TYPE && trg->value != NULL) {
		err = getType(scope, trg, &t, &v);
		if (err != NOWDB_OK) return err;
	}
	if (trg->stype == NOWDB_AST_CONTEXT && trg->value != NULL) {
		err = nowdb_model_getEdgeByName(scope->model,
		                             trg->value, &e);
		if (err != NOWDB_OK) return err;
	}
	op = nowdb_ast_field(ast);
	if (op != NULL) {

		/*
		fprintf(stderr, "EXPR: %s\n", (char*)op->value);
		*/

		err = getExpr(scope, v, e, 0, trg, op, &w, &x);
		if (err != NOWDB_OK) {
			if (t != NULL) {
				nowdb_expr_destroy(t); free(t);
			}
			return err;
		}
		if (nowdb_expr_guessType(w) != NOWDB_TYP_BOOL) {
			if (t != NULL) {
				nowdb_expr_destroy(t); free(t);
			}
			nowdb_expr_destroy(w); free(w);
			INVALIDAST("where is not boolean");
		}
		if (x) {
			if (t != NULL) {
				nowdb_expr_destroy(t); free(t);
			}
			nowdb_expr_destroy(w); free(w);
			INVALIDAST("aggregates not allowed in where");
		}
	}
	if (t != NULL && w != NULL) {
		err = nowdb_expr_newOp(&and, NOWDB_EXPR_OP_AND, t, w);
		if (err != NOWDB_OK) {
			nowdb_expr_destroy(t);
			nowdb_expr_destroy(w);
			free(t); t = NULL;
			free(w); w = NULL;
			return err;
		}
		*filter = and;

	} else if (t != NULL) *filter = t; else *filter = w;

	// nowdb_expr_show(*filter, stderr); fprintf(stderr, "\n");

	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Destroy list of fields
 * -----------------------------------------------------------------------
 */
static inline void destroyFieldList(ts_algo_list_t *list) {
	ts_algo_list_node_t *runner, *tmp;
	nowdb_expr_t exp;

	if (list == NULL) return;
	runner=list->head;
	while(runner!=NULL) {
		exp = runner->cont;
		nowdb_expr_destroy(exp); free(exp);
		tmp = runner->nxt;
		ts_algo_list_remove(list, runner);
		free(runner); runner=tmp;
	}
}

/* -----------------------------------------------------------------------
 * Destroy list of aggregates
 * -----------------------------------------------------------------------
 */
static inline void destroyFunList(ts_algo_list_t *list) {
	ts_algo_list_node_t *runner, *tmp;
	nowdb_fun_t *f;

	if (list == NULL) return;
	runner=list->head;
	while(runner!=NULL) {
		f = runner->cont;
		nowdb_fun_destroy(f);
		free(f);
		tmp = runner->nxt;
		ts_algo_list_remove(list, runner);
		free(runner); runner=tmp;
	}
}

/* ------------------------------------------------------------------------
 * Try to interpret string as time
 * ------------------------------------------------------------------------
 */
static inline int tryTime(char *str, void **value) {
	size_t s;
	nowdb_time_t t;

	if (str == NULL) return -1;
	s = strlen(str);
	if (s > 30) return -1;
	if (s == 10) {
		if (nowdb_time_fromString(str,
		    NOWDB_DATE_FORMAT, &t)!=0) {
			return -1;
		}
	} else {
		if (nowdb_time_fromString(str,
		    NOWDB_TIME_FORMAT, &t)!=0) {
			return -1;
		}
	}
	*value = malloc(8);
	if (*value == NULL) return -1;
	memcpy(*value, &t, 8);
	return 0;
}

/* ------------------------------------------------------------------------
 * Get const value
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getConstValue(nowdb_type_t *typ,
                                        char         *str,
                                        void      **value) {
	nowdb_err_t err;
	int64_t x;
	char *tmp;

	if (*typ != NOWDB_TYP_TEXT &&
	    *typ != NOWDB_TYP_NOTHING) {
		*value = malloc(sizeof(nowdb_key_t));
		if (value == NULL) {
			NOMEM("allocating value");
			return err;
		}
	}
	switch(*typ) {
	case NOWDB_TYP_TEXT:
		if (tryTime(str, value) == 0) {
			*typ = NOWDB_TYP_TIME;
			return NOWDB_OK;
		}
		*value = strdup(str);
		if (*value == NULL) {
			NOMEM("allocating value");
			return err;
		}
		return NOWDB_OK;

	case NOWDB_TYP_BOOL:
		if (strcasecmp(str, "true") == 0) x=1;
		else if (strcasecmp(str, "false") == 0) x=0;
		else {
			free(*value); *value = NULL;
			return nowdb_err_get(nowdb_err_invalid,
			                         FALSE, OBJECT,
			                     "invalid boolean");
		}
		memcpy(*value, &x, sizeof(nowdb_value_t));
		return NOWDB_OK;

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

	case NOWDB_TYP_NOTHING:
		*value = NULL;
		return NOWDB_OK;

	default:
		if (*value != NULL) {
			free(*value); *value = NULL;
		}
		return nowdb_err_get(nowdb_err_panic, FALSE, OBJECT,
	                                          "unexpected type");
	}
	if (*tmp != 0) {
		free(*value); *value = NULL;
		return nowdb_err_get(nowdb_err_invalid,FALSE,OBJECT,
	                            "conversion of constant failed");
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Get vertex field
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getVertexField(nowdb_scope_t    *scope,
                                         nowdb_expr_t       *exp,
                                         nowdb_model_vertex_t *v,
                                         nowdb_ast_t      *field) {
	nowdb_err_t err;
	nowdb_model_prop_t *p;

	if (v == NULL) {
		INVALIDAST("vertex type is NULL");
	}
	err = nowdb_model_getPropByName(scope->model,
	                                   v->roleid,
	                            field->value, &p);
	if (err != NOWDB_OK) return err;

	err = nowdb_expr_newVertexField(exp, field->value, v->roleid,
	                                         p->propid, p->value);
	if (err != NOWDB_OK) return err;
	if (p->pk) {
		FIELD(*exp)->off = NOWDB_OFF_VERTEX;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Get edge field
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getEdgeField(nowdb_scope_t  *scope,
                                       nowdb_expr_t     *exp,
                                       nowdb_model_edge_t *e,
                                       nowdb_ast_t    *field) {
	nowdb_err_t err;
	nowdb_model_pedge_t *p;

	if (e == NULL) {
		INVALIDAST("edge type is NULL");
	}
	err = nowdb_model_getPedgeByName(scope->model,
	                                    e->edgeid,
	                            field->value, &p);
	if (err != NOWDB_OK) return err;

	err = nowdb_expr_newEdgeField(exp, field->value,
	                       p->off, p->value, e->num);
	if (err != NOWDB_OK) return err;
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Make agg function
 * shall be recursive (for future use with expressions)
 * -----------------------------------------------------------------------
 */
static nowdb_err_t makeAgg(nowdb_scope_t    *scope,
                           nowdb_model_vertex_t *v,
                           nowdb_model_edge_t   *e,
                           char            needtxt,
                           nowdb_ast_t        *trg,
                           nowdb_ast_t        *fun,
                           int                  op,
                           nowdb_expr_t      *expr) {
	nowdb_ast_t *param;
	nowdb_err_t err;
	nowdb_content_t cont;
	nowdb_fun_t *f;
	nowdb_expr_t myx=NULL;
	char dummy;

	cont = trg->stype == NOWDB_AST_CONTEXT?NOWDB_CONT_EDGE:
	                                       NOWDB_CONT_VERTEX;
	param = nowdb_ast_param(fun);
	if (param != NULL) {
		err = getExpr(scope, v, e, needtxt, trg, param, &myx, &dummy);
		if (err != NOWDB_OK) return err;
	}
	err = nowdb_fun_new(&f, op, cont, myx, NULL);
	if (err != NOWDB_OK) return err;

	err = nowdb_expr_newAgg(expr, NOWDB_FUN_AS_AGG(f));
	if (err != NOWDB_OK) {
		nowdb_fun_destroy(f); free(f);
		return err;
	}
	return NOWDB_OK;
}

static nowdb_err_t getInList(nowdb_ast_t    *ast,
                             ts_algo_list_t *ops) {
	ts_algo_tree_t *tree;
	nowdb_err_t err;
	nowdb_ast_t *o;
	nowdb_type_t t;
	nowdb_expr_t expr;
	void *value=NULL;

	o = nowdb_ast_nextParam(ast);
	if (o == NULL) INVALIDAST("empty 'IN' list");
	t = nowdb_ast_type(o->stype);
	err = nowdb_expr_newTree(&tree, t);
	if (err != NOWDB_OK) return err;

	while(o != NULL) {
		if (t == NOWDB_TYP_NOTHING) {
			o = nowdb_ast_nextParam(o); continue;
		}
		err = getConstValue(&t, o->value, &value);
		if (err != NOWDB_OK) {
			fprintf(stderr, "error in getConstValue\n");
			ts_algo_tree_destroy(tree); free(tree);
			return err;
		}

		if (ts_algo_tree_insert(tree, value) != TS_ALGO_OK) {
			NOMEM("tree.insert");
			ts_algo_tree_destroy(tree); free(tree);
			free(value);
			return err;
		}
		o = nowdb_ast_nextParam(o);
	}
	err = nowdb_expr_constFromTree(&expr, tree, t);
	if (err != NOWDB_OK) {
		ts_algo_tree_destroy(tree); free(tree);
		return err;
	}
	if (ts_algo_list_append(ops, expr) != TS_ALGO_OK) {
		NOMEM("list.append");
		nowdb_expr_destroy(expr); free(expr);
		return err;
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Helper: get key from text
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t getKey(nowdb_scope_t *scope,
                                 char          *txt,
                                 nowdb_key_t   *key) {
	nowdb_err_t err;

	err = nowdb_text_getKey(scope->text, txt, key);
	if (err != NOWDB_OK) {
		if (nowdb_err_contains(err, nowdb_err_key_not_found)) {
			*key = NOWDB_TEXT_UNKNOWN;
			nowdb_err_release(err); 
			return NOWDB_OK;
		}
		return err;
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Helper: rebuild tree as uint
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t rebuildTree(nowdb_scope_t *scope,
                                      nowdb_const_t *src,
                                      nowdb_expr_t  *trg) {
	nowdb_err_t err=NOWDB_OK;
	ts_algo_list_t *tmp=NULL;
	ts_algo_list_node_t *run;
	ts_algo_tree_t     *tree;

	*trg = NULL;

	err = nowdb_expr_newTree(&tree, NOWDB_TYP_UINT);
	if (err != NOWDB_OK) return err;

	if (src->tree->count == 0) {
		err = nowdb_expr_constFromTree(trg, tree, NOWDB_TYP_UINT);
		if (err != NOWDB_OK) {
			ts_algo_tree_destroy(tree); free(tree);
			return err;
		}
		return NOWDB_OK;
	}
	tmp = ts_algo_tree_toList(src->tree);
	if (tmp == NULL) {
		NOMEM("tree.toList");
		goto cleanup;
	}
	for(run=tmp->head; run!=NULL; run=run->nxt) {
		nowdb_key_t *k;

		k = malloc(sizeof(nowdb_key_t));
		if (k == NULL) {
			NOMEM("allocating key");
			break;
			
		}
		err = getKey(scope, run->cont, k);
		if (err != NOWDB_OK) {
			free(k); break;
		}
		if (ts_algo_tree_insert(tree, k) != TS_ALGO_OK) {
			NOMEM("tree.insert");
			free(k); break;
		}
	}
	if (err == NOWDB_OK) {
		err = nowdb_expr_constFromTree(trg, tree, NOWDB_TYP_UINT);
	}
	
cleanup:
	if (tmp != NULL) {
		ts_algo_list_destroy(tmp); free(tmp);
	}
	if (err != NOWDB_OK && tree != NULL) {
		ts_algo_tree_destroy(tree); free(tree);
		return err;
	}
	return err;
}

/* -----------------------------------------------------------------------
 * Helper: rebuild constant as uint
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t rebuildConst(nowdb_scope_t *scope,
                                       nowdb_const_t *src,
                                       nowdb_expr_t  *trg) {
	nowdb_key_t key;
	nowdb_err_t err;

	if (src->tree != NULL) {
		err = rebuildTree(scope, src, trg);
		if (err != NOWDB_OK) return err;
	} else {
		err = getKey(scope, src->value, &key);
		if (err != NOWDB_OK) return err;

		err = nowdb_expr_newConstant(trg, &key, NOWDB_TYP_UINT);
		if (err != NOWDB_OK) return err;
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Helper: optimise family triangle
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t textoptimise(nowdb_scope_t *scope,
                                       nowdb_expr_t  *o) {
	nowdb_err_t err;
	nowdb_expr_t f, c, c2;

	if (!nowdb_expr_family3(*o)) return NOWDB_OK;
	if (!nowdb_expr_getFieldAndConst(*o, &f, &c)) {
		PANIC("cannot get field and const from family triangle");
	}
	if (FIELD(f)->type != NOWDB_TYP_NOTHING &&
	    FIELD(f)->type != NOWDB_TYP_TEXT) return NOWDB_OK;
	if (CONST(c)->type != NOWDB_TYP_TEXT) return NOWDB_OK;

	// set f usekey 
	nowdb_expr_usekey(f);

	// correct c with uint
	err = rebuildConst(scope, CONST(c), &c2);
	if (err != NOWDB_OK) return err;

	// replace constant
	nowdb_expr_replace(*o, c2);
	nowdb_expr_destroy(c); free(c);

	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Helper for makeOp
 * -----------------------------------------------------------------------
 */
#define DESTROYLIST(l) \
	ts_algo_list_node_t *run, *tmp; \
	run = l.head; \
	while(run!=NULL) { \
		nowdb_expr_destroy(run->cont); \
		free(run->cont); \
		tmp = run->nxt; \
		free(run); \
		run=tmp; \
	}

/* -----------------------------------------------------------------------
 * Recursively get operator
 * -----------------------------------------------------------------------
 */
static nowdb_err_t makeOp(nowdb_scope_t    *scope,
                          nowdb_model_vertex_t *v,
                          nowdb_model_edge_t   *e,
                          char            needtxt,
                          nowdb_ast_t        *trg,
                          nowdb_ast_t      *field,
                          int                  op,
                          nowdb_expr_t      *expr,
                          char               *agg) {
	ts_algo_list_t ops;
	nowdb_expr_t exp;
	nowdb_err_t err;
	nowdb_ast_t *o;

	// get operands
	ts_algo_list_init(&ops);
	o = nowdb_ast_param(field);
	while (o != NULL) {
		
		/*
		fprintf(stderr, "%s param %s\n", (char*)field->value,
		                                 (char*)o->value);
		*/
		
		err = getExpr(scope, v, e, needtxt, trg, o, &exp, agg);
		if (err != NOWDB_OK) {
			// destroy list and values, etc.
			DESTROYLIST(ops);
			return err;
		}
		if (ts_algo_list_append(&ops, exp) != TS_ALGO_OK) {
			DESTROYLIST(ops);
			nowdb_expr_destroy(exp); free(exp);
			NOMEM("list.append");
			return err;
		}
		if (op == NOWDB_EXPR_OP_IN) {
			err = getInList(o, &ops);
			if (err != NOWDB_OK) {
				DESTROYLIST(ops);
				return err;
			}
			break;
		}
		o = nowdb_ast_nextParam(o);
	}

	err = nowdb_expr_newOpL(expr, op, &ops);
	if (err != NOWDB_OK) {
		DESTROYLIST(ops);
		return err;
	}
	ts_algo_list_destroy(&ops);

	// this is a bit frustating,
	// we rework this expression
	// because we can evaluate it more efficiently
	// by replacing text with keys
	if (!needtxt) {
		err = textoptimise(scope, expr);
		if (err != NOWDB_OK) {
			nowdb_expr_destroy(*expr);
			free(*expr);
			return err;
		}
	}
	return NOWDB_OK;
}
#undef DESTROYLIST

/* -----------------------------------------------------------------------
 * Make function
 * -----------------------------------------------------------------------
 */
static nowdb_err_t makeFun(nowdb_scope_t    *scope,
                           nowdb_model_vertex_t *v,
                           nowdb_model_edge_t   *e,
                           char            needtxt,
                           nowdb_ast_t        *trg,
                           nowdb_ast_t      *field,
                           nowdb_expr_t      *expr,
                           char               *agg) {
	int op;
	char x;

	// fprintf(stderr, "FUN: %s\n", (char*)field->value);
	op = nowdb_op_fromName(field->value, &x);
	if (op < 0) {
		INVALIDAST("unknown operator");
	}
	if (x) {
		*agg=1;
		return makeAgg(scope, v, e, needtxt, trg, field, op, expr);
	}
	return makeOp(scope, v, e, needtxt, trg, field, op, expr, agg);
}

/* -----------------------------------------------------------------------
 * Recursively get expression
 * -----------------------------------------------------------------------
 */
static nowdb_err_t getExpr(nowdb_scope_t    *scope,
                           nowdb_model_vertex_t *v,
                           nowdb_model_edge_t   *e,
                           char            needtxt,
                           nowdb_ast_t        *trg,
                           nowdb_ast_t      *field,
                           nowdb_expr_t      *expr,
                           char               *agg) {
	nowdb_err_t err;
	nowdb_type_t typ;
	void *value=NULL;

	/* expression */
	if (field->ntype == NOWDB_AST_FUN ||
	    field->ntype == NOWDB_AST_OP) {

		// fprintf(stderr, "FUN: %s\n", (char*)field->value);

		err = makeFun(scope, v, e, needtxt, trg, field, expr, agg);
		if (err != NOWDB_OK) return err;


	} else if (field->ntype == NOWDB_AST_VALUE) {
		typ = nowdb_ast_type(field->stype);
		err = getConstValue(&typ, field->value, &value);
		if (err != NOWDB_OK) return err;

		err = nowdb_expr_newConstant(expr, value, typ);
		if (err != NOWDB_OK) return err;

		// fprintf(stderr, "type: %d / %d\n",
		//        field->vtype, field->stype);

		if (value != NULL) free(value);

	} else {
		/* we need to distinguish the target! */
		if (trg->stype == NOWDB_AST_CONTEXT) {
			err = getEdgeField(scope, expr, e, field);
			if (err != NOWDB_OK) return err;

		} else if (trg->stype == NOWDB_AST_TYPE) {
			err = getVertexField(scope, expr, v, field);
			if (err != NOWDB_OK) return err;

		} else {
			/*
			fprintf(stderr, "STYPE: %d\n",
			trg->stype);
			*/
			INVALIDAST("unknown target");
		}
	}

	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Get fields for projection, grouping and ordering 
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t filterAgg(nowdb_expr_t expr,
                                    ts_algo_list_t *l) {
	nowdb_err_t          err;
	ts_algo_list_t       tmp;
	ts_algo_list_node_t *run;
	nowdb_agg_t         *agg;

	ts_algo_list_init(&tmp);
	err = nowdb_expr_filter(expr, NOWDB_EXPR_AGG, &tmp);
	if (err != NOWDB_OK) return err;

	for(run=tmp.head; run!=NULL; run=run->nxt) {
		agg = run->cont;
		if (ts_algo_list_append(l, agg->agg) != TS_ALGO_OK) {
			ts_algo_list_destroy(&tmp);
			NOMEM("list.append");
			return err;
		}
	}
	ts_algo_list_destroy(&tmp);
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Get fields for projection, grouping and ordering 
 * The name is misleading, it should be more like 'getExprs'
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t getFields(nowdb_scope_t    *scope,
                                    nowdb_ast_t        *trg,
                                    nowdb_ast_t        *ast,
                                    char            needtxt,
                                    ts_algo_list_t **fields, 
                                    ts_algo_list_t **aggs) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_model_vertex_t *v=NULL;
	nowdb_model_edge_t   *e=NULL;
	nowdb_expr_t exp;
	nowdb_ast_t *field;
	char agg;
	char dagg=0;

	// fprintf(stderr, "target: %s (%d)\n", (char*)trg->value, trg->stype);

	// get vertex type
	if (trg->stype == NOWDB_AST_TYPE && trg->value != NULL) {
		err = nowdb_model_getVertexByName(scope->model,
		                               trg->value, &v);
		if (err != NOWDB_OK) return err;
	}

	// get edge type
	if (trg->stype == NOWDB_AST_CONTEXT && trg->value != NULL) {
		err = nowdb_model_getEdgeByName(scope->model,
		                             trg->value, &e);
		if (err != NOWDB_OK) return err;
	}

	*fields = calloc(1, sizeof(ts_algo_list_t));
	if (*fields == NULL) {
		NOMEM("allocating list");
		return err;
	}
	ts_algo_list_init(*fields);

	field = nowdb_ast_field(ast);
	while (field != NULL) {

		// fprintf(stderr, "FIELD: %s\n", (char*)field->value);

		agg = 0;
		err = getExpr(scope, v, e, needtxt, trg, field, &exp, &agg);
		if (err != NOWDB_OK) break;
		
		if (agg) {
			if (aggs == NULL) {
				INVALIDAST("aggregates not allowed here");
			}
			if (*aggs == NULL) {
				*aggs = calloc(1, sizeof(ts_algo_list_t));
				if (*aggs == NULL) {
					NOMEM("allocating list");
					break;
				}
				ts_algo_list_init(*aggs);
				dagg=1;
			}
			err = filterAgg(exp, *aggs);
			if (err != NOWDB_OK) {
				nowdb_expr_destroy(exp); free(exp);
				break;
			}
		}
		
		/* testing only! */
		if (ts_algo_list_append(*fields, exp) != TS_ALGO_OK) {
			nowdb_expr_destroy(exp); free(exp);
			NOMEM("list.append");
			break;
		}
		field = nowdb_ast_field(field);
	}
	if (err != NOWDB_OK) {
		fprintf(stderr, "ERROR: "); nowdb_err_print(err);

		destroyFieldList(*fields);
		free(*fields); *fields=NULL;

		if (dagg) {
			destroyFunList(*aggs);
			free(*aggs); *aggs=NULL;
		}
	}
	return err;
}

/* -----------------------------------------------------------------------
 * grouping and projection must be equivalent
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t compareForGrouping(ts_algo_list_t *grp,
                                             ts_algo_list_t *sel) {
	ts_algo_list_node_t *grun, *srun;
	nowdb_expr_t gf, sf;

	srun = sel->head;
	for(grun=grp->head; grun != NULL; grun=grun->nxt) {
		if (srun == NULL) INVALIDAST("projection incomplete");
		gf = grun->cont;
		sf = srun->cont;
		if (!nowdb_expr_equal(gf, sf)) {
			INVALIDAST("projection and grouping differ");
		}
		srun = srun->nxt;
	}
	for (;srun != NULL; srun=srun->nxt) {
		if (nowdb_expr_has(srun->cont, NOWDB_EXPR_AGG)) continue;
		INVALIDAST("projection and grouping differ");
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Adjust target to what it s according to model
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t adjustTarget(nowdb_scope_t *scope,
                                       nowdb_ast_t   *trg) {
	nowdb_err_t   err;
	nowdb_content_t t;

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
	nowdb_expr_t   filter = NULL;
	ts_algo_list_t *pj, *grp=NULL, *agg=NULL, *ord=NULL;
	ts_algo_list_t idxes;
	nowdb_err_t   err;
	nowdb_ast_t  *trg, *from, *sel, *group=NULL, *order=NULL;
	nowdb_ast_t  *field;
	nowdb_plan_t *stp;
	char hasAgg=0;

	from = nowdb_ast_from(ast);
	if (from == NULL) INVALIDAST("no 'from' in DQL");

	trg = nowdb_ast_target(from);
	if (trg == NULL) INVALIDAST("no target in from");

	// what is this???
	err = adjustTarget(scope, trg);
	if (err != NOWDB_OK) return err;

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

	/* init index list */
	ts_algo_list_init(&idxes);

	/* the selection of indices follows the precedence
	 * group > order > where,
	 * i.e. if we have grouping, use the index for grouping
	 *      if we have ordering, use the index for ordering
	 * only if we don't have grouping nor ordering,
	 *         we use the indices for the where.
	 *
	 * This is, because we currently don't have a way
	 * to order or to group without indices.
	 * At the end, when we have such means,
	 * the precedence should be the contrary:
	 * whenever we can use an index for filtering,
	 * do so! It is better to sort some 1000 data points,
	 * then to scan the whole database -- even when it is
	 * by means of an index.
	 * Exception: when we have a small range! */

	/* check for aggregates */
	sel = nowdb_ast_select(ast);
	if (sel != NULL) {
		field = nowdb_ast_field(sel);
		while(field != NULL) {
			if (field->ntype == NOWDB_AST_FUN) {
				hasAgg = 1; break;
			}
			field = nowdb_ast_field(field);
		}
	}

	/* get group by */
	group = nowdb_ast_group(ast);
	if (group != NULL) {
		// do we need text in group by?
		err = getFields(scope, trg, group, 1, &grp, NULL);
		if (err != NOWDB_OK) {
			if (filter != NULL) {
				nowdb_expr_destroy(filter); free(filter);
			}
			nowdb_plan_destroy(plan, FALSE); return err;
		}
		/* find index for group by */
		err = getGroupOrderIndex(scope, trg->stype,
		                  trg->value, grp, &idxes);
		if (err != NOWDB_OK) {
			if (filter != NULL) {
				nowdb_expr_destroy(filter); free(filter);
			}
			if (grp != NULL) {
				destroyFieldList(grp); free(grp);
			}
			nowdb_plan_destroy(plan, FALSE); return err;
		}
	}

	/* get order by */
	order = nowdb_ast_order(ast);
	if (order != NULL && idxes.len == 0) {
		err = getFields(scope, trg, order, 1, &ord, NULL);
		if (err != NOWDB_OK) {
			if (filter != NULL) {
				nowdb_expr_destroy(filter); free(filter);
			}
			nowdb_plan_destroy(plan, FALSE); return err;
		}
		/* find index for order by */
		err = getGroupOrderIndex(scope, trg->stype,
		                  trg->value, ord, &idxes);
		if (err != NOWDB_OK) {
			if (filter != NULL) {
				nowdb_expr_destroy(filter); free(filter);
			}
			if (grp != NULL) {
				destroyFieldList(grp); free(grp);
			}
			if (ord != NULL) {
				destroyFieldList(ord); free(ord);
			}
			nowdb_plan_destroy(plan, FALSE); return err;
		}
	}

	/* find indices for filter */
	if (idxes.len == 0) {
		err = getIndices(scope, trg->stype,
		                        trg->value, filter, &idxes);
		if (err != NOWDB_OK) {
			if (filter != NULL) {
				nowdb_expr_destroy(filter); free(filter);
			}
			if (grp != NULL) {
				destroyFieldList(grp); free(grp);
			}
			if (ord != NULL) {
				destroyFieldList(ord); free(ord);
			}
			nowdb_plan_destroy(plan, FALSE); return err;
		}
	}

	/* create reader from target or index */
	stp = malloc(sizeof(nowdb_plan_t));
	if (stp == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
	                                         "allocating plan");
		if (filter != NULL) {
			nowdb_expr_destroy(filter); free(filter);
		}
		if (grp != NULL) {
			destroyFieldList(grp); free(grp);
		}
		if (ord != NULL) {
			destroyFieldList(ord); free(ord);
		}
		ts_algo_list_destroy(&idxes);
		nowdb_plan_destroy(plan, FALSE); return err;
	}

	stp->ntype = NOWDB_PLAN_READER;
	if (idxes.len == 1 && grp == NULL && ord == NULL) {
		// fprintf(stderr, "CHOOSING SEARCH\n");
		stp->stype = NOWDB_PLAN_SEARCH_;
		stp->helper = trg->stype;
		stp->name = trg->value;
		stp->load = idxes.head->cont;

 	/* this is for group without aggregates and without filter */
	} else if (idxes.len == 1 &&
	           order  == NULL &&
	           filter == NULL &&
	           hasAgg == 0) {
		// fprintf(stderr, "CHOOSING KRANGE\n");
		stp->stype = NOWDB_PLAN_KRANGE_;
		stp->helper = trg->stype;
		stp->name = trg->value;
		stp->load = idxes.head->cont;

 	/* this is for order and group with aggregates */
	} else if (idxes.len == 1) {
		// fprintf(stderr, "CHOOSING FRANGE\n");
		stp->stype = NOWDB_PLAN_FRANGE_;
		stp->helper = trg->stype;
		stp->name = trg->value;
		stp->load = idxes.head->cont;
	
	} else {
		// fprintf(stderr, "CHOOSING FULLSCAN\n");
		stp->stype = NOWDB_PLAN_FS_; /* default is fullscan+ */
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
			nowdb_expr_destroy(filter); free(filter);
		}
		if (grp != NULL) {
			destroyFieldList(grp); free(grp);
		}
		if (ord != NULL) {
			destroyFieldList(ord); free(ord);
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
				nowdb_expr_destroy(filter); free(filter);
			}
			if (grp != NULL) {
				destroyFieldList(grp); free(grp);
			}
			if (ord != NULL) {
				destroyFieldList(ord); free(ord);
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
				nowdb_expr_destroy(filter); free(filter);
			}
			if (grp != NULL) {
				destroyFieldList(grp); free(grp);
			}
			if (ord != NULL) {
				destroyFieldList(ord); free(ord);
			}
			free(stp); return err;
		}
	}

	/* add group by */
	if (group != NULL) {
		stp = malloc(sizeof(nowdb_plan_t));
		if (stp == NULL) {
			NOMEM("allocating plan");
			if (grp != NULL) {
				destroyFieldList(grp); free(grp);
			}
			if (ord != NULL) {
				destroyFieldList(ord); free(ord);
			}
			nowdb_plan_destroy(plan, FALSE); return err;
		}

		stp->ntype = NOWDB_PLAN_GROUPING;
		stp->stype = 0;
		stp->helper = 0;
		stp->name = NULL;
		stp->load = grp; 
	
		if (ts_algo_list_append(plan, stp) != TS_ALGO_OK) {
			NOMEM("list.append");
			if (grp != NULL) {
				destroyFieldList(grp); free(grp);
			}
			if (agg != NULL) {
				destroyFunList(agg); free(agg);
			}
			if (ord != NULL) {
				destroyFieldList(ord); free(ord);
			}
			nowdb_plan_destroy(plan, FALSE);
			free(stp); return err;
		}


	/* add order by */
	} else  if (order != NULL) {
		stp = malloc(sizeof(nowdb_plan_t));
		if (stp == NULL) {
			NOMEM("allocating plan");
			if (ord != NULL) {
				destroyFieldList(ord); free(ord);
			}
			nowdb_plan_destroy(plan, FALSE); return err;
		}

		stp->ntype = NOWDB_PLAN_ORDERING;
		stp->stype = 0;
		stp->helper = 0;
		stp->name = NULL;
		stp->load = ord; 
	
		if (ts_algo_list_append(plan, stp) != TS_ALGO_OK) {
			NOMEM("list.append");
			if (ord != NULL) {
				destroyFieldList(ord); free(ord);
			}
			nowdb_plan_destroy(plan, FALSE);
			free(stp); return err;
		}
	}

	/* add projection */
	if (sel == NULL) return NOWDB_OK;
	if (sel->stype == NOWDB_AST_STAR) return NOWDB_OK;

	err = getFields(scope, trg, sel, 1, &pj, &agg);
	if (err != NOWDB_OK) {
		if (agg != NULL) {
			destroyFunList(agg); free(agg);
		}
		nowdb_plan_destroy(plan, FALSE); return err;
	}

	if (grp != NULL) {
		err = compareForGrouping(grp, pj);
		if (err != NOWDB_OK) {
			if (agg != NULL) {
				destroyFunList(agg); free(agg);
			}
			destroyFieldList(pj); free(pj);
			nowdb_plan_destroy(plan, FALSE); return err;
		}
	}

	stp = malloc(sizeof(nowdb_plan_t));
	if (stp == NULL) {
		NOMEM("allocating plan");
		if (agg != NULL) {
			destroyFunList(agg); free(agg);
		}
		destroyFieldList(pj); free(pj);
		nowdb_plan_destroy(plan, FALSE); return err;
	}

	stp->ntype = NOWDB_PLAN_PROJECTION;
	stp->stype = 0;
	stp->helper = 0;
	stp->name = NULL;
	stp->load = pj; 

	if (ts_algo_list_append(plan, stp) != TS_ALGO_OK) {
		NOMEM("list.append");
		if (agg != NULL) {
			destroyFunList(agg); free(agg);
		}
		destroyFieldList(pj); free(pj);
		nowdb_plan_destroy(plan, FALSE);
		free(stp); return err;
	}
	if (agg != NULL) {
		stp = malloc(sizeof(nowdb_plan_t));
		if (stp == NULL) {
			NOMEM("allocating plan");
			if (agg != NULL) {
				destroyFunList(agg); free(agg);
			}
			nowdb_plan_destroy(plan, FALSE);
			return err;
		}

		stp->ntype = NOWDB_PLAN_AGGREGATES;
		stp->stype = 0;
		stp->helper = 0;
		stp->name = NULL;
		stp->load = agg; 
	
		if (ts_algo_list_append(plan, stp) != TS_ALGO_OK) {
			NOMEM("list.append");
			if (agg != NULL) {
				destroyFunList(agg); free(agg);
			}
			nowdb_plan_destroy(plan, FALSE);
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
		// fprintf(stderr, "destroying node [%d]\n", node->ntype);
		if (node->load != NULL) {
			if (cont && node->ntype == NOWDB_PLAN_FILTER) {
				nowdb_expr_destroy(node->load);
				free(node->load);
			}
			if (node->ntype == NOWDB_PLAN_PROJECTION) {
				destroyFieldList(node->load);
				free(node->load);
			}
			if (node->ntype == NOWDB_PLAN_GROUPING) {
				destroyFieldList(node->load);
				free(node->load);
			}
			if (node->ntype == NOWDB_PLAN_AGGREGATES) {
				destroyFunList(node->load);
				free(node->load);
			}
			if (node->ntype == NOWDB_PLAN_ORDERING) {
				destroyFieldList(node->load);
				free(node->load);
			}
			if (node->ntype == NOWDB_PLAN_READER) {
				if (node->stype == NOWDB_PLAN_SEARCH_ ||
				    node->stype == NOWDB_PLAN_FRANGE_ ||
				    node->stype == NOWDB_PLAN_KRANGE_ ||
				    node->stype == NOWDB_PLAN_CRANGE_) 
				{
					nowdb_plan_idx_t *pidx = node->load;
					destroyPlanIdx(pidx);
				}
			}
		}
		free(node); tmp = runner->nxt;
		ts_algo_list_remove(plan, runner);
		free(runner); runner = tmp;
	}
}

/* ------------------------------------------------------------------------
 * show expr list
 * ------------------------------------------------------------------------
 */
static void showExprList(nowdb_plan_t *node, FILE *stream) {
	ts_algo_list_node_t *run;
	if (node->load != NULL) {
		for(run=((ts_algo_list_t*)node->load)->head;
		    run!=NULL; run=run->nxt) 
		{
			nowdb_expr_show(run->cont, stream);
			if (run->nxt != NULL) {
				fprintf(stderr, ", ");
			}
		}
	}
}

/* ------------------------------------------------------------------------
 * show plan node
 * ------------------------------------------------------------------------
 */
static void showNode(nowdb_plan_t *node, FILE *stream) {
	if (node == NULL) return;
	// switch!
	if (node->ntype == NOWDB_PLAN_FILTER) {
		fprintf(stream, "WHERE: ");
		if (node->load != NULL) {
			nowdb_expr_show(node->load, stream);
		}
	}
	if (node->ntype == NOWDB_PLAN_PROJECTION) {
		fprintf(stream, "SELECT: ");
		showExprList(node, stream);
	}
	if (node->ntype == NOWDB_PLAN_GROUPING) {
		fprintf(stream, "GROUP BY: ");
	}
	if (node->ntype == NOWDB_PLAN_AGGREGATES) {
		fprintf(stream, "AGG: ");
		showExprList(node, stream);
	}
	if (node->ntype == NOWDB_PLAN_ORDERING) {
		fprintf(stream, "ORDER BY: ");
	}
	if (node->ntype == NOWDB_PLAN_READER) {
		switch(node->stype) {
		case NOWDB_PLAN_SEARCH_: fprintf(stream, "SEARCH"); break;
		case NOWDB_PLAN_FRANGE_: fprintf(stream, "FRANGE"); break;
		case NOWDB_PLAN_KRANGE_: fprintf(stream, "KRANGE"); break;
		case NOWDB_PLAN_CRANGE_: fprintf(stream, "CRANGE"); break;
		case NOWDB_PLAN_FS_: fprintf(stream, "FULLSCAN"); break;
		default: fprintf(stream, "UNKNOWN READER");
		}
	}
}

/* ------------------------------------------------------------------------
 * show plan
 * ------------------------------------------------------------------------
 */
void nowdb_plan_show(ts_algo_list_t *plan, FILE *stream) {
	ts_algo_list_node_t *run;
	if (plan == NULL) return;
	for(run=plan->head; run!=NULL; run=run->nxt) {
		showNode(run->cont, stream);
		fprintf(stream, "\n");
	}
}
