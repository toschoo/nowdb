/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Generic expressions
 * ========================================================================
 */
#include <nowdb/fun/expr.h>
#include <nowdb/fun/fun.h>

#include <stdarg.h>
#include <stdio.h>
#include <math.h>

static char *OBJECT = "EXPR";

#define NOMEM(s) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, s);

#define INVALID(s) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, s);

#define INVALIDTYPE(s) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, s);

#define FIELD(x) \
	((nowdb_field_t*)x)

#define CONST(x) \
	((nowdb_const_t*)x)

#define OP(x) \
	((nowdb_op_t*)x)

#define REF(x) \
	((nowdb_ref_t*)x)

#define AGG(x) \
	((nowdb_agg_t*)x)

#define FUN(x) \
	((nowdb_fun_t*)x)

typedef struct {
	uint32_t etype;
} dummy_t;

#define EXPR(x) \
	((dummy_t*)x)

/* -----------------------------------------------------------------------
 * Get expression type
 * -----------------------------------------------------------------------
 */
int nowdb_expr_type(nowdb_expr_t expr) {
	if (expr == NULL) return -1;
	return EXPR(expr)->etype;
}

/* -----------------------------------------------------------------------
 * Create expression
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_newEdgeField(nowdb_expr_t *expr, uint32_t off) {
	nowdb_err_t err;

	*expr = calloc(1,sizeof(nowdb_field_t));
	if (*expr == NULL)  {
		NOMEM("allocating expression");
		return err;
	}

	FIELD(*expr)->etype = NOWDB_EXPR_FIELD;

	FIELD(*expr)->name = NULL;
	FIELD(*expr)->text = NULL;
	FIELD(*expr)->target = NOWDB_TARGET_EDGE;
	FIELD(*expr)->off = (int)off;
	
	return NOWDB_OK;
}

nowdb_err_t nowdb_expr_newVertexField(nowdb_expr_t  *expr,
                                      char      *propname,
                                      nowdb_roleid_t role,
                                      nowdb_key_t propid) {
	nowdb_err_t err;

	*expr = calloc(1,sizeof(nowdb_field_t));
	if (*expr == NULL)  {
		NOMEM("allocating expression");
		return err;
	}

	FIELD(*expr)->etype = NOWDB_EXPR_FIELD;

	FIELD(*expr)->text = NULL;
	FIELD(*expr)->target = NOWDB_TARGET_VERTEX;
	FIELD(*expr)->off = NOWDB_OFF_PROP;
	FIELD(*expr)->role = role;
	FIELD(*expr)->propid = propid;
	FIELD(*expr)->pk = 0;

	FIELD(*expr)->name = strdup(propname);
	if (FIELD(*expr)->name == NULL) {
		NOMEM("allocating field name");
		free(*expr); *expr = NULL;
		return err;
	}
	return NOWDB_OK;
}


nowdb_err_t nowdb_expr_newConstant(nowdb_expr_t *expr,
                                   void        *value,
                                   nowdb_type_t  type) {
	nowdb_err_t err;

	*expr = calloc(1,sizeof(nowdb_const_t));
	if (*expr == NULL)  {
		NOMEM("allocating expression");
		return err;
	}

	CONST(*expr)->etype = NOWDB_EXPR_CONST;
	CONST(*expr)->type = type;

	if (type == NOWDB_TYP_TEXT) {
		CONST(*expr)->value = strdup(value);
		if (CONST(*expr)->value == NULL) {
			NOMEM("allocating value");
			free(*expr); *expr = NULL;
			return err;
		}
	} else {
		CONST(*expr)->value = malloc(8);
		if (CONST(*expr)->value == NULL) {
			NOMEM("allocating value");
			free(*expr); *expr = NULL;
			return err;
		}
		memcpy(CONST(*expr)->value, value, 8);
	}
	return NOWDB_OK;
}

static inline int getArgs(int o);

static inline nowdb_err_t initOp(nowdb_expr_t *expr,
                                 uint32_t       fun,
                                 uint32_t       len) {
	nowdb_err_t err;

	*expr = calloc(1,sizeof(nowdb_op_t));
	if (*expr == NULL)  {
		NOMEM("allocating expression");
		return err;
	}

	OP(*expr)->etype = NOWDB_EXPR_OP;
	OP(*expr)->argv = NULL;
	OP(*expr)->types = NULL;
	OP(*expr)->results = NULL;

	OP(*expr)->fun = fun;
	OP(*expr)->args  = len;

	OP(*expr)->types = calloc(len, sizeof(nowdb_type_t));
	if (OP(*expr)->types == NULL) {
		NOMEM("allocating expression operand types");
		nowdb_expr_destroy(*expr);
		free(*expr); *expr = NULL;
		return err;
	}

	OP(*expr)->results = calloc(len, sizeof(uint64_t));
	if (OP(*expr)->results == NULL) {
		NOMEM("allocating expression operand results");
		nowdb_expr_destroy(*expr);
		free(*expr); *expr = NULL;
		return err;
	}

	OP(*expr)->argv = calloc(len, sizeof(nowdb_expr_t));
	if (OP(*expr)->argv == NULL) {
		NOMEM("allocating expression operands");
		nowdb_expr_destroy(*expr);
		free(*expr); *expr = NULL;
		return err;
	}

	return NOWDB_OK;
}

nowdb_err_t nowdb_expr_newOpL(nowdb_expr_t  *expr,
                              uint32_t        fun,
                              ts_algo_list_t *ops) {
	nowdb_err_t err;
	ts_algo_list_node_t *run;
	int i=0;

	if (ops == NULL) INVALID("list parameter is NULL");

	err = initOp(expr, fun, ops->len);
	if (err != NOWDB_OK) return err;

	for(run = ops->head;run!=NULL;run=run->nxt) {
		OP(*expr)->argv[i] = run->cont; i++;
	}
	return NOWDB_OK;
}

nowdb_err_t nowdb_expr_newOpV(nowdb_expr_t  *expr,
                              uint32_t        fun,
                              uint32_t        len,
                              nowdb_expr_t   *ops) {
	nowdb_err_t err;

	err = initOp(expr, fun, len);
	if (err != NOWDB_OK) return err;

	for(int i=0; i<len; i++) {
		OP(*expr)->argv[i] = ops[i];
	}
	return NOWDB_OK;
}

nowdb_err_t nowdb_expr_newOp(nowdb_expr_t *expr, uint32_t fun, ...) {
	nowdb_err_t err;
	va_list args;

	int nr = getArgs(fun);

	if (nr < 0) INVALID("unknown operator");

	err = initOp(expr, fun, nr);
	if (err != NOWDB_OK) return err;
	
	va_start(args,fun);
	for(int i=0;i<nr;i++) {
		OP(*expr)->argv[i] = va_arg(args, nowdb_expr_t);
	}
	va_end(args);

	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Create Reference expression
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_newRef(nowdb_expr_t *expr, nowdb_expr_t ref) {
	nowdb_err_t err;

	*expr = calloc(1,sizeof(nowdb_ref_t));
	if (*expr == NULL)  {
		NOMEM("allocating expression");
		return err;
	}

	REF(*expr)->etype = NOWDB_EXPR_REF;
	REF(*expr)->ref = ref;

	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Create Agg expression
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_newAgg(nowdb_expr_t *expr, nowdb_aggfun_t agg) {
	nowdb_err_t err;

	*expr = calloc(1,sizeof(nowdb_agg_t));
	if (*expr == NULL)  {
		NOMEM("allocating expression");
		return err;
	}

	AGG(*expr)->etype = NOWDB_EXPR_AGG;
	AGG(*expr)->agg = agg;

	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Destroy Field
 * -----------------------------------------------------------------------
 */
static void destroyField(nowdb_field_t *field) {
	if (field->name != NULL) {
		free(field->name); field->name = NULL;
	}
}

/* -----------------------------------------------------------------------
 * Destroy Const
 * -----------------------------------------------------------------------
 */
static void destroyConst(nowdb_const_t *cst) {
	if (cst->value != NULL) {
		free(cst->value); cst->value = NULL;
	}
}

/* -----------------------------------------------------------------------
 * Destroy Operator
 * -----------------------------------------------------------------------
 */
static void destroyOp(nowdb_op_t *op) {
	if (op->argv != NULL) {
		for(int i=0;i<op->args;i++) {
			nowdb_expr_destroy(op->argv[i]);
			free(op->argv[i]); op->argv[i] = NULL;
		}
		free(op->argv); op->argv = NULL; op->args=0;
	}
	if (op->types != NULL) {
		free(op->types); op->types = NULL;
	}
	if (op->results != NULL) {
		free(op->results); op->results = NULL;
	}
}

/* -----------------------------------------------------------------------
 * Destroy expression
 * -----------------------------------------------------------------------
 */
void nowdb_expr_destroy(nowdb_expr_t expr) {
	if (expr == NULL) return;
	switch(EXPR(expr)->etype) {
	case NOWDB_EXPR_FIELD:
		destroyField(FIELD(expr)); break;
	case NOWDB_EXPR_CONST:
		destroyConst(CONST(expr)); break;
	case NOWDB_EXPR_OP:
		destroyOp(OP(expr)); break;
	case NOWDB_EXPR_REF: return;
	case NOWDB_EXPR_AGG: return;
	default: return;
	}
}

static void destroyVertexList(ts_algo_list_t *vm) {
	ts_algo_list_node_t *run;

	for(run=vm->head; run!=NULL; run=run->nxt) {
		free(run->cont);
	}
	ts_algo_list_destroy(vm);
}

static void destroyEdgeList(ts_algo_list_t *em) {
	ts_algo_list_node_t *run;

	for(run=em->head; run!=NULL; run=run->nxt) {
		free(run->cont);
	}
	ts_algo_list_destroy(em);
}

void nowdb_eval_destroy(nowdb_eval_t *eval) {
	if (eval == NULL) return;
	if (eval->tlru != NULL) {
		nowdb_ptlru_destroy(eval->tlru);
		free(eval->tlru); eval->tlru = NULL;
	}
	destroyVertexList(&eval->vm);
	destroyEdgeList(&eval->em);
}

static nowdb_err_t copyField(nowdb_field_t *src,
                             nowdb_expr_t  *trg)  {
	nowdb_err_t err;

	if (src->target == NOWDB_TARGET_EDGE) {
		return nowdb_expr_newEdgeField(trg, src->off); 	
	} else {
		err = nowdb_expr_newVertexField(trg, src->name,
		                                     src->role,
		                                     src->propid);
		if (err != NOWDB_OK) return err;
		FIELD(*trg)->type = src->type;
		return NOWDB_OK;
	}
}

static nowdb_err_t copyConst(nowdb_const_t *src,
                             nowdb_expr_t  *trg) {
	return nowdb_expr_newConstant(trg, src->value,
	                                   src->type);
}

#define DESTROYOPS(o,s) \
	for(int i=0;i<s;i++) { \
		nowdb_expr_destroy(ops[i]); free(ops[i]); \
	} \
	free(ops);

static nowdb_err_t copyOp(nowdb_op_t   *src,
                          nowdb_expr_t *trg) {
	nowdb_expr_t *ops=NULL;
	nowdb_err_t   err;

	if (src->args > 0) {
		ops = calloc(src->args, sizeof(nowdb_expr_t));
		if (ops == NULL) {
			NOMEM("allocating operands");
			return err;
		}
		for(int i=0; i<src->args; i++) {
			err = nowdb_expr_copy(src->argv[i], ops+i);
			if (err != NOWDB_OK) {
				DESTROYOPS(ops, i);
				return err;
			}
		}
	}
	err = nowdb_expr_newOpV(trg, src->fun, src->args, ops);
	if (err != NOWDB_OK) {
		DESTROYOPS(ops, src->args);
		return err;
	}
	free(ops);
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Copy reference
 * -----------------------------------------------------------------------
 */
static nowdb_err_t copyRef(nowdb_ref_t *src, nowdb_expr_t *trg) {
	return nowdb_expr_newRef(trg, src->ref);
}

/* -----------------------------------------------------------------------
 * Copy aggregate
 * -----------------------------------------------------------------------
 */
static nowdb_err_t copyAgg(nowdb_agg_t *src, nowdb_expr_t *trg) {
	return nowdb_expr_newAgg(trg, src->agg);
}

/* -----------------------------------------------------------------------
 * Copy expression
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_copy(nowdb_expr_t  src,
                            nowdb_expr_t *trg) {
	switch(EXPR(src)->etype) {
	case NOWDB_EXPR_FIELD:
		return copyField(FIELD(src), trg);
	case NOWDB_EXPR_CONST:
		return copyConst(CONST(src), trg);
	case NOWDB_EXPR_OP:
		return copyOp(OP(src), trg);
	case NOWDB_EXPR_REF:
		return copyRef(REF(src), trg);
	case NOWDB_EXPR_AGG:
		return copyAgg(AGG(src), trg);
	default: INVALID("unknown expression type");
	}
}

/* -----------------------------------------------------------------------
 * Two field expressions are equivalent
 * -----------------------------------------------------------------------
 */
static inline char fieldEqual(nowdb_field_t *one,
                              nowdb_field_t *two) 
{
	if (one->target != two->target) return 0;

	// different edges???
	if (one->target == NOWDB_TARGET_EDGE) {
		return (one->off  == two->off);
	}
	if (one->role != two->role) return 0;
	if (strcasecmp(one->name, two->name) == 0) return 1;
	return 0;
}

/* -----------------------------------------------------------------------
 * Two const expressions are equivalent
 * -----------------------------------------------------------------------
 */
static inline char constEqual(nowdb_const_t *one,
                              nowdb_const_t *two) 
{
	if (one->type != two->type) return 0;
	if (one->value == NULL && two->value == NULL) return 1;
	switch(one->type) {
	case NOWDB_TYP_TEXT:
		return (strcmp((char*)one->value, (char*)two->value) == 0);
	case NOWDB_TYP_INT:
	case NOWDB_TYP_DATE:
	case NOWDB_TYP_TIME:
		return (*(uint64_t*)one->value == *(uint64_t*)two->value);

	case NOWDB_TYP_UINT:
		return (*(int64_t*)one->value == *(int64_t*)two->value);

	case NOWDB_TYP_FLOAT:
		return (*(double*)one->value == *(double*)two->value);

	case NOWDB_TYP_BOOL:
		return (*(char*)one->value == *(char*)two->value);

	default: return 0;
	}
}

/* -----------------------------------------------------------------------
 * Two op expressions are equivalent
 * -----------------------------------------------------------------------
 */
static inline char opEqual(nowdb_op_t *one,
                           nowdb_op_t *two) 
{
	if (one->fun != two->fun) return 0;
	if (one->args != two->args) return 0;

	for(int i=0; i<one->args; i++) {
		if (!nowdb_expr_equal(one->argv[i],
		                      two->argv[i])) return 0;
	}
	return 1;
}

/* -----------------------------------------------------------------------
 * Two expressions are equivalent
 * -----------------------------------------------------------------------
 */
char nowdb_expr_equal(nowdb_expr_t one,
                      nowdb_expr_t two) {

	if (one == NULL && two == NULL) return 1;
	else if (one == NULL || two == NULL) return 0;
	if (EXPR(one)->etype != EXPR(two)->etype) return 0;

	switch(EXPR(one)->etype) {
	case NOWDB_EXPR_FIELD: return fieldEqual(FIELD(one), FIELD(two));
	case NOWDB_EXPR_CONST: return constEqual(CONST(one), CONST(two));
	case NOWDB_EXPR_REF: return (REF(one)->ref == REF(two)->ref);
	case NOWDB_EXPR_AGG: return (AGG(one)->agg == AGG(two)->agg);
	case NOWDB_EXPR_OP: return opEqual(OP(one), OP(two));
	default: return 0;
	}
}

/* -----------------------------------------------------------------------
 * Filter expressions of certain type
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_filter(nowdb_expr_t   expr,
                              uint32_t       type,
                              ts_algo_list_t *res) {
	nowdb_err_t err;

	if (EXPR(expr)->etype == type) {
		if (ts_algo_list_append(res, expr) != TS_ALGO_OK) {
			NOMEM("list.append");
			return err;
		}
	} else if (EXPR(expr)->etype == NOWDB_EXPR_OP) {
		for(int i=0;i<OP(expr)->args;i++) {
			err = nowdb_expr_filter(OP(expr)->argv[i],
			                               type, res);
			if (err != NOWDB_OK) {
				ts_algo_list_destroy(res);
				ts_algo_list_init(res);
				return err;
			}
		}
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Has expressions of certain type
 * -----------------------------------------------------------------------
 */
char nowdb_expr_has(nowdb_expr_t   expr,
                    uint32_t       type) {
	if (EXPR(expr)->etype == type) return 1;
	if (EXPR(expr)->etype == NOWDB_EXPR_OP) {
		for(int i=0;i<OP(expr)->args;i++) {
			if (nowdb_expr_has(OP(expr)->argv[i], type)) return 1;
		}
	}
	return 0;
}

/* -----------------------------------------------------------------------
 * Find edge model
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t findEdge(nowdb_eval_t *hlp,
                                   nowdb_edge_t *src) {
	nowdb_err_t err;
	ts_algo_list_node_t *run;
	nowdb_edge_helper_t *em;

	if (hlp->ce != NULL &&
	    hlp->ce->e->edgeid == src->edge) return NOWDB_OK;

	for(run=hlp->em.head; run!=NULL; run=run->nxt) {
		em = run->cont;
		if (em->e->edgeid == src->edge) return NOWDB_OK;
	}
	em = calloc(1,sizeof(nowdb_edge_helper_t));
	if (em == NULL) {
		NOMEM("allocating edge helper");
		return err;
	}
	err = nowdb_model_getEdgeById(hlp->model,
		              src->edge, &em->e);
	if (err != NOWDB_OK) {
		free(em); 
		return err;
	}

	err = nowdb_model_getVertexById(hlp->model,
		            em->e->origin, &em->o);
	if (err != NOWDB_OK) {
		free(em); 
		return err;
	}

	err = nowdb_model_getVertexById(hlp->model,
		            em->e->destin, &em->d);
	if (err != NOWDB_OK) {
		free(em);
		return err;
	}
	if (ts_algo_list_append(&hlp->em, em) != TS_ALGO_OK) {
		free(em); em = NULL;
		NOMEM("list.append");
		return err;
	}
	hlp->ce = em;
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Find vertex model
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t findVertex(nowdb_eval_t *hlp,char *src) {
	return NOWDB_OK;
	/*
	nowdb_err_t err;
	ts_algo_list_node_t *run;
	nowdb_vertex_helper_t *vm;

	if (hlp->cv != NULL &&
	    hlp->cv->v->roleid == *(nowdb_roleid_t*)src) return NOWDB_OK;

	for(run=hlp->vm->head; run!=NULL; run=run->nxt) {
		v = run->cont;
		if (vm->v->roleid == *(nowdb_roleid_t*)src &&
		    (vm->p->propid == *(nowdb_propid_t*)(src+off))) {
			return NOWDB_OK;
		}
	}
	vm = calloc(1,sizeof(nowdb_vertex_helper_t));
	if (vm == NULL) {
		NOMEM("allocating vertex helper");
		return err;
	}
	if (row->fields[i].name == NULL) continue;
		err = nowdb_model_getPropById(hlp->model,
		                   *(nowdb_roleid_t*)src,
		                   *(nowdb_propid_t*)(src+off),
                                                        &vm->p);
		if (err != NOWDB_OK) return err;
	}
	return NOWDB_OK;
	*/
}

/* ------------------------------------------------------------------------
 * Helper: get text and cache it
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getText(nowdb_eval_t *hlp,
                                  nowdb_key_t   key,
                                  char        **str) {
	nowdb_err_t err;

	err = nowdb_ptlru_get(hlp->tlru, key, str);
	if (err != NOWDB_OK) return err;

	if (*str != NULL) return NOWDB_OK;

	err = nowdb_text_getText(hlp->text, key, str);
	if (err != NOWDB_OK) return err;

	return nowdb_ptlru_add(hlp->tlru, key, *str);
}

/* ------------------------------------------------------------------------
 * save a lot of code
 * ------------------------------------------------------------------------
 */
#define HANDLETEXT(what) \
	*t = (char)NOWDB_TYP_TEXT; \
	if (hlp->needtxt) { \
		err = getText(hlp, *what, &field->text); \
		if (err != NOWDB_OK) return err; \
		*res = field->text; \
	} else { \
		*res=what; \
	}

/* ------------------------------------------------------------------------
 * Helper: get EdgeValue
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getEdgeValue(nowdb_field_t *field,
                                       nowdb_eval_t  *hlp,
                                       nowdb_edge_t  *src,
                                       nowdb_type_t  *t,
                                       void         **res) {
	nowdb_value_t *w=NULL;
	nowdb_err_t  err;

	switch(field->off) {
	case NOWDB_OFF_EDGE:

		*t = (char)NOWDB_TYP_TEXT;
		if (hlp->needtxt) {
			*res = hlp->ce->e->name;
		} else {
			*res = &src->edge; 
		}
		break;

	case NOWDB_OFF_ORIGIN:
		if (hlp->ce->o->vid == NOWDB_MODEL_TEXT) {
			HANDLETEXT(&src->origin);
		} else {
			*t = NOWDB_TYP_UINT;
			*res = &src->origin;
		}
		break;

	case NOWDB_OFF_DESTIN:
		if (hlp->ce->d->vid == NOWDB_MODEL_TEXT) {
			HANDLETEXT(&src->destin);
		} else {
			*t = NOWDB_TYP_UINT;
			*res = &src->destin;
		}
		break;

	case NOWDB_OFF_LABEL:
		if (hlp->ce->e->label == NOWDB_MODEL_TEXT) {
			HANDLETEXT(&src->label);
		} else {
			*t = NOWDB_TYP_UINT;
			*res = &src->label;
		}
		break;

	case NOWDB_OFF_TMSTMP:
		*t = NOWDB_TYP_TIME;
		*res = &src->timestamp;
		break;

	case NOWDB_OFF_WEIGHT:
		w = &src->weight;
		*t = hlp->ce->e->weight;

	case NOWDB_OFF_WEIGHT2:
		if (w == NULL) {
			w = &src->weight2;
			*t = hlp->ce->e->weight2;
		}
		switch(*t) {
		case NOWDB_TYP_TEXT:
			if (hlp->needtxt)  {
				HANDLETEXT(w);
			}
			break;

		case NOWDB_TYP_DATE:
		case NOWDB_TYP_TIME:
		case NOWDB_TYP_FLOAT:
		case NOWDB_TYP_INT:
		case NOWDB_TYP_UINT:
			*res = w;
			break;

		default: break;
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: get VertexValue
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getVertexValue(nowdb_field_t *field,
                                         nowdb_eval_t  *hlp,
                                         char          *src,
                                         nowdb_type_t  *t,
                                         void         **res) {
	nowdb_err_t err;

	*t = field->type;
	if (*t == NOWDB_TYP_TEXT) {
		HANDLETEXT((nowdb_key_t*)(src+field->off));
	} else {
		*res = src+field->off;
	}
	return NOWDB_OK;
}


/* -----------------------------------------------------------------------
 * Evaluate field
 * -----------------------------------------------------------------------
 */
static nowdb_err_t evalField(nowdb_field_t *field,
                             nowdb_eval_t  *hlp,
                             char          *row,
                             nowdb_type_t  *typ,
                             void         **res) {
	nowdb_err_t err;

	if (field->off < 0) INVALID("uninitialised expression");

	if (field->target == NOWDB_TARGET_EDGE) {
		err = findEdge(hlp, (nowdb_edge_t*)row);
		if (err != NOWDB_OK) return err;

		err = getEdgeValue(field, hlp, (nowdb_edge_t*)row, typ, res);
		if (err != NOWDB_OK) return err;
		
	} else {
		/*
		err = findVertex(hlp, row);
		if (err != NOWDB_OK) return err;
		*/

		err = getVertexValue(field, hlp, row, typ, res);
		if (err != NOWDB_OK) return err;
	}

	/*
	*typ = field->type;
	*res = row+field->off;
	*/

	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Evaluate Fun (predeclaration, implementation below)
 * -----------------------------------------------------------------------
 */
static nowdb_err_t evalFun(uint32_t     fun,
                           void        **argv,
                           nowdb_type_t *types,
                           nowdb_type_t *t,
                           void         *res);

/* -----------------------------------------------------------------------
 * Evaluate Fun (predeclaration, implementation below)
 * -----------------------------------------------------------------------
 */
static inline nowdb_type_t evalType(uint32_t      fun,
                                    nowdb_type_t *types);

/* -----------------------------------------------------------------------
 * Evaluate operation
 * -----------------------------------------------------------------------
 */
static nowdb_err_t evalOp(nowdb_op_t   *op,
                          nowdb_eval_t *hlp,
                          char         *row,
                          nowdb_type_t *typ,
                          void        **res) {
	nowdb_err_t err;

	if (op->argv != NULL) {
		for(int i=0; i<op->args; i++) {
			err = nowdb_expr_eval(op->argv[i],
			                      hlp, row,
			                      op->types+i,
			                      op->results+i);
			if (err != NOWDB_OK) return err; 
		}
	}

	// determine type
	if (op->types != NULL) { 
		*typ = evalType(op->fun, op->types);
		if (*typ < 0) INVALIDTYPE("unknown operation");
	}
	err = evalFun(op->fun, op->results, op->types, typ, &op->res);
	if (err != NOWDB_OK) return err;

	fprintf(stderr, "%d: %u\n", op->fun, *typ);
	*res=&op->res;
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Evaluate aggregate
 * TODO:
 * When we will have expressions within aggregates,
 * this needds to be changed!!!
 * -----------------------------------------------------------------------
 */
static nowdb_err_t evalAgg(nowdb_agg_t  *agg,
                           nowdb_eval_t *hlp,
                           char         *row,
                           nowdb_type_t *typ,
                           void        **res) 
{
	// is NOTHING for sum
	*typ = FUN(agg->agg)->otype;
	*res = &FUN(agg->agg)->r1;
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Evaluate expression
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_eval(nowdb_expr_t expr,
                            nowdb_eval_t *hlp,
                            char         *row,
                            nowdb_type_t *typ,
                            void        **res) {
	if (expr == NULL) return NOWDB_OK;
	switch(EXPR(expr)->etype) {
	case NOWDB_EXPR_FIELD:
		return evalField(FIELD(expr), hlp, row, typ, res);

	case NOWDB_EXPR_CONST:
		// text ???
		*typ = CONST(expr)->type;
		*res = CONST(expr)->value;
		return NOWDB_OK;

	case NOWDB_EXPR_OP:
		return evalOp(OP(expr), hlp, row, typ, res);

	case NOWDB_EXPR_REF:
		return nowdb_expr_eval(REF(expr)->ref, hlp, row, typ, res);

	case NOWDB_EXPR_AGG:
		return evalAgg(AGG(expr), hlp, row, typ, res);

	default:
		INVALID("unknown expression type");
	}
}

#define ADD(v0,v1,v2) \
	v0=v1+v2;

#define SUB(v0,v1,v2) \
	v0=v1-v2;

#define MUL(v0,v1,v2) \
	v0=v1*v2;

#define QUOT(v0,v1,v2) \
	if (v2 != 0) v0=v1/v2;

#define DIV(v0,v1,v2) \
	if ((double)v2 != 0) v0=(double)v1/(double)v2;

#define REM(v0,v1,v2) \
	v0 = v2 == 0 ? 0 : v1%v2;

// distinguish types!!!
// there are versions for double and long!
#define POW(v0,v1,v2) \
	v0 = pow(v1,v2);

#define LOG(v0,v1) \
	v0 = log(v1);

#define CEIL(v0,v1) \
	v0 = ceil(v1);

#define FLOOR(v0,v1) \
	v0 = floor(v1);

#define ROUND(v0,v1) \
	v0 = round(v1);

#define ABS(v0,v1) \
	v0 = fabs(v1);

#define MOV(r1,r2) \
	memcpy(r1, r2, 8);

#define PERFM(o) \
	switch(*t) { \
	case NOWDB_TYP_UINT: \
		o(*(uint64_t*)res, *(uint64_t*)argv[0], *(uint64_t*)argv[1]); \
		return NOWDB_OK; \
	case NOWDB_TYP_DATE: \
	case NOWDB_TYP_TIME: \
	case NOWDB_TYP_INT: \
		o(*(int64_t*)res, *(int64_t*)argv[0], *(int64_t*)argv[1]); \
		return NOWDB_OK; \
	case NOWDB_TYP_FLOAT: \
		o(*(double*)res, *(double*)argv[0], *(double*)argv[1]); \
		return NOWDB_OK; \
	default: return NOWDB_OK; \
	}

#define PERFMNF(o) \
	switch(*t) { \
	case NOWDB_TYP_UINT: \
		o(*(uint64_t*)res, *(uint64_t*)argv[0], *(uint64_t*)argv[1]); \
		return NOWDB_OK; \
	case NOWDB_TYP_DATE: \
	case NOWDB_TYP_TIME: \
	case NOWDB_TYP_INT: \
		o(*(int64_t*)res, *(int64_t*)argv[0], *(int64_t*)argv[1]); \
	default: return NOWDB_OK; \
	}

#define PERFM1(o) \
	switch(*t) { \
	case NOWDB_TYP_UINT: \
		o(*(uint64_t*)res, *(uint64_t*)argv[0]); \
		return NOWDB_OK; \
	case NOWDB_TYP_DATE: \
	case NOWDB_TYP_TIME: \
	case NOWDB_TYP_INT: \
		o(*(int64_t*)res, *(int64_t*)argv[0]); \
		return NOWDB_OK; \
	case NOWDB_TYP_FLOAT: \
		o(*(double*)res, *(double*)argv[0]); \
		return NOWDB_OK; \
	default: return NOWDB_OK; \
	}

#define TOFLOAT() \
	switch(types[0]) { \
	case NOWDB_TYP_UINT: \
		f = (double)(*(uint64_t*)argv[0]); \
		MOV(res, &f); break; \
	case NOWDB_TYP_DATE: \
	case NOWDB_TYP_TIME: \
	case NOWDB_TYP_INT: \
		f = (double)(*(int64_t*)argv[0]); \
		MOV(res, &f); break; \
	default: \
		MOV(res, argv[0]); break; \
	}

#define TOINT() \
	switch(types[0]) { \
	case NOWDB_TYP_UINT: \
		l = (int64_t)(*(uint64_t*)argv[0]); \
		MOV(res, &l); break; \
	case NOWDB_TYP_FLOAT: \
		l = (int64_t)(*(double*)argv[0]); \
		MOV(res, &l); break; \
	default: \
		MOV(res, argv[0]); break; \
	}

#define TOUINT() \
	switch(types[0]) { \
	case NOWDB_TYP_FLOAT: \
		u = (uint64_t)(*(double*)argv[0]); \
		MOV(res, &u); break; \
	case NOWDB_TYP_DATE: \
	case NOWDB_TYP_TIME: \
	case NOWDB_TYP_INT: \
		u = (uint64_t)(*(int64_t*)argv[0]); \
		MOV(res, &u); break; \
	default: \
		MOV(res, argv[0]); break; \
	}

/* -----------------------------------------------------------------------
 * Evaluate Fun
 * -----------------------------------------------------------------------
 */
static nowdb_err_t evalFun(uint32_t      fun,
                           void        **argv,
                           nowdb_type_t *types,
                           nowdb_type_t *t,
                           void         *res) {
	double f;
	int64_t l;
	uint64_t u;

	switch(fun) {

	/* -----------------------------------------------------------------------
	 * Conversions
	 * -----------------------------------------------------------------------
	 */
	case NOWDB_EXPR_OP_FLOAT: TOFLOAT(); break;
	case NOWDB_EXPR_OP_UINT: TOUINT(); break; 
	case NOWDB_EXPR_OP_INT: 
	case NOWDB_EXPR_OP_TIME: TOINT(); break;
	case NOWDB_EXPR_OP_TEXT: 
		return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT, NULL);

	/* -----------------------------------------------------------------------
	 * Arithmetic
	 * -----------------------------------------------------------------------
	 */
	case NOWDB_EXPR_OP_ADD: PERFM(ADD);
	case NOWDB_EXPR_OP_SUB: PERFM(SUB);
	case NOWDB_EXPR_OP_MUL: PERFM(MUL);
	case NOWDB_EXPR_OP_DIV:
		if (*t == NOWDB_TYP_FLOAT) {
			PERFM(DIV);
		} else {
			PERFM(QUOT);
		}

	case NOWDB_EXPR_OP_REM:
		if (*t == NOWDB_TYP_FLOAT) {
			INVALIDTYPE("remainder with float");
		} else {
			PERFMNF(REM);
		}

	case NOWDB_EXPR_OP_POW: PERFM(POW);

	case NOWDB_EXPR_OP_ROOT:
		return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT, NULL);

	case NOWDB_EXPR_OP_LOG: PERFM1(LOG);

	case NOWDB_EXPR_OP_CEIL: PERFM1(CEIL);
	case NOWDB_EXPR_OP_FLOOR: PERFM1(FLOOR);
	case NOWDB_EXPR_OP_ROUND: PERFM1(ROUND);
	case NOWDB_EXPR_OP_ABS: PERFM1(ABS);
		return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT, NULL);

	/* -----------------------------------------------------------------------
	 * Logic
	 * -----------------------------------------------------------------------
	 */
	case NOWDB_EXPR_OP_EQ:
	case NOWDB_EXPR_OP_NE:
	case NOWDB_EXPR_OP_LT:
	case NOWDB_EXPR_OP_GT:
	case NOWDB_EXPR_OP_LE:
	case NOWDB_EXPR_OP_GE:
	case NOWDB_EXPR_OP_NOT:
	case NOWDB_EXPR_OP_AND:
	case NOWDB_EXPR_OP_OR:
	case NOWDB_EXPR_OP_XOR:
		return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT, NULL);

	/* -----------------------------------------------------------------------
	 * TIME
	 * -----------------------------------------------------------------------
	 */
	case NOWDB_EXPR_OP_CENTURY:
	case NOWDB_EXPR_OP_YEAR:
	case NOWDB_EXPR_OP_MONTH:
	case NOWDB_EXPR_OP_MDAY:
	case NOWDB_EXPR_OP_WDAY:
	case NOWDB_EXPR_OP_WEEK:
	case NOWDB_EXPR_OP_HOUR:
	case NOWDB_EXPR_OP_MIN:
	case NOWDB_EXPR_OP_SEC:
	case NOWDB_EXPR_OP_MILLI:
	case NOWDB_EXPR_OP_MICRO:
	case NOWDB_EXPR_OP_NANO:
	case NOWDB_EXPR_OP_BIN:
	case NOWDB_EXPR_OP_FORMAT:
		return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT, NULL);

	/* -----------------------------------------------------------------------
	 * Bitwise
	 * -----------------------------------------------------------------------
	 */

	/* -----------------------------------------------------------------------
	 * String
	 * -----------------------------------------------------------------------
	 */
	case NOWDB_EXPR_OP_SUBSTR:
	case NOWDB_EXPR_OP_LENGTH:
	case NOWDB_EXPR_OP_STRCAT:
	case NOWDB_EXPR_OP_POS:
		return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT, NULL);

	default: INVALID("unknown function");

	}
	return NOWDB_OK;
}

static inline int getArgs(int o) {
	switch(o) {
	case NOWDB_EXPR_OP_FLOAT:
	case NOWDB_EXPR_OP_INT:
	case NOWDB_EXPR_OP_UINT:
	case NOWDB_EXPR_OP_TIME:
	case NOWDB_EXPR_OP_TEXT: return 1;

	case NOWDB_EXPR_OP_ADD:
	case NOWDB_EXPR_OP_SUB: 
	case NOWDB_EXPR_OP_MUL: 
	case NOWDB_EXPR_OP_DIV: 
	case NOWDB_EXPR_OP_REM:
	case NOWDB_EXPR_OP_POW: return 2;

	case NOWDB_EXPR_OP_LOG: 
	case NOWDB_EXPR_OP_ABS:
	case NOWDB_EXPR_OP_CEIL:
	case NOWDB_EXPR_OP_FLOOR:
	case NOWDB_EXPR_OP_ROUND: return 1;
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Evaluate Type
 * -----------------------------------------------------------------------
 */
static inline nowdb_type_t evalType(uint32_t        fun,
                                    nowdb_type_t *types) {
	switch(fun) {

	case NOWDB_EXPR_OP_FLOAT: return NOWDB_TYP_FLOAT;
	case NOWDB_EXPR_OP_INT: return NOWDB_TYP_INT;
	case NOWDB_EXPR_OP_UINT: return NOWDB_TYP_UINT;
	case NOWDB_EXPR_OP_TIME: return NOWDB_TYP_TIME;
	case NOWDB_EXPR_OP_TEXT: return NOWDB_TYP_TEXT;

	case NOWDB_EXPR_OP_ADD:
	case NOWDB_EXPR_OP_SUB: 
	case NOWDB_EXPR_OP_MUL: 
	case NOWDB_EXPR_OP_DIV: 
	case NOWDB_EXPR_OP_REM:
	case NOWDB_EXPR_OP_POW: return types[0]; /* check and correct */

	case NOWDB_EXPR_OP_LOG: 
	case NOWDB_EXPR_OP_ABS:
	case NOWDB_EXPR_OP_CEIL:
	case NOWDB_EXPR_OP_FLOOR:
	case NOWDB_EXPR_OP_ROUND: return types[0];
	default: return -1;
	}
}

int nowdb_op_fromName(char *op, char *agg) {
	*agg = 0;
	if (strcmp(op, "+") == 0) return NOWDB_EXPR_OP_ADD;
	if (strcmp(op, "-") == 0) return NOWDB_EXPR_OP_SUB;
	if (strcmp(op, "*") == 0) return NOWDB_EXPR_OP_MUL;
	if (strcmp(op, "/") == 0) return NOWDB_EXPR_OP_DIV;
	if (strcmp(op, "%") == 0) return NOWDB_EXPR_OP_REM;
	if (strcmp(op, "^") == 0) return NOWDB_EXPR_OP_POW;

	if (strcasecmp(op, "log") == 0) return NOWDB_EXPR_OP_LOG;
	if (strcasecmp(op, "abs") == 0) return NOWDB_EXPR_OP_ABS;
	if (strcasecmp(op, "ceil") == 0) return NOWDB_EXPR_OP_CEIL;
	if (strcasecmp(op, "floor") == 0) return NOWDB_EXPR_OP_FLOOR;
	if (strcasecmp(op, "round") == 0) return NOWDB_EXPR_OP_ROUND;

	if (strcasecmp(op, "tofloat") == 0) return NOWDB_EXPR_OP_FLOAT;
	if (strcasecmp(op, "toint") == 0) return NOWDB_EXPR_OP_INT;
	if (strcasecmp(op, "touint") == 0) return NOWDB_EXPR_OP_UINT;
	if (strcasecmp(op, "totime") == 0) return NOWDB_EXPR_OP_TIME;

	*agg = 1;

	return nowdb_fun_fromName(op);
}

