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
 * Create edge field expression
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

/* -----------------------------------------------------------------------
 * Create vertex field expression
 * -----------------------------------------------------------------------
 */
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
	FIELD(*expr)->off = NOWDB_OFF_VALUE;
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

/* -----------------------------------------------------------------------
 * Create constant value expression
 * -----------------------------------------------------------------------
 */
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
		CONST(*expr)->valbk = malloc(8);
		if (CONST(*expr)->valbk == NULL) {
			NOMEM("allocating value");
			free(*expr); *expr = NULL;
			return err;
		}
		memcpy(CONST(*expr)->value, value, 8);
		memcpy(CONST(*expr)->valbk, value, 8);
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Predeclaration get Args
 * -----------------------------------------------------------------------
 */
static inline int getArgs(int o);

/* -----------------------------------------------------------------------
 * Helper: Initialise Operator
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t initOp(nowdb_expr_t *expr,
                                 uint32_t       fun,
                                 uint32_t       len) {
	nowdb_err_t err;
	int sz;

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

	sz = len==0?1:len;

	OP(*expr)->types = calloc(sz, sizeof(nowdb_type_t));
	if (OP(*expr)->types == NULL) {
		NOMEM("allocating expression operand types");
		nowdb_expr_destroy(*expr);
		free(*expr); *expr = NULL;
		return err;
	}

	OP(*expr)->results = calloc(sz, sizeof(uint64_t));
	if (OP(*expr)->results == NULL) {
		NOMEM("allocating expression operand results");
		nowdb_expr_destroy(*expr);
		free(*expr); *expr = NULL;
		return err;
	}

	OP(*expr)->argv = calloc(sz, sizeof(nowdb_expr_t));
	if (OP(*expr)->argv == NULL) {
		NOMEM("allocating expression operands");
		nowdb_expr_destroy(*expr);
		free(*expr); *expr = NULL;
		return err;
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Create new operator (pass arguments as list)
 * -----------------------------------------------------------------------
 */
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

/* -----------------------------------------------------------------------
 * Create new operator (pass arguments as array)
 * -----------------------------------------------------------------------
 */
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

/* -----------------------------------------------------------------------
 * Create new operator (pass arguments as va_list)
 * -----------------------------------------------------------------------
 */
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
	if (cst->valbk != NULL) {
		free(cst->valbk); cst->valbk = NULL;
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

/* -----------------------------------------------------------------------
 * Helper: destroy vertex list
 * -----------------------------------------------------------------------
 */
static void destroyVertexList(ts_algo_list_t *vm) {
	ts_algo_list_node_t *run;

	for(run=vm->head; run!=NULL; run=run->nxt) {
		free(run->cont);
	}
	ts_algo_list_destroy(vm);
}

/* -----------------------------------------------------------------------
 * Helper: destroy edge list
 * -----------------------------------------------------------------------
 */
static void destroyEdgeList(ts_algo_list_t *em) {
	ts_algo_list_node_t *run;

	for(run=em->head; run!=NULL; run=run->nxt) {
		free(run->cont);
	}
	ts_algo_list_destroy(em);
}

/* -----------------------------------------------------------------------
 * Destroy evaluation helper
 * -----------------------------------------------------------------------
 */
void nowdb_eval_destroy(nowdb_eval_t *eval) {
	if (eval == NULL) return;
	if (eval->tlru != NULL) {
		nowdb_ptlru_destroy(eval->tlru);
		free(eval->tlru); eval->tlru = NULL;
	}
	destroyVertexList(&eval->vm);
	destroyEdgeList(&eval->em);
}

/* -----------------------------------------------------------------------
 * Helper: copy field
 * -----------------------------------------------------------------------
 */
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

/* -----------------------------------------------------------------------
 * Helper: copy constant value
 * -----------------------------------------------------------------------
 */
static nowdb_err_t copyConst(nowdb_const_t *src,
                             nowdb_expr_t  *trg) {
	return nowdb_expr_newConstant(trg, src->value,
	                                   src->type);
}

/* -----------------------------------------------------------------------
 * Helper: destropy operands
 * -----------------------------------------------------------------------
 */
#define DESTROYOPS(o,s) \
	for(int i=0;i<s;i++) { \
		nowdb_expr_destroy(ops[i]); free(ops[i]); \
	} \
	free(ops);

/* -----------------------------------------------------------------------
 * Helper: copy operation
 * -----------------------------------------------------------------------
 */
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
	if (one->value == NULL) return 0;
	if (two->value == NULL) return 0;
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
	if (hlp->needtxt && what != NULL) { \
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
                                         uint64_t      rmap,
                                         char          *src,
                                         nowdb_type_t  *t,
                                         void         **res) {
	nowdb_err_t err;

	/*
	fprintf(stderr, "VERTEX: %u (=%lu/%u)\n", field->off, 
	                     *(uint64_t*)(src+field->off), field->type);
	*/

	// this magic formula should be defined somewhere
	int i = (field->off-4)/8;
	if ((rmap & 1<<i) == 0) {
		*t = 0; return NOWDB_OK;
	}

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
                             uint64_t      rmap,
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
		err = getVertexValue(field, hlp, rmap, row, typ, res);
		if (err != NOWDB_OK) return err;
	}

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
static inline int evalType(nowdb_op_t *op);

/* -----------------------------------------------------------------------
 * Evaluate operation
 * -----------------------------------------------------------------------
 */
static nowdb_err_t evalOp(nowdb_op_t   *op,
                          nowdb_eval_t *hlp,
                          uint64_t     rmap,
                          char         *row,
                          nowdb_type_t *typ,
                          void        **res) {
	nowdb_err_t err;

	if (op->argv != NULL) {
		for(int i=0; i<op->args; i++) {
			err = nowdb_expr_eval(op->argv[i],
			                      hlp, rmap, row,
			                      op->types+i,
			                      op->results+i);
			if (err != NOWDB_OK) return err; 
		}
	}

	// determine type
	if (op->types != NULL) { 
		*typ = evalType(op);
		if ((int)*typ < 0) INVALIDTYPE("wrong type in operation");
	}
	if (*typ == 0) {
		// treat is null here
		
	}
	err = evalFun(op->fun, op->results, op->types, typ, &op->res);
	if (err != NOWDB_OK) return err;
	*res=&op->res;
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Evaluate aggregate
 * -----------------------------------------------------------------------
 */
static nowdb_err_t evalAgg(nowdb_agg_t  *agg,
                           nowdb_type_t *typ,
                           void        **res) 
{
	*typ = FUN(agg->agg)->otype;
	*res = &FUN(agg->agg)->r1;
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Evaluate constant expression
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t evalConst(nowdb_const_t *cst,
                                    nowdb_type_t  *typ,
                                    void         **res) {
	*typ = cst->type;
	if (cst->type != NOWDB_TYP_TEXT) {
		memcpy(cst->value, cst->valbk, 8);
	}
	*res = cst->value;
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Evaluate expression
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_eval(nowdb_expr_t expr,
                            nowdb_eval_t *hlp,
                            uint64_t     rmap,
                            char         *row,
                            nowdb_type_t *typ,
                            void        **res) {
	if (expr == NULL) return NOWDB_OK;
	switch(EXPR(expr)->etype) {
	case NOWDB_EXPR_FIELD:
		return evalField(FIELD(expr), hlp, rmap, row, typ, res);

	case NOWDB_EXPR_CONST:
		return evalConst(CONST(expr), typ, res);

	case NOWDB_EXPR_OP:
		return evalOp(OP(expr), hlp, rmap, row, typ, res);

	case NOWDB_EXPR_REF:
		return nowdb_expr_eval(REF(expr)->ref, hlp,
		                       rmap, row, typ, res);

	case NOWDB_EXPR_AGG:
		return evalAgg(AGG(expr), typ, res);

	default:
		INVALID("unknown expression type");
	}
}

/* -----------------------------------------------------------------------
 * Built-in functions and operators
 * -----------------------------------------------------------------------
 */
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

/* -----------------------------------------------------------------------
 * Perform 2-place operation
 * -----------------------------------------------------------------------
 */
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

/* -----------------------------------------------------------------------
 * Perform 2-place operation, float not allowd
 * -----------------------------------------------------------------------
 */
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

/* -----------------------------------------------------------------------
 * Perform one-argument operator/function
 * -----------------------------------------------------------------------
 */
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

/* -----------------------------------------------------------------------
 * Convert to float
 * -----------------------------------------------------------------------
 */
#define TOFLOAT(t,x,z) \
	switch(t) { \
	case NOWDB_TYP_UINT: \
		f = (double)(*(uint64_t*)x); \
		MOV(z, &f); break; \
	case NOWDB_TYP_DATE: \
	case NOWDB_TYP_TIME: \
	case NOWDB_TYP_INT: \
		f = (double)(*(int64_t*)x); \
		MOV(z, &f); break; \
	default: \
		MOV(z, x); break; \
	}

/* -----------------------------------------------------------------------
 * Convert to int
 * -----------------------------------------------------------------------
 */
#define TOINT(t,x,z) \
	switch(t) { \
	case NOWDB_TYP_UINT: \
		l = (int64_t)(*(uint64_t*)x); \
		MOV(z, &l); break; \
	case NOWDB_TYP_FLOAT: \
		l = (int64_t)(*(double*)x); \
		MOV(z, &l); break; \
	default: \
		MOV(z, x); break; \
	}

/* -----------------------------------------------------------------------
 * Convert to unsigned int
 * -----------------------------------------------------------------------
 */
#define TOUINT(t,x,z) \
	switch(t) { \
	case NOWDB_TYP_FLOAT: \
		u = (uint64_t)(*(double*)x); \
		MOV(z, &u); break; \
	case NOWDB_TYP_DATE: \
	case NOWDB_TYP_TIME: \
	case NOWDB_TYP_INT: \
		u = (uint64_t)(*(int64_t*)x); \
		MOV(z, &u); break; \
	default: \
		MOV(z, x); break; \
	}

/* -----------------------------------------------------------------------
 * Get time component
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t getTimeComp(uint32_t fun, void *arg, void *res) {
	struct tm tm;
	int64_t c;

	if (nowdb_time_break(*(nowdb_time_t*)arg, &tm) != 0) {
		return nowdb_err_get(nowdb_err_time, TRUE, OBJECT,
		                      "cannot convert to OS time");
	}
	switch(fun) {
	case NOWDB_EXPR_OP_YEAR: c = tm.tm_year + 1900; break;
	case NOWDB_EXPR_OP_MONTH: c = tm.tm_mon + 1; break;
	case NOWDB_EXPR_OP_MDAY: c = tm.tm_mday; break;
	case NOWDB_EXPR_OP_WDAY: c = tm.tm_wday; break;
	case NOWDB_EXPR_OP_YDAY: c = tm.tm_yday; break;
	case NOWDB_EXPR_OP_HOUR: c = tm.tm_hour; break;
	case NOWDB_EXPR_OP_MIN: c = tm.tm_min; break;
	case NOWDB_EXPR_OP_SEC: c = tm.tm_sec; break;
	default: INVALID("unknown time component");
	}
	memcpy(res, &c, 8);
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Get time sub-second component
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t getTimeSubComp(uint32_t fun, void *arg, void *res) {
	nowdb_system_time_t tm;
	int64_t c;

	if (nowdb_time_toSystem(*(nowdb_time_t*)arg, &tm) != 0) {
		return nowdb_err_get(nowdb_err_time, TRUE, OBJECT,
		                      "cannot convert to OS time");
	}
	switch(fun) {
	case NOWDB_EXPR_OP_MILLI: c = tm.tv_nsec/1000000; break;
	case NOWDB_EXPR_OP_MICRO: c = tm.tv_nsec/1000; break;
	case NOWDB_EXPR_OP_NANO: c = tm.tv_nsec; break;
	default: INVALID("unknown time component");
	}
	memcpy(res, &c, 8);
	return NOWDB_OK;
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
	case NOWDB_EXPR_OP_FLOAT: TOFLOAT(types[0], argv[0], res); break;
	case NOWDB_EXPR_OP_UINT: TOUINT(types[0], argv[0], res); break; 
	case NOWDB_EXPR_OP_INT: 
	case NOWDB_EXPR_OP_TIME: TOINT(types[0], argv[0], res); break;
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
		return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT, NULL);

	case NOWDB_EXPR_OP_YEAR:
	case NOWDB_EXPR_OP_MONTH:
	case NOWDB_EXPR_OP_MDAY:
	case NOWDB_EXPR_OP_WDAY:
	case NOWDB_EXPR_OP_YDAY:
	case NOWDB_EXPR_OP_HOUR:
	case NOWDB_EXPR_OP_MIN:
	case NOWDB_EXPR_OP_SEC:
		if (types[0] != NOWDB_TYP_TIME &&
		    types[0] != NOWDB_TYP_DATE) {
			INVALIDTYPE("not a time value");
		}
		return getTimeComp(fun, argv[0], res);

	case NOWDB_EXPR_OP_MILLI:
	case NOWDB_EXPR_OP_MICRO:
	case NOWDB_EXPR_OP_NANO:
		if (types[0] != NOWDB_TYP_TIME &&
		    types[0] != NOWDB_TYP_DATE) 
		{
			INVALIDTYPE("not a time value");
		}
		return getTimeSubComp(fun, argv[0], res);

	case NOWDB_EXPR_OP_DAWN:
		(*(nowdb_time_t*)res) = NOWDB_TIME_DAWN; return NOWDB_OK;
	case NOWDB_EXPR_OP_DUSK:
		(*(nowdb_time_t*)res) = NOWDB_TIME_DUSK; return NOWDB_OK;
	case NOWDB_EXPR_OP_EPOCH:
		(*(nowdb_time_t*)res) = 0; return NOWDB_OK;

	case NOWDB_EXPR_OP_NOW:
		if (nowdb_time_now(res) != 0) {
			return nowdb_err_get(nowdb_err_time, TRUE, OBJECT,
			                          "getting currrent time");
		}
		return NOWDB_OK;

	case NOWDB_EXPR_OP_BIN:
	case NOWDB_EXPR_OP_FORMAT:
		return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT, NULL);

	/* -----------------------------------------------------------------------
	 * Conditionals
	 * -----------------------------------------------------------------------
	 */

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

/* -----------------------------------------------------------------------
 * Get number of arguments
 * -----------------------------------------------------------------------
 */
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

	case NOWDB_EXPR_OP_YEAR:
	case NOWDB_EXPR_OP_MONTH:
	case NOWDB_EXPR_OP_MDAY:
	case NOWDB_EXPR_OP_WDAY:
	case NOWDB_EXPR_OP_YDAY:
	case NOWDB_EXPR_OP_HOUR:
	case NOWDB_EXPR_OP_MIN:
	case NOWDB_EXPR_OP_SEC:
	case NOWDB_EXPR_OP_MILLI:
	case NOWDB_EXPR_OP_MICRO:
	case NOWDB_EXPR_OP_NANO: return 1;

	case NOWDB_EXPR_OP_DAWN:
	case NOWDB_EXPR_OP_DUSK:
	case NOWDB_EXPR_OP_EPOCH:
	case NOWDB_EXPR_OP_NOW: return 0;

	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Check if type is numeric
 * -----------------------------------------------------------------------
 */
static inline char isNumeric(nowdb_type_t t) {
	switch(t) {
	case NOWDB_TYP_TEXT:
	case NOWDB_TYP_BOOL: return 0;
	default: return 1;
	}
}

/* -----------------------------------------------------------------------
 * Enforce type x
 * -----------------------------------------------------------------------
 */
static inline void enforceType(nowdb_op_t *op,
                               nowdb_type_t t) {
	double  f;
	int64_t l;

	for(int i=0; i<op->args; i++) {
		switch(t) {
		case NOWDB_TYP_FLOAT:
			TOFLOAT(op->types[i], op->results[i],
			                      op->results[i]);
			op->types[i] = t;
			break;

		case NOWDB_TYP_INT:
		case NOWDB_TYP_TIME:
		case NOWDB_TYP_DATE:
			TOINT(op->types[i], op->results[i],
			                    op->results[i]);
			op->types[i] = t;
			break;

		default: return;
		} 
	}
}

/* -----------------------------------------------------------------------
 * Check and correct
 * -----------------------------------------------------------------------
 */
static inline int correctNumTypes(nowdb_op_t *op) {
	char differ=0;
	int t = op->types[0];

	if (t == NOWDB_TYP_TEXT) return -1;
	if (t == NOWDB_TYP_BOOL) return -1;

	for(int i=1; i<op->args; i++) {
		if (op->types[i] == NOWDB_TYP_TEXT) return -1;
		if (op->types[i] == NOWDB_TYP_BOOL) return -1;

		if (t != op->types[i]) {
			differ=1;
			if (t            != NOWDB_TYP_FLOAT &&
			    op->types[i] == NOWDB_TYP_FLOAT) {
				t = NOWDB_TYP_FLOAT; continue;
			}
			if (t            == NOWDB_TYP_UINT &&
			   (op->types[i] == NOWDB_TYP_INT  ||
			    op->types[i] == NOWDB_TYP_TIME ||
			    op->types[i] == NOWDB_TYP_DATE)){ 
				t = op->types[i]; continue;
			}
		}
	}
	if (!differ) return t;
	enforceType(op, t);
	return t;
}

/* -----------------------------------------------------------------------
 * Evaluate Type
 * -----------------------------------------------------------------------
 */
static inline int evalType(nowdb_op_t *op) {

	switch(op->fun) {

	case NOWDB_EXPR_OP_FLOAT: return NOWDB_TYP_FLOAT;
	case NOWDB_EXPR_OP_INT: return NOWDB_TYP_INT;
	case NOWDB_EXPR_OP_UINT: return NOWDB_TYP_UINT;
	case NOWDB_EXPR_OP_TIME: return NOWDB_TYP_TIME;
	case NOWDB_EXPR_OP_TEXT: return NOWDB_TYP_TEXT;

	case NOWDB_EXPR_OP_ADD:
	case NOWDB_EXPR_OP_SUB: 
	case NOWDB_EXPR_OP_MUL: 
	case NOWDB_EXPR_OP_DIV: return correctNumTypes(op);

	case NOWDB_EXPR_OP_POW: 
		if (!isNumeric(op->types[0]) ||
		    !isNumeric(op->types[1])) return -1;
                enforceType(op,NOWDB_TYP_FLOAT);
		return NOWDB_TYP_FLOAT;

	case NOWDB_EXPR_OP_REM:
		if (!isNumeric(op->types[0]) ||
		    !isNumeric(op->types[1])) return -1;
		if (op->types[0] == NOWDB_TYP_FLOAT ||
		    op->types[1] == NOWDB_TYP_FLOAT) return -1;
		return correctNumTypes(op);

	case NOWDB_EXPR_OP_ABS:
		if (!isNumeric(op->types[0])) return -1;
		return op->types[0];

	case NOWDB_EXPR_OP_LOG:
	case NOWDB_EXPR_OP_CEIL:
	case NOWDB_EXPR_OP_FLOOR:
	case NOWDB_EXPR_OP_ROUND:
		if (op->types[0] != NOWDB_TYP_FLOAT) {
			enforceType(op, NOWDB_TYP_FLOAT);
		}
		return NOWDB_TYP_FLOAT;

	case NOWDB_EXPR_OP_YEAR:
	case NOWDB_EXPR_OP_MONTH:
	case NOWDB_EXPR_OP_MDAY:
	case NOWDB_EXPR_OP_WDAY:
	case NOWDB_EXPR_OP_YDAY:
	case NOWDB_EXPR_OP_HOUR:
	case NOWDB_EXPR_OP_MIN:
	case NOWDB_EXPR_OP_SEC: return NOWDB_TYP_UINT;

	case NOWDB_EXPR_OP_MILLI:
	case NOWDB_EXPR_OP_MICRO:
	case NOWDB_EXPR_OP_NANO: return NOWDB_TYP_UINT;

	case NOWDB_EXPR_OP_DAWN:
	case NOWDB_EXPR_OP_DUSK:
	case NOWDB_EXPR_OP_EPOCH:
	case NOWDB_EXPR_OP_NOW: return NOWDB_TYP_TIME;

	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Get operator, function or aggregate from name/symbol
 * -----------------------------------------------------------------------
 */
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

	if (strcasecmp(op, "year") == 0) return NOWDB_EXPR_OP_YEAR;
	if (strcasecmp(op, "month") == 0) return NOWDB_EXPR_OP_MONTH;
	if (strcasecmp(op, "mday") == 0) return NOWDB_EXPR_OP_MDAY;
	if (strcasecmp(op, "wday") == 0) return NOWDB_EXPR_OP_WDAY;
	if (strcasecmp(op, "yday") == 0) return NOWDB_EXPR_OP_YDAY;
	if (strcasecmp(op, "hour") == 0) return NOWDB_EXPR_OP_HOUR;
	if (strcasecmp(op, "minute") == 0) return NOWDB_EXPR_OP_MIN;
	if (strcasecmp(op, "second") == 0) return NOWDB_EXPR_OP_SEC;
	if (strcasecmp(op, "milli") == 0) return NOWDB_EXPR_OP_MILLI;
	if (strcasecmp(op, "micro") == 0) return NOWDB_EXPR_OP_MICRO;
	if (strcasecmp(op, "nano") == 0) return NOWDB_EXPR_OP_NANO;
	if (strcasecmp(op, "dawn") == 0) return NOWDB_EXPR_OP_DAWN;
	if (strcasecmp(op, "dusk") == 0) return NOWDB_EXPR_OP_DUSK;
	if (strcasecmp(op, "now") == 0) return NOWDB_EXPR_OP_NOW;
	if (strcasecmp(op, "epoch") == 0) return NOWDB_EXPR_OP_EPOCH;

	*agg = 1;

	return nowdb_fun_fromName(op);
}
