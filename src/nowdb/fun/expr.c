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

#define CONSTOP(x,i) \
	CONST(OP(x)->argv[i])

#define FIELDOP(x,i) \
	FIELD(OP(x)->argv[i])

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
 * Expression is boolean operation
 * -----------------------------------------------------------------------
 */
char nowdb_expr_bool(nowdb_expr_t expr) {
	if (expr == NULL) return -1;
	if (EXPR(expr)->etype == NOWDB_EXPR_OP) {
		return (OP(expr)->fun == NOWDB_EXPR_OP_TRUE  ||
		        OP(expr)->fun == NOWDB_EXPR_OP_FALSE ||
		        OP(expr)->fun == NOWDB_EXPR_OP_AND   ||
		        OP(expr)->fun == NOWDB_EXPR_OP_OR    ||
		        OP(expr)->fun == NOWDB_EXPR_OP_NOT);
	}
	return 0;
}

/* -----------------------------------------------------------------------
 * Expression is comparison operation
 * -----------------------------------------------------------------------
 */
char nowdb_expr_compare(nowdb_expr_t expr) {
	if (expr == NULL) return -1;
	if (EXPR(expr)->etype == NOWDB_EXPR_OP) {
		return (OP(expr)->fun == NOWDB_EXPR_OP_EQ ||
		        OP(expr)->fun == NOWDB_EXPR_OP_NE ||
		        OP(expr)->fun == NOWDB_EXPR_OP_GT ||
		        OP(expr)->fun == NOWDB_EXPR_OP_LT ||
		        OP(expr)->fun == NOWDB_EXPR_OP_GE ||
		        OP(expr)->fun == NOWDB_EXPR_OP_LE ||
		        OP(expr)->fun == NOWDB_EXPR_OP_IN);
	}
	return 0;
}

/* ------------------------------------------------------------------------
 * Callbacks for 'IN' tree
 * ------------------------------------------------------------------------
 */
#define TREETYPE(t) \
	(nowdb_type_t)(uint64_t)(((ts_algo_tree_t*)t)->rsc)

static ts_algo_cmp_t eqcompare(void *tree, void *one, void *two) {
	int x;
	switch(TREETYPE(tree)) {
	case NOWDB_TYP_TEXT:      // handle text as text
	case NOWDB_TYP_LONGTEXT:
		/*
		fprintf(stderr, "comparing %s and %s\n", (char*)one,
		                                         (char*)two);
		*/
		x = strcmp(one, two);
		if (x < 0) return ts_algo_cmp_less;
		if (x > 0) return ts_algo_cmp_greater;
		return ts_algo_cmp_equal;

	case NOWDB_TYP_UINT:
		
		/*
		fprintf(stderr, "comparing %lu and %lu\n", *(uint64_t*)one,
		                                           *(uint64_t*)two);
		*/
		
		if (*(uint64_t*)one <
		    *(uint64_t*)two) return ts_algo_cmp_less;
		if (*(uint64_t*)one >
		    *(uint64_t*)two) return ts_algo_cmp_greater;
		return ts_algo_cmp_equal;

	case NOWDB_TYP_DATE:
	case NOWDB_TYP_TIME:
	case NOWDB_TYP_INT:
		/*
		fprintf(stderr, "comparing %ld and %ld\n", *(int64_t*)one,
		                                           *(int64_t*)two);
		*/
		if (*(int64_t*)one <
		    *(int64_t*)two) return ts_algo_cmp_less;
		if (*(int64_t*)one >
		    *(int64_t*)two) return ts_algo_cmp_greater;
		return ts_algo_cmp_equal;

	case NOWDB_TYP_FLOAT:
		/*
		fprintf(stderr, "comparing %f and %f\n", *(double*)one,
		                                         *(double*)two);
		*/
		if (*(double*)one <
		    *(double*)two) return ts_algo_cmp_less;
		if (*(double*)one >
		    *(double*)two) return ts_algo_cmp_greater;
		return ts_algo_cmp_equal;

	default: return ts_algo_cmp_greater;

	// BOOL
	}
}

static ts_algo_rc_t noupdate(void *ignore, void *o, void *n) {
	free(n);
	return TS_ALGO_OK;
}

static void valdestroy(void *ignore, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	free(*n); *n=NULL;
}

/* -----------------------------------------------------------------------
 * Create edge field expression
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_newEdgeField(nowdb_expr_t *expr,
                                    char     *propname,
                                    uint32_t       off,
                                    nowdb_type_t  type,
                                    uint16_t       num) {
	nowdb_err_t err;

	*expr = calloc(1,sizeof(nowdb_field_t));
	if (*expr == NULL)  {
		NOMEM("allocating expression");
		return err;
	}

	FIELD(*expr)->etype = NOWDB_EXPR_FIELD;

	FIELD(*expr)->name = NULL;
	FIELD(*expr)->text = NULL;
	FIELD(*expr)->content = NOWDB_CONT_EDGE;
	FIELD(*expr)->off = (int)off;
	FIELD(*expr)->type = type;
	FIELD(*expr)->num = num;

	if (off > NOWDB_OFF_USER) {
		nowdb_edge_getCtrl(num, off,
		     &FIELD(*expr)->ctrlbit,
		     &FIELD(*expr)->ctrlbyte);
	}
	if (propname != NULL) {
		FIELD(*expr)->name = strdup(propname);
		if (FIELD(*expr)->name == NULL) {
			NOMEM("allocating field name");
			free(*expr); *expr = NULL;
			return err;
		}
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Create vertex field with offset 
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_newVertexOffField(nowdb_expr_t *expr, uint32_t off) {
	nowdb_err_t err;

	*expr = calloc(1,sizeof(nowdb_field_t));
	if (*expr == NULL)  {
		NOMEM("allocating expression");
		return err;
	}

	FIELD(*expr)->etype = NOWDB_EXPR_FIELD;

	FIELD(*expr)->name = NULL;
	FIELD(*expr)->text = NULL;
	FIELD(*expr)->content = NOWDB_CONT_VERTEX;
	FIELD(*expr)->off = (int)off;
	FIELD(*expr)->type = off==NOWDB_OFF_ROLE?
	                          NOWDB_TYP_SHORT:
	                          NOWDB_TYP_UINT;
	
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Create vertex field expression
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_newVertexField(nowdb_expr_t  *expr,
                                      char      *propname,
                                      nowdb_roleid_t role,
                                      nowdb_key_t  propid,
                                      nowdb_type_t   type) {
	nowdb_err_t err;

	*expr = calloc(1,sizeof(nowdb_field_t));
	if (*expr == NULL)  {
		NOMEM("allocating expression");
		return err;
	}

	FIELD(*expr)->etype = NOWDB_EXPR_FIELD;

	FIELD(*expr)->text = NULL;
	FIELD(*expr)->content = NOWDB_CONT_VERTEX;
	FIELD(*expr)->off = NOWDB_OFF_VALUE;
	FIELD(*expr)->role = role;
	FIELD(*expr)->propid = propid;
	FIELD(*expr)->pk = 0;
	FIELD(*expr)->name = NULL;
	FIELD(*expr)->type = type;

	if (propname != NULL) {
		FIELD(*expr)->name = strdup(propname);
		if (FIELD(*expr)->name == NULL) {
			NOMEM("allocating field name");
			free(*expr); *expr = NULL;
			return err;
		}
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Tell field not to use text
 * -----------------------------------------------------------------------
 */
void nowdb_expr_usekey(nowdb_expr_t expr) {
	if (expr == NULL) return;
	if (EXPR(expr)->etype != NOWDB_EXPR_FIELD) return;
	if (FIELD(expr)->type != NOWDB_TYP_TEXT &&
	    FIELD(expr)->type != NOWDB_TYP_NOTHING) return;
	FIELD(expr)->usekey = 1; // this overrules the model
	FIELD(expr)->type = NOWDB_TYP_UINT;
}

/* -----------------------------------------------------------------------
 * Helper: Create and init constant value expression
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t initConst(nowdb_expr_t *expr,
                                    nowdb_type_t  type) {
	nowdb_err_t err;

	*expr = calloc(1,sizeof(nowdb_const_t));
	if (*expr == NULL)  {
		NOMEM("allocating expression");
		return err;
	}

	CONST(*expr)->etype = NOWDB_EXPR_CONST;
	CONST(*expr)->type  = type;
	CONST(*expr)->value = NULL;
	CONST(*expr)->tree  = NULL;

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

	err = initConst(expr, type);
	if (err != NOWDB_OK) return err;

	if (type == NOWDB_TYP_NOTHING) return NOWDB_OK;
	if (type == NOWDB_TYP_TEXT) {
		CONST(*expr)->value = strdup(value);
		if (CONST(*expr)->value == NULL) {
			NOMEM("allocating value");
			free(*expr); *expr = NULL;
			return err;
		}
	} else {
		int sz = type == NOWDB_TYP_SHORT?4:8;
		CONST(*expr)->value = malloc(sz);
		if (CONST(*expr)->value == NULL) {
			NOMEM("allocating value");
			free(*expr); *expr = NULL;
			return err;
		}
		memcpy(CONST(*expr)->value, value, sz);
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Create the 'IN' tree
 * ------------------------------------------------------------------------
 */
static nowdb_err_t fillInTree(ts_algo_tree_t *tree,
                              ts_algo_list_t *list) {
	nowdb_err_t          err;
	ts_algo_list_node_t *run,*tmp;

	run=list->head;
	while(run!=NULL) {
		if (ts_algo_tree_insert(tree, run->cont) != TS_ALGO_OK)
		{
			NOMEM("tree.insert");
			return err;
		}
		tmp = run->nxt;
		ts_algo_list_remove(list, run); free(run);
		run=tmp;
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Create Constant expression representing an 'in' list from a list
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_constFromList(nowdb_expr_t   *expr,
                                     ts_algo_list_t *list,
                                     nowdb_type_t    type) {
	nowdb_err_t err;
	ts_algo_tree_t *tree;

	err = nowdb_expr_newTree(&tree, type);
	if (err != NOWDB_OK) return err;

	err = fillInTree(tree, list);
	if (err != NOWDB_OK) {
		ts_algo_tree_destroy(tree); free(tree);
		return err;
	}

	err = nowdb_expr_constFromTree(expr, tree, type);
	if (err != NOWDB_OK) {
		ts_algo_tree_destroy(tree); free(tree);
		return err;
	}

	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Create Constant expression representing an 'in' list from a tree
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_constFromTree(nowdb_expr_t   *expr,
                                     ts_algo_tree_t *tree,
                                     nowdb_type_t    type) {
	nowdb_err_t err;

	err = initConst(expr, type);
	if (err != NOWDB_OK) return err;

	CONST(*expr)->tree = tree;

	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Create such a tree for constant expression
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_newTree(ts_algo_tree_t **tree,
                               nowdb_type_t    type) {
	nowdb_err_t err;

	*tree = ts_algo_tree_new(&eqcompare, NULL, &noupdate,
	                         &valdestroy, &valdestroy);
	if (*tree == NULL) {
		NOMEM("tree.new");
		return err;
	}
	(*tree)->rsc = (void*)(uint64_t)type;
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
	OP(*expr)->text = NULL;

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
	if (cst->tree != NULL) {
		ts_algo_tree_destroy(cst->tree);
		free(cst->tree); cst->tree = NULL;
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
	if (op->text != NULL) {
		free(op->text); op->text = NULL;
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
 * Destroy evaluation helper
 * -----------------------------------------------------------------------
 */
void nowdb_eval_destroy(nowdb_eval_t *eval) {
	if (eval == NULL) return;
	if (eval->tlru != NULL) {
		nowdb_ptlru_destroy(eval->tlru);
		free(eval->tlru); eval->tlru = NULL;
	}
}

/* -----------------------------------------------------------------------
 * Helper: copy field
 * -----------------------------------------------------------------------
 */
static nowdb_err_t copyField(nowdb_field_t *src,
                             nowdb_expr_t  *trg)  {
	nowdb_err_t err;

	if (src->content == NOWDB_CONT_EDGE) {
		err = nowdb_expr_newEdgeField(trg, src->name, src->off,
		                                   src->type, src->num);
		
	} else {
		err = src->name == NULL?
		      nowdb_expr_newVertexOffField(trg, src->off):
		      nowdb_expr_newVertexField(trg, src->name,
		                                     src->role,
		                                     src->propid,
		                                     src->type);
	}
	if (err != NOWDB_OK) return err;
	FIELD(*trg)->usekey = src->usekey;
	return NOWDB_OK;
}

#define DESTROYLIST(l) \
	for(run=l->head;run!=NULL;run=run->nxt) { \
		free(run->cont); \
	} \
	ts_algo_list_destroy(l);

static inline nowdb_err_t copyInList(nowdb_type_t      t,
                                     ts_algo_list_t *src,
                                     ts_algo_list_t *trg) {
	nowdb_err_t err;
	ts_algo_list_node_t *run;
	void *value;

	for(run=src->head;run!=NULL;run=run->nxt) {
		if (t == NOWDB_TYP_TEXT     ||
		    t == NOWDB_TYP_LONGTEXT) {
			value = strdup(run->cont);
		} else if (t == NOWDB_TYP_SHORT) {
			value = malloc(4);
			if (value != NULL) {
				memcpy(value, run->cont, 4);
			}
		} else {
			value = malloc(8);
			if (value != NULL) {
				memcpy(value, run->cont, 8);
			}
		}
		if (value == NULL) {
			NOMEM("copying 'IN' tree");
			return err;
		}
		if (ts_algo_list_append(trg, value) != TS_ALGO_OK) {
			NOMEM("list.append");
			free(value);
			DESTROYLIST(trg);
			return err;
		}
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Helper: copy constant value
 * -----------------------------------------------------------------------
 */
static nowdb_err_t copyConst(nowdb_const_t *src,
                             nowdb_expr_t  *trg) {
	if (src->tree == NULL) {
		return nowdb_expr_newConstant(trg, src->value,
		                                   src->type);
	} else {
		nowdb_err_t     err;
		ts_algo_list_t *tmp;
		ts_algo_list_t  tmp2;
		tmp = ts_algo_tree_toList(src->tree);
		if (tmp == NULL) {
			NOMEM("tree.toList");
			return err;
		}
		ts_algo_list_init(&tmp2);
		err = copyInList(src->type, tmp, &tmp2);
		ts_algo_list_destroy(tmp); free(tmp);
		if (err != NOWDB_OK) return err;
		
		err = nowdb_expr_constFromList(trg, &tmp2, src->type);
		ts_algo_list_destroy(&tmp2);
		return err;
	}
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
	if (one->content != two->content) return 0;

	// different edges???
	if (one->content == NOWDB_CONT_EDGE) {
		return (one->off  == two->off);
	}
	if (one->role != two->role) return 0;
	if (strcasecmp(one->name, two->name) == 0) return 1;
	return 0;
}

/* -----------------------------------------------------------------------
 * Two trees are equivalent
 * -----------------------------------------------------------------------
 */
static inline char treeEqual(nowdb_const_t *one,
                             nowdb_const_t *two) {
	ts_algo_list_t *fst, *snd;
	ts_algo_list_node_t *r1, *r2;

	if (one->tree->count == 0 &&
	    two->tree->count == 0) return 1;
	if (one->tree->count == 0) return 0;
	if (two->tree->count == 0) return 0;

	fst = ts_algo_tree_toList(one->tree);
	if (fst == NULL) return 0;

	snd = ts_algo_tree_toList(two->tree);
	if (snd == NULL) {
		ts_algo_list_destroy(fst); free(fst);
		return 0;
	}
	r1 = fst->head; r2 = snd->head;
	while(r1 != NULL && r2 != NULL) {
		if (eqcompare(one->tree,
		              r1->cont,
		              r2->cont) != ts_algo_cmp_equal) 
		{
			ts_algo_list_destroy(fst); free(fst);
			ts_algo_list_destroy(snd); free(snd);
			return 0;
		}
		r1=r1->nxt; r2=r2->nxt;
	}
	ts_algo_list_destroy(fst); free(fst);
	ts_algo_list_destroy(snd); free(snd);

	if (r1 == NULL && r2 == NULL) return 1;

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
	if (one->value == NULL && two->value == NULL &&
	    one->tree  == NULL && two->tree  == NULL) return 1;
	if (one->value == NULL && one->tree == NULL) return 0;
	if (two->value == NULL && two->tree == NULL) return 0;
	if (one->tree  != NULL &&
	    two->tree  != NULL) return treeEqual(one, two);
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
	if (( !field->usekey) && what != NULL) { \
		*t = (char)NOWDB_TYP_TEXT; \
		err = getText(hlp, *(nowdb_key_t*)what, &field->text); \
		if (err != NOWDB_OK) return err; \
		*res = field->text; \
	} else { \
		*t = (char)NOWDB_TYP_UINT; \
		*res=what; \
	}

#define HANDLENULL(src,f) \
	if (!(*(int*)(src+NOWDB_OFF_USER+f->ctrlbyte) & \
	                           (1 << f->ctrlbit))) \
	{ \
		*t = NOWDB_TYP_NOTHING; break; \
	} \


/* ------------------------------------------------------------------------
 * Helper: get EdgeValue
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getEdgeValue(nowdb_field_t *field,
                                       nowdb_eval_t  *hlp,
                                       char          *src,
                                       nowdb_type_t  *t,
                                       void         **res) {
	void *u=NULL;
	nowdb_err_t  err;

	*res = src+field->off;

	switch(field->off) {
	case NOWDB_OFF_ORIGIN:
		if (field->type == NOWDB_TYP_TEXT) {
			HANDLETEXT((src+NOWDB_OFF_ORIGIN));
		} else {
			*t = NOWDB_TYP_UINT; // why uint?
			*res = src+NOWDB_OFF_ORIGIN;
		}
		break;

	case NOWDB_OFF_DESTIN:
		if (field->type == NOWDB_TYP_TEXT) {
			HANDLETEXT((src+NOWDB_OFF_DESTIN));
		} else {
			*t = NOWDB_TYP_UINT; // why uint?
			*res = src+NOWDB_OFF_DESTIN;
		}
		break;

	/*
	case NOWDB_OFF_STAMP:
		*t = NOWDB_TYP_TIME;
		*res = src+NOWDB_OFF_STAMP;
		break;
	*/

	default:
		HANDLENULL(src,field);
		
		u = src+field->off;
		*t = field->type;

		switch(*t) {
		case NOWDB_TYP_TEXT:
			HANDLETEXT(u);
			break;

		case NOWDB_TYP_DATE:
		case NOWDB_TYP_TIME:
		case NOWDB_TYP_FLOAT:
		case NOWDB_TYP_INT:
		case NOWDB_TYP_UINT:
			*res = u;
			break;
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: get raw VertexValue
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getRawVertexValue(nowdb_field_t *field,
                                            nowdb_eval_t  *hlp,
                                            char          *src,
                                            nowdb_type_t  *t,
                                            void         **res) {

	*t = field->type;
	*res = src+field->off;

	/*
	fprintf(stderr, "RAW %d: %lu / %lu\n", field->off,
 	                 ((nowdb_vertex_t*)src)->property,
	                    ((nowdb_vertex_t*)src)->value);
	*/

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

	if (field->name == NULL) {
		// fprintf(stderr, "raw with map %lu\n", rmap);
		return getRawVertexValue(field, hlp, src, t, res);
	}

	/*
	fprintf(stderr, "VERTEX: %u (=%lu/%u)\n", field->off, 
	                     *(uint64_t*)(src+field->off), field->type);
	*/

	// this magic formula should be defined somewhere
	int i = (field->off-4)/8;
	// fprintf(stderr, "rmap: %lu (%d, %d)\n", rmap, i, field->off);
	if ((rmap & 1<<i) == 0) {
		// fprintf(stderr, "%s evaluates to NULL\n", field->name);
		*res = src+field->off;
		*t = NOWDB_TYP_NOTHING; return NOWDB_OK;
	}

	*t = field->type; // where does the type come from?
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

	if (field->content == NOWDB_CONT_EDGE) {
		err = getEdgeValue(field, hlp, row, typ, res);
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
                           int         args,
                           void        **argv,
                           nowdb_type_t *types,
                           nowdb_type_t *t,
                           void         *res);

/* -----------------------------------------------------------------------
 * Evaluate Fun (predeclaration, implementation below)
 * -----------------------------------------------------------------------
 */
static inline int evalType(nowdb_op_t *op, char guess);

#define SETRESULT(t,x) \
	*typ = t; op->res=x; *res=&op->res;

#define SETXRESULT(t,x) \
	*typ = t; memcpy(&op->res,&x,sizeof(char*)); *res=(char*)op->res;

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

	// evaluate operands
	if (op->argv != NULL) {
		for(int i=0; i<op->args; i++) {

			// the "main" thing
			err = nowdb_expr_eval(op->argv[i],
			                      hlp, rmap, row,
			                      op->types+i,
			                      op->results+i);
			if (err != NOWDB_OK) return err;

			// "short" circuit is longer than the main thing :-(
			if (i == 0) {
				if (op->fun == NOWDB_EXPR_OP_AND &&
			            *(nowdb_value_t*)op->results[0] == 0) {
					SETRESULT(NOWDB_TYP_BOOL, 0);
					return NOWDB_OK;
				}
				if (op->fun == NOWDB_EXPR_OP_OR &&
				    *((nowdb_value_t*)op->results[0]) != 0) {
					SETRESULT(NOWDB_TYP_BOOL, 1);
					return NOWDB_OK;
				}
				if (op->fun == NOWDB_EXPR_OP_IS) {
					if (op->types[0] == 0) {
						SETRESULT(NOWDB_TYP_BOOL, 1);
					} else {
						SETRESULT(NOWDB_TYP_BOOL, 0);
					}
					return NOWDB_OK;
				}
				if (op->fun == NOWDB_EXPR_OP_ISN) {
					if (op->types[0] == 0) {
						SETRESULT(NOWDB_TYP_BOOL, 0);
					} else {
						SETRESULT(NOWDB_TYP_BOOL, 1);
					}
					return NOWDB_OK;
				}
			}
			if (op->fun == NOWDB_EXPR_OP_COAL) {
				if (op->types[i] != NOWDB_TYP_NOTHING) {
					if (op->types[i] == NOWDB_TYP_TEXT) {
						SETXRESULT(NOWDB_TYP_TEXT,
						           op->results[i]);
					} else {
						SETRESULT(op->types[i],
						     *(nowdb_value_t*)
						       op->results[i]);
					}
					return NOWDB_OK;
				}
			}
		}
	}

	// determine type
	if (op->types != NULL) { 
		*typ = evalType(op,0);
		if ((int)*typ < 0) {
			INVALIDTYPE("wrong type in operation");
		}
	}

	// evaluate function
	err = evalFun(op->fun, op->args,
	              op->results, op->types,
	              typ, &op->res);
	if (err != NOWDB_OK) return err;

	// pointer or value?
	if (*typ == NOWDB_TYP_TEXT)
	{
		*res=(char*)op->res;
	} else {
		*res=&op->res;
	}
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
	if (cst->type == NOWDB_TYP_NOTHING) return NOWDB_OK;
	if (cst->value != NULL) *res = cst->value;
	else if (cst->tree != NULL) *res = cst->tree;
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

/* ------------------------------------------------------------------------
 * Helper: find filter offset in key offsets
 * ------------------------------------------------------------------------
 */
static inline void getOff(nowdb_expr_t *expr,
                          uint32_t mx,
                          uint16_t sz,
                          uint16_t *off,
                          int      *o,
                          int      *idx) {
	
	*o=-1; *idx=-1;
	for (int z=0; z<mx; z++) {
		if (EXPR(expr[z])->etype == NOWDB_EXPR_FIELD) {
			for(int i=0; i<sz; i++) {
				if (off[i] == FIELD(expr[z])->off) {
					*o = i; if (*idx >= 0) return;
				}
			}
		}
		if (EXPR(expr[z])->etype == NOWDB_EXPR_CONST) {
			*idx = z; if (*o >= 0) return;
		}
	}
}

/* ------------------------------------------------------------------------
 * Predeclaration
 * ------------------------------------------------------------------------
 */
static void findRange(nowdb_expr_t expr,
                      uint16_t sz, uint16_t *off,
                      char *rstart,   char *rend,
                      nowdb_bitmap32_t      *map);

/* ------------------------------------------------------------------------
 * Helper: recursively extract key range from op
 * ------------------------------------------------------------------------
 */
static void rangeOp(nowdb_op_t *op,
                    uint16_t sz, uint16_t *off,
                    char *rstart,   char *rend,
                    nowdb_bitmap32_t      *map) {
	int o, i;

	switch(op->fun) {
	case NOWDB_EXPR_OP_AND:
		findRange(op->argv[0], sz, off,
		            rstart, rend, map);
		findRange(op->argv[1], sz, off,
		            rstart, rend, map);
		return;

	case NOWDB_EXPR_OP_JUST:
		findRange(op->argv[0], sz, off,
		            rstart, rend, map);
		return;

	case NOWDB_EXPR_OP_EQ:
		getOff(op->argv, op->args, sz, off, &o, &i);
		if (o < 0 || i < 0) return;
		memcpy(rstart+o*8, CONST(op->argv[i])->value, 8);
		memcpy(rend+o*8, CONST(op->argv[i])->value, 8);
		*map |= (1<<o);
		*map |= (65536<<o);
		return; 

	case NOWDB_EXPR_OP_GE:
	case NOWDB_EXPR_OP_GT:
		getOff(op->argv, op->args, sz, off, &o, &i);
		if (o < 0 || i < 0) return;
		memcpy(rstart+o*8, CONST(op->argv[i])->value, 8);
		*map |= (1<<o);
		return;

	case NOWDB_EXPR_OP_LE:
	case NOWDB_EXPR_OP_LT:
		getOff(op->argv, op->args, sz, off, &o, &i);
		if (o < 0 || i < 0) return;
		memcpy(rend+o*8, CONST(op->argv[i])->value, 8);
		*map |= (65536<<o);
		return;

	default: return;
	}
}

/* ------------------------------------------------------------------------
 * Helper: recursively extract key range from filter
 * ------------------------------------------------------------------------
 */
static void findRange(nowdb_expr_t expr,
                      uint16_t sz, uint16_t *off,
                      char *rstart,   char *rend,
                      nowdb_bitmap32_t      *map) {

	if (expr == NULL) return;
	if (EXPR(expr)->etype == NOWDB_EXPR_OP) {
		rangeOp(OP(expr), sz, off, rstart, rend, map);
	}
}

/* ------------------------------------------------------------------------
 * Extract key range from expression
 * ------------------------------------------------------------------------
 */
char nowdb_expr_range(nowdb_expr_t expr,
                      uint16_t sz, uint16_t *off,
                      char *rstart, char *rend) {
	nowdb_bitmap32_t map = 0;

	findRange(expr, sz, off, rstart, rend, &map);
	return (popcount32(map) == 2*sz);
}

/* ------------------------------------------------------------------------
 * Helper: get field and const index from op
 * ------------------------------------------------------------------------
 */
static inline char getFieldAndConst(nowdb_expr_t op, int *f, int *c) {
	*f=-1; *c=-1;
	if (OP(op)->args < 2) return 0;
	for(int i=0;i<2;i++) {
		if (EXPR(OP(op)->argv[i])->etype == NOWDB_EXPR_FIELD) *f = i;
		if (EXPR(OP(op)->argv[i])->etype == NOWDB_EXPR_CONST) *c = i;
	}
	if (*f == -1 || *c == -1) return 0;
	return 1;
}

/* ------------------------------------------------------------------------
 * We have a family triangle
 * ------------------------------------------------------------------------
 */
char nowdb_expr_family3(nowdb_expr_t op) {
	int c,f;

	if (op == NULL) return 0;
	if (EXPR(op)->etype != NOWDB_EXPR_OP) return 0;
	if (OP(op)->fun != NOWDB_EXPR_OP_EQ &&
	    OP(op)->fun != NOWDB_EXPR_OP_NE &&
	    OP(op)->fun != NOWDB_EXPR_OP_IN) return 0;
	if (getFieldAndConst(op, &f, &c)) return 1;
	return 0;
}

/* ------------------------------------------------------------------------
 * Get field and constant from a family triangle
 * ------------------------------------------------------------------------
 */
char nowdb_expr_getFieldAndConst(nowdb_expr_t op,
                                 nowdb_expr_t *f,
                                 nowdb_expr_t *c) {
	int i,j;
	if (!getFieldAndConst(op, &i, &j)) {
		*f = NULL; *c = NULL; return 0;
	}
	*f = (nowdb_expr_t)FIELDOP(op, i);
	*c = (nowdb_expr_t)CONSTOP(op, j);
	return 1;
}

/* ------------------------------------------------------------------------
 * Replace an operand in a family triangle
 * ------------------------------------------------------------------------
 */
void nowdb_expr_replace(nowdb_expr_t op,
                        nowdb_expr_t x) {
	int f, c;
	if (EXPR(op)->etype != NOWDB_EXPR_OP) return;
	if (!getFieldAndConst(op, &f, &c)) return;
	if (EXPR(x)->etype == NOWDB_EXPR_FIELD) {
		OP(op)->argv[f] = x;
	} else if (EXPR(x)->etype == NOWDB_EXPR_CONST) {
		OP(op)->argv[c] = x;
	}
}

/* ------------------------------------------------------------------------
 * Extract period from expression
 * ------------------------------------------------------------------------
 */
void nowdb_expr_period(nowdb_expr_t expr,
                       nowdb_time_t *start,
                       nowdb_time_t *end) {
	int f,c;
	if (expr == NULL) return;

	if (nowdb_expr_type(expr) != NOWDB_EXPR_OP) return;

	switch(OP(expr)->fun) {
	case NOWDB_EXPR_OP_AND:
		nowdb_expr_period(OP(expr)->argv[0], start, end);
		nowdb_expr_period(OP(expr)->argv[1], start, end);
		return;

	case NOWDB_EXPR_OP_JUST:
		nowdb_expr_period(OP(expr)->argv[0], start, end);
		return;

	case NOWDB_EXPR_OP_EQ:
		if (!getFieldAndConst(expr, &f, &c)) return;
		if (FIELDOP(expr,f)->off != NOWDB_OFF_TMSTMP) return;
		if (CONSTOP(expr,c)->type != NOWDB_TYP_TIME) return;
		memcpy(start, CONSTOP(expr, c)->value, 8);
		memcpy(end, CONSTOP(expr,c)->value, 8);
		return; 

	case NOWDB_EXPR_OP_GE:
	case NOWDB_EXPR_OP_GT:
		if (!getFieldAndConst(expr, &f, &c)) return;
		if (FIELDOP(expr,f)->off != NOWDB_OFF_TMSTMP) return;
		if (CONSTOP(expr,c)->type != NOWDB_TYP_TIME) return;
		memcpy(start, CONSTOP(expr,c)->value, 8);
		if (OP(expr)->fun == NOWDB_EXPR_OP_GT) (*start)++;
		return; 

	case NOWDB_EXPR_OP_LE:
	case NOWDB_EXPR_OP_LT:
		if (!getFieldAndConst(expr, &f, &c)) return;
		if (FIELDOP(expr,f)->off != NOWDB_OFF_TMSTMP) return;
		if (CONSTOP(expr,c)->type != NOWDB_TYP_TIME) return;
		memcpy(end, CONSTOP(expr,c)->value, 8);
		if (OP(expr)->fun == NOWDB_EXPR_OP_LT) (*end)--;
		return;

	default: return;
	}
}

/* -----------------------------------------------------------------------
 * Guess type before evaluation
 * -----------------------------------------------------------------------
 */
int nowdb_expr_guessType(nowdb_expr_t expr) {
	switch(EXPR(expr)->etype) {
	case NOWDB_EXPR_FIELD: return (int)FIELD(expr)->type;
	case NOWDB_EXPR_CONST: return (int)CONST(expr)->type;
	case NOWDB_EXPR_OP: return evalType(OP(expr), 1);
	case NOWDB_EXPR_REF: return nowdb_expr_guessType(REF(expr)->ref);
	case NOWDB_EXPR_AGG: return (int)FUN(AGG(expr)->agg)->otype;
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Show value
 * -----------------------------------------------------------------------
 */
static void showValue(void *value, nowdb_type_t t, FILE *stream) {
	switch(t) {
	case NOWDB_TYP_TEXT:
		fprintf(stream, "%s", (char*)value); break;
	case NOWDB_TYP_UINT:
		fprintf(stream, "%lu", *(uint64_t*)value); break;
	case NOWDB_TYP_SHORT:
		fprintf(stream, "%u", *(uint32_t*)value); break;
	case NOWDB_TYP_TIME:
	case NOWDB_TYP_DATE:
	case NOWDB_TYP_INT:
		fprintf(stream, "%ld", *(int64_t*)value); break;
	case NOWDB_TYP_FLOAT:
		fprintf(stream, "%f", *(double*)value); break;
	case NOWDB_TYP_BOOL:
		if (*(int64_t*)value) {
			fprintf(stream, "true");
		} else {
			fprintf(stream, "false");
		}
	}
}

/* -----------------------------------------------------------------------
 * Show field
 * -----------------------------------------------------------------------
 */
static void showField(nowdb_field_t *f, FILE *stream) {
	if (f->content == NOWDB_CONT_EDGE) {
		fprintf(stream, "%d", f->off); // off to name

	} else if (f->name != NULL) {
		fprintf(stream, "%s (%u)", f->name, f->off);
	} else {
		fprintf(stream, "offset %d", f->off);
	}
}

/* -----------------------------------------------------------------------
 * Show const
 * -----------------------------------------------------------------------
 */
static void showConst(nowdb_const_t *cst, FILE *stream) {
	if (cst->tree != NULL) {
		fprintf(stream, "[...]");
	} else {
		showValue(cst->value, cst->type, stream);
	}
}

/* -----------------------------------------------------------------------
 * Show args as function
 * -----------------------------------------------------------------------
 */
static void showargs(nowdb_op_t *op, char *fun, FILE *stream) {
	fprintf(stream, "%s(", fun);
	for(int i=0; i<op->args; i++) {
		nowdb_expr_show(op->argv[i], stream);
		if (i<op->args-1) fprintf(stream, ", ");
	}
	fprintf(stream, ")");
}

/* -----------------------------------------------------------------------
 * Show op
 * -----------------------------------------------------------------------
 */
static void showOp(nowdb_op_t *op, FILE *stream) {
	switch(op->fun) {
	case NOWDB_EXPR_OP_FLOAT: return showargs(op, "tofloat", stream);
	case NOWDB_EXPR_OP_INT: return showargs(op, "toint", stream);
	case NOWDB_EXPR_OP_UINT: return showargs(op, "touint", stream);
	case NOWDB_EXPR_OP_TIME:return showargs(op, "totime", stream);
	case NOWDB_EXPR_OP_TEXT:return showargs(op, "totext", stream);

	case NOWDB_EXPR_OP_ADD: return showargs(op, "+", stream);
	case NOWDB_EXPR_OP_SUB:  return showargs(op, "-", stream);
	case NOWDB_EXPR_OP_MUL:  return showargs(op, "*", stream);
	case NOWDB_EXPR_OP_DIV:  return showargs(op, "/", stream);

	case NOWDB_EXPR_OP_POW:  return showargs(op, "^", stream);

	case NOWDB_EXPR_OP_REM: return showargs(op, "rem", stream);

	case NOWDB_EXPR_OP_ABS: return showargs(op, "abs", stream);

	case NOWDB_EXPR_OP_LOG: return showargs(op, "log", stream);
	case NOWDB_EXPR_OP_CEIL: return showargs(op, "ceil", stream);
	case NOWDB_EXPR_OP_FLOOR: return showargs(op, "floor", stream);
	case NOWDB_EXPR_OP_ROUND: return showargs(op, "round", stream);

	case NOWDB_EXPR_OP_SIN: return showargs(op, "sin", stream);
	case NOWDB_EXPR_OP_COS: return showargs(op, "cos", stream);
	case NOWDB_EXPR_OP_TAN: return showargs(op, "tan", stream);
	case NOWDB_EXPR_OP_ASIN: return showargs(op, "asin", stream);
	case NOWDB_EXPR_OP_ACOS: return showargs(op, "acos", stream);
	case NOWDB_EXPR_OP_ATAN: return showargs(op, "atan", stream);
	case NOWDB_EXPR_OP_SINH: return showargs(op, "sinh", stream);
	case NOWDB_EXPR_OP_COSH: return showargs(op, "cosh", stream);
	case NOWDB_EXPR_OP_TANH: return showargs(op, "tanh", stream);
	case NOWDB_EXPR_OP_ASINH: return showargs(op, "asinh", stream);
	case NOWDB_EXPR_OP_ACOSH: return showargs(op, "acosh", stream);
	case NOWDB_EXPR_OP_ATANH: return showargs(op, "atanh", stream);

	case NOWDB_EXPR_OP_PI: return showargs(op, "pi", stream);
	case NOWDB_EXPR_OP_E: return showargs(op, "e", stream);

	case NOWDB_EXPR_OP_YEAR: return showargs(op, "year", stream);
	case NOWDB_EXPR_OP_MONTH: return showargs(op, "month", stream);
	case NOWDB_EXPR_OP_MDAY: return showargs(op, "mday", stream);
	case NOWDB_EXPR_OP_WDAY: return showargs(op, "wday", stream);
	case NOWDB_EXPR_OP_YDAY: return showargs(op, "yday", stream);
	case NOWDB_EXPR_OP_HOUR: return showargs(op, "hour", stream);
	case NOWDB_EXPR_OP_MIN: return showargs(op, "minute", stream);
	case NOWDB_EXPR_OP_SEC: return showargs(op, "scond", stream);

	case NOWDB_EXPR_OP_MILLI: return showargs(op, "milli", stream);
	case NOWDB_EXPR_OP_MICRO: return showargs(op, "micro", stream);
	case NOWDB_EXPR_OP_NANO: return showargs(op, "nano", stream);

	case NOWDB_EXPR_OP_DAWN: return showargs(op, "dawn", stream);
	case NOWDB_EXPR_OP_DUSK: return showargs(op, "dusk", stream);
	case NOWDB_EXPR_OP_EPOCH: return showargs(op, "epoch", stream);
	case NOWDB_EXPR_OP_NOW: return showargs(op, "now", stream);

	case NOWDB_EXPR_OP_EQ: return showargs(op, "=", stream);
	case NOWDB_EXPR_OP_NE: return showargs(op, "!=", stream);
	case NOWDB_EXPR_OP_LT: return showargs(op, "<", stream);
	case NOWDB_EXPR_OP_GT: return showargs(op, ">", stream);
	case NOWDB_EXPR_OP_LE: return showargs(op, "<=", stream);
	case NOWDB_EXPR_OP_GE: return showargs(op, ">=", stream);
	case NOWDB_EXPR_OP_IN: return showargs(op, "in", stream);
	case NOWDB_EXPR_OP_IS: return showargs(op, "ISNULL", stream);
	case NOWDB_EXPR_OP_ISN: return showargs(op, "ISNOTNULL", stream);
	case NOWDB_EXPR_OP_TRUE: return showargs(op, "true", stream);
	case NOWDB_EXPR_OP_FALSE: return showargs(op, "false", stream);
	case NOWDB_EXPR_OP_NOT: return showargs(op, "not", stream);
	case NOWDB_EXPR_OP_JUST: return showargs(op, "just", stream);
	case NOWDB_EXPR_OP_AND: return showargs(op, "and", stream);
	case NOWDB_EXPR_OP_OR: return showargs(op, "or", stream);
	case NOWDB_EXPR_OP_WHEN: return showargs(op, "when", stream);
	case NOWDB_EXPR_OP_ELSE: return showargs(op, "else", stream);
	case NOWDB_EXPR_OP_COAL: return showargs(op, "coalesce", stream);

	case NOWDB_EXPR_OP_VERSION: return showargs(op, "version", stream);

	default: showargs(op, "unknown", stream);
	}
}

/* -----------------------------------------------------------------------
 * Show agg
 * -----------------------------------------------------------------------
 */
static void showAgg(nowdb_agg_t *agg, FILE *stream) {
	fprintf(stderr, "agg");
	return;
}

/* -----------------------------------------------------------------------
 * Show expression
 * -----------------------------------------------------------------------
 */
void nowdb_expr_show(nowdb_expr_t expr, FILE *stream) {
	if (expr == NULL) return;
	switch(EXPR(expr)->etype) {
	case NOWDB_EXPR_FIELD:
		return showField(FIELD(expr),stream);
	case NOWDB_EXPR_CONST:
		return showConst(CONST(expr),stream);
	case NOWDB_EXPR_OP:
		return showOp(OP(expr),stream);
	case NOWDB_EXPR_REF:
		return nowdb_expr_show(REF(expr)->ref,stream);
	case NOWDB_EXPR_AGG:
		return showAgg(AGG(expr),stream);
	default:
		fprintf(stream, "unknown expression type");
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

#define SIN(v0,v1) \
	v0 = sin(v1);

#define COS(v0,v1) \
	v0 = cos(v1);

#define TAN(v0,v1) \
	v0 = tan(v1);

#define ASIN(v0,v1) \
	v0 = asin(v1);

#define ACOS(v0,v1) \
	v0 = acos(v1);

#define ATAN(v0,v1) \
	v0 = atan(v1);

#define SINH(v0,v1) \
	v0 = sinh(v1);

#define COSH(v0,v1) \
	v0 = cosh(v1);

#define TANH(v0,v1) \
	v0 = tanh(v1);

#define ASINH(v0,v1) \
	v0 = asinh(v1);

#define ACOSH(v0,v1) \
	v0 = acosh(v1);

#define ATANH(v0,v1) \
	v0 = atanh(v1);

#define MOV(r1,r2) \
	memcpy(r1, r2, 8);

#define COPYADDR(r1,r2) \
	memcpy(r1, r2, sizeof(char*));

#define MOV(r1,r2) \
	memcpy(r1, r2, 8);

#define EQ(v0,v1,v2) \
	v0 = (v1==v2)

#define NE(v0,v1,v2) \
	v0 = (v1!=v2)

#define GT(v0,v1,v2) \
	v0 = (v1>v2)

#define LT(v0,v1,v2) \
	v0 = (v1<v2)

#define GE(v0,v1,v2) \
	v0 = (v1>=v2)

#define LE(v0,v1,v2) \
	v0 = (v1<=v2)

#define LE(v0,v1,v2) \
	v0 = (v1<=v2)

#define NOT(v0,v1) \
	v0=!v1

#define JUST(v0,v1) \
	v0=v1

#define AND(v0,v1,v2) \
	v0=(v1&&v2)

#define OR(v0,v1,v2) \
	v0=(v1||v2)

#define SEQ(v0,v1,v2) \
	v0 = (strcmp(v1,v2)==0)

#define SNE(v0,v1,v2) \
	v0 = (strcmp(v1,v2)!=0)

/* -----------------------------------------------------------------------
 * Perform any comparison operation (not text)
 * -----------------------------------------------------------------------
 */
#define PERFCOMP(o) \
	if (types[0] != types[1]) { \
		INVALIDTYPE("types in comparison differ"); \
	} \
	switch(types[0]) { \
	case NOWDB_TYP_UINT: \
		o(*(int64_t*)res, *(uint64_t*)argv[0], *(uint64_t*)argv[1]); \
		return NOWDB_OK; \
	case NOWDB_TYP_DATE: \
	case NOWDB_TYP_TIME: \
	case NOWDB_TYP_INT: \
	case NOWDB_TYP_BOOL: \
		o(*(int64_t*)res, *(int64_t*)argv[0], *(int64_t*)argv[1]); \
		return NOWDB_OK; \
	case NOWDB_TYP_FLOAT: \
		o(*(int64_t*)res, *(double*)argv[0], *(double*)argv[1]); \
		return NOWDB_OK; \
	default: return NOWDB_OK; \
	}

/* -----------------------------------------------------------------------
 * Perform any 2-placed operation (on short)
 * -----------------------------------------------------------------------
 */
#define PERFSHORT(o) \
	o(*(int64_t*)res, *(uint32_t*)argv[0], *(uint32_t*)argv[1]); \
	return NOWDB_OK;

/* -----------------------------------------------------------------------
 * Perform any 2-placed text->boolean operation
 * -----------------------------------------------------------------------
 */
#define PERFSTR(o) \
	o(*(int64_t*)res, (char*)argv[0], (char*)argv[1]); \
	return NOWDB_OK;

/* -----------------------------------------------------------------------
 * Perform 2-place boolean operation
 * -----------------------------------------------------------------------
 */
#define PERFBOOL(o) \
	if (types[0] == NOWDB_TYP_TEXT) { \
		o(*(int64_t*)res, (int64_t)argv[0], *(uint64_t*)argv[1]); \
	} else if (types[1] == NOWDB_TYP_TEXT) { \
		o(*(int64_t*)res, *(int64_t*)argv[0], (uint64_t)argv[1]); \
	} else { \
		o(*(int64_t*)res, *(int64_t*)argv[0], *(int64_t*)argv[1]); \
	} \
	return NOWDB_OK;

/* -----------------------------------------------------------------------
 * Perform 1-place boolean operation
 * -----------------------------------------------------------------------
 */
#define PER1BOOL(o) \
	if (types[0] == NOWDB_TYP_TEXT) { \
		o(*(int64_t*)res, (int64_t)argv[0]); \
	} else { \
		o(*(int64_t*)res, *(int64_t*)argv[0]); \
	} \
	return NOWDB_OK;

/* -----------------------------------------------------------------------
 * Perform 2-place numeric operation
 * -----------------------------------------------------------------------
 */
#define PERFMN(o) \
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
 * Find in tree
 * -----------------------------------------------------------------------
 */
#define FINDIN() \
	*(nowdb_bool_t*)res = (ts_algo_tree_find(argv[1], \
	                               argv[0]) != NULL); \
	return NOWDB_OK;

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
static nowdb_err_t evalFun(uint32_t       fun,
                           int            args,
                           void         **argv,
                           nowdb_type_t *types,
                           nowdb_type_t *t,
                           void         *res) {
	double f;
	int64_t l;
	uint64_t u;

	/* -----------------------------------------------------------------------
	 * If nothing: ignore function
	 * -----------------------------------------------------------------------
	 */
	if (*t == NOWDB_TYP_NOTHING) {
		if (fun == NOWDB_EXPR_OP_IS) *(nowdb_value_t*)res = 1;
		else if (fun == NOWDB_EXPR_OP_ISN) *(nowdb_value_t*)res = 0;
		return NOWDB_OK;
	}
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
	case NOWDB_EXPR_OP_ADD: PERFMN(ADD);
	case NOWDB_EXPR_OP_SUB: PERFMN(SUB);
	case NOWDB_EXPR_OP_MUL: PERFMN(MUL);
	case NOWDB_EXPR_OP_DIV:
		if (*t == NOWDB_TYP_FLOAT) {
			PERFMN(DIV);
		} else {
			PERFMN(QUOT);
		}

	case NOWDB_EXPR_OP_REM:
		if (*t == NOWDB_TYP_FLOAT) {
			INVALIDTYPE("remainder with float");
		} else {
			PERFMNF(REM);
		}

	case NOWDB_EXPR_OP_POW: PERFMN(POW);

	case NOWDB_EXPR_OP_ROOT:
		return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT, NULL);

	case NOWDB_EXPR_OP_LOG: PERFM1(LOG);
	case NOWDB_EXPR_OP_SIN: PERFM1(SIN);
	case NOWDB_EXPR_OP_COS: PERFM1(COS);
	case NOWDB_EXPR_OP_TAN: PERFM1(TAN);
	case NOWDB_EXPR_OP_ASIN: PERFM1(ASIN);
	case NOWDB_EXPR_OP_ACOS: PERFM1(ACOS);
	case NOWDB_EXPR_OP_ATAN: PERFM1(ATAN);
	case NOWDB_EXPR_OP_SINH: PERFM1(SINH);
	case NOWDB_EXPR_OP_COSH: PERFM1(COSH);
	case NOWDB_EXPR_OP_TANH: PERFM1(TANH);
	case NOWDB_EXPR_OP_ASINH: PERFM1(ASINH);
	case NOWDB_EXPR_OP_ACOSH: PERFM1(ACOSH);
	case NOWDB_EXPR_OP_ATANH: PERFM1(ATANH);

	case NOWDB_EXPR_OP_PI:
		*(double*)res = M_PI; return NOWDB_OK;

	case NOWDB_EXPR_OP_E:
		*(double*)res = M_E; return NOWDB_OK;
		
	case NOWDB_EXPR_OP_CEIL: PERFM1(CEIL);
	case NOWDB_EXPR_OP_FLOOR: PERFM1(FLOOR);
	case NOWDB_EXPR_OP_ROUND: PERFM1(ROUND);
	case NOWDB_EXPR_OP_ABS: PERFM1(ABS);

	/* -----------------------------------------------------------------------
	 * Logic
	 * -----------------------------------------------------------------------
	 */
	case NOWDB_EXPR_OP_EQ:
		if (types[0] == NOWDB_TYP_TEXT) {
			if (types[1] != NOWDB_TYP_TEXT) {
				INVALIDTYPE("not a text value");
			}
			PERFSTR(SEQ);
		} else if (types[0] == NOWDB_TYP_SHORT) {
			// fprintf(stderr, "comparing short\n");
			PERFSHORT(EQ);
		} else {
			PERFCOMP(EQ);
		}

	case NOWDB_EXPR_OP_NE:
		if (types[0] == NOWDB_TYP_TEXT) {
			if (types[1] != NOWDB_TYP_TEXT) {
				INVALIDTYPE("not a time value");
			}
			PERFSTR(SNE);
		} else if (types[0] == NOWDB_TYP_SHORT) {
			PERFSHORT(EQ);
		} else {
			PERFCOMP(NE);
		}
	case NOWDB_EXPR_OP_LT: PERFCOMP(LT);
	case NOWDB_EXPR_OP_GT: PERFCOMP(GT);
	case NOWDB_EXPR_OP_LE: PERFCOMP(LE);
	case NOWDB_EXPR_OP_GE: PERFCOMP(GE);

	case NOWDB_EXPR_OP_IN: FINDIN();

	case NOWDB_EXPR_OP_IS: 
		*(nowdb_value_t*)res = FALSE; return NOWDB_OK;
	case NOWDB_EXPR_OP_ISN: 
		*(nowdb_value_t*)res = TRUE; return NOWDB_OK;
 
	case NOWDB_EXPR_OP_TRUE:
		(*(nowdb_bool_t*)res) = TRUE; return NOWDB_OK;
	case NOWDB_EXPR_OP_FALSE:
		(*(nowdb_bool_t*)res) = FALSE; return NOWDB_OK;

	case NOWDB_EXPR_OP_NOT: PER1BOOL(NOT);
	case NOWDB_EXPR_OP_JUST: PER1BOOL(JUST);

	case NOWDB_EXPR_OP_AND: PERFBOOL(AND);
	case NOWDB_EXPR_OP_OR: PERFBOOL(OR);

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
		    types[0] != NOWDB_TYP_DATE &&
                    types[0] != NOWDB_TYP_UINT &&
                    types[0] != NOWDB_TYP_INT) {
			INVALIDTYPE("not a time value");
		}
		return getTimeComp(fun, argv[0], res);

	case NOWDB_EXPR_OP_MILLI:
	case NOWDB_EXPR_OP_MICRO:
	case NOWDB_EXPR_OP_NANO:
		if (types[0] != NOWDB_TYP_TIME &&
		    types[0] != NOWDB_TYP_DATE &&
                    types[0] != NOWDB_TYP_UINT &&
                    types[0] != NOWDB_TYP_INT) 
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
	case NOWDB_EXPR_OP_ELSE:
		if (argv[0] == NULL) {
			*t = NOWDB_TYP_NOTHING;
			// set res
		} else if (types[0] == NOWDB_TYP_TEXT) {
			COPYADDR(res, &argv[0]); *t = types[0];
		} else {
			COPYADDR(res, argv[0]); *t=types[0];
		}
		return NOWDB_OK;

	case NOWDB_EXPR_OP_WHEN:
		if (types[0] != NOWDB_TYP_BOOL) {
			INVALIDTYPE("first operand of WHEN is not a boolean");
		}
		if (*(int64_t*)argv[0]) {
			if (argv[1] == NULL) {
				*t = NOWDB_TYP_NOTHING;
				// set res
			} else if (types[1] == NOWDB_TYP_TEXT) {
				COPYADDR(res, &argv[1]); *t = types[1];
			} else {
				COPYADDR(res, argv[1]); *t = types[1];
			}
		} else {
			if (argv[2] == NULL) {
				*t = NOWDB_TYP_NOTHING;
				// set res
			} else if (types[2] == NOWDB_TYP_TEXT) {
				COPYADDR(res, &argv[2]); *t = types[2];
			} else {
				COPYADDR(res, argv[2]); *t = types[2];
			}
		}
		return NOWDB_OK;

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

	/* -----------------------------------------------------------------------
	 * Internals
	 * -----------------------------------------------------------------------
	 */
	case NOWDB_EXPR_OP_VERSION:
		COPYADDR(res, &nowdb_version_string); break;

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
	case NOWDB_EXPR_OP_ROUND:
	case NOWDB_EXPR_OP_SIN: 
	case NOWDB_EXPR_OP_COS: 
	case NOWDB_EXPR_OP_TAN:
	case NOWDB_EXPR_OP_ASIN: 
	case NOWDB_EXPR_OP_ACOS: 
	case NOWDB_EXPR_OP_ATAN:
	case NOWDB_EXPR_OP_SINH: 
	case NOWDB_EXPR_OP_COSH: 
	case NOWDB_EXPR_OP_TANH:
	case NOWDB_EXPR_OP_ASINH: 
	case NOWDB_EXPR_OP_ACOSH: 
	case NOWDB_EXPR_OP_ATANH: return 1;

	case NOWDB_EXPR_OP_PI:
	case NOWDB_EXPR_OP_E: return 0;

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

	case NOWDB_EXPR_OP_EQ:
	case NOWDB_EXPR_OP_NE:
	case NOWDB_EXPR_OP_LT:
	case NOWDB_EXPR_OP_GT:
	case NOWDB_EXPR_OP_LE:
	case NOWDB_EXPR_OP_GE:
	case NOWDB_EXPR_OP_IN: return 2;

	case NOWDB_EXPR_OP_IS:
	case NOWDB_EXPR_OP_ISN: return 1;

	case NOWDB_EXPR_OP_NOT:
	case NOWDB_EXPR_OP_JUST: return 1;

	case NOWDB_EXPR_OP_TRUE:
	case NOWDB_EXPR_OP_FALSE: return 0;

	case NOWDB_EXPR_OP_AND:
	case NOWDB_EXPR_OP_OR: return 2;

	case NOWDB_EXPR_OP_WHEN: return 3;

	case NOWDB_EXPR_OP_ELSE: return 1;
	case NOWDB_EXPR_OP_COAL: return 1;

	case NOWDB_EXPR_OP_VERSION: return 0;

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
			if (EXPR(op->argv[i])->etype == NOWDB_EXPR_CONST) {
				CONST(op->argv[i])->type = t;
			}
			break;

		case NOWDB_TYP_INT:
		case NOWDB_TYP_TIME:
		case NOWDB_TYP_DATE:
			TOINT(op->types[i], op->results[i],
			                    op->results[i]);
			op->types[i] = t;
			if (EXPR(op->argv[i])->etype == NOWDB_EXPR_CONST) {
				CONST(op->argv[i])->type = t;
			}
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
static inline int evalType(nowdb_op_t *op, char guess) {

	if (!guess) {
		for (int i=0; i<op->args; i++) {
			if (op->types[i] == NOWDB_TYP_NOTHING) {
				if (op->fun == NOWDB_EXPR_OP_WHEN ||
				    op->fun == NOWDB_EXPR_OP_ELSE) {
					continue;
				}
				if (op->fun == NOWDB_EXPR_OP_IS ||
                                    op->fun == NOWDB_EXPR_OP_ISN) 
				{
					return NOWDB_TYP_BOOL;
				}
				return NOWDB_TYP_NOTHING;
			} else {
				if (op->fun == NOWDB_EXPR_OP_COAL) {
					return op->types[i];
				}
			}
		}
	}
	switch(op->fun) {

	case NOWDB_EXPR_OP_FLOAT: return NOWDB_TYP_FLOAT;
	case NOWDB_EXPR_OP_INT: return NOWDB_TYP_INT;
	case NOWDB_EXPR_OP_UINT: return NOWDB_TYP_UINT;
	case NOWDB_EXPR_OP_TIME: return NOWDB_TYP_TIME;
	case NOWDB_EXPR_OP_TEXT: return NOWDB_TYP_TEXT;

	case NOWDB_EXPR_OP_ADD:
	case NOWDB_EXPR_OP_SUB: 
	case NOWDB_EXPR_OP_MUL: 
	case NOWDB_EXPR_OP_DIV:	if (guess) return NOWDB_TYP_INT;
				return correctNumTypes(op);

	case NOWDB_EXPR_OP_POW:
		if (guess) return NOWDB_TYP_FLOAT; 
		if (!isNumeric(op->types[0]) ||
		    !isNumeric(op->types[1])) return -1;
                enforceType(op,NOWDB_TYP_FLOAT);
		return NOWDB_TYP_FLOAT;

	case NOWDB_EXPR_OP_REM:
		if (guess) return NOWDB_TYP_INT; 
		if (!isNumeric(op->types[0]) ||
		    !isNumeric(op->types[1])) return -1;
		if (op->types[0] == NOWDB_TYP_FLOAT ||
		    op->types[1] == NOWDB_TYP_FLOAT) return -1;
		return correctNumTypes(op);

	case NOWDB_EXPR_OP_ABS:
		if (guess) return NOWDB_TYP_INT; 
		if (!isNumeric(op->types[0])) return -1;
		return op->types[0];

	case NOWDB_EXPR_OP_LOG:
	case NOWDB_EXPR_OP_CEIL:
	case NOWDB_EXPR_OP_FLOOR:
	case NOWDB_EXPR_OP_ROUND:
	case NOWDB_EXPR_OP_SIN:
	case NOWDB_EXPR_OP_COS:
	case NOWDB_EXPR_OP_TAN:
	case NOWDB_EXPR_OP_ASIN:
	case NOWDB_EXPR_OP_ACOS:
	case NOWDB_EXPR_OP_ATAN:
	case NOWDB_EXPR_OP_SINH:
	case NOWDB_EXPR_OP_COSH:
	case NOWDB_EXPR_OP_TANH:
	case NOWDB_EXPR_OP_ASINH:
	case NOWDB_EXPR_OP_ACOSH:
	case NOWDB_EXPR_OP_ATANH:
		if (guess) return NOWDB_TYP_FLOAT; 
		if (op->types[0] != NOWDB_TYP_FLOAT) {
			enforceType(op, NOWDB_TYP_FLOAT);
		}
		return NOWDB_TYP_FLOAT;

	case NOWDB_EXPR_OP_PI:
	case NOWDB_EXPR_OP_E: return NOWDB_TYP_FLOAT;

	// test that all types are time or int
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

	// test that all types are equal 
	case NOWDB_EXPR_OP_EQ:
	case NOWDB_EXPR_OP_NE:
	case NOWDB_EXPR_OP_LT:
	case NOWDB_EXPR_OP_GT:
	case NOWDB_EXPR_OP_LE:
	case NOWDB_EXPR_OP_GE:
		correctNumTypes(op); return NOWDB_TYP_BOOL;

	case NOWDB_EXPR_OP_IN:
	case NOWDB_EXPR_OP_IS:
	case NOWDB_EXPR_OP_ISN:
	case NOWDB_EXPR_OP_TRUE:
	case NOWDB_EXPR_OP_FALSE:
	case NOWDB_EXPR_OP_NOT:
	case NOWDB_EXPR_OP_JUST:
	case NOWDB_EXPR_OP_AND:
	case NOWDB_EXPR_OP_OR: return NOWDB_TYP_BOOL;

	case NOWDB_EXPR_OP_WHEN:
		if (op->types[1] != NOWDB_TYP_NOTHING) return op->types[1];
		if (op->types[2] != NOWDB_TYP_NOTHING) return op->types[2];
		if (guess) return NOWDB_TYP_BOOL;
		return NOWDB_TYP_NOTHING;

	case NOWDB_EXPR_OP_ELSE: return op->types[0];

	case NOWDB_EXPR_OP_COAL:
		for(int i=0;i<op->args;i++) {
			if (op->types[i] != NOWDB_TYP_NOTHING)
				return op->types[i];
		}
		return NOWDB_TYP_NOTHING;

	case NOWDB_EXPR_OP_VERSION: return NOWDB_TYP_TEXT;

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

	if (strcasecmp(op, "=") == 0) return NOWDB_EXPR_OP_EQ;
	if (strcasecmp(op, "!=") == 0) return NOWDB_EXPR_OP_NE;
	if (strcasecmp(op, "<>") == 0) return NOWDB_EXPR_OP_NE;
	if (strcasecmp(op, "<") == 0) return NOWDB_EXPR_OP_LT;
	if (strcasecmp(op, ">") == 0) return NOWDB_EXPR_OP_GT;
	if (strcasecmp(op, "<=") == 0) return NOWDB_EXPR_OP_LE;
	if (strcasecmp(op, ">=") == 0) return NOWDB_EXPR_OP_GE;
	if (strcasecmp(op, "in") == 0) return NOWDB_EXPR_OP_IN;
	if (strcasecmp(op, "is") == 0) return NOWDB_EXPR_OP_IS;

	if (strcasecmp(op, "log") == 0) return NOWDB_EXPR_OP_LOG;
	if (strcasecmp(op, "abs") == 0) return NOWDB_EXPR_OP_ABS;
	if (strcasecmp(op, "ceil") == 0) return NOWDB_EXPR_OP_CEIL;
	if (strcasecmp(op, "floor") == 0) return NOWDB_EXPR_OP_FLOOR;
	if (strcasecmp(op, "round") == 0) return NOWDB_EXPR_OP_ROUND;

	if (strcasecmp(op, "sin") == 0) return NOWDB_EXPR_OP_SIN;
	if (strcasecmp(op, "sine") == 0) return NOWDB_EXPR_OP_SIN;
	if (strcasecmp(op, "cos") == 0) return NOWDB_EXPR_OP_COS;
	if (strcasecmp(op, "cosine") == 0) return NOWDB_EXPR_OP_COS;
	if (strcasecmp(op, "tan") == 0) return NOWDB_EXPR_OP_TAN;
	if (strcasecmp(op, "tang") == 0) return NOWDB_EXPR_OP_TAN;
	if (strcasecmp(op, "tangent") == 0) return NOWDB_EXPR_OP_TAN;
	if (strcasecmp(op, "asin") == 0) return NOWDB_EXPR_OP_ASIN;
	if (strcasecmp(op, "asine") == 0) return NOWDB_EXPR_OP_ASIN;
	if (strcasecmp(op, "arcsine") == 0) return NOWDB_EXPR_OP_ASIN;
	if (strcasecmp(op, "arcsin") == 0) return NOWDB_EXPR_OP_ASIN;
	if (strcasecmp(op, "acos") == 0) return NOWDB_EXPR_OP_ACOS;
	if (strcasecmp(op, "acosine") == 0) return NOWDB_EXPR_OP_ACOS;
	if (strcasecmp(op, "arccos") == 0) return NOWDB_EXPR_OP_ACOS;
	if (strcasecmp(op, "arccosine") == 0) return NOWDB_EXPR_OP_ACOS;
	if (strcasecmp(op, "atan") == 0) return NOWDB_EXPR_OP_ATAN;
	if (strcasecmp(op, "atang") == 0) return NOWDB_EXPR_OP_ATAN;
	if (strcasecmp(op, "atangent") == 0) return NOWDB_EXPR_OP_ATAN;
	if (strcasecmp(op, "arctan") == 0) return NOWDB_EXPR_OP_ATAN;
	if (strcasecmp(op, "arctang") == 0) return NOWDB_EXPR_OP_ATAN;
	if (strcasecmp(op, "arctangent") == 0) return NOWDB_EXPR_OP_ATAN;
	if (strcasecmp(op, "sinh") == 0) return NOWDB_EXPR_OP_SINH;
	if (strcasecmp(op, "cosh") == 0) return NOWDB_EXPR_OP_COSH;
	if (strcasecmp(op, "tanh") == 0) return NOWDB_EXPR_OP_TANH;
	if (strcasecmp(op, "asinh") == 0) return NOWDB_EXPR_OP_ASINH;
	if (strcasecmp(op, "arcsinh") == 0) return NOWDB_EXPR_OP_ASINH;
	if (strcasecmp(op, "acosh") == 0) return NOWDB_EXPR_OP_ACOSH;
	if (strcasecmp(op, "arccosh") == 0) return NOWDB_EXPR_OP_ACOSH;
	if (strcasecmp(op, "atanh") == 0) return NOWDB_EXPR_OP_ATANH;
	if (strcasecmp(op, "arctanh") == 0) return NOWDB_EXPR_OP_ATANH;

	if (strcasecmp(op, "pi") == 0) return NOWDB_EXPR_OP_PI;
	if (strcasecmp(op, "e") == 0) return NOWDB_EXPR_OP_E;

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

	if (strcasecmp(op, "true") == 0) return NOWDB_EXPR_OP_TRUE;
	if (strcasecmp(op, "false") == 0) return NOWDB_EXPR_OP_FALSE;
	if (strcasecmp(op, "not") == 0) return NOWDB_EXPR_OP_NOT;
	if (strcasecmp(op, "just") == 0) return NOWDB_EXPR_OP_JUST;
	if (strcasecmp(op, "and") == 0) return NOWDB_EXPR_OP_AND;
	if (strcasecmp(op, "or") == 0) return NOWDB_EXPR_OP_OR;
	if (strcasecmp(op, "isn") == 0) return NOWDB_EXPR_OP_ISN;
	if (strcasecmp(op, "isnt") == 0) return NOWDB_EXPR_OP_ISN;
	if (strcasecmp(op, "isnot") == 0) return NOWDB_EXPR_OP_ISN;
	if (strcasecmp(op, "is not") == 0) return NOWDB_EXPR_OP_ISN;
	if (strcasecmp(op, "when") == 0) return NOWDB_EXPR_OP_WHEN;
	if (strcasecmp(op, "else") == 0) return NOWDB_EXPR_OP_ELSE;
	if (strcasecmp(op, "coal") == 0) return NOWDB_EXPR_OP_COAL;
	if (strcasecmp(op, "coalesce") == 0) return NOWDB_EXPR_OP_COAL;

	if (strcasecmp(op, "version") == 0) return NOWDB_EXPR_OP_VERSION;

	*agg = 1;

	return nowdb_fun_fromName(op);
}
