/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Generic expressions
 * ========================================================================
 */
#include <nowdb/fun/expr.h>

static char *OBJECT = "EXPR";

#define NOMEM(s) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, s);

#define INVALID(s) \
	return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, s);

#define FIELD(x) \
	((nowdb_field_t*)x)

#define CONST(x) \
	((nowdb_const_t*)x)

#define OP(x) \
	((nowdb_op_t*)x)

typedef struct {
	uint32_t etype;
} dummy_t;

#define EXPR(x) \
	((dummy_t*)x)

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
	FIELD(*expr)->off = -1;
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
	CONST(*expr)->value = value;

	return NOWDB_OK;
}

nowdb_err_t nowdb_expr_newOp(nowdb_expr_t  *expr,
                             uint32_t        fun,
                             ts_algo_list_t *ops) {
	nowdb_err_t err;
	ts_algo_list_node_t *run;
	int i=0;

	*expr = calloc(1,sizeof(nowdb_const_t));
	if (*expr == NULL)  {
		NOMEM("allocating expression");
		return err;
	}

	OP(*expr)->etype = NOWDB_EXPR_OP;
	OP(*expr)->argv = NULL;
	OP(*expr)->types = NULL;
	OP(*expr)->results = NULL;

	OP(*expr)->fun = fun;
	OP(*expr)->args  = ops->len;

	OP(*expr)->types = calloc(ops->len, sizeof(nowdb_type_t));
	if (OP(*expr)->types == NULL) {
		NOMEM("allocating expression operand types");
		nowdb_expr_destroy(*expr);
		free(*expr); *expr = NULL;
		return err;
	}

	OP(*expr)->results = calloc(ops->len, sizeof(uint64_t));
	if (OP(*expr)->results == NULL) {
		NOMEM("allocating expression operand results");
		nowdb_expr_destroy(*expr);
		free(*expr); *expr = NULL;
		return err;
	}

	OP(*expr)->argv = calloc(ops->len, sizeof(nowdb_expr_t));
	if (OP(*expr)->argv == NULL) {
		NOMEM("allocating expression operands");
		nowdb_expr_destroy(*expr);
		free(*expr); *expr = NULL;
		return err;
	}
	for(run = ops->head;run!=NULL;run=run->nxt) {
		OP(*expr)->argv[i] = run->cont; i++;
	}
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
 * Destroy 
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
	default: return;
	}
}

/* -----------------------------------------------------------------------
 * Copy expression
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_copy(nowdb_expr_t  src,
                            nowdb_expr_t *trg);

/* -----------------------------------------------------------------------
 * Evaluate field
 * -----------------------------------------------------------------------
 */
static nowdb_err_t evalField(nowdb_field_t *field,
                             char          *row,
                             nowdb_type_t  *typ,
                             void         **res) {
	if (field->off < 0) INVALID("uninitialised expression");
	*res = row+field->off;
	*typ = field->type;
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Evaluate Fun (predeclaration, implementation below)
 * -----------------------------------------------------------------------
 */
static nowdb_err_t evalFun(uint32_t      fun,
                           nowdb_type_t *types,
                           void        **argv,
                           nowdb_type_t *typ,
                           void        **res);

/* -----------------------------------------------------------------------
 * Evaluate operation
 * -----------------------------------------------------------------------
 */
static nowdb_err_t evalOp(nowdb_op_t   *op,
                          char         *row,
                          nowdb_type_t *typ,
                          void        **res) {
	nowdb_err_t err;

	if (op->argv != NULL) {
		for(int i=0; i<op->args; i++) {
			err = nowdb_expr_eval(op->argv[i], row,
			                      op->types+i,
			                      op->results+i);
			if (err != NOWDB_OK) return err; 
		}
	}

	err = evalFun(op->fun, op->types, op->results, typ, &op->res);
	if (err != NOWDB_OK) return err;

	*res = op->res;
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Evaluate expression
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_eval(nowdb_expr_t expr,
                            char         *row,
                            nowdb_type_t *typ,
                            void        **res) {
	if (expr == NULL) return NOWDB_OK;
	switch(EXPR(expr)->etype) {
	case NOWDB_EXPR_FIELD:
		return evalField(FIELD(expr), row, typ, res);

	case NOWDB_EXPR_CONST:
		*typ = CONST(expr)->type;
		*res = CONST(expr)->value;
		return NOWDB_OK;

	case NOWDB_EXPR_OP:
		return evalOp(OP(expr), row, typ, res);

	default:
		INVALID("unknown expression type");
	}
}

/* -----------------------------------------------------------------------
 * Evaluate Fun
 * -----------------------------------------------------------------------
 */
static nowdb_err_t evalFun(uint32_t      fun,
                           nowdb_type_t *types,
                           void        **argv,
                           nowdb_type_t *typ,
                           void        **res) {
	switch(fun) {

	/* -----------------------------------------------------------------------
	 * Conversions
	 * -----------------------------------------------------------------------
	 */
	case NOWDB_EXPR_OP_FLOAT:
	case NOWDB_EXPR_OP_INT:
	case NOWDB_EXPR_OP_UINT:
	case NOWDB_EXPR_OP_TIME:
	case NOWDB_EXPR_OP_TEXT:
		return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT, NULL);

	/* -----------------------------------------------------------------------
	 * Arithmetic
	 * -----------------------------------------------------------------------
	 */
	case NOWDB_EXPR_OP_ADD:
	case NOWDB_EXPR_OP_SUB:
	case NOWDB_EXPR_OP_MUL:
	case NOWDB_EXPR_OP_DIV:
	case NOWDB_EXPR_OP_REM:
	case NOWDB_EXPR_OP_POW:
	case NOWDB_EXPR_OP_ROOT:
	case NOWDB_EXPR_OP_LOG:
	case NOWDB_EXPR_OP_CEIL:
	case NOWDB_EXPR_OP_FLOOR:
	case NOWDB_EXPR_OP_ROUND:
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
}
