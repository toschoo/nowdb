/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Generic expressions
 * ========================================================================
 */
#ifndef nowdb_expr_decl
#define nowdb_expr_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/types/time.h>
#include <nowdb/model/types.h>
#include <nowdb/model/model.h>
#include <nowdb/text/text.h>
#include <nowdb/mem/ptlru.h>

#include <tsalgo/list.h>

/* -----------------------------------------------------------------------
 * Expression types:
 * -----------------
 * const: constant value
 * field: database field
 * op   : built-in function ('operator')
 * fun  : user-defined function
 * agg  : aggregate
 * -----------------------------------------------------------------------
 */
#define NOWDB_EXPR_CONST 1
#define NOWDB_EXPR_FIELD 2
#define NOWDB_EXPR_OP    3
#define NOWDB_EXPR_FUN   4
#define NOWDB_EXPR_AGG   5

/* ------------------------------------------------------------------------
 * Expression is an anonymous type
 * ------------------------------------------------------------------------
 */
typedef struct nowdb_expr_t* nowdb_expr_t;

/* ------------------------------------------------------------------------
 * Know the type of an expression
 * ------------------------------------------------------------------------
 */
int nowdb_expr_type(nowdb_expr_t expr);

/* ------------------------------------------------------------------------
 * Field Expression
 * ------------------------------------------------------------------------
 */
typedef struct {
	uint32_t        etype; /* expression type         */
	nowdb_target_t target; /* vertex or edge?         */
	int               off; /* offset into data source */
	nowdb_type_t     type; /* Type of field           */
	char            *text; /* string if off is text   */
	char            *name; /* property name if vertex */
	nowdb_roleid_t   role; /* vertex type if vertex   */
	nowdb_key_t    propid; /* propid if vertex        */
	char               pk; /* primary key if vertex   */
} nowdb_field_t;

/* ------------------------------------------------------------------------
 * Convert expression to field
 * ------------------------------------------------------------------------
 */
#define NOWDB_EXPR_TOFIELD(x) \
	((nowdb_field_t*)x)

/* ------------------------------------------------------------------------
 * Constant Expression
 * ------------------------------------------------------------------------
 */
typedef struct {
	uint32_t    etype; /* expression type         */
	void       *value; /* the value    */
	nowdb_type_t type; /* and its type */
} nowdb_const_t;

/* ------------------------------------------------------------------------
 * Convert expression to Const
 * ------------------------------------------------------------------------
 */
#define NOWDB_EXPR_TOCONST(x) \
	((nowdb_const_t*)x)

/* ------------------------------------------------------------------------
 * Operator Expression
 * ------------------------------------------------------------------------
 */
typedef struct {
	uint32_t      etype;  /* expression type             */
	uint32_t        fun;  /* Operator type (+,-,*,/,...) */
	uint32_t       args;  /* Number of operands          */
	nowdb_expr_t  *argv;  /* Operands                    */
	nowdb_type_t  *types; /* types of the arg results    */
	void      **results;  /* arg results                 */
	nowdb_value_t   res;  /* result                      */
	                      /* text????                    */
} nowdb_op_t;

/* ------------------------------------------------------------------------
 * Convert expression to OP
 * ------------------------------------------------------------------------
 */
#define NOWDB_EXPR_TOOP(x) \
	((nowdb_op_t*)x)

/* ------------------------------------------------------------------------
 * Aggregate Expression
 * ------------------------------------------------------------------------
 */

/* ------------------------------------------------------------------------
 * Edge Model Helper
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_model_edge_t   *e;
	nowdb_model_vertex_t *o;
	nowdb_model_vertex_t *d;
} nowdb_edge_helper_t;

/* ------------------------------------------------------------------------
 * Vertex Model Helper
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_model_vertex_t *v;
	nowdb_model_prop_t   *p;
} nowdb_vertex_helper_t;

/* ------------------------------------------------------------------------
 * Evaluation helper
 * the models (v, p, e, o, d) shall be lists!
 * one list for edge  : e, o, d
 * one list for vertex: v, with a tree of p
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_model_t      *model; /* the model to use for evaluation   */
	nowdb_text_t        *text; /* the text  to use for evaluation   */
	nowdb_ptlru_t       *tlru; /* private text lru cache            */
	ts_algo_list_t         em; /* edge model helper                 */
	ts_algo_list_t         vm; /* vertex model helper (tree???)     */
	nowdb_edge_helper_t   *ce; /* current edge helper               */
	nowdb_vertex_helper_t *cv; /* currrent vertext helper           */
	char            needtxt; /* we need to evaluate text          */
} nowdb_eval_t;

void nowdb_eval_destroy(nowdb_eval_t *eval);

/* -----------------------------------------------------------------------
 * Create expression
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_newEdgeField(nowdb_expr_t *expr, uint32_t off);

nowdb_err_t nowdb_expr_newVertexField(nowdb_expr_t  *expr,
                                      char      *propname,
                                      nowdb_roleid_t role,
                                      nowdb_key_t propid);

nowdb_err_t nowdb_expr_newConstant(nowdb_expr_t  *expr,
                                   void         *value,
                                   nowdb_type_t  type);

nowdb_err_t nowdb_expr_newOpL(nowdb_expr_t   *expr,
                              uint32_t         fun,
                              ts_algo_list_t  *ops);

nowdb_err_t nowdb_expr_newOp(nowdb_expr_t *expr, uint32_t fun, ...);


// newAgg

/* -----------------------------------------------------------------------
 * Destroy expression
 * -----------------------------------------------------------------------
 */
void nowdb_expr_destroy(nowdb_expr_t expr);

/* -----------------------------------------------------------------------
 * Copy expression
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_copy(nowdb_expr_t  src,
                            nowdb_expr_t *trg);

/* -----------------------------------------------------------------------
 * Evaluate expression
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_eval(nowdb_expr_t expr,
                            nowdb_eval_t *hlp,
                            char         *row,
                            nowdb_type_t *typ,
                            void        **res);

/* -----------------------------------------------------------------------
 * Conversions
 * -----------------------------------------------------------------------
 */
#define NOWDB_EXPR_OP_FLOAT 1
#define NOWDB_EXPR_OP_INT   2
#define NOWDB_EXPR_OP_UINT  3
#define NOWDB_EXPR_OP_TIME  4
#define NOWDB_EXPR_OP_TEXT  5

/* -----------------------------------------------------------------------
 * Arithmetic
 * -----------------------------------------------------------------------
 */
#define NOWDB_EXPR_OP_ADD   10
#define NOWDB_EXPR_OP_SUB   11
#define NOWDB_EXPR_OP_MUL   12
#define NOWDB_EXPR_OP_DIV   13
#define NOWDB_EXPR_OP_REM   14
#define NOWDB_EXPR_OP_POW   15
#define NOWDB_EXPR_OP_ROOT  16
#define NOWDB_EXPR_OP_LOG   17
#define NOWDB_EXPR_OP_CEIL  18
#define NOWDB_EXPR_OP_FLOOR 19
#define NOWDB_EXPR_OP_ROUND 20
#define NOWDB_EXPR_OP_ABS   21

/* -----------------------------------------------------------------------
 * Logic
 * -----------------------------------------------------------------------
 */
#define NOWDB_EXPR_OP_EQ  101
#define NOWDB_EXPR_OP_NE  102
#define NOWDB_EXPR_OP_LT  103
#define NOWDB_EXPR_OP_GT  104
#define NOWDB_EXPR_OP_LE  105
#define NOWDB_EXPR_OP_GE  106
#define NOWDB_EXPR_OP_NOT 107
#define NOWDB_EXPR_OP_AND 108
#define NOWDB_EXPR_OP_OR  109
#define NOWDB_EXPR_OP_XOR 110

/* -----------------------------------------------------------------------
 * TIME
 * -----------------------------------------------------------------------
 */
#define NOWDB_EXPR_OP_CENTURY 1001
#define NOWDB_EXPR_OP_YEAR    1002
#define NOWDB_EXPR_OP_MONTH   1003
#define NOWDB_EXPR_OP_MDAY    1004
#define NOWDB_EXPR_OP_WDAY    1005
#define NOWDB_EXPR_OP_WEEK    1006
#define NOWDB_EXPR_OP_HOUR    1007
#define NOWDB_EXPR_OP_MIN     1008
#define NOWDB_EXPR_OP_SEC     1009
#define NOWDB_EXPR_OP_MILLI   1010
#define NOWDB_EXPR_OP_MICRO   1011
#define NOWDB_EXPR_OP_NANO    1012
#define NOWDB_EXPR_OP_BIN     1013
#define NOWDB_EXPR_OP_FORMAT  1014

/* -----------------------------------------------------------------------
 * Bitwise
 * -----------------------------------------------------------------------
 */

/* -----------------------------------------------------------------------
 * String
 * -----------------------------------------------------------------------
 */
#define NOWDB_EXPR_OP_SUBSTR 10001
#define NOWDB_EXPR_OP_LENGTH 10002
#define NOWDB_EXPR_OP_STRCAT 10003
#define NOWDB_EXPR_OP_POS    10004
#endif
