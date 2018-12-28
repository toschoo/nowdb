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
#include <tsalgo/tree.h>

#include <stdio.h>

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
#define NOWDB_EXPR_USR   4
#define NOWDB_EXPR_REF   5
#define NOWDB_EXPR_AGG   6

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
 * Expession is boolean / expression is comparison
 * ------------------------------------------------------------------------
 */
char nowdb_expr_bool(nowdb_expr_t expr);
char nowdb_expr_compare(nowdb_expr_t expr);

/* ------------------------------------------------------------------------
 * Field Expression
 * ------------------------------------------------------------------------
 */
typedef struct {
	uint32_t        etype; /* expression type (field) */
	nowdb_target_t target; /* vertex or edge?         */
	int               off; /* offset into data source */
	nowdb_type_t     type; /* Type of field           */
	char            *text; /* string if off is text   */
	char            *name; /* property name if vertex */
	nowdb_roleid_t   role; /* vertex type if vertex   */
	nowdb_key_t    propid; /* propid if vertex        */
	char               pk; /* primary key if vertex   */
	char           usekey; /* use key instead of text */
} nowdb_field_t;

/* ------------------------------------------------------------------------
 * Convert expression to field
 * ------------------------------------------------------------------------
 */
#define NOWDB_EXPR_TOFIELD(x) \
	((nowdb_field_t*)x)

/* -----------------------------------------------------------------------
 * Tell field not to use text
 * -----------------------------------------------------------------------
 */
void nowdb_expr_usekey(nowdb_expr_t expr);

/* ------------------------------------------------------------------------
 * Constant Expression
 * -------------------
 * notice the backup value:
 * the value of a constant might change due to conversions,
 * the type information, however, will not.
 * we therefore recreate the original value from the backup
 * on each evaluation.
 * ------------------------------------------------------------------------
 */
typedef struct {
	uint32_t       etype; /* expression type (const) */
	void          *value; /* the value               */
	void          *valbk; /* backup                  */
	ts_algo_tree_t *tree; /* tree for 'in'           */
	nowdb_type_t    type; /* and its type            */
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
	uint32_t       etype;  /* expression type (op)                  */
	uint32_t         fun;  /* Operator type (+,-,*,/,...)           */
	uint32_t        args;  /* Number of operands                    */
	nowdb_expr_t   *argv;  /* Operands                              */
	nowdb_type_t  *types;  /* types of the arg results              */
	void       **results;  /* pointers to arg results               */
	nowdb_value_t    res;  /* result holds a value or a pointer     */
	char           *text;  /* when op result is a text manipulation */
} nowdb_op_t;

/* ------------------------------------------------------------------------
 * Convert expression to OP
 * ------------------------------------------------------------------------
 */
#define NOWDB_EXPR_TOOP(x) \
	((nowdb_op_t*)x)

/* ------------------------------------------------------------------------
 * Reference Expression (???)
 * ------------------------------------------------------------------------
 */
typedef struct {
	uint32_t    etype; /* expression type (ref) */
	nowdb_expr_t  ref; /* from where it comes   */
} nowdb_ref_t;

/* ------------------------------------------------------------------------
 * Convert expression to ref
 * ------------------------------------------------------------------------
 */
#define NOWDB_EXPR_TOREF(x) \
	((nowdb_ref_t*)x)

/* ------------------------------------------------------------------------
 * Anonymous aggregate
 * ------------------------------------------------------------------------
 */
typedef struct nowdb_fun_t* nowdb_aggfun_t;

/* ------------------------------------------------------------------------
 * Aggregate Expression
 * ------------------------------------------------------------------------
 */
typedef struct {
	uint32_t  etype;    /* expression type (agg) */
	nowdb_aggfun_t agg; /* from where it comes   */
} nowdb_agg_t;

/* ------------------------------------------------------------------------
 * Convert expression to ref
 * ------------------------------------------------------------------------
 */
#define NOWDB_EXPR_TOAGG(x) \
	((nowdb_agg_t*)x)

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
	char              needtxt; /* we need to evaluate text          */
} nowdb_eval_t;

/* -----------------------------------------------------------------------
 * Destroy evaluation helper
 * -----------------------------------------------------------------------
 */
void nowdb_eval_destroy(nowdb_eval_t *eval);

/* -----------------------------------------------------------------------
 * Create EdgeField expression
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_newEdgeField(nowdb_expr_t *expr, uint32_t off);

/* -----------------------------------------------------------------------
 * Create EdgeField expression
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_newVertexOffField(nowdb_expr_t *expr, uint32_t off);

/* -----------------------------------------------------------------------
 * Create VertexField expression
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_newVertexField(nowdb_expr_t  *expr,
                                      char      *propname,
                                      nowdb_roleid_t role,
                                      nowdb_key_t propid);

/* -----------------------------------------------------------------------
 * Create Constant expression
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_newConstant(nowdb_expr_t  *expr,
                                   void         *value,
                                   nowdb_type_t  type);

/* -----------------------------------------------------------------------
 * Create Constant expression representing an 'in' list from a list
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_constFromList(nowdb_expr_t   *expr,
                                     ts_algo_list_t *list,
                                     nowdb_type_t    type);

/* -----------------------------------------------------------------------
 * Create Constant expression representing an 'in' list from a tree
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_constFromTree(nowdb_expr_t   *expr,
                                     ts_algo_tree_t *tree,
                                     nowdb_type_t    type);

/* -----------------------------------------------------------------------
 * Create such a tree for constant expression
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_newTree(ts_algo_tree_t **tree,
                               nowdb_type_t    type);

/* -----------------------------------------------------------------------
 * Create Operator expression (with arguments passed in as list)
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_newOpL(nowdb_expr_t   *expr,
                              uint32_t         fun,
                              ts_algo_list_t  *ops);

/* -----------------------------------------------------------------------
 * Create Operator expression (with arguments passed in as argv)
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_newOpV(nowdb_expr_t  *expr,
                              uint32_t        fun,
                              uint32_t        len,
                              nowdb_expr_t   *ops);

/* -----------------------------------------------------------------------
 * Create Operator expression (with arguments passed in as va_list)
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_newOp(nowdb_expr_t *expr, uint32_t fun, ...);

/* -----------------------------------------------------------------------
 * Create Reference expression
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_newRef(nowdb_expr_t *expr, nowdb_expr_t ref);

/* -----------------------------------------------------------------------
 * Create Agg expression
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_newAgg(nowdb_expr_t *expr, nowdb_aggfun_t agg);

/* -----------------------------------------------------------------------
 * Get operator code from name
 * -----------------------------------------------------------------------
 */
int nowdb_op_fromName(char *op, char *agg);

/* -----------------------------------------------------------------------
 * Destroy expression
 * -----------------------------------------------------------------------
 */
void nowdb_expr_destroy(nowdb_expr_t expr);

/* ------------------------------------------------------------------------
 * We have a family triangle:
 *
 *                O
 *               / \
 *              F   C
 *
 * where O is either EQ, NE or IN
 * F is either Field or Const
 * and C is Const if F is Field or Field if F is Const
 * in this case, we can use reduced code, i.e.
 * we can use keys instead of elaborate text.
 * ------------------------------------------------------------------------
 */
char nowdb_expr_family3(nowdb_expr_t op);

/* ------------------------------------------------------------------------
 * Get field and constant from a family triangle 
 * where, on success, f will point to the field
 * and c to the constant.
 * ------------------------------------------------------------------------
 */
char nowdb_expr_getFieldAndConst(nowdb_expr_t op,
                                 nowdb_expr_t *f,
                                 nowdb_expr_t *c);

/* ------------------------------------------------------------------------
 * Replace an operand in a family triangle
 * ------------------------------------------------------------------------
 */
void nowdb_expr_replace(nowdb_expr_t op,
                        nowdb_expr_t x);

/* -----------------------------------------------------------------------
 * Guess type before evaluation
 * -----------------------------------------------------------------------
 */
int nowdb_expr_guessType(nowdb_expr_t expr);

/* -----------------------------------------------------------------------
 * Copy expression
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_copy(nowdb_expr_t  src,
                            nowdb_expr_t *trg);

/* -----------------------------------------------------------------------
 * Two expressions are equivalent
 * -----------------------------------------------------------------------
 */
char nowdb_expr_equal(nowdb_expr_t one,
                      nowdb_expr_t two);

/* -----------------------------------------------------------------------
 * Filter expressions of certain type
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_filter(nowdb_expr_t   expr,
                              uint32_t       type,
                              ts_algo_list_t *res);

/* -----------------------------------------------------------------------
 * Has expressions of certain type
 * -----------------------------------------------------------------------
 */
char nowdb_expr_has(nowdb_expr_t   expr,
                    uint32_t       type);

/* -----------------------------------------------------------------------
 * Evaluate expression
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_expr_eval(nowdb_expr_t expr,
                            nowdb_eval_t *hlp,
                            uint64_t     rmap,
                            char         *row,
                            nowdb_type_t *typ,
                            void        **res);

/* ------------------------------------------------------------------------
 * Extract time period from expression
 * ------------------------------------------------------------------------
 */
void nowdb_expr_period(nowdb_expr_t expr,
                       nowdb_time_t *start,
                       nowdb_time_t *end);

/* ------------------------------------------------------------------------
 * Extract key range from expression
 * ------------------------------------------------------------------------
 */
char nowdb_expr_range(nowdb_expr_t expr,
                      uint16_t sz, uint16_t *off,
                      char *rstart, char *rend);

/* -----------------------------------------------------------------------
 * Show expression
 * -----------------------------------------------------------------------
 */
void nowdb_expr_show(nowdb_expr_t expr, FILE *stream);

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
#define NOWDB_EXPR_OP_EQ    101
#define NOWDB_EXPR_OP_NE    102
#define NOWDB_EXPR_OP_LT    103
#define NOWDB_EXPR_OP_GT    104
#define NOWDB_EXPR_OP_LE    105
#define NOWDB_EXPR_OP_GE    106
#define NOWDB_EXPR_OP_IN    107
#define NOWDB_EXPR_OP_IS    108
#define NOWDB_EXPR_OP_ISN   109
#define NOWDB_EXPR_OP_COAL  111
#define NOWDB_EXPR_OP_TRUE  150
#define NOWDB_EXPR_OP_FALSE 151
#define NOWDB_EXPR_OP_NOT   152
#define NOWDB_EXPR_OP_JUST  153
#define NOWDB_EXPR_OP_AND   154
#define NOWDB_EXPR_OP_OR    155
#define NOWDB_EXPR_OP_WHEN  157
#define NOWDB_EXPR_OP_ELSE  158

/* -----------------------------------------------------------------------
 * TIME
 * -----------------------------------------------------------------------
 */
#define NOWDB_EXPR_OP_CENTURY 1001
#define NOWDB_EXPR_OP_YEAR    1002
#define NOWDB_EXPR_OP_MONTH   1003
#define NOWDB_EXPR_OP_MDAY    1004
#define NOWDB_EXPR_OP_WDAY    1005
#define NOWDB_EXPR_OP_YDAY    1006
#define NOWDB_EXPR_OP_HOUR    1007
#define NOWDB_EXPR_OP_MIN     1008
#define NOWDB_EXPR_OP_SEC     1009
#define NOWDB_EXPR_OP_MILLI   1010
#define NOWDB_EXPR_OP_MICRO   1011
#define NOWDB_EXPR_OP_NANO    1012
#define NOWDB_EXPR_OP_DAWN    1013
#define NOWDB_EXPR_OP_DUSK    1014
#define NOWDB_EXPR_OP_NOW     1015
#define NOWDB_EXPR_OP_EPOCH   1016
#define NOWDB_EXPR_OP_BIN     1020
#define NOWDB_EXPR_OP_FORMAT  1021

/* -----------------------------------------------------------------------
 * Bitwise
 * -----------------------------------------------------------------------
 */

/* -----------------------------------------------------------------------
 * Geo
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
