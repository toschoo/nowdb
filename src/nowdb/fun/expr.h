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

typedef struct nowdb_expr_t* nowdb_expr_t;

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
 * Constant Expression
 * ------------------------------------------------------------------------
 */
typedef struct {
	uint32_t    etype; /* expression type         */
	void       *value; /* the value    */
	nowdb_type_t type; /* and its type */
} nowdb_const_t;

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
 * Aggregate Expression
 * ------------------------------------------------------------------------
 */

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
nowdb_err_t nowdb_expr_eval(nowdb_expr_t  expr,
                            char         *row,
                            nowdb_type_t *typ,
                            void        **res);

#endif
