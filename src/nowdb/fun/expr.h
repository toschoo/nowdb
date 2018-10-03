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

#define NOWDB_EXPR_CONST 1
#define NOWDB_EXPR_FIELD 2
#define NOWDB_EXPR_FUN   3
#define NOWDB_EXPR_OP    4

typedef struct nowdb_expr_t {
	uint32_t          exp; /* const, field, function or operator     */
	uint32_t           op; /* operator / function                    */
	nowdb_target_t target; /* context or vertex (or neither)         */
	uint32_t          off; /* identifies the field for edges         */
	char            *name; /* name of a property                     */
	nowdb_key_t    propid; /* propid for this property               */
	void           *value; /* constant value                         */
	uint16_t          typ; /* type of constant value                 */
	nowdb_bitmap8_t flags; /* what to do with the field              */
	uint32_t         func; /* non-aggregate to apply to the field    */
	uint32_t          agg; /* aggregate function to apply on the row */
	struct nowdb_expr_t *kids; /* operands                           */
} nowdb_expr_t;

/* -----------------------------------------------------------------------
 * Arithmetic
 * -----------------------------------------------------------------------
 */
#define NOWDB_EXPR_OP_ADD  10
#define NOWDB_EXPR_OP_SUB  11
#define NOWDB_EXPR_OP_MUL  12
#define NOWDB_EXPR_OP_DIV  13
#define NOWDB_EXPR_OP_REM  14
#define NOWDB_EXPR_OP_POW  15
#define NOWDB_EXPR_OP_ROOT 16
#define NOWDB_EXPR_OP_LOG  17

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
 * Bitwise
 * -----------------------------------------------------------------------
 */

/* -----------------------------------------------------------------------
 * String
 * -----------------------------------------------------------------------
 */


#endif
