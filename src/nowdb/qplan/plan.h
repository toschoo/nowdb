/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Execution Plan
 * TODO:
 * this is still heavily under construction;
 * in fact, it's currently just a link from ast to plan
 * ========================================================================
 */
#ifndef nowdb_plan_decl
#define nowdb_plan_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/reader/reader.h>
#include <nowdb/sql/ast.h>
#include <nowdb/scope/scope.h>

#include <tsalgo/list.h>

/* ------------------------------------------------------------------------
 * Types of plan nodes
 * ------------------------------------------------------------------------
 */
#define NOWDB_PLAN_SUMMARY    1
#define NOWDB_PLAN_READER     2
#define NOWDB_PLAN_FILTER     4
#define NOWDB_PLAN_GROUPING   5
#define NOWDB_PLAN_AGGREGATES 6
#define NOWDB_PLAN_ORDERING   7
#define NOWDB_PLAN_PROJECTION 8

/* ------------------------------------------------------------------------
 * Reader Types:
 * -------------
 * - fullscan
 * - fullscan+
 * - search
 * - search+
 * - range 
 * - range+
 * ------------------------------------------------------------------------
 */
#define NOWDB_PLAN_FS       10
#define NOWDB_PLAN_FS_      11
#define NOWDB_PLAN_SEARCH   20
#define NOWDB_PLAN_SEARCH_  21
#define NOWDB_PLAN_FRANGE   30
#define NOWDB_PLAN_FRANGE_  31
#define NOWDB_PLAN_MRANGE   40
#define NOWDB_PLAN_MRANGE_  41
#define NOWDB_PLAN_KRANGE   50
#define NOWDB_PLAN_KRANGE_  51
#define NOWDB_PLAN_CRANGE   60
#define NOWDB_PLAN_CRANGE_  61
#define NOWDB_PLAN_COUNTALL 70

/* ------------------------------------------------------------------------
 * Plan node
 * ------------------------------------------------------------------------
 */
typedef struct {
	uint32_t ntype; /* node type                 */
	uint32_t stype; /* subtype i                 */
	int     helper; /* generic number to store   */
	char     *name; /* name of something         */
	void     *load; /* pointer to some structure */
} nowdb_plan_t;

/* ------------------------------------------------------------------------
 * Simplified index descriptor
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_index_t  *idx;    /* the index                   */
	char           *keys;   /* the keys as buffer of bytes */
	ts_algo_tree_t **maps;  /* maps in case of mrange      */
} nowdb_plan_idx_t;

/* ------------------------------------------------------------------------
 * Create plan from ast
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_plan_fromAst(nowdb_scope_t  *scope,
                               nowdb_ast_t    *ast,
                               ts_algo_list_t *plan);

/* ------------------------------------------------------------------------
 * Destroy plan
 * if content is set, the content of the plan is destroyed
 * (only if it is not successfully passed on to cursor)
 * ------------------------------------------------------------------------
 */
void nowdb_plan_destroy(ts_algo_list_t *plan, char content);

/* ------------------------------------------------------------------------
 * show plan
 * ------------------------------------------------------------------------
 */
void nowdb_plan_show(ts_algo_list_t *plan, FILE *stream);

#define NOWDB_PLAN_OK_FIELD 1
#define NOWDB_PLAN_OK_CONST 2
#define NOWDB_PLAN_OK_OP    4
#define NOWDB_PLAN_OK_AGG   8

#define NOWDB_PLAN_NEED_TEXT 1024 

#define NOWDB_PLAN_OK_ALL(limits) \
	limits = NOWDB_PLAN_OK_FIELD | \
	         NOWDB_PLAN_OK_CONST | \
	         NOWDB_PLAN_OK_OP    | \
	         NOWDB_PLAN_OK_AGG   | \
                 NOWDB_PLAN_NEED_TEXT

#define NOWDB_PLAN_OK_REMOVE(limits, k) \
	limits ^= k

/* ------------------------------------------------------------------------
 * Get expression from plan
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_plan_getExpr(nowdb_scope_t    *scope,
                               nowdb_model_vertex_t *v,
                               nowdb_model_edge_t   *e,
                               nowdb_ast_t        *trg,
                               nowdb_ast_t      *field,
                               uint32_t         limits,
                               nowdb_expr_t      *expr,
                               char               *agg);
#endif
