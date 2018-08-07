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
#define NOWDB_PLAN_KRANGE   40
#define NOWDB_PLAN_KRANGE_  41
#define NOWDB_PLAN_CRANGE   50
#define NOWDB_PLAN_CRANGE_  51

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
	nowdb_index_t *idx;
	char          *keys;
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

#endif
