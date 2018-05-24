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
#include <nowdb/query/ast.h>

#include <tsalgo/list.h>

/* ------------------------------------------------------------------------
 * Types of plan nodes
 * ------------------------------------------------------------------------
 */
#define NOWDB_PLAN_SUMMARY    1
#define NOWDB_PLAN_READER     2
#define NOWDB_PLAN_ITER       3
#define NOWDB_PLAN_FILTER     4
#define NOWDB_PLAN_GROUPING   5
#define NOWDB_PLAN_ORDERING   6
#define NOWDB_PLAN_PROJECTION 7

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
#define NOWDB_READER_FS       10 
#define NOWDB_READER_FS_      11  
#define NOWDB_READER_SEARCH   20
#define NOWDB_READER_SEARCH_  21
#define NOWDB_READER_RANGE    30
#define NOWDB_READER_RANGE_   31

/* ------------------------------------------------------------------------
 * Iterator Types:
 * ---------------
 * - sequential
 * - merge
 * - join
 * ------------------------------------------------------------------------
 */
#define NOWDB_ITER_SEQ   1
#define NOWDB_ITER_MERGE 2
#define NOWDB_ITER_JOIN  3

/* ------------------------------------------------------------------------
 * ascending/descending
 * ------------------------------------------------------------------------
 */
#define NOWDB_ORD_ASC   1
#define NOWDB_ORD_DESC  2

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
 * Create plan from ast
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_plan_fromAst(nowdb_ast_t *ast, ts_algo_list_t *plan);

/* ------------------------------------------------------------------------
 * Destroy plan
 * ------------------------------------------------------------------------
 */
void nowdb_plan_destroy(ts_algo_list_t *plan);

#endif
