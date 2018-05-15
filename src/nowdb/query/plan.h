/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Execution Plan
 * ========================================================================
 */
#ifndef nowdb_plan_decl
#define nowdb_plan_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/reader/reader.h>
#include <nowdb/query/ast.h>

#include <tsalgo/list.h>

#define NOWDB_PLAN_SUMMARY    1
#define NOWDB_PLAN_READER     2
#define NOWDB_PLAN_ITERATOR   3
#define NOWDB_PLAN_GROUPING   4
#define NOWDB_PLAN_ORDERING   5
#define NOWDB_PLAN_PROJECTION 6

#define NOWDB_READER_FS       10 
#define NOWDB_READER_FS_      11  
#define NOWDB_READER_SEARCH   20
#define NOWDB_READER_SEARCH_  21
#define NOWDB_READER_RANGE    30
#define NOWDB_READER_RANGE_   31

#define NOWDB_ITER_SEQ   1
#define NOWDB_ITER_MERGE 2
#define NOWDB_ITER_JOIN  3

#define NOWDB_ORD_ASC   1
#define NOWDB_ORD_DESC  2

typedef struct {
	uint32_t ntype;
	uint32_t stype;
	int     target;
	char     *name;
} nowdb_plan_t;

nowdb_err_t nowdb_plan_fromAst(nowdb_ast_t *ast, ts_algo_list_t *plan);

#endif
