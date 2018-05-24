/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Statement Interface
 * ========================================================================
 * Unique interface to execute statements
 * ========================================================================
 */
#ifndef nowdb_query_stmt_decl
#define nowdb_query_stmt_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/io/dir.h>
#include <nowdb/scope/scope.h>
#include <nowdb/query/ast.h>

/* -----------------------------------------------------------------------
 * Result Types of Statement Execution
 * ------------
 * A statement may return:
 * - nothing
 * - a report (lines affected, execution time)
 * - a plan (currently not used)
 * - a scope ("use")
 * - a cursor
 * -----------------------------------------------------------------------
 */
#define NOWDB_QRY_RESULT_NOTHING 1
#define NOWDB_QRY_RESULT_REPORT  2
#define NOWDB_QRY_RESULT_PLAN    3
#define NOWDB_QRY_RESULT_SCOPE   4
#define NOWDB_QRY_RESULT_CURSOR  5

/* -----------------------------------------------------------------------
 * Result
 * -----------------------------------------------------------------------
 */
typedef struct {
	int  resType; /* Result Type           */
	void *result; /* Pointer to the result */
} nowdb_qry_result_t;

/* -----------------------------------------------------------------------
 * Handle statement
 * ----------------
 * The function receives
 * - an ast (which is either a ddl, dll, dml, dql or misc)
 * - a scope (which may be NULL)
 * - a base directory
 * - a pointer to the result
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_stmt_handle(nowdb_ast_t *ast,
                          nowdb_scope_t *scope,
                          nowdb_path_t    base,
                      nowdb_qry_result_t *res);

/* -----------------------------------------------------------------------
 * A report
 * -----------------------------------------------------------------------
 */
typedef struct {
	uint64_t    affected; /* affected rows       */
	uint64_t      errors; /* number of errors    */
	nowdb_time_t runtime; /* running times in us */
} nowdb_qry_report_t;

#endif
