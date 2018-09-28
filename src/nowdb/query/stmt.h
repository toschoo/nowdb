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
#include <nowdb/sql/ast.h>

#include <tsalgo/list.h>

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
#define NOWDB_QRY_RESULT_ROW     5
#define NOWDB_QRY_RESULT_CURSOR  6
#define NOWDB_QRY_RESULT_OP      7

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
 * - a list of additional resources (e.g. open scopes)
 * - a base directory
 * - a pointer to the result
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_stmt_handle(nowdb_ast_t *ast,
                          nowdb_scope_t *scope,
                          void            *rsc,
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

/* -----------------------------------------------------------------------
 * A row
 * -----------------------------------------------------------------------
 */
typedef struct nowdb_qry_row_t {
	char     *row; /* row buffer */
	uint32_t   sz; /* size       */
} nowdb_qry_row_t;

#endif
