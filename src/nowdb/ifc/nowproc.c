/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * 
 * This file is part of the NOWDB Stored Procedure Library.
 *
 * The NOWDB Stored Procedure Library is free software;
 * you can redistribute it and/or modify it under the terms
 * of the GNU Lesser General Public License 
 * as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * The NOWDB Stored Procedure Library
 * is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with the NOWDB CLIENT Library; if not, see
 * <http://www.gnu.org/licenses/>.
 *  
 * ========================================================================
 * NOWDB Stored Procedure LIBRARY
 * ========================================================================
 */
#include <nowdb/nowproc.h>
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/query/stmt.h>
#include <nowdb/ifc/proc.h>
#include <nowdb/ifc/nowdb.h>

#include <string.h>
#include <time.h>
#include <limits.h>
#include <errno.h>
#include <ctype.h>

#define PROC(x) \
	((nowdb_proc_t*) x)

#define LIB(x) \
	((nowdb_t*) x)

/* ------------------------------------------------------------------------
 * Result
 * ------------------------------------------------------------------------
 */
struct nowdb_dbresult_t {
	uint16_t         rtype;
	int            errcode;
	char           *errmsg;
        nowdb_qry_result_t res;
};

/* ------------------------------------------------------------------------
 * Result types
 * ------------------------------------------------------------------------
 */
int nowdb_dbresult_type(nowdb_dbresult_t res) {
	if (res == NULL) return -1;
	return res->rtype;
}

/* ------------------------------------------------------------------------
 * Get Status
 * ----------
 * The status is either OK or NOK
 * ------------------------------------------------------------------------
 */
int nowdb_dbresult_status(nowdb_dbresult_t res) {
	if (res == NULL) return -1;
	return (res->errcode == 0);
}

/* ------------------------------------------------------------------------
 * Get NOWDB Error Code
 * ------------------------------------------------------------------------
 */
int nowdb_dbresult_errcode(nowdb_dbresult_t res) {
	if (res == NULL) return -1;
	return (res->errcode);
}

/* ------------------------------------------------------------------------
 * Get Error Details
 * ------------------------------------------------------------------------
 */
const char *nowdb_dbresult_details(nowdb_dbresult_t res) {
	if (res == NULL) return NULL;
	return (res->errmsg);
}

/* ------------------------------------------------------------------------
 * Get Report
 * ------------------------------------------------------------------------
 */
void nowdb_dbresult_report(nowdb_dbresult_t res,
		           uint64_t *affected,
		           uint64_t *errors,
		           uint64_t *runtime) {
	nowdb_qry_report_t *rep;

	*affected = 0;
	*errors   = 0;
	*runtime  = 0;

	if (res == NULL) return;
	if (res->rtype != NOWDB_DBRESULT_REPORT) return;
	if (res->res.result == NULL) return;

	rep = res->res.result;

	*affected = rep->affected;
	*errors   = rep->errors;
	*runtime  = rep->runtime;

}

/* ------------------------------------------------------------------------
 * Check if result is 'EOF'
 * ------------------------------------------------------------------------
 */
int nowdb_dbresult_eof(nowdb_dbresult_t res) {
	if (res == NULL) return 0;
	return (res->errcode == nowdb_err_eof);
}

/* ------------------------------------------------------------------------
 * Destroy result 
 * ------------------------------------------------------------------------
 */
void nowdb_dbresult_destroy(nowdb_dbresult_t res) {
	if (res == NULL) return;
	if (res->res.result != NULL) {
		// to be defined
	}
	free(res);
}

#define DBRES(x) \
	((nowdb_dbresult_t)x)

#define QRES(x) \
	((nowdb_qry_result_t*)x)

/* ------------------------------------------------------------------------
 * Helper: make result
 * ------------------------------------------------------------------------
 */
static nowdb_dbresult_t mkResult(int rtype) {
	struct nowdb_dbresult_t *r;

	r = calloc(1, sizeof(struct nowdb_dbresult_t));
	if (r == NULL) return NULL;

	r->rtype = rtype;
	r->errcode = 0;
	r->errmsg  = NULL;

	r->res.resType = NOWDB_QRY_RESULT_NOTHING;
	r->res.result = NULL;

	return r;
}

/* ------------------------------------------------------------------------
 * Create a result 'OK'
 * ------------------------------------------------------------------------
 */
nowdb_dbresult_t nowdb_dbresult_success() {
	struct nowdb_dbresult_t *r;

	r = mkResult(NOWDB_DBRESULT_STATUS);
	if (r == NULL) return NULL;

	return r;
}

/* ------------------------------------------------------------------------
 * Create a result from error
 * ------------------------------------------------------------------------
 */
nowdb_dbresult_t nowdb_dbresult_makeError(int errcode,
                                          char  *msg) {
	struct nowdb_dbresult_t *r;

	r = mkResult(NOWDB_DBRESULT_STATUS);
	if (r == NULL) return NULL;

	r->errcode = errcode;
	r->errmsg = strdup(msg); // may be NULL!

	return r;
}

/* ------------------------------------------------------------------------
 * Create a result from report
 * ------------------------------------------------------------------------
 */
nowdb_dbresult_t nowdb_dbresult_makeReport(uint64_t affected,
                                           uint64_t errors,
                                           uint64_t runtime) {
	struct nowdb_dbresult_t *r;
	nowdb_qry_report_t *qr;

	r = mkResult(NOWDB_DBRESULT_REPORT);
	if (r == NULL) return NULL;

	r->res.resType = NOWDB_QRY_RESULT_REPORT;

	qr = calloc(1, sizeof(nowdb_qry_report_t));
	if (qr == NULL) {
		free(r); return NULL;
	}

	r->res.result = qr;
	return r;
}

/* ------------------------------------------------------------------------
 * Create a result from row
 * ------------------------------------------------------------------------
 */
nowdb_dbresult_t nowdb_dbresult_makeRow(int fields, ...);

/* ------------------------------------------------------------------------
 * Create a result from empty row
 * ------------------------------------------------------------------------
 */
nowdb_dbresult_t nowdb_dbresult_fromEmptyRow(int fields);

/* ------------------------------------------------------------------------
 * Wrap result
 * ------------------------------------------------------------------------
 */
nowdb_dbresult_t nowdb_dbresult_wrap(void *result) {
	struct nowdb_dbresult_t *r;

	if (result == NULL) return mkResult(NOWDB_DBRESULT_STATUS);
	switch(QRES(result)->resType) {
	case NOWDB_QRY_RESULT_NOTHING:
		return mkResult(NOWDB_DBRESULT_STATUS);

	case NOWDB_QRY_RESULT_REPORT:
		r = mkResult(NOWDB_DBRESULT_REPORT); break;

	case NOWDB_QRY_RESULT_CURSOR:
		r = mkResult(NOWDB_DBRESULT_CURSOR); break;

	/* row ! */
	default:
		fprintf(stderr, "unknown result type\n");
		return NULL;
	}
	if (r == NULL) return NULL;
	if (result == NULL ||
	    QRES(result)->resType == NOWDB_QRY_RESULT_NOTHING) return r;

	r->res.resType = QRES(result)->resType;
	r->res.result = QRES(result)->result;

	return r;
}

/* ------------------------------------------------------------------------
 * Add something to a result row
 * ------------------------------------------------------------------------
 */
int nowdb_dbresult_addRow(int index, char type, void *value);

/* ------------------------------------------------------------------------
 * Single SQL statement
 * ------------------------------------------------------------------------
 */
int nowdb_dbexec_statement(nowdb_db_t         db,
                           char       *statement,
                           nowdb_dbresult_t *res) {
	nowdb_err_t err;
	nowdb_ast_t *ast=NULL;
	nowdb_qry_result_t r;
	size_t s;
	int x;

	fprintf(stderr, "exec!\n");
	fprintf(stderr, "DB    : %p\n", db);
	fprintf(stderr, "stmt  : %s\n", statement);
	fprintf(stderr, "result: %p\n", res);

	s = strnlen(statement, 4097);
	if (s > 4096) {
		fprintf(stderr, "string too long!\n");
		return -1;
	}
	if (s <= 0) {
		fprintf(stderr, "invalid string!\n");
		return -1;
	}
	x = nowdbsql_parser_buffer(PROC(db)->parser,
                                   statement, s, &ast);
	if (x != 0) {
		fprintf(stderr, "cannot parse statement!\n");
		return -1;
	}
	if (ast == NULL) {
		fprintf(stderr, "no ast!\n");
		return -1;
	}

	err = nowdb_stmt_handle(ast, PROC(db)->scope, db,
	                         LIB(PROC(db)->lib)->base, &r);
	nowdb_ast_destroy(ast); free(ast);

	// build error result
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}

	// switch result
	// NOTHING
	// STATUS*
	// REPORT
	// CURSOR
	// ROW*
	// *still to be handled in stmt

	fprintf(stderr, "result type: %d\n", r.resType);

	// result
	return 0;
}

/* ------------------------------------------------------------------------
 * Get the next row
 * ------------------------------------------------------------------------
 */
int nowdb_dbrow_next(nowdb_dbrow_t row);

/* ------------------------------------------------------------------------
 * Rewind to first row
 * ------------------------------------------------------------------------
 */
void nowdb_dbrow_rewind(nowdb_dbrow_t row);

/* ------------------------------------------------------------------------
 * Get field from row
 * ---------
 * The 'fieldth' field is selected
 * indexing starts with 0
 * ------------------------------------------------------------------------
 */
void *nowdb_dbrow_field(nowdb_dbrow_t row, int field, int *type);

/* ------------------------------------------------------------------------
 * Copy row 
 * ------------------------------------------------------------------------
 */
nowdb_dbrow_t nowdb_dbrow_copy(nowdb_dbrow_t row);

/* ------------------------------------------------------------------------
 * Write all rows in human-readable form to file
 * ------------------------------------------------------------------------
 */
int nowdb_dbrow_write(FILE *file, nowdb_dbrow_t row);

/* ------------------------------------------------------------------------
 * Cursor
 * ------------------------------------------------------------------------
 */
typedef struct nowdb_dbcur_t* nowdb_dbcur_t;

/* ------------------------------------------------------------------------
 * Open cursor from given result
 * ------------------------------------------------------------------------
 */
int nowdb_dbcur_open(nowdb_dbresult_t  res,
                      nowdb_dbcur_t *cur);

/* ------------------------------------------------------------------------
 * Close cursor
 * ------------------------------------------------------------------------
 */
int nowdb_dbcur_close(nowdb_dbcur_t cur);

/* ------------------------------------------------------------------------
 * Fetch next bunch of rows from server
 * ------------------------------------------------------------------------
 */
int nowdb_dbcur_fetch(nowdb_dbcur_t  cur);

/* ------------------------------------------------------------------------
 * Get first row from cursor
 * ------------------------------------------------------------------------
 */
nowdb_dbrow_t nowdb_dbcur_row(nowdb_dbcur_t cur);

/* ------------------------------------------------------------------------
 * Get error code from cursor
 * ------------------------------------------------------------------------
 */
int nowdb_dbcur_errcode(nowdb_dbcur_t res);

/* ------------------------------------------------------------------------
 * Get error details from cursor
 * ------------------------------------------------------------------------
 */
const char *nowdb_dbcur_details(nowdb_dbcur_t res);

/* ------------------------------------------------------------------------
 * Check whether cursor is 'EOF'
 * ------------------------------------------------------------------------
 */
int nowdb_dbcur_eof(nowdb_dbcur_t cur);

/* ------------------------------------------------------------------------
 * Check whether cursor has error
 * ------------------------------------------------------------------------
 */
int nowdb_dbcur_ok(nowdb_dbcur_t cur);

