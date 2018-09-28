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
#include <nowdb/query/row.h>
#include <nowdb/ifc/proc.h>
#include <nowdb/ifc/nowdb.h>

#include <string.h>
#include <time.h>
#include <limits.h>
#include <errno.h>
#include <ctype.h>

static char *OBJECT = "PROCIFC";

#define BUFSIZE 0x12000
#define MAXROW  0x1000

#define PARSERR(m) \
	err = nowdb_err_get(nowdb_err_parser, FALSE, OBJECT, m); \
	*res = mkErrResult(err);

#define INVALID(m) \
	err = nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, m); \
	*res = mkErrResult(err);

#define INVALIDERR(m) \
	err = nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, m);

#define CURSETERR(rc,m) \
	CUR(cur)->errcode = rc; \
	if (CUR(cur)->err != NOWDB_OK) { \
		nowdb_err_release(CUR(cur)->err); \
	} \
	CUR(cur)->err = nowdb_err_get(rc, FALSE, OBJECT, m);

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
	nowdb_err_t        err;
	char              *buf;
        uint32_t            sz;
        int                 lo;
        int                off;
        nowdb_qry_result_t res;
};

#define CUR(x) \
	((nowdb_dbresult_t) x)

#define ROW(x) \
	((nowdb_dbresult_t) x)

#define NOWDBCUR(x) \
	CUR(x)->res.result

/* ------------------------------------------------------------------------
 * Result types
 * ------------------------------------------------------------------------
 */
int nowdb_dbresult_type(nowdb_dbresult_t res) {
	if (res == NULL) return -1;
	return res->rtype;
}

void nowdb_dbresult_result(nowdb_dbresult_t res, nowdb_qry_result_t *r) {
	if (res == NULL) return;
	r->resType = res->res.resType;
	r->result = res->res.result;
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
 * Get NOWDB Error
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_dbresult_err(nowdb_dbresult_t res) {
	if (res == NULL) return NULL;
	return (res->err);
}

/* ------------------------------------------------------------------------
 * Get Error Details
 * ------------------------------------------------------------------------
 */
char *nowdb_dbresult_details(nowdb_dbresult_t res) {
	if (res == NULL) return NULL;
	return nowdb_err_describe(res->err, ';');
	
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
void nowdb_dbresult_destroy(nowdb_dbresult_t res, int all) {
	if (res == NULL) return;
	if (res->err != NULL) {
		nowdb_err_release(res->err);
		res->err = NOWDB_OK;
	}
	if (all && res->res.result != NULL) {
		// switch
		// report
		// cursor
		// row
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
	r->err = NOWDB_OK;

	r->res.resType = NOWDB_QRY_RESULT_NOTHING;
	r->res.result = NULL;

	return r;
}

/* ------------------------------------------------------------------------
 * Helper: make result from err
 * ------------------------------------------------------------------------
 */
static nowdb_dbresult_t mkErrResult(nowdb_err_t err) {
	struct nowdb_dbresult_t *r;

	r = mkResult(NOWDB_DBRESULT_STATUS);
	if (r == NULL) return NULL;
	if (err == NOWDB_OK) return r;
	r->errcode = err->errcode;
	r->err = err;
	return r; // may NULL!
}

/* ------------------------------------------------------------------------
 * Helper: make result from err
 * ------------------------------------------------------------------------
 */
static void qResDestroy(nowdb_qry_result_t *res) {
	if (res == NULL) return;
	switch(res->resType) {

	case NOWDB_QRY_RESULT_NOTHING: return;

	case NOWDB_QRY_RESULT_REPORT:
		if (res->result != NULL) {
			free(res->result); res->result = NULL;
		}
		break;

	case NOWDB_QRY_RESULT_ROW:
		if (res->result != NULL) {
			free(res->result); res->result = NULL;
		}
		break;

	case NOWDB_QRY_RESULT_CURSOR:
		if (res->result != NULL) {
			nowdb_cursor_destroy(res->result);
			free(res->result); res->result = NULL;
		}
		break;

	default: return;
		
	}
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
	r->err = nowdb_err_get(errcode, FALSE, "python", msg);

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
 * Make a row result
 * ------------------------------------------------------------------------
 */
nowdb_dbrow_t nowdb_dbresult_makeRow() {
	return (nowdb_dbrow_t)mkResult(NOWDB_DBRESULT_ROW);
}

/* ------------------------------------------------------------------------
 * Add something to a row
 * ------------------------------------------------------------------------
 */
int nowdb_dbresult_add2Row(nowdb_dbrow_t row, char t, void *value) 
{
	char *tmp = NULL;
	nowdb_qry_row_t *qr=NULL;

	if (row == NULL) return -1;

	if (ROW(row)->res.result == NULL) {
		qr = calloc(1,sizeof(nowdb_qry_row_t));
		if (qr == NULL) return -1;

		ROW(row)->res.resType = NOWDB_QRY_RESULT_ROW;
	} else {
		qr = ROW(row)->res.result;
	}

	tmp = nowdb_row_addValue(qr->row, t, value, &qr->sz);
	if (tmp == NULL) {
		if (qr->row != NULL) {
			free(qr->row); qr->row = NULL;
		}
		free(qr); qr = NULL;
		ROW(row)->res.resType = NOWDB_QRY_RESULT_NOTHING;
		ROW(row)->res.result  = NULL;
	}
	qr->row = tmp;
	ROW(row)->res.result = qr;
	return 0;
}

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

	case NOWDB_QRY_RESULT_ROW:
		r = mkResult(NOWDB_DBRESULT_ROW); break;

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

	/*
	fprintf(stderr, "exec!\n");
	fprintf(stderr, "DB    : %p\n", db);
	fprintf(stderr, "stmt  : %s\n", statement);
	fprintf(stderr, "result: %p\n", res);
	*/

	s = strnlen(statement, 4097);
	if (s > 4096) {
		INVALID("string too long! (max: 4096)");
		return -1;
	}
	if (s <= 0) {
		INVALID("empty string");
		return -1;
	}
	x = nowdbsql_parser_buffer(PROC(db)->parser,
                                   statement, s, &ast);
	if (x != 0) {
		PARSERR((char*)nowdbsql_parser_errmsg(PROC(db)->parser));
		return -1;
	}
	if (ast == NULL) {
		PARSERR("unknown parsing error");
		return -1;
	}

	err = nowdb_stmt_handle(ast, PROC(db)->scope, db,
	                         LIB(PROC(db)->lib)->base, &r);
	nowdb_ast_destroy(ast); free(ast);

	// build error result
	if (err != NOWDB_OK) {
		*res = mkErrResult(err);
		return -1;
	}

	// fprintf(stderr, "result type: %d\n", r.resType);

	*res = nowdb_dbresult_wrap(&r);
	if (*res == NULL) {
		qResDestroy(&r);	
		return -1;
	}

	return 0;
}

/* ------------------------------------------------------------------------
 * End of string
 * ------------------------------------------------------------------------
 */
static inline int findEndOfStr(char *buf, int sz, int idx) {
	return nowdb_row_findEndOfStr(buf, sz, idx);
}

/* ------------------------------------------------------------------------
 * End of row
 * ------------------------------------------------------------------------
 */
static inline int findEORow(char *buf, uint32_t sz, int idx) {
	return nowdb_row_findEOR(buf, sz, idx);
}

/* ------------------------------------------------------------------------
 * Find last complete row
 * ------------------------------------------------------------------------
 */
static inline int findLastRow(char *buf, int sz) {
	return nowdb_row_findLastRow(buf, sz);
}

/* ------------------------------------------------------------------------
 * Get the next row
 * ------------------------------------------------------------------------
 */
int nowdb_dbrow_next(nowdb_dbrow_t p) {
	int i,j;

	/* search start of next */
	i = findEORow(ROW(p)->buf, ROW(p)->sz, ROW(p)->off);
	if (i < 0) return -1;

	/* make sure the row is complete */
	j = findEORow(ROW(p)->buf, ROW(p)->sz, i);
	if (j<0) return -1;

	ROW(p)->off = i;
	
	/*
	fprintf(stderr, "advancing to row %d (max: %d)\n",
	                         ROW(p)->off, ROW(p)->sz);
	*/
	return 0;
}

/* ------------------------------------------------------------------------
 * Rewind to first row
 * ------------------------------------------------------------------------
 */
void nowdb_dbrow_rewind(nowdb_dbrow_t p) {
	ROW(p)->off = 0;
}

/* ------------------------------------------------------------------------
 * Row offset (for debugging)
 * ------------------------------------------------------------------------
 */
int nowdb_dbrow_off(nowdb_dbrow_t p) {
	return ROW(p)->off;
}

/* ------------------------------------------------------------------------
 * Get nth field in current row
 * ------------------------------------------------------------------------
 */
void *nowdb_dbrow_field(nowdb_dbrow_t p, int field, int *type) {
	int i;
	int f=0;

	for(i=ROW(p)->off; i<ROW(p)->sz && ROW(p)->buf[i] != NOWDB_EOR;)  {
		if (f==field) {
			*type = (int)ROW(p)->buf[i]; i++;
			return (ROW(p)->buf+i);
		}
		if (ROW(p)->buf[i] == NOWDB_TYP_TEXT) {
			i = findEndOfStr(ROW(p)->buf,
			                 ROW(p)->sz,i);
			if (i < 0) break;
		} else {
			i+=9;
		}
		f++;
	}
	return NULL;
}

/* ------------------------------------------------------------------------
 * Write all rows in human-readable form to file
 * TODO: check that print respects leftovers!
 * ------------------------------------------------------------------------
 */
int nowdb_dbrow_write(FILE *file, nowdb_dbrow_t row) {
	int rc;

	rc = nowdb_row_print(ROW(row)->buf, ROW(row)->sz, file);
	if (rc != 0) return rc;
	return 0;
}

/* ------------------------------------------------------------------------
 * Open cursor from given result
 * ------------------------------------------------------------------------
 */
int nowdb_dbcur_open(nowdb_dbresult_t cur) {
	nowdb_err_t err;

	CUR(cur)->buf = malloc(BUFSIZE);
	if (CUR(cur)->buf == NULL) {
		CURSETERR(nowdb_err_no_mem, "allocating fetch buffer");
		return -1;
	}
	CUR(cur)->off = 0;
	CUR(cur)->lo  = 0;
	CUR(cur)->sz  = 0;

	err = nowdb_cursor_open(NOWDBCUR(cur));
	if (err != NOWDB_OK) {
		if (CUR(cur)->err != NOWDB_OK) {
			nowdb_err_release(CUR(cur)->err);
		}
		CUR(cur)->err = err;
		CUR(cur)->errcode = err->errcode;
		free(CUR(cur)->buf);
		CUR(cur)->buf = NULL;
		return -1;
	}
	return 0;
}

/* ------------------------------------------------------------------------
 * Close cursor
 * ------------------------------------------------------------------------
 */
void nowdb_dbcur_close(nowdb_dbcur_t cur) {
	if (CUR(cur)->buf != NULL) {
		free(CUR(cur)->buf);
		CUR(cur)->buf = NULL;
	}
	if (CUR(cur)->err != NULL) {
		nowdb_err_release(CUR(cur)->err);
	}
	nowdb_cursor_destroy(NOWDBCUR(cur));
	free(NOWDBCUR(cur)); NOWDBCUR(cur) = NULL;
}

/* ------------------------------------------------------------------------
 * Rescue bytes of incomplete row at end of cursor
 * ------------------------------------------------------------------------
 */
static inline int leftover(nowdb_dbcur_t cur) {
	int l=0;
	char *buf = CUR(cur)->buf;

	CUR(cur)->lo = 0;
	if (CUR(cur)->sz == 0) return 0;

	l = findLastRow(buf, CUR(cur)->sz);
	if (l < 0) {
		CURSETERR(nowdb_err_invalid, "no last row");
		return -1;
	}
	if (l >= ROW(cur)->sz) return 0;

	CUR(cur)->lo = CUR(cur)->sz - l;
	if (CUR(cur)->lo >= MAXROW) {
		CURSETERR(nowdb_err_too_big, "row too big");
		return -1;
	}
	memcpy(buf, buf+l, CUR(cur)->lo);
	return 0;
}

/* ------------------------------------------------------------------------
 * Fetch next bunch of rows from server
 * ------------------------------------------------------------------------
 */
int nowdb_dbcur_fetch(nowdb_dbcur_t  cur) {
	char *buf;
	nowdb_err_t err;
	uint32_t cnt=0;

	// already at eof
	if (CUR(cur)->errcode == nowdb_err_eof) return -1;
	if (leftover(cur) != 0) return -1;

	buf = CUR(cur)->buf+CUR(cur)->lo;

	// fetch
	CUR(cur)->sz = 0;
	err = nowdb_cursor_fetch(NOWDBCUR(cur), buf,
	                         BUFSIZE-CUR(cur)->lo,
	                         &CUR(cur)->sz, &cnt);
	CUR(cur)->sz += CUR(cur)->lo;
	if (err != NOWDB_OK) {
		if (err->errcode == nowdb_err_eof) {
			if (CUR(cur)->sz > 0) {
				nowdb_err_release(err);
				return 0;
			}
		}
		if (CUR(cur)->err != NOWDB_OK) {
			nowdb_err_release(CUR(cur)->err);
		}
		CUR(cur)->err = err;
		CUR(cur)->errcode = err->errcode;
		return -1;
	}
	return 0;
}

/* ------------------------------------------------------------------------
 * Get first row from cursor
 * ------------------------------------------------------------------------
 */
nowdb_dbrow_t nowdb_dbcur_row(nowdb_dbcur_t cur) {
	struct nowdb_dbresult_t *cp;

	if (cur == NULL) return NULL;

	cp = mkResult(CUR(cur)->rtype);
	if (cp == NULL) return NULL;

	ROW(cp)->buf = CUR(cur)->buf;
        ROW(cp)->sz = CUR(cur)->sz + CUR(cur)->lo;
        ROW(cp)->lo = 0;
        ROW(cp)->off = 0;

	return (nowdb_dbrow_t)cp;
}

/* ------------------------------------------------------------------------
 * Get error code from cursor
 * ------------------------------------------------------------------------
 */
int nowdb_dbcur_errcode(nowdb_dbcur_t res) {
	if (res == NULL) return 0;
	return DBRES(res)->errcode;
}

/* ------------------------------------------------------------------------
 * Get error details from cursor
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_dbcur_err(nowdb_dbcur_t res) {
	if (res == NULL) return NOWDB_OK;
	return DBRES(res)->err;
}

/* ------------------------------------------------------------------------
 * Check whether cursor is 'EOF'
 * ------------------------------------------------------------------------
 */
int nowdb_dbcur_eof(nowdb_dbcur_t cur) {
	return nowdb_dbresult_eof(CUR(cur));
}

/* ------------------------------------------------------------------------
 * Check whether cursor has error
 * ------------------------------------------------------------------------
 */
int nowdb_dbcur_ok(nowdb_dbcur_t cur) {
	return nowdb_dbresult_status(CUR(cur));
}
