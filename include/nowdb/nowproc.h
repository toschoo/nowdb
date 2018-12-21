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
#ifndef NOWDB_PROC_DECL
#define NOWDB_PROC_DECL

#include <nowdb/errcode.h>

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>

/* ------------------------------------------------------------------------
 * Nowdb interface
 * ------------------------------------------------------------------------
 */
typedef struct nowdb_proc_t* nowdb_db_t;

/* ------------------------------------------------------------------------
 * Generic Result
 * ------------------------------------------------------------------------
 */
typedef struct nowdb_dbresult_t* nowdb_dbresult_t;

/* ------------------------------------------------------------------------
 * Result types
 * ------------------------------------------------------------------------
 */
int nowdb_dbresult_type(nowdb_dbresult_t res);

#define NOWDB_DBRESULT_NOTHING 0
#define NOWDB_DBRESULT_STATUS  33
#define NOWDB_DBRESULT_REPORT  34
#define NOWDB_DBRESULT_ROW     35
#define NOWDB_DBRESULT_CURSOR  36
#define NOWDB_DBRESULT_UNKNOWN 40

/* ------------------------------------------------------------------------
 * Get Status
 * ----------
 * The status is either OK or NOK
 * ------------------------------------------------------------------------
 */
int nowdb_dbresult_status(nowdb_dbresult_t res);

/* ------------------------------------------------------------------------
 * Get NOWDB Error Code
 * ------------------------------------------------------------------------
 */
int nowdb_dbresult_errcode(nowdb_dbresult_t res);

/* ------------------------------------------------------------------------
 * Get Error Details
 * ------------------------------------------------------------------------
 */
char *nowdb_dbresult_details(nowdb_dbresult_t res);

/* ------------------------------------------------------------------------
 * Get Report
 * ------------------------------------------------------------------------
 */
void nowdb_dbresult_report(nowdb_dbresult_t res,
		           uint64_t *affected,
		           uint64_t *errors,
		           uint64_t *runtime);

/* ------------------------------------------------------------------------
 * Check if result is 'EOF'
 * ------------------------------------------------------------------------
 */
int nowdb_dbresult_eof(nowdb_dbresult_t res);

/* ------------------------------------------------------------------------
 * Destroy result 
 * ------------------------------------------------------------------------
 */
void nowdb_dbresult_destroy(nowdb_dbresult_t res, int all);

/* ------------------------------------------------------------------------
 * Wrap result
 * ------------------------------------------------------------------------
 */
nowdb_dbresult_t nowdb_dbresult_wrap(void *result);

/* ------------------------------------------------------------------------
 * Create a result 'OK'
 * ------------------------------------------------------------------------
 */
nowdb_dbresult_t nowdb_dbresult_success();

/* ------------------------------------------------------------------------
 * Create a result from error
 * ------------------------------------------------------------------------
 */
nowdb_dbresult_t nowdb_dbresult_makeError(int errcode,
                                          char  *msg);

/* ------------------------------------------------------------------------
 * Create a result from report
 * ------------------------------------------------------------------------
 */
nowdb_dbresult_t nowdb_dbresult_makeReport(uint64_t affected,
                                           uint64_t errors,
                                           uint64_t runtime);

/* ------------------------------------------------------------------------
 * Single SQL statement
 * ------------------------------------------------------------------------
 */
int nowdb_dbexec_statement(nowdb_db_t         db,
                           char       *statement,
                           nowdb_dbresult_t *res);

/* ------------------------------------------------------------------------
 * Row
 * ------------------------------------------------------------------------
 */
typedef struct nowdb_dbrow_t* nowdb_dbrow_t;

/* ------------------------------------------------------------------------
 * Create row result
 * ------------------------------------------------------------------------
 */
nowdb_dbrow_t nowdb_dbresult_makeRow();

/* ------------------------------------------------------------------------
 * Add something to a result row
 * ------------------------------------------------------------------------
 */
int nowdb_dbresult_add2Row(nowdb_dbrow_t row, char t, void *value);

/* ------------------------------------------------------------------------
 * Bytes left in row
 * ------------------------------------------------------------------------
 */
int nowdb_dbresult_addCapacity(nowdb_dbrow_t row);

/* ------------------------------------------------------------------------
 * Close row (i.e.: add EOR)
 * ------------------------------------------------------------------------
 */
int nowdb_dbresult_closeRow(nowdb_dbrow_t row);

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
 * Return the row offset (debugging)
 * ------------------------------------------------------------------------
 */
int nowdb_dbrow_off(nowdb_dbrow_t row);

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
int nowdb_dbcur_open(nowdb_dbresult_t cur);

/* ------------------------------------------------------------------------
 * Close cursor
 * ------------------------------------------------------------------------
 */
void nowdb_dbcur_close(nowdb_dbcur_t cur);

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
char *nowdb_dbcur_details(nowdb_dbcur_t res);

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

#endif

