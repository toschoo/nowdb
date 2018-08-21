/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * 
 * This file is part of the NOWDB CLIENT Library.
 *
 * The NOWDB CLIENT Library is free software; you can redistribute it
 * and/or modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * The NOWDB CLIENT Library
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
 * NOWDB CLIENT LIBRARY
 * ========================================================================
 */
#ifndef NOWDB_CLIENT_DECL 
#define NOWDB_CLIENT_DECL

/* ------------------------------------------------------------------------
 * Server error codes
 * ------------------------------------------------------------------------
 */
#include <nowdb/errcode.h>

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>

/* ------------------------------------------------------------------------
 * Client error codes
 * ------------------------------------------------------------------------
 */
#define NOWDB_OK 0
#define NOWDB_ERR_NOMEM   -1
#define NOWDB_ERR_NOCON   -2
#define NOWDB_ERR_NOSOCK  -3
#define NOWDB_ERR_ADDR    -4
#define NOWDB_ERR_NORES   -5
#define NOWDB_ERR_INVALID -6
#define NOWDB_ERR_NOREAD  -101
#define NOWDB_ERR_NOWRITE -102
#define NOWDB_ERR_NOOPEN  -103
#define NOWDB_ERR_NOCLOSE -104
#define NOWDB_ERR_NOUSE   -105
#define NOWDB_ERR_PROTO   -106
#define NOWDB_ERR_TOOBIG  -107
#define NOWDB_ERR_OSERR   -108
#define NOWDB_ERR_FORMAT  -109
#define NOWDB_ERR_CURZC   -110
#define NOWDB_ERR_CURCL   -111

#define NOWDB_ERR_EOF nowdb_err_eof

/* ------------------------------------------------------------------------
 * Explain error
 * ------------------------------------------------------------------------
 */
const char *nowdb_err_explain(int err);

/* ------------------------------------------------------------------------
 * Time is a 64-bit signed integer
 * ------------------------------------------------------------------------
 */
typedef int64_t nowdb_time_t;

/* ------------------------------------------------------------------------
 * The earliest and latest possible time points.
 * The concrete meaning depends on epoch and units.
 * For the UNIX epoch with nanosecond unit, 
 *         DAWN is in the year 1677
 *         DUSK is in the year 2262
 * ------------------------------------------------------------------------
 */
#define NOWDB_TIME_DAWN (nowdb_time_t)(LONG_MIN)
#define NOWDB_TIME_DUSK (nowdb_time_t)(LONG_MAX)

/* ------------------------------------------------------------------------
 * Standard date and time formats (ISO8601)
 * time: "2006-01-02T15:04:05"
 * date: "2006-01-02"
 * ------------------------------------------------------------------------
 */
#define NOWDB_TIME_FORMAT "%Y-%m-%dT%H:%M:%S"
#define NOWDB_DATE_FORMAT "%Y-%m-%d"

/* ------------------------------------------------------------------------
 * Get current system time as nowdb time
 * ------------------------------------------------------------------------
 */
nowdb_time_t nowdb_time_get();

/* ------------------------------------------------------------------------
 * Get time from string
 * ------------------------------------------------------------------------
 */
int nowdb_time_parse(const char *buf,
                     const char *frm,
                     nowdb_time_t *t);

/* ------------------------------------------------------------------------
 * Write time to string
 * ------------------------------------------------------------------------
 */
int nowdb_time_show(nowdb_time_t  t,
                    const char *frm,
                          char *buf,
                        size_t max);

/* ------------------------------------------------------------------------
 * Convert nowdb time to unix timespec
 * ------------------------------------------------------------------------
 */
nowdb_time_t nowdb_time_fromUnix(const struct timespec *tp); 

/* ------------------------------------------------------------------------
 * Convert unix timespec to nowdb time
 * ------------------------------------------------------------------------
 */
int nowdb_time_toUnix(nowdb_time_t t, struct timespec *tp); 

/* ------------------------------------------------------------------------
 * Flags
 * ------------------------------------------------------------------------
 */
#define NOWDB_FLAGS_NOTHING 0
#define NOWDB_FLAGS_TEXT    1
#define NOWDB_FLAGS_LE      2
#define NOWDB_FLAGS_BE      4

/* ------------------------------------------------------------------------
 * Connection
 * ------------------------------------------------------------------------
 */
typedef struct nowdb_con_t* nowdb_con_t;

/* ------------------------------------------------------------------------
 * Connect to a server
 * ------------------------------------------------------------------------
 */
int nowdb_connect(nowdb_con_t *con,
                  char *host, short port,
                  char *user, char *pw,
                  int flags);

/* ------------------------------------------------------------------------
 * Close connection
 * ------------------------------------------------------------------------
 */
int nowdb_connection_close(nowdb_con_t con);

/* ------------------------------------------------------------------------
 * Destroy connection
 * ------------------
 * Only to be used as least resort when close failed;
 * in particular one must not first close and then destroy,
 * that would cause a double free exception.
 * ------------------------------------------------------------------------
 */
void nowdb_connection_destroy(nowdb_con_t con);

/* ------------------------------------------------------------------------
 * Generic Result
 * ------------------------------------------------------------------------
 */
typedef struct nowdb_result_t* nowdb_result_t;

/* ------------------------------------------------------------------------
 * Result types
 * ------------------------------------------------------------------------
 */
int nowdb_result_type(nowdb_result_t res);

#define NOWDB_RESULT_NOTHING 0
#define NOWDB_RESULT_STATUS  0x21
#define NOWDB_RESULT_REPORT  0x22
#define NOWDB_RESULT_ROW     0x23
#define NOWDB_RESULT_CURSOR  0x24

/* ------------------------------------------------------------------------
 * Get Status
 * ----------
 * The status is either OK or NOK
 * ------------------------------------------------------------------------
 */
int nowdb_result_status(nowdb_result_t res);

/* ------------------------------------------------------------------------
 * Get NOWDB Error Code
 * ------------------------------------------------------------------------
 */
int nowdb_result_errcode(nowdb_result_t res);

/* ------------------------------------------------------------------------
 * Get Error Details
 * ------------------------------------------------------------------------
 */
const char *nowdb_result_details(nowdb_result_t res);

/* ------------------------------------------------------------------------
 * Get Report
 * ------------------------------------------------------------------------
 */
void nowdb_result_report(nowdb_result_t res,
		         uint64_t *affected,
		         uint64_t *errors,
		         uint64_t *runtime);

/* ------------------------------------------------------------------------
 * Check if result is 'EOF'
 * ------------------------------------------------------------------------
 */
int nowdb_result_eof(nowdb_result_t res);

/* ------------------------------------------------------------------------
 * Destroy result 
 * ------------------------------------------------------------------------
 */
void nowdb_result_destroy(nowdb_result_t res);

/* ------------------------------------------------------------------------
 * Single SQL statement
 * ------------------------------------------------------------------------
 */
int nowdb_exec_statement(nowdb_con_t     con,
                         char     *statement,
                         nowdb_result_t *res);

/* ------------------------------------------------------------------------
 * Single SQL statement (zerocopy)
 * ------------------------------------------------------------------------
 */
int nowdb_exec_statementZC(nowdb_con_t     con,
                           char     *statement,
                           nowdb_result_t *res);

/* ------------------------------------------------------------------------
 * Row
 * ------------------------------------------------------------------------
 */
typedef struct nowdb_row_t* nowdb_row_t;

/* ------------------------------------------------------------------------
 * Get the next row
 * ------------------------------------------------------------------------
 */
int nowdb_row_next(nowdb_row_t row);

/* ------------------------------------------------------------------------
 * Rewind to first row
 * ------------------------------------------------------------------------
 */
void nowdb_row_rewind(nowdb_row_t row);

/* ------------------------------------------------------------------------
 * Get field from row
 * ---------
 * The 'fieldth' field is selected
 * indexing starts with 0
 * ------------------------------------------------------------------------
 */
void *nowdb_row_field(nowdb_row_t row, int field, int *type);

/* ------------------------------------------------------------------------
 * Copy row 
 * ------------------------------------------------------------------------
 */
nowdb_row_t nowdb_row_copy(nowdb_row_t row);

/* ------------------------------------------------------------------------
 * Write all rows in human-readable form to file
 * ------------------------------------------------------------------------
 */
int nowdb_row_write(FILE *file, nowdb_row_t row);

/* ------------------------------------------------------------------------
 * Cursor
 * ------------------------------------------------------------------------
 */
typedef struct nowdb_cursor_t* nowdb_cursor_t;

/* ------------------------------------------------------------------------
 * Open cursor from given result
 * ------------------------------------------------------------------------
 */
int nowdb_cursor_open(nowdb_result_t  res,
                      nowdb_cursor_t *cur);

/* ------------------------------------------------------------------------
 * Close cursor
 * ------------------------------------------------------------------------
 */
int nowdb_cursor_close(nowdb_cursor_t cur);

/* ------------------------------------------------------------------------
 * Fetch next bunch of rows from server
 * ------------------------------------------------------------------------
 */
int nowdb_cursor_fetch(nowdb_cursor_t  cur);

/* ------------------------------------------------------------------------
 * Get first row from cursor
 * ------------------------------------------------------------------------
 */
nowdb_row_t nowdb_cursor_row(nowdb_cursor_t cur);

/* ------------------------------------------------------------------------
 * Get error code from cursor
 * ------------------------------------------------------------------------
 */
int nowdb_cursor_errcode(nowdb_cursor_t res);

/* ------------------------------------------------------------------------
 * Get error details from cursor
 * ------------------------------------------------------------------------
 */
const char *nowdb_cursor_details(nowdb_cursor_t res);

/* ------------------------------------------------------------------------
 * Check whether cursor is 'EOF'
 * ------------------------------------------------------------------------
 */
int nowdb_cursor_eof(nowdb_cursor_t cur);

/* ------------------------------------------------------------------------
 * Check whether cursor has error
 * ------------------------------------------------------------------------
 */
int nowdb_cursor_ok(nowdb_cursor_t cur);

#endif 
