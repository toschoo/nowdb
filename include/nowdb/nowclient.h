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

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>

/* ------------------------------------------------------------------------
 * Error codes
 * ------------------------------------------------------------------------
 */
#define NOWDB_OK          0
#define NOWDB_ERR_NOMEM   1
#define NOWDB_ERR_NOCON   2
#define NOWDB_ERR_NORES   3
#define NOWDB_ERR_EOF     4
#define NOWDB_ERR_INVALID 5
#define NOWDB_ERR_NOREAD  6
#define NOWDB_ERR_NOWRITE 7
#define NOWDB_ERR_NOOPEN  8
#define NOWDB_ERR_NOCLOSE 9
#define NOWDB_ERR_NOUSE   10
#define NOWDB_ERR_PROTO   11
#define NOWDB_ERR_TOOBIG  12
#define NOWDB_ERR_OSERR   13
#define NOWDB_ERR_FORMAT  14

/* ------------------------------------------------------------------------
 * Describe error
 * ------------------------------------------------------------------------
 */
const char *nowdb_err_describe(int err);

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

typedef int64_t nowdb_time_t;

/* ------------------------------------------------------------------------
 * Get current system time as nowdb time
 * Errors:
 * - OS Error
 * ------------------------------------------------------------------------
 */
int nowdb_time_now(nowdb_time_t *time);

/* ------------------------------------------------------------------------
 * Get time from string
 * ------------------------------------------------------------------------
 */
int nowdb_time_fromString(const char *buf,
                          const char *frm,
                          nowdb_time_t *t);

/* ------------------------------------------------------------------------
 * Write time to string
 * ------------------------------------------------------------------------
 */
int nowdb_time_toString(nowdb_time_t  t,
                        const char *frm,
                              char *buf,
                             size_t max);

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

int nowdb_connect(nowdb_con_t *con,
                  char *host, short port,
                  char *user, char *pw,
                  int flags);

int nowdb_connection_close(nowdb_con_t con);
void nowdb_connection_destroy(nowdb_con_t con);

/* ------------------------------------------------------------------------
 * Possible results
 * ------------------------------------------------------------------------
 */
typedef struct nowdb_result_t* nowdb_result_t;
typedef struct nowdb_row_t* nowdb_row_t;
typedef struct nowdb_report_t* nowdb_report_t;
typedef struct nowdb_cursor_t* nowdb_cursor_t;

int nowdb_result_type(nowdb_result_t res);

#define NOWDB_RESULT_NOTHING 0
#define NOWDB_RESULT_STATUS  1
#define NOWDB_RESULT_REPORT  2
#define NOWDB_RESULT_ROW     3
#define NOWDB_RESULT_CURSOR  4

int nowdb_result_status(nowdb_result_t res);
const char *nowdb_result_details(nowdb_result_t res);
short nowdb_result_errcode(nowdb_result_t res);

nowdb_report_t nowdb_result_report(nowdb_result_t res);

nowdb_row_t nowdb_result_row(nowdb_result_t res);
const char *nowdb_row_next(nowdb_row_t row);
void nowdb_row_rewind(nowdb_row_t row);
void *nowdb_row_field(nowdb_row_t row, int field, int *type);
int nowdb_row_write(FILE *file, nowdb_row_t row);

nowdb_cursor_t nowdb_result_cursor(nowdb_result_t res);

void nowdb_result_destroy(nowdb_result_t res);

/* ------------------------------------------------------------------------
 * Single SQL statement
 * ------------------------------------------------------------------------
 */
int nowdb_exec_statement(nowdb_con_t     con,
                         char     *statement,
                         nowdb_result_t *res);

/* ------------------------------------------------------------------------
 * Cursor
 * ------------------------------------------------------------------------
 */
int nowdb_cursor_open(nowdb_con_t     con,
                      char     *statement,
                      nowdb_cursor_t *cur);

int nowdb_cursor_close(nowdb_cursor_t cur);

int nowdb_cursor_fetch(nowdb_cursor_t cur,
                       nowdb_row_t   *row);

int nowdb_cursor_fetchBulk(nowdb_cursor_t  cur,
                           uint32_t  requested,
                           uint32_t   received,
                           nowdb_row_t   *row);

/* ------------------------------------------------------------------------
 * Misc
 * ------------------------------------------------------------------------
 */
int nowdb_use(nowdb_con_t con, char *db, nowdb_result_t *res);

#endif 
