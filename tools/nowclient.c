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
#include <nowdb/nowclient.h>

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>

#define EXECZC(sql) \
	err = nowdb_exec_statementZC(con, sql, &res); \
	if (err != 0) { \
		fprintf(stderr, "cannot execute statement: %d\n", err); \
		rc = EXIT_FAILURE; goto cleanup; \
	} \
	if (nowdb_result_status(res) != 0) { \
		fprintf(stderr, "ERROR %hd: %s\n", \
		        nowdb_result_errcode(res), \
		       nowdb_result_details(res)); \
		nowdb_result_destroy(res); \
		rc = EXIT_FAILURE; goto cleanup; \
	}

#define EXEC(sql) \
	err = nowdb_exec_statement(con, sql, &res); \
	if (err != 0) { \
		fprintf(stderr, "cannot execute statement: %d\n", err); \
		rc = EXIT_FAILURE; goto cleanup; \
	} \
	if (nowdb_result_status(res) != 0) { \
		fprintf(stderr, "ERROR %hd: %s\n", \
		        nowdb_result_errcode(res), \
		       nowdb_result_details(res)); \
		nowdb_result_destroy(res); \
		rc = EXIT_FAILURE; goto cleanup; \
	}

int main() {
	int rc = EXIT_SUCCESS;
	int err;
	int flags=0;
	nowdb_con_t con;
	nowdb_result_t res;
	nowdb_cursor_t cur;

	err = nowdb_connect(&con, "127.0.0.1", 55505, NULL, NULL, flags);
	if (err != 0) {
		fprintf(stderr, "cannot get connection: %d\n", err);
		return EXIT_FAILURE;
	}

	EXECZC("use retail");
	nowdb_result_destroy(res);
	EXECZC("create tiny context myctx");
	nowdb_result_destroy(res);
	EXECZC("drop context myctx");
	nowdb_result_destroy(res);

	EXEC("select edge, destin, timestamp, weight from tx\
	      where edge = 'buys_product' and destin = 1960");
	//    where edge = 'buys_product' and origin = 419800000002");

	err = nowdb_cursor_open(res, &cur);
	if (err != NOWDB_OK) {
		fprintf(stderr, "NOT A CURSOR\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	while (nowdb_cursor_ok(cur)) {
		err = nowdb_row_write(stdout, nowdb_cursor_row(cur));
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot write row: %d\n", err);
		}
		err = nowdb_cursor_fetch(cur);
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot fetch: %d\n", err);
			rc = EXIT_FAILURE; break;
		}
	}
	if (!nowdb_cursor_eof(cur)) {
		fprintf(stderr, "ERROR %d: %s\n",
		        nowdb_cursor_errcode(cur),
		        nowdb_cursor_details(cur));
		rc = EXIT_FAILURE;
	}
	err = nowdb_cursor_close(cur);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot close cursor: %d\n", err);
		rc = EXIT_FAILURE; goto cleanup;
	}
	
cleanup:
	err = nowdb_connection_close(con);
	if (err != 0) {
		fprintf(stderr, "cannot get connection: %d\n", err);
		// describe error
		nowdb_connection_destroy(con);
		return EXIT_FAILURE;
	}
	return rc;
}
