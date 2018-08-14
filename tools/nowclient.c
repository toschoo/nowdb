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

#define EXEC(sql) \
	err = nowdb_exec_statement(con, sql, &res); \
	if (err != 0) { \
		fprintf(stderr, "cannot execute statement: %d\n", err); \
		rc = EXIT_FAILURE; goto cleanup; \
	} \
	if (nowdb_result_status(res) != 0) { \
		fprintf(stderr, "ERROR: %s\n", nowdb_result_details(res)); \
	} \
	nowdb_result_destroy(res);

#define DQL(sql) \
	err = nowdb_exec_statement(con, sql, &res); \
	if (err != 0) { \
		fprintf(stderr, "cannot execute statement: %d\n", err); \
		rc = EXIT_FAILURE; goto cleanup; \
	} \
	if (nowdb_result_status(res) != 0) { \
		fprintf(stderr, "ERROR: %s\n", nowdb_result_details(res)); \
	} \
	err = nowdb_row_write(stdout, nowdb_result_row(res)); \
	if (err != NOWDB_OK) { \
		fprintf(stderr, "cannot write row: %d\n", err); \
	} \
	nowdb_result_destroy(res);
	

int main() {
	int rc = EXIT_SUCCESS;
	int err;
	int flags=0;
	nowdb_con_t con;
	nowdb_result_t res;

	// flags = NOWDB_FLAGS_TEXT;
	err = nowdb_connect(&con, "127.0.0.1", 55505, NULL, NULL, flags);
	if (err != 0) {
		fprintf(stderr, "cannot get connection: %d\n", err);
		// describe error
		return EXIT_FAILURE;
	}

	err = nowdb_use(con, "retail", &res);
	if (err != 0) {
		fprintf(stderr, "cannot use retail: %d\n", err);
		// describe error
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (nowdb_result_status(res) != 0) {
		fprintf(stderr, "ERROR: %s\n", nowdb_result_details(res));
	}
	nowdb_result_destroy(res);

	EXEC("create tiny context myctx");
	EXEC("drop context myctx");

	DQL("select edge, timestamp, weight from tx\
	       where edge = 'buys_product' and origin = 0");
	
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
