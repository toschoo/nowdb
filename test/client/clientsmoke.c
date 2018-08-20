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

#include <common/bench.h>

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

#define EXECDLL(sql) \
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
	} \
	if (nowdb_result_type(res) != NOWDB_RESULT_REPORT) { \
		fprintf(stderr, "NOT A REPORT: %d\n", \
		             nowdb_result_type(res)); \
		nowdb_result_destroy(res); \
		rc = EXIT_FAILURE; goto cleanup; \
	} \
	nowdb_result_report(res, &aff, &errs, &rt); \
	fprintf(stderr, "loaded %ld with %ld errors in %ldus\n", \
	                aff, errs, rt); 

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

#define EXECFAULTY(sql) \
	fprintf(stderr, "executing %s\n", sql); \
	err = nowdb_exec_statement(con, sql, &res); \
	if (err != 0) { \
		fprintf(stderr, "cannot execute statement: %d\n", err); \
		rc = EXIT_FAILURE; goto cleanup; \
	} \
	if (nowdb_result_status(res) == 0) { \
		fprintf(stderr, "faulty statement status == 0!\n"); \
		rc = EXIT_FAILURE; goto cleanup; \
	} \
	fprintf(stderr, "ERROR %hd: %s\n", \
	        nowdb_result_errcode(res), \
	        nowdb_result_details(res)); \

int main() {
	int rc = EXIT_SUCCESS;
	int err;
	int flags=0;
	nowdb_con_t con;
	nowdb_result_t res;
	nowdb_cursor_t cur;
	struct timespec t1, t2;
	uint64_t count;
	double   s;
	uint64_t cnt=0;
	double   w, wt=0;
	int t;
	char *tmp;
	nowdb_row_t row;

	err = nowdb_connect(&con, "127.0.0.1", 55505, NULL, NULL, flags);
	if (err != 0) {
		fprintf(stderr, "cannot get connection: %d\n", err);
		return EXIT_FAILURE;
	}

	EXECZC("use retail");
	nowdb_result_destroy(res);
	
	/*
	uint64_t aff, errs, rt;
	EXECZC("create tiny context myctx if not exists");
	nowdb_result_destroy(res);
	EXECDLL("load 'rsc/kilo.csv' into myctx");
	nowdb_result_destroy(res);
	EXECZC("drop context myctx if exists");
	nowdb_result_destroy(res);
	*/

	// expect error
	EXECFAULTY("select edge from nosuchcontext");
	nowdb_result_destroy(res);

	EXEC("select edge, origin, count(*), sum(weight) from tx\
	      where edge = 'buys_product' and destin = 1960");
	//    where edge = 'buys_product' and origin = 0");
	err = nowdb_cursor_open(res, &cur);
	if (err != NOWDB_OK) {
		fprintf(stderr, "NOT A CURSOR\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	row = nowdb_cursor_row(cur);
	if (row == NULL) {
		fprintf(stderr, "no row!\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	nowdb_row_write(stderr, row);
	tmp = nowdb_row_field(row, 2, &t);
	if (tmp == NULL) {
		fprintf(stderr, "cannot get count\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	memcpy(&count, tmp, 8);
	tmp = nowdb_row_field(row, 3, &t);
	if (tmp == NULL) {
		fprintf(stderr, "cannot get sum\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	memcpy(&s, tmp, 8);

	err = nowdb_cursor_close(cur);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot close cursor: %d\n", err);
		rc = EXIT_FAILURE; goto cleanup;
	}

	fprintf(stderr, "EXPECTING: %lu / %.4f\n", count, s);

	timestamp(&t1);
	EXEC("select edge, destin, timestamp, weight from tx\
	      where edge = 'buys_product' and destin = 1960");
	//    where edge = 'buys_product' and origin = 0");
	//    where edge = 'buys_product' and origin = 419800000002");

	err = nowdb_cursor_open(res, &cur);
	if (err != NOWDB_OK) {
		fprintf(stderr, "NOT A CURSOR\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	while (nowdb_cursor_ok(cur)) {
		// count rows
		// row = nowdb_row_copy(nowdb_cursor_row(cur));
		row = nowdb_cursor_row(cur);
		if (row == NULL) {
			fprintf(stderr, "NO ROW!\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		cnt++;
		tmp = nowdb_row_field(row, 3, &t);
		if (tmp == NULL) {
			fprintf(stderr, "cannot get weight!\n");
			// nowdb_result_destroy(row);
			rc = EXIT_FAILURE; goto cleanup;
		}
		memcpy(&w, tmp, 8); wt += w;

		while ((err = nowdb_row_next(row)) == NOWDB_OK) {
			cnt++;

			tmp = nowdb_row_field(row, 3, &t);
			if (tmp == NULL) {
				fprintf(stderr, "cannot get weight!\n");
				// nowdb_result_destroy(row);
				rc = EXIT_FAILURE; goto cleanup;
			}
			memcpy(&w, tmp, 8); wt += w;
		}
		// nowdb_result_destroy(row);
		if (err != NOWDB_ERR_EOF) {
			fprintf(stderr, "cannot count rows: %d on %lu\n",
			                                       err,cnt);
			rc = EXIT_FAILURE; goto cleanup;
		}
		row = nowdb_cursor_row(cur);
		nowdb_row_rewind(row);
		err = nowdb_row_write(stdout, row);
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot write row: %d on %lu\n",
			                                       err,cnt);
			rc = EXIT_FAILURE; goto cleanup;
		}
		err = nowdb_cursor_fetch(cur);
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot fetch: %d\n", err);
			rc = EXIT_FAILURE; break;
		}
		// timestamp(&t4);
		// fprintf(stderr, "fetch: %ldus\n", minus(&t4, &t3)/1000);
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
	timestamp(&t2);
	fprintf(stderr, "time: %ldus\n", minus(&t2, &t1)/1000);

	if (cnt != count) {
		fprintf(stderr, "unexpected count: %lu != %lu\n", cnt, count);
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (wt != s) {
		fprintf(stderr, "unexpected sum: %.4f != %.4f\n", wt, s);
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
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}
