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
#include <unistd.h>

#define HALFEDGE  8192
#define FULLEDGE 16384

#define HALFVRTX  8192
#define FULLVRTX 16384

#define EDGES "rsc/edge100.csv"
#define PRODS "rsc/products100.csv"
#define CLIENTS "rsc/clients100.csv"

#define PRODUCT 1
#define CLIENT  2

typedef struct {
	int       id;
	char str[64];
} vrtx_t;

typedef struct {
	int origin;
	int destin;
	int64_t timestamp;
	float weight;
	float weight2;
} edge_t;

edge_t *edges = NULL;

vrtx_t *products = NULL;
vrtx_t *clients = NULL;

uint64_t wanted;

#define EDGET(x) \
	((edge_t*)x)

int edgecompr(const void *lft, const void *rig) {
	if (EDGET(lft)->origin < EDGET(rig)->origin) return -1;
	if (EDGET(lft)->origin > EDGET(rig)->origin) return  1;
	return 0;
}

int writeEdges(char *path, int halves, int hprods, int hclients) {
	int err;
	nowdb_time_t base;
	nowdb_time_t nsecs;
	FILE *f;
	int x = halves * HALFEDGE;
	int h, m, s, ns;
	double w, w2;
	int c, p;
	int mxclients = hclients * HALFVRTX;
	int mxprods = hprods * HALFVRTX;
	char *loadstr = 
	"buys;%d;%d;0;2018-08-28T%02d:%02d:%02d.%03d;%.2f;%.2f\n";

	edges = calloc(x, sizeof(edge_t));
	if (edges == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return -1;
	}
	
	f = fopen(path, "w");
	if (f == NULL) {
		perror("cannot open vertex file");
		return -1;
	}

	err = nowdb_time_parse("2018-08-28T00:00:00",
	                    NOWDB_TIME_FORMAT, &base);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot get time base\n");
		fclose(f);
		return -1;
	}
	for(int i=0; i<x; i++) {

		do c = rand()%mxclients; while(c == 0);
		do p = rand()%mxprods; while(p == 0);

		h = rand()%24;
		m = rand()%60;
		s = rand()%60;
		ns = rand()%1000;

		do w = (double)(rand()%50) / 7; while(w == 0);
		do w2 = (double)(rand()%27) / 2; while(w2 == 0);

		w *= w2;

		fprintf(f, loadstr,  
		clients[c].id, products[p].id, h, m, s, ns, w, w2);

		edges[i].origin = clients[c].id;
		edges[i].destin = products[p].id;
		edges[i].weight = w;
		edges[i].weight2 = w2;

		nsecs = 1000000l * (int64_t)ns +
		        1000000000l * (int64_t)s +
			1000000000l * 60l * (int64_t)m +
		        1000000000l * 60l * 60l * (int64_t)h;

		edges[i].timestamp = base + nsecs;
	}
	fclose(f);
	qsort(edges, x, sizeof(edge_t), &edgecompr);
	return 0;
}

void randomString(char *str, int max) {
	int x;

	do x = rand()%max; while(x < 10);
	for(int i=0; i<x; i++) {
		str[i] = rand()%27;
		str[i]+=64;
	}
	str[x] = 0;
}

inline int prepareProducts(int halves) {
	int x = halves * HALFVRTX;
	int p = 0;

	products = calloc(x, sizeof(vrtx_t));
	if (products == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return -1;
	}
	for(int i=0; i<x; i++) {
		do p = rand()%(2*x); while(p==0);
		products[i].id = p;
		randomString(products[i].str, 60);
	}
	return 0;
}

inline int prepareClients(int halves) {
	int x = halves * HALFVRTX;
	int c = 0;

	clients = calloc(x, sizeof(vrtx_t));
	if (clients == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return -1;
	}
	for(int i=0; i<x; i++) {
		do c = rand()%(2*x); while(c==0);
		c += 9000000;
		clients[i].id = c;
		randomString(clients[i].str, 60);
	}
	wanted = clients[rand()%x].id;
	return 0;
}

int writeProducts(FILE *f, int halves) {
	int x = halves * HALFVRTX;

	fprintf(f, "prod_key;prod_desc\n");
	for(int i=0; i<x; i++) {
		fprintf(f, "%d;%s\n", products[i].id, products[i].str);
	}
	return 0;
}

int writeClients(FILE *f, int halves) {
	int x = halves * HALFVRTX;

	fprintf(f, "client_id;client_name\n");
	for(int i=0; i<x; i++) {
		fprintf(f, "%d;%s\n", clients[i].id, clients[i].str);
	}
	return 0;
}

int writeVrtx(char *path, int type, int halves) {
	FILE *f;
	int rc = 0;

	rc = type == PRODUCT ? prepareProducts(halves)
	                     : prepareClients(halves);
	if (rc != 0) return -1;

	f = fopen(path, "w");
	if (f == NULL) {
		perror("cannot open vertex file");
		return -1;
	}
	rc = type == PRODUCT ? writeProducts(f, halves)
	                     : writeClients(f, halves);
	fclose(f);
	return rc;
}

#define EXECZC(sql) \
	fprintf(stderr, "executing %s\n", sql); \
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
	nowdb_result_destroy(res);

#define EXECDLL(sql) \
	fprintf(stderr, "executing %s\n", sql); \
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
	nowdb_result_destroy(res); \
	fprintf(stderr, "loaded %ld with %ld errors in %ldus\n", \
	                aff, errs, rt); 

#define EXECCUR(sql) \
	fprintf(stderr, "executing %s\n", sql); \
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

#define EXEC(sql) \
	fprintf(stderr, "executing %s\n", sql); \
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
	} \
	nowdb_result_destroy(res);

#define FORGET(sql) \
	err = nowdb_exec_statement(con, sql, &res); \
	if (err != 0) { \
		fprintf(stderr, "cannot execute statement: %d\n", err); \
		rc = EXIT_FAILURE; goto cleanup; \
	} \
	nowdb_result_destroy(res);

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
	nowdb_result_destroy(res);

int main() {
	int rc = EXIT_SUCCESS;
	int err;
	int flags=0;
	nowdb_con_t con;
	nowdb_result_t res;
	nowdb_cursor_t cur;
	struct timespec t1, t2;
	uint64_t aff, errs, rt;
	uint64_t count;
	double   s;
	uint64_t cnt=0;
	double   w, wt=0;
	int t;
	char *tmp;
	char q[128];
	nowdb_row_t row;

	edges = NULL;
	products = NULL;
	clients = NULL;

        srand(time(NULL) ^ (uint64_t)&printf);

	err = nowdb_connect(&con, "127.0.0.1", "55505", NULL, NULL, flags);
	if (err != 0) {
		fprintf(stderr, "cannot get connection: %d\n", err);
		return EXIT_FAILURE;
	}

	// some syntactically wrong statements
	fprintf(stderr, "error cases\n");
	EXECFAULTY("create dabu client200");
	EXECFAULTY("create db client300");

	// now create the database
	fprintf(stderr, "now start real things\n");
	EXEC("drop database client100 if exists");
	EXEC("create database client100");

	// use it
	EXEC("use client100");

	// yet another error
	EXECFAULTY("select edge from nosuchcontext");

	EXEC("create tiny index vidx_rovi on vertex (role, vid)");
	EXEC("create index vidx_ropo on vertex (role, property)");

	EXEC("create tiny table sales set stress=constant");

	EXEC("create index cidx_ctx_de on sales (destin, edge)");
	EXEC("create index cidx_ctx_oe on sales (origin, edge)");

	EXEC("create type product (\
		prod_key uint primary key, \
		prod_desc text)");

	EXEC("create type client (\
		client_id uint primary key, \
		client_name text)");

	EXEC("create edge buys (\
		origin client, \
		destination product, \
		weight float, \
		weight2 float)");

	if (writeVrtx(PRODS, PRODUCT, 1) != 0) {
		fprintf(stderr, "cannot write products\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (writeVrtx(CLIENTS, CLIENT, 2) != 0) {
		fprintf(stderr, "cannot write clients\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (writeEdges(EDGES, 5, 1, 2) != 0) {
		fprintf(stderr, "cannot write edges\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

	EXECDLL("load 'rsc/products100.csv' into vertex \
		  use header as product");
	EXECDLL("load 'rsc/clients100.csv' into vertex \
		  use header as client");
	EXECDLL("load 'rsc/edge100.csv' into sales as edge");

	fprintf(stderr, "wait a second!\n");
	sleep(5);

	sprintf(q, "select count(*), sum(weight) from sales\
	             where edge = 'buys' and origin=%lu", wanted);
	EXECCUR(q);

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
	tmp = nowdb_row_field(row, 0, &t);
	if (tmp == NULL) {
		fprintf(stderr, "cannot get count\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	memcpy(&count, tmp, 8);
	tmp = nowdb_row_field(row, 1, &t);
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

	sprintf(q, "select edge, destin, timestamp, weight from sales\
	             where edge = 'buys' and origin=%lu", wanted);
	EXECCUR(q);

	err = nowdb_cursor_open(res, &cur);
	if (err != NOWDB_OK) {
		fprintf(stderr, "NOT A CURSOR\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	while (nowdb_cursor_ok(cur)) {
		// count rows
		row = nowdb_cursor_row(cur);
		if (row == NULL) {
			fprintf(stderr, "NO ROW!\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		cnt++;
		tmp = nowdb_row_field(row, 3, &t);
		if (tmp == NULL) {
			fprintf(stderr, "cannot get weight!\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		memcpy(&w, tmp, 8); wt += w;

		while ((err = nowdb_row_next(row)) == NOWDB_OK) {
			cnt++;

			tmp = nowdb_row_field(row, 3, &t);
			if (tmp == NULL) {
				fprintf(stderr, "cannot get weight!\n");
				rc = EXIT_FAILURE; goto cleanup;
			}
			memcpy(&w, tmp, 8); wt += w;
		}
		if (err != NOWDB_ERR_EOF) {
			fprintf(stderr, "cannot count rows: %d on %lu\n",
			                                       err,cnt);
			rc = EXIT_FAILURE; goto cleanup;
		}
		row = nowdb_cursor_row(cur);

		/*
		nowdb_row_rewind(row);
		err = nowdb_row_write(stdout, row);
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot write row: %d on %lu\n",
			                                       err,cnt);
			rc = EXIT_FAILURE; goto cleanup;
		}
		*/
		err = nowdb_cursor_fetch(cur);
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot fetch: %d\n", err);
			rc = EXIT_FAILURE; break;
		}
	}
	if (!nowdb_cursor_eof(cur)) {
		fprintf(stderr, "this is not EOF!\n");
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
	if (edges != NULL) free(edges);
	if (products != NULL) free(products);
	if (clients != NULL) free(clients);
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
