/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Test the tester
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <common/scopes.h>
#include <common/bench.h>

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

#define SCOPE "rsc/scope100"
#define EDGES "rsc/edge100.csv"
#define PRODS "rsc/products100.csv"
#define CLIENTS "rsc/clients100.csv"

#define HALFEDGE  8192
#define FULLEDGE 16384

#define HALFVRTX  8192
#define FULLVRTX 16384

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

#define EDGET(x) \
	((edge_t*)x)

int edgecompr(const void *lft, const void *rig) {
	if (EDGET(lft)->origin < EDGET(rig)->origin) return -1;
	if (EDGET(lft)->origin > EDGET(rig)->origin) return  1;
	return 0;
}

int edgecount(int h) {
	int mx = h*HALFEDGE;
	int x = 1;
	int origin = edges[0].origin;
	for(int i=0;i<mx;i++) {
		if (edges[i].origin != origin) {
			x++; origin = edges[i].origin;
		}
	}
	fprintf(stderr, "%d\n", x);
	return x;
}

vrtx_t *products = NULL;
vrtx_t *clients = NULL;

int writeEdges(nowdb_path_t path, int halves, int hprods, int hclients) {
	nowdb_err_t err;
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

	err = nowdb_time_fromString("2018-08-28T00:00:00",
	                         NOWDB_TIME_FORMAT, &base);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot get time base\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
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

int writeVrtx(nowdb_path_t path, int type, int halves) {
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

int testBuffer(nowdb_scope_t *scope) {
	int rc = 0;
	int c = 0;
	nowdb_err_t     err;
	nowdb_index_t  *idx;
	ts_algo_list_t  files;
	nowdb_context_t *ctx;
	nowdb_reader_t *reader;
	nowdb_edge_t   *last, *cur;
	struct timespec t1, t2;

	timestamp(&t1);

	// get context
	err = nowdb_scope_getContext(scope, "sales", &ctx);
	if (err != NOWDB_OK) {
		fprintf(stderr, "context not found\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}

	// get index
	err = nowdb_scope_getIndexByName(scope, "cidx_ctx_oe", &idx);
	if (err != NOWDB_OK) {
		fprintf(stderr, "index not found\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}

	// get pending files
	ts_algo_list_init(&files);
	err = nowdb_store_getAllWaiting(&ctx->store, &files);
	if (err != NOWDB_OK) {
		fprintf(stderr, "get waiting failed\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}

	// count files
	fprintf(stderr, "we have %u files\n", files.len);
	
	// create buffer
	err = nowdb_reader_bufidx(&reader, &files, idx, NULL,
	                           NOWDB_ORD_ASC, NULL, NULL);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create buffer\n");
		nowdb_store_destroyFiles(&ctx->store, &files);
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}

	timestamp(&t2);
	fprintf(stderr, "running time: %ldus\n", minus(&t2, &t1)/1000);

	// count half
	fprintf(stderr, "bufreader has: %u\n",
	        reader->size/reader->recsize);

	if (reader->size/reader->recsize != HALFEDGE) {
		fprintf(stderr, "wrong size in bufreader: %u\n", reader->size);
		rc = -1; goto cleanup;
	}

	last = NULL;
	for(int i=0; i<reader->size; i+=64) {
		if (last == NULL) {
			last = (nowdb_edge_t*)(reader->buf+i);
			cur = (nowdb_edge_t*)(reader->buf+i);
		} else {
			cur = (nowdb_edge_t*)(reader->buf+i);
			if (cur->origin < last->origin) {
				fprintf(stderr, "buffer is not ordered\n");
				rc = -1; break;
			}
		}
		// fprintf(stderr, "%lu\n", cur->origin);
	}
	if (rc != 0) goto cleanup;

	c=0;
	for(;;) {
		err = nowdb_reader_move(reader);
		if (err != NOWDB_OK) break;

		char *src = nowdb_reader_page(reader);
		if (src == NULL) {
			fprintf(stderr, "page is NULL\n");
			rc = -1; goto cleanup;
		}
		for(int i=0; i<NOWDB_IDX_PAGE; i+=reader->recsize) {
			if (memcmp(src+i, nowdb_nullrec,
			          reader->recsize) == 0) break;
			c++;
		}
	}
	if (err != NOWDB_OK) {
		if (err->errcode == nowdb_err_eof) {
			nowdb_err_release(err); err = NOWDB_OK;
		} else {
			nowdb_err_print(err);
			nowdb_err_release(err);
			rc = -1; goto cleanup;
		}
	}
	if (c != reader->size/reader->recsize) {
		fprintf(stderr, "counted: %d, expected: %u\n",
		                              c, reader->size);
		rc = -1; goto cleanup;
	}

cleanup:
	nowdb_reader_destroy(reader); free(reader);
	nowdb_store_destroyFiles(&ctx->store, &files);
	return rc;
}

int64_t countResult(nowdb_scope_t *scope,
                    char          *stmt) {
	struct timespec t1, t2;
	nowdb_err_t err = NOWDB_OK;
	nowdb_cursor_t *cur;
	int64_t res=0;
	char *buf;
	uint32_t osz = 0;
	uint32_t cnt = 0;
	char more = 1;

	timestamp(&t1);
	cur = openCursor(scope, stmt);
	if (cur == NULL) {
		fprintf(stderr, "cannot open cursor\n");
		return -1;
	}

	buf = malloc(8192);
	if (buf == NULL) {
		fprintf(stderr, "out-of-mem\n");
		closeCursor(cur);
		return -1;
	}

	while(more) {
		err = nowdb_cursor_fetch(cur, buf, 8192, &osz, &cnt);
		if (err != NOWDB_OK) {
			if (err->errcode == nowdb_err_eof) {
				nowdb_err_release(err); err = NOWDB_OK;
				if (osz == 0) break;
			}
			more = 0;
		}
		// fprintf(stderr, "%u\n", cnt);
		res += (int64_t)cnt;
		// nowdb_row_write(buf, 8192, stderr);
	}
	free(buf);
	closeCursor(cur);
	timestamp(&t2);

	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot fetch\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	fprintf(stderr, "running time: %ldus\n", minus(&t2, &t1)/1000);
	return res;
}

int checkResult(int       h,
                int  origin,
                int  destin,
                int64_t  tp,
                int64_t res) 
{
	int mx = h*HALFEDGE;
	int64_t total=0;

	for(int i=0; i<mx; i++) {
		if ((origin == 0 || edges[i].origin == origin) &&
		    (destin == 0 || edges[i].destin == destin) &&
		    (tp     == 0 || edges[i].timestamp == tp)) {
			total++;
		}
	}
	if (total != res) {
		fprintf(stderr, "expected: %ld for %d/%d/%ld\n",
		        total, origin, destin, tp);
		return -1;
	}
	return 0;
}

char *getRandom(int       halves,
                int      *origin,
                int      *destin,
                nowdb_time_t *tp,
                char        *frm) 
{
	char *sql;
	int x = halves*HALFEDGE;
	int i = rand()%x;

	*origin = edges[i].origin;
	*destin = edges[i].destin;
	*tp = edges[i].timestamp;

	sql = malloc(strlen(frm) + 24 + 1);
	if (sql == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return NULL;
	}
	sprintf(sql, frm, *origin, *destin, *tp);
	return sql;
}

#define EXECSTMT(stmt) \
	if (execStmt(scope, stmt) != 0) { \
		fprintf(stderr, "cannot execute statement %s\n", stmt); \
		rc = EXIT_FAILURE; goto cleanup; \
	}

#define COUNTRESULT(stmt) \
	res = countResult(scope, stmt); \
	if (res < 0) { \
		fprintf(stderr, "cannot count result %s\n", stmt); \
		rc = EXIT_FAILURE; goto cleanup; \
	} \
	if (sql != NULL) {\
		free(sql); sql = NULL; \
	} \
	fprintf(stderr, "result: %ld\n", res);

#define CHECKRESULT(h,o,d,t) \
	if (checkResult(h, o, d, t, res) != 0) { \
		rc = EXIT_FAILURE; goto cleanup; \
	}

#define GETRANDOM(h, o, d, tp, frm, sql) \
	*sql = getRandom(h,o,d,tp, frm); \
	if (*sql == NULL) { \
		fprintf(stderr, "no sql statement\n"); \
		rc = EXIT_FAILURE; goto cleanup; \
	} \
	// fprintf(stderr, "%s\n", *sql);

#define COUNTDISTINCT(h) \
	if (edgecount(h) != res) {\
		fprintf(stderr, "result differs\n"); \
		rc = EXIT_FAILURE; goto cleanup; \
	}

#define SQLIDX "\
select edge, origin, destin, timestamp from sales \
 where edge = 'buys' \
   and origin = %d \
   and destin = %d \
   and timestamp = %ld"

#define SQLFULL "\
select edge, origin, destin, timestamp from sales \
 where origin = %d \
   and destin = %d \
   and timestamp = %ld"

#define SQLORD "\
select edge, origin, destin, timestamp from sales \
 order by origin, edge"

#define SQLGRP "\
select origin, edge from sales \
 group by origin, edge"
	
int main() {
	int64_t res=0;
	int rc = EXIT_SUCCESS;
	nowdb_scope_t *scope = NULL;
	int o, d;
	nowdb_time_t tp;
	char *sql=NULL;

	srand(time(NULL) ^ (uint64_t)&printf);

	if (!nowdb_init()) {
		fprintf(stderr, "cannot init library\n");
		return EXIT_FAILURE;
	}
	if (initScopes() != 0) {
		fprintf(stderr, "cannot init scopes\n");
		return EXIT_FAILURE;
	}
	if (doesExistScope(SCOPE)) {
		if (dropScope(SCOPE) != 0) {
			fprintf(stderr, "cannot drop scope\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	scope = createScope(SCOPE);
	if (scope == NULL) {
		fprintf(stderr, "cannot create scope\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (openScope(scope) != 0) {
		fprintf(stderr, "cannot open scope\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

	EXECSTMT("create tiny index vidx_rovi on vertex (role, vid)");
	EXECSTMT("create index vidx_ropo on vertex (role, property)");

	EXECSTMT("create tiny table sales set stress=constant");

	EXECSTMT("create index cidx_ctx_de on sales (destin, edge)");
	EXECSTMT("create index cidx_ctx_oe on sales (origin, edge)");

	EXECSTMT("create type product (\
	            prod_key uint primary key, \
	            prod_desc text)");

	EXECSTMT("create type client (\
	            client_id uint primary key, \
	            client_name text)");

	EXECSTMT("create edge buys (\
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

	/*
	EXECSTMT("load 'rsc/products100.csv' into vertex \
	           use header as product");

	EXECSTMT("load 'rsc/clients100.csv' into vertex \
	           use header as client");
	*/

	EXECSTMT("load 'rsc/edge100.csv' into sales as edge");

	if (waitscope(scope, "sales") != 0) {
		fprintf(stderr, "cannot wait for scope\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

	// test fullscan
	COUNTRESULT("select * from sales");
	CHECKRESULT(5, 0, 0, 0);

	COUNTRESULT("select edge, origin from sales");
	// COUNTRESULT("select * from sales");
	CHECKRESULT(5, 0, 0, 0);

	// test fullscan
	fprintf(stderr, "FULLSCAN\n");
	for(int i=0; i<10; i++) {
		GETRANDOM(5, &o, &d, &tp, SQLFULL, &sql);
		COUNTRESULT(sql);
		CHECKRESULT(5, o, d, tp);
	}

	// test index
	fprintf(stderr, "INDEX SEARCH\n");
	for(int i=0; i<10; i++) {
		GETRANDOM(5, &o, &d, &tp, SQLIDX, &sql);
		COUNTRESULT(sql);
		CHECKRESULT(5, o, d, tp);
	}

	// test bufreader
	fprintf(stderr, "BUFFER\n");
	if (testBuffer(scope) != 0) {
		fprintf(stderr, "testBuffer failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

	// test order
	fprintf(stderr, "ORDER\n");
	for(int i=0; i<10; i++) {    // RANGE SCAN
		COUNTRESULT(SQLORD); res += HALFEDGE;
		CHECKRESULT(5, 0, 0, 0);
	}

	// KRANGE
	fprintf(stderr, "KRANGE\n");
	for(int i=0; i<10; i++) {    
		COUNTRESULT(SQLGRP);
		// COUNTDISTINCT(5);
	}

cleanup:
	if (sql != NULL) free(sql);
	if (scope != NULL) {
		closeScope(scope);
		nowdb_scope_destroy(scope); free(scope);
	}
	if (clients != NULL) free(clients);
	if (products != NULL) free(products);
	if (edges != NULL) free(edges);

	closeScopes();
	nowdb_close();

	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}
