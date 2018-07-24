/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Test the tester
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <common/scopes.h>

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

vrtx_t *products = NULL;
vrtx_t *clients = NULL;

int writeEdges(nowdb_path_t path, int halfs, int hprods, int hclients) {
	FILE *f;
	int x = halfs * HALFEDGE;
	int h, m, s, ns;
	double w, w2;
	int c, p;
	int mxclients = hclients * HALFVRTX;
	int mxprods = hprods * HALFVRTX;
	char *loadstr = 
	"buys;%d;%d;0;2018-08-28T%02d:%02d:%02d.%03d;%.2f;%.2f\n";
	
	f = fopen(path, "w");
	if (f == NULL) {
		perror("cannot open vertex file");
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
	}
	fclose(f);
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

inline int prepareProducts(int halfs) {
	int x = halfs * HALFVRTX;
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

inline int prepareClients(int halfs) {
	int x = halfs * HALFVRTX;
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

int writeProducts(FILE *f, int halfs) {
	int x = halfs * HALFVRTX;

	fprintf(f, "prod_key;prod_desc\n");
	for(int i=0; i<x; i++) {
		fprintf(f, "%d;%s\n", products[i].id, products[i].str);
	}
	return 0;
}

int writeClients(FILE *f, int halfs) {
	int x = halfs * HALFVRTX;

	fprintf(f, "client_id;client_name\n");
	for(int i=0; i<x; i++) {
		fprintf(f, "%d;%s\n", clients[i].id, clients[i].str);
	}
	return 0;
}

int writeVrtx(nowdb_path_t path, int type, int halfs) {
	FILE *f;
	int rc = 0;

	rc = type == PRODUCT ? prepareProducts(halfs)
	                     : prepareClients(halfs);
	if (rc != 0) return -1;

	f = fopen(path, "w");
	if (f == NULL) {
		perror("cannot open vertex file");
		return -1;
	}
	rc = type == PRODUCT ? writeProducts(f, halfs)
	                     : writeClients(f, halfs);
	fclose(f);
	return rc;
}

#define EXECSTMT(stmt) \
	if (execStmt(scope, stmt) != 0) { \
		fprintf(stderr, "cannot execute statement %s\n", stmt); \
		rc = EXIT_FAILURE; goto cleanup; \
	}

int main() {
	int rc = EXIT_SUCCESS;
	nowdb_scope_t *scope = NULL;

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

	if (writeVrtx(PRODS, PRODUCT, 5) != 0) {
		fprintf(stderr, "cannot write products\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (writeVrtx(CLIENTS, CLIENT, 7) != 0) {
		fprintf(stderr, "cannot write clients\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (writeEdges(EDGES, 5, 5, 7) != 0) {
		fprintf(stderr, "cannot write clients\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

	EXECSTMT("load 'rsc/products100.csv' into vertex \
	           use header as product");

	EXECSTMT("load 'rsc/clients100.csv' into vertex \
	           use header as client");

	EXECSTMT("load 'rsc/edge100.csv' into sales as edge");

	if (waitscope(scope, "sales") != 0) {
		fprintf(stderr, "cannot wait for scope\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

cleanup:
	closeScopes();
	nowdb_close();
	if (scope != NULL) {
		closeScope(scope);
		nowdb_scope_destroy(scope); free(scope);
	}
	fprintf(stderr, "freeing clients\n");
	if (clients != NULL) free(clients);
	fprintf(stderr, "freeing products\n");
	if (products != NULL) free(products);

	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}
