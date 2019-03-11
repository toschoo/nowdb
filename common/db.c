/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Standard database
 * ========================================================================
 */
#include <common/db.h>

#define SCOPE "rsc/db10"
#define EDGES "rsc/edgedb10.csv"
#define PRODS "rsc/productsdb10.csv"
#define CLIENTS "rsc/clientsdb10.csv"

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
	float price;
	float quantity;
} edge_t;

static edge_t *edges = NULL;

#define EDGET(x) \
	((edge_t*)x)

static int edgecompr(const void *lft, const void *rig) {
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

static vrtx_t *products = NULL;
static vrtx_t *clients = NULL;

static int writeEdges(nowdb_path_t path, int halves,
                           int hprods, int hclients) {
	int rc;
	nowdb_time_t base;
	nowdb_time_t nsecs;
	FILE *f;
	int x = halves * HALFEDGE;
	int h, m, s, ns;
	double price, quant;
	int c, p;
	int mxclients = hclients * HALFVRTX;
	int mxprods = hprods * HALFVRTX;
	char *loadstr = 
	"%d;%d;2018-08-28T%02d:%02d:%02d.%03d;%.2f;%.2f\n";

	fprintf(stderr, "writing %d edges\n", x);

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

	rc = nowdb_time_fromString("2018-08-28T00:00:00",
	                       NOWDB_TIME_FORMAT, &base);
	if (rc != 0) {
		fprintf(stderr, "cannot get time base: %d\n", rc);
		fclose(f);
		return -1;
	}
	fprintf(f, "origin;destin;stamp;price;quantity\n");
	for(int i=0; i<x; i++) {

		do c = rand()%mxclients; while(c == 0);
		do p = rand()%mxprods; while(p == 0);

		h = rand()%24;
		m = rand()%60;
		s = rand()%60;
		ns = rand()%1000;

		do price = (double)(rand()%50) / 7; while(price == 0);
		do quant = (double)(rand()%27) / 2; while(quant == 0);

		price *= quant;

		fprintf(f, loadstr,  
		clients[c].id, products[p].id, h, m, s, ns, price, quant);

		edges[i].origin = clients[c].id;
		edges[i].destin = products[p].id;
		edges[i].price  = price;
		edges[i].quantity = quant;

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

static void randomString(char *str, int max) {
	int x;

	do x = rand()%max; while(x < 10);
	for(int i=0; i<x; i++) {
		str[i] = rand()%27;
		str[i]+=64;
	}
	str[x] = 0;
}

static inline int prepareProducts(int halves) {
	int x = halves * HALFVRTX;
	int p = 0;

	products = calloc(x, sizeof(vrtx_t));
	if (products == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return -1;
	}
	for(int i=0; i<x; i++) {
		do p = rand()%(10000000); while(p==0);
		products[i].id = p;
		randomString(products[i].str, 60);
	}
	return 0;
}

static inline int prepareClients(int halves) {
	int x = halves * HALFVRTX;
	int c = 0;

	clients = calloc(x, sizeof(vrtx_t));
	if (clients == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return -1;
	}
	for(int i=0; i<x; i++) {
		do c = rand()%(10000000); while(c==0);
		c += 9000000;
		clients[i].id = c;
		randomString(clients[i].str, 60);
	}
	return 0;
}

static int writeProducts(FILE *f, int halves) {
	int x = halves * HALFVRTX;

	fprintf(f, "prod_key;prod_desc\n");
	for(int i=0; i<x; i++) {
		fprintf(f, "%d;%s\n", products[i].id, products[i].str);
	}
	return 0;
}

static int writeClients(FILE *f, int halves) {
	int x = halves * HALFVRTX;

	fprintf(f, "client_id;client_name\n");
	for(int i=0; i<x; i++) {
		fprintf(f, "%d;%s\n", clients[i].id, clients[i].str);
	}
	return 0;
}

static int writeVrtx(nowdb_path_t path, int type, int halves) {
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

#define EXECSTMT(stmt) \
	if (execStmt(scope, stmt) != 0) { \
		fprintf(stderr, "cannot execute statement %s\n", stmt); \
		rc = -1; goto cleanup; \
	}

int createDB(int hedges, int hprods, int hclients) {
	int rc = 0;
	nowdb_scope_t *scope;

	if (doesExistScope(SCOPE)) {
		return 0;
	}
	scope = createScope(SCOPE);
	if (scope == NULL) {
		fprintf(stderr, "cannot create scope\n");
		return -1;
	}
	if (openScope(scope) != 0) {
		fprintf(stderr, "cannot open scope\n");
		nowdb_scope_destroy(scope); free(scope);
		return -1;
	}

	EXECSTMT("create type product (\
	            prod_key uint primary key, \
	            prod_desc text)");

	EXECSTMT("create type client (\
	            client_id uint primary key, \
	            client_name text)");

	EXECSTMT("create stamped edge buys (\
	            origin      client, \
	            destination product, \
	            price       float, \
	            quantity    float)");

	if (writeVrtx(PRODS, PRODUCT, hprods) != 0) {
		fprintf(stderr, "cannot write products\n");
		rc = -1; goto cleanup;
	}
	if (writeVrtx(CLIENTS, CLIENT, hclients) != 0) {
		fprintf(stderr, "cannot write clients\n");
		rc = -1; goto cleanup;
	}
	if (writeEdges(EDGES, hedges, hprods, hclients) != 0) {
		fprintf(stderr, "cannot write edges\n");
		rc = -1; goto cleanup;
	}

	EXECSTMT("load 'rsc/productsdb10.csv' into product use header \
	          set errors='rsc/productsdb10.err'");

	EXECSTMT("load 'rsc/clientsdb10.csv' into client use header \
	          set errors='rsc/clientsdb10.err'");

	fprintf(stderr, "loading edges\n");
	EXECSTMT("load 'rsc/edgedb10.csv' into buys use header \
	          set errors='rsc/edgedb10.err'");

	if (waitscope(scope, "buys") != 0) {
		fprintf(stderr, "cannot wait for scope\n");
		rc = -1; goto cleanup;
	}
cleanup:
	nowdb_scope_close(scope);
	nowdb_scope_destroy(scope);
	free(scope);
	return rc;
}

int dropDB() {
	if (dropScope(SCOPE) != 0) {
		fprintf(stderr, "cannot drop scope\n");
		return -1;
	}
	return 0;
}

nowdb_scope_t *openDB() {
	nowdb_scope_t *scope;
	scope = mkScope(SCOPE);
	if (scope == NULL) return NULL;
	if (openScope(scope) != 0) {
		nowdb_scope_destroy(scope);
		free(scope); return NULL;
	}
	return scope;
}

void closeDB(nowdb_scope_t *scope) {
	closeScope(scope);
	nowdb_scope_destroy(scope);
	free(scope);
}

int64_t countResults(nowdb_scope_t *scope,
                     const char    *stmt);

int checkEdgeResult(nowdb_scope_t *scope,
                    int origin, int destin,
                    nowdb_time_t tstp,
                    int64_t result);

char *getRandomSQL(int          halves,
                   int          *origin,
                   int          *destin,
                   nowdb_time_t *tp,
                   char         *frm);

