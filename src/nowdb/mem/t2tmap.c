/* ========================================================================
 * (c) Tobias Schoofs, 2018 -- 2019
 * ========================================================================
 * Text to text Map
 * ========================================================================
 */
#include <nowdb/mem/t2tmap.h>

typedef struct {
	char *key;
	char *val;
} node_t;

#define NODE(x) \
	((node_t*)x)

static ts_algo_cmp_t t2tcompare(void *rsc, void *one, void *two) {
	int x = strcmp(NODE(one)->key, NODE(two)->key);
	if (x < 0) return ts_algo_cmp_less;
	if (x > 0) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

static void t2tdestroy(void *rsc, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	if (NODE(*n)->key != NULL) free(NODE(*n)->key);
	if (NODE(*n)->val != NULL) free(NODE(*n)->val);
	free(*n); *n = NULL;
}

static ts_algo_rc_t noupdate(void *rsc, void *o, void *n) {
	return TS_ALGO_OK;
}

/* ------------------------------------------------------------------------
 * New Map
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_t2tmap_new(nowdb_t2tmap_t **map) {
	*map = ts_algo_tree_new(t2tcompare, NULL,
	          noupdate,t2tdestroy,t2tdestroy);
	if (*map == NULL) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE,
		                       "t2tmap", "tree.new");
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Init Map
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_t2tmap_init(nowdb_t2tmap_t *map) {
	if (ts_algo_tree_init(map, t2tcompare, NULL,
	            noupdate,t2tdestroy,t2tdestroy) != TS_ALGO_OK) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE,
		                       "t2tmap", "tree.init");
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Destroy Map
 * ------------------------------------------------------------------------
 */
void nowdb_t2tmap_destroy(nowdb_t2tmap_t *map) {
	ts_algo_tree_destroy(map);
}

/* ------------------------------------------------------------------------
 * Get data from Map
 * ------------------------------------------------------------------------
 */
char *nowdb_t2tmap_get(nowdb_t2tmap_t *map,
                       char           *key) {
	node_t *r, pattern;

	pattern.key = key;
	r = ts_algo_tree_find(map, &pattern);
	if (r == NULL) return NULL;
	return r->val;
}

/* ------------------------------------------------------------------------
 * Add data to Map
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_t2tmap_add(nowdb_t2tmap_t *map,
                             char           *key,
                             char           *val) {
	node_t *n;

	n = calloc(1,sizeof(node_t));
	if (n == NULL) return nowdb_err_get(nowdb_err_no_mem, FALSE,
	                            "t2tmap", "allocating map node");
	n->key = key;
	n->val = val;
	if (ts_algo_tree_insert(map, n) != TS_ALGO_OK) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE,
	                             "t2tmap", "tree.insert");
	}
	return NOWDB_OK;
}

