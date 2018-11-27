/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Indexer
 * ========================================================================
 */
#include <nowdb/store/indexer.h>
#include <tsalgo/list.h>

static char *OBJECT = "xer";

/* ------------------------------------------------------------------------
 * Helper
 * ------------------------------------------------------------------------
 */
typedef struct {
	beet_compare_t compare;
	void *rsc;
	uint32_t *off;
	uint32_t  keysz;
} xhelper_t;

/* ------------------------------------------------------------------------
 * What we store in the index
 * ------------------------------------------------------------------------
 */
typedef struct {
	char         *keys;
	uint64_t    map[2];
} xnode_t;

#define XHLP(x) ((xhelper_t*)x)
#define XNODE(x) ((xnode_t*)x)
#define XTREE(x) ((ts_algo_tree_t*)x)
#define XHLPT(x) (XHLP(XTREE(x)->rsc))

/* ------------------------------------------------------------------------
 * Compare converter
 * ------------------------------------------------------------------------
 */
ts_algo_cmp_t xercompare(void *x, void *left, void *right) {
	char cmp = XHLPT(x)->compare(XNODE(left)->keys,
	                             XNODE(right)->keys,
	                             XHLPT(x)->rsc);
	if (cmp == BEET_CMP_LESS) return ts_algo_cmp_less;
	if (cmp == BEET_CMP_GREATER) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * Destroy 
 * ------------------------------------------------------------------------
 */
void xerdestroy(void *x, void **n) {
	if (n != NULL && *n != NULL) {
		if (XNODE(*n)->keys != NULL) {
			free(XNODE(*n)->keys);	
			XNODE(*n)->keys = NULL;	
		}
		free(*n); *n = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Update Callback
 * ------------------------------------------------------------------------
 */
ts_algo_rc_t xerupdate(void *x, void *o, void *n) {
	uint64_t k=1;
	if (*XHLPT(x)->off == 0) {
		XNODE(o)->map[0] = 1;
	} else if (*XHLPT(x)->off == 64) {
		XNODE(o)->map[1] = 1;
	} else if (*XHLPT(x)->off < 64) {
		k <<= *XHLPT(x)->off;
		XNODE(o)->map[0] |= k;
	} else {
		k <<= (*XHLPT(x)->off-64);
		XNODE(o)->map[1] |= k;
	}
	xerdestroy(x, &n);
	return TS_ALGO_OK;
}

/* ------------------------------------------------------------------------
 * Initialise one indexer
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_indexer_init(nowdb_indexer_t *xer,
                               nowdb_index_t   *idx,
                               uint32_t         isz) {
	nowdb_err_t err;
	xhelper_t   *x;
	xer->idx = idx;

	err = nowdb_index_use(xer->idx);
	if (err != NOWDB_OK) return err;

	xer->tree = ts_algo_tree_new(&xercompare, NULL,
	                             &xerupdate,
	                             &xerdestroy,
	                             &xerdestroy);
	if (xer->tree == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                                  FALSE, OBJECT, "tree.new");
	x = malloc(sizeof(xhelper_t));
	if (x == NULL) {
		ts_algo_tree_destroy(xer->tree);
		free(xer->tree); xer->tree = NULL;
		return nowdb_err_get(nowdb_err_no_mem,
		   FALSE, OBJECT, "allocating helper");
	}

	x->compare = nowdb_index_getCompare(idx);
	x->rsc = nowdb_index_getResource(idx);
	xer->tree->rsc = x;

	x->keysz = isz==64?nowdb_index_keySizeEdge(x->rsc):
		           nowdb_index_keySizeVertex(x->rsc);
	if (x->keysz == 0) {
		free(x);
		ts_algo_tree_destroy(xer->tree);
		free(xer->tree); xer->tree = NULL;
		return nowdb_err_get(nowdb_err_invalid,
		     FALSE, OBJECT, "invalid keysize");
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Destroy one indexer
 * ------------------------------------------------------------------------
 */
void nowdb_indexer_destroy(nowdb_indexer_t *xer) {
	if (xer == NULL) return;
	if (xer->idx != NULL) {
		NOWDB_IGNORE(nowdb_index_enduse(xer->idx));
	}
	if (xer->tree != NULL) {
		if (xer->tree->rsc != NULL) free(xer->tree->rsc);
		ts_algo_tree_destroy(xer->tree);
		free(xer->tree); xer->tree = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Helper: insert one into one index
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t doxer(nowdb_indexer_t *xer,
                                uint32_t         isz,
                                char            *rec) {
	xnode_t *n;

	n = malloc(sizeof(xnode_t));
	if (n == NULL) return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
	                                                "allocating xnode");
	n->keys = malloc(XHLPT(xer->tree)->keysz);
	if (n->keys == NULL) {
		free(n);
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                             "allocating index key");
	}
	memset(n->map, 0, 16);

	if (isz == 64) { // this is bad!
		nowdb_index_grabEdgeKeys(XHLPT(xer->tree)->rsc,
		                                  rec, n->keys);
	} else {
		nowdb_index_grabVertexKeys(XHLPT(xer->tree)->rsc,
		                                    rec, n->keys);
	}
	xerupdate(xer->tree, n, NULL);
	if (ts_algo_tree_insert(xer->tree, n) != TS_ALGO_OK) {
		xerdestroy(NULL, (void**)&n);
		return nowdb_err_get(nowdb_err_no_mem,
		        FALSE, OBJECT, "tree.insert");
	}
	return NOWDB_OK;
}

static inline void revokeResidence(nowdb_plru12_t *lru, char *buf) {
	if (lru == NULL) return;
	nowdb_plru12_revoke(lru, *(nowdb_roleid_t*)(buf+NOWDB_OFF_ROLE),
	                         *(nowdb_key_t*)(buf+NOWDB_OFF_VERTEX));
}

/* ------------------------------------------------------------------------
 * Index a buffer using an array of indexers
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_indexer_index(nowdb_indexer_t *xers,
                                uint32_t         n,
                                nowdb_pageid_t   pge,
                                nowdb_plru12_t  *lru,
                                uint32_t         isz,
                                uint32_t         bsz ,
                                char            *buf) {
	nowdb_err_t err;
	uint32_t o = 0;
	uint32_t m = bsz/isz;
	ts_algo_list_t *recs;
	ts_algo_list_node_t *runner;
	xnode_t *node;

	/* initialise offset */
	for(int i=0; i<n; i++) {
		XHLP(xers[i].tree->rsc)->off = &o;
	}
	/* insert all records into trees */
	for(; o<m; o++) {
		for(int i=0; i<n; i++) {
			err = doxer(xers+i, isz, buf+isz*o);
			if (err != NOWDB_OK) return err;
		}
		revokeResidence(lru, buf+isz*o);
	}
	/* insert unique keys into indices */
	for(int i=0; i<n; i++) {
		recs = ts_algo_tree_toList(xers[i].tree);
		if (recs == NULL) return nowdb_err_get(nowdb_err_no_mem,
		                          FALSE, OBJECT, "tree.toList");
		for(runner=recs->head; runner!=NULL; runner=runner->nxt) {
			node = runner->cont;
			err = nowdb_index_insert(xers[i].idx, node->keys,
			                                 pge, node->map);
			if (err != NOWDB_OK) {
				ts_algo_list_destroy(recs); free(recs);
				return err;
			}
			ts_algo_tree_delete(xers[i].tree, runner->cont);
		}
		ts_algo_list_destroy(recs); free(recs);
	}
	return NOWDB_OK;
}
