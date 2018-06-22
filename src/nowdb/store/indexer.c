/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Indexer
 * ========================================================================
 */
#include <nowdb/store/indexer.h>

static char *OBJECT = "xer";

/* ------------------------------------------------------------------------
 * Helper
 * ------------------------------------------------------------------------
 */
typedef struct {
	beet_compare_t compare;
	void *rsc;
	uint32_t *off;
} xhelper_t;

/* ------------------------------------------------------------------------
 * What we store in the index
 * ------------------------------------------------------------------------
 */
typedef struct {
	char      *keys;
	uint64_t map[2];
} xnode_t;

#define XHLP(x) ((xhelper_t*)x)
#define XNODE(x) ((xnode_t*)x)

/* ------------------------------------------------------------------------
 * Compare converter
 * Careful: the index compares keys and needs a key resource!
 * ------------------------------------------------------------------------
 */
ts_algo_cmp_t xercompare(void *x, void *left, void *right) {
	char cmp = XHLP(x)->compare(XNODE(left)->keys,
	                            XNODE(right)->keys,
	                            XHLP(x)->rsc);
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
	if (*XHLP(x)->off == 0) {
		XNODE(o)->map[0] = 1;
	} else if (*XHLP(x)->off == 64) {
		XNODE(o)->map[1] = 1;
	} else {
		k <<= *XHLP(x)->off;
		if (*XHLP(x)->off < 64) {
			XNODE(o)->map[0] |= k;
		} else {
			XNODE(o)->map[1] |= k;
		}
	}
	xerdestroy(x, &n);
	return TS_ALGO_OK;
}

/* ------------------------------------------------------------------------
 * Initialise one indexer
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_indexer_init(nowdb_indexer_t *xer,
                               nowdb_index_t   *idx) {
	xhelper_t   *x;
	xer->idx = idx;
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
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Destroy one indexer
 * ------------------------------------------------------------------------
 */
void nowdb_indexer_destroy(nowdb_indexer_t *xer) {
	if (xer == NULL) return;
	if (xer->tree != NULL) {
		ts_algo_tree_destroy(xer->tree);
		free(xer->tree); xer->tree = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Helper: insert one into one index
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t doxer(nowdb_indexer_t *xer,
                                nowdb_pageid_t   pge,
                                uint32_t         isz,
                                char            *rec) {
	// grab keys
	// count up bitmap
	// insert
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Index a buffer using an array of indexers
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_indexer_index(nowdb_indexer_t *xers,
                                uint32_t         n,
                                nowdb_pageid_t   pge,
                                uint32_t         isz,
                                uint32_t         bsz ,
                                char            *buf) {
	nowdb_err_t err;
	uint32_t o = 0;
	uint32_t m = bsz/isz;

	for(int i=0; i<n; i++) {
		XHLP(xers[i].tree->rsc)->off = &o;
	}
	for(; o<m; o++) {
		for(int i=0; i<n; i++) {
			err = doxer(xers+i, pge, isz, buf+isz*o);
			if (err != NOWDB_OK) return err;
		}
	}
	return NOWDB_OK;
}

