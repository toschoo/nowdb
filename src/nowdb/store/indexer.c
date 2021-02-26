/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Indexer
 * ========================================================================
 */
#include <nowdb/store/indexer.h>
#include <nowdb/store/store.h>
#include <tsalgo/list.h>

static char *OBJECT = "xer";

/* ------------------------------------------------------------------------
 * Helper
 * ------------------------------------------------------------------------
 */
typedef struct {
	beet_compare_t compare;
	void     *rsc;
	uint32_t *off;
	uint32_t  keysz;
} xhelper_t;

/* ------------------------------------------------------------------------
 * What we store in the index
 * ------------------------------------------------------------------------
 */
typedef struct {
	char     *keys;
	uint8_t  *map;
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
		if (XNODE(*n)->map != NULL) {
			free(XNODE(*n)->map);	
			XNODE(*n)->map = NULL;	
		}
		free(*n); *n = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Update Callback
 * ------------------------------------------------------------------------
 */
ts_algo_rc_t xerupdate(void *x, void *o, void *n) {
	uint8_t k=1;
	int d = (*XHLPT(x)->off)/8;

	k <<= ((*XHLPT(x)->off)%8);
	XNODE(o)->map[d] |= k;

	xerdestroy(x, &n);
	return TS_ALGO_OK;
}

/* ------------------------------------------------------------------------
 * Initialise one indexer
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_indexer_init(nowdb_indexer_t *xer,
                               nowdb_index_t   *idx) {
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

	x->keysz = nowdb_index_keySize(x->rsc);
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
                                nowdb_store_t *store,
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

	n->map = malloc(store->setsize);
	if (n->map == NULL) {
		free(n->keys); free(n);
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                             "allocating index key");
	}
	memset(n->map, 0, store->setsize);
	nowdb_index_grabKeys(XHLPT(xer->tree)->rsc, rec, n->keys);
	xerupdate(xer->tree, n, NULL);
	if (ts_algo_tree_insert(xer->tree, n) != TS_ALGO_OK) {
		xerdestroy(NULL, (void**)&n);
		return nowdb_err_get(nowdb_err_no_mem,
		        FALSE, OBJECT, "tree.insert");
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Revoke residence for indexed records (vertex only)
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t revokeResidence(nowdb_store_t *store, char *buf) {
	nowdb_err_t err;

	if (store->lru == NULL) return NOWDB_OK;

	err = nowdb_lock_write(&store->lock);
	if (err != NOWDB_OK) return err;

	nowdb_plru12_revoke(store->lru,
	                  *(nowdb_roleid_t*)(buf+NOWDB_OFF_ROLE),
	                  *(nowdb_key_t*)(buf+NOWDB_OFF_VERTEX));

	err = nowdb_unlock_write(&store->lock);
	if (err != NOWDB_OK) return err;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Index a buffer using an array of indexers
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_indexer_index(nowdb_indexer_t *xers,
                                uint32_t         n,
                                nowdb_pageid_t   pge,
                                void            *store,
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
			err = doxer(xers+i, store, buf+isz*o);
			if (err != NOWDB_OK) return err;
		}
		err = revokeResidence(store, buf+isz*o);
		if (err != NOWDB_OK) return err;
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
