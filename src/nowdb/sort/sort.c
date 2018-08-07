/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Comparing, searching and sorting
 * ========================================================================
 */

#include <nowdb/sort/sort.h>
#include <nowdb/index/index.h>

static char *OBJECT = "sort";

/* ------------------------------------------------------------------------
 * Generic sorting
 * ------------------------------------------------------------------------
 */
void nowdb_mem_sort(char *buf, uint32_t size, uint32_t recsize,
                           nowdb_comprsc_t compare, void *args) {
	qsort_r(buf, (size_t)size, (size_t)recsize, compare, args);
}

/* ------------------------------------------------------------------------
 * Wrap beet compare
 * ------------------------------------------------------------------------
 */
#define WRAP(x) \
	((nowdb_sort_wrapper_t*)x)

nowdb_cmp_t nowdb_sort_wrapBeet(const void *left,
                                const void *right,
                                void     *wrapper) {
	return (nowdb_cmp_t)WRAP(wrapper)->cmp(left, right,
	                    WRAP(wrapper)->rsc);
}

#define KEYS(x) \
	((nowdb_index_keys_t*)x)

#define KEYCMP(left,right,o) \
	if ((*(nowdb_key_t*)((char*)left+o)) < \
	    (*(nowdb_key_t*)((char*)right+o))) return NOWDB_SORT_LESS; \
	if ((*(nowdb_key_t*)((char*)left+o)) > \
	    (*(nowdb_key_t*)((char*)right+o))) return NOWDB_SORT_GREATER;

/* this should be for the specific type */
#define WEIGHTCMP(left,right,o) \
	if ((*(nowdb_value_t*)((char*)left+o)) < \
	    (*(nowdb_value_t*)((char*)right+o))) return NOWDB_SORT_LESS; \
	if ((*(nowdb_value_t*)((char*)left+o)) > \
	    (*(nowdb_value_t*)((char*)right+o))) return NOWDB_SORT_GREATER;

/* this should be with rounding */
#define TMSTMPCMP(left,right,o) \
	if ((*(nowdb_time_t*)((char*)left+o)) < \
	    (*(nowdb_time_t*)((char*)right+o))) \
		return NOWDB_SORT_LESS; \
	if ((*(nowdb_time_t*)((char*)left+o)) > \
	    (*(nowdb_time_t*)((char*)right+o))) \
		return NOWDB_SORT_GREATER;

#define ROLECMP(left,right,o) \
	if ((*(nowdb_roleid_t*)((char*)left+o)) < \
	    (*(nowdb_roleid_t*)((char*)right+o))) \
		return NOWDB_SORT_LESS; \
	if ((*(nowdb_roleid_t*)((char*)left+o)) > \
	    (*(nowdb_roleid_t*)((char*)right+o))) \
		return NOWDB_SORT_GREATER;

/* ------------------------------------------------------------------------
 * Generic edge compare using index keys (asc)
 * ------------------------------------------------------------------------
 */
nowdb_cmp_t nowdb_sort_edge_keys_compare(const void *left,
                                         const void *right,
                                         void       *keys) {
	for(int i=0; i<KEYS(keys)->sz; i++) {
		switch(KEYS(keys)->off[i]) {
		case NOWDB_OFF_EDGE:
		case NOWDB_OFF_ORIGIN:
		case NOWDB_OFF_DESTIN:
		case NOWDB_OFF_LABEL:
			KEYCMP(left, right, KEYS(keys)->off[i]);

		case NOWDB_OFF_TMSTMP:
			TMSTMPCMP(left, right, KEYS(keys)->off[i]);

		case NOWDB_OFF_WEIGHT:
		case NOWDB_OFF_WEIGHT2:
			WEIGHTCMP(left, right, KEYS(keys)->off[i]);
		}
	}
	return NOWDB_SORT_EQUAL;
}

/* ------------------------------------------------------------------------
 * Generic edge compare using index keys (desc)
 * ------------------------------------------------------------------------
 */
nowdb_cmp_t nowdb_sort_edge_keys_compareD(const void *left,
                                          const void *right,
                                          void       *keys) {
	return nowdb_sort_edge_keys_compare(right, left, keys);
}

/* ------------------------------------------------------------------------
 * Generic vertex compare using index keys (desc)
 * ------------------------------------------------------------------------
 */
nowdb_cmp_t nowdb_sort_vertex_keys_compare(const void *left,
                                           const void *right,
                                           void       *keys) {
	for(int i=0; i<KEYS(keys)->sz; i++) {
		switch(KEYS(keys)->off[i]) {
		case NOWDB_OFF_VERTEX:
		case NOWDB_OFF_PROP:
			KEYCMP(left, right, KEYS(keys)->off[i]);

		case NOWDB_OFF_VALUE:
			WEIGHTCMP(left, right, KEYS(keys)->off[i]);

		case NOWDB_OFF_ROLE:
			ROLECMP(left, right, KEYS(keys)->off[i]);
		}
	}
	return NOWDB_SORT_EQUAL;
}

/* ------------------------------------------------------------------------
 * Generic vertex compare using index keys (desc)
 * ------------------------------------------------------------------------
 */
nowdb_cmp_t nowdb_sort_vertex_keys_compareD(const void *left,
                                            const void *right,
                                            void       *keys) {
	return nowdb_sort_vertex_keys_compare(right, left, keys);
}

/* ------------------------------------------------------------------------
 * Sorting edges
 * ------------------------------------------------------------------------
 */
void nowdb_mem_sort_edge(nowdb_edge_t *buf,
                         uint32_t      num,
                         nowdb_ord_t   ord) {
	if (ord == NOWDB_ORD_ASC) {
		nowdb_mem_sort((char*)buf, num*64, 64,
		       &nowdb_sort_edge_compare, NULL);
	} else if (ord == NOWDB_ORD_DESC) {
		nowdb_mem_sort((char*)buf, num*64, 64,
		      &nowdb_sort_edge_compareD, NULL);
	}
}

/* ------------------------------------------------------------------------
 * Sorting vertices
 * ------------------------------------------------------------------------
 */
void nowdb_mem_sort_vertex(nowdb_vertex_t *buf,
                           uint32_t        num,
                           nowdb_ord_t     ord) {
	if (ord == NOWDB_ORD_ASC) {
		nowdb_mem_sort((char*)buf, num*32, 32,
		     &nowdb_sort_vertex_compare, NULL);
	} else if (ord == NOWDB_ORD_DESC) {
		nowdb_mem_sort((char*)buf, num*32, 32,
		    &nowdb_sort_vertex_compareD, NULL);
	}
}

/* ------------------------------------------------------------------------
 * Standard (asc) compare for edges
 * ------------------------------------------------------------------------
 */
nowdb_cmp_t nowdb_sort_edge_compare(const void *left,
                                    const void *right,
                                    void      *ignore)
{
	if (((nowdb_edge_t*)left)->origin <  
	    ((nowdb_edge_t*)right)->origin) return NOWDB_SORT_LESS;
	if (((nowdb_edge_t*)left)->origin >
	    ((nowdb_edge_t*)right)->origin) return NOWDB_SORT_GREATER;

	if (((nowdb_edge_t*)left)->edge   <
	    ((nowdb_edge_t*)right)->edge) return NOWDB_SORT_LESS;
	if (((nowdb_edge_t*)left)->edge   >
	    ((nowdb_edge_t*)right)->edge) return NOWDB_SORT_GREATER;

	if (((nowdb_edge_t*)left)->destin <
	    ((nowdb_edge_t*)right)->destin) return NOWDB_SORT_LESS;
	if (((nowdb_edge_t*)left)->destin >
	    ((nowdb_edge_t*)right)->destin) return NOWDB_SORT_GREATER;

	if (((nowdb_edge_t*)left)->label  <
	    ((nowdb_edge_t*)right)->label) return NOWDB_SORT_LESS;
	if (((nowdb_edge_t*)left)->label >
	    ((nowdb_edge_t*)right)->label) return NOWDB_SORT_GREATER;

	if (((nowdb_edge_t*)left)->timestamp <
	    ((nowdb_edge_t*)right)->timestamp) return NOWDB_SORT_LESS;
	if (((nowdb_edge_t*)left)->timestamp >
	    ((nowdb_edge_t*)right)->timestamp) return NOWDB_SORT_GREATER;

	return NOWDB_SORT_EQUAL;
}

/* ------------------------------------------------------------------------
 * Standard (desc) compare for edges
 * ------------------------------------------------------------------------
 */
nowdb_cmp_t nowdb_sort_edge_compareD(const void *left,
                                     const void *right,
                                     void      *ignore)
{
	return nowdb_sort_edge_compare(right, left, ignore);
}

/* ------------------------------------------------------------------------
 * Standard compare (asc) for vertices
 * ------------------------------------------------------------------------
 */
nowdb_cmp_t nowdb_sort_vertex_compare(const void *left,
                                      const void *right,
                                      void      *ignore)
{
	if (((nowdb_vertex_t*)left)->role <
	    ((nowdb_vertex_t*)right)->role) return NOWDB_SORT_LESS;
	if (((nowdb_vertex_t*)left)->role >
	    ((nowdb_vertex_t*)right)->role) return NOWDB_SORT_GREATER;

	if (((nowdb_vertex_t*)left)->vertex <
	    ((nowdb_vertex_t*)right)->vertex) return NOWDB_SORT_LESS;
	if (((nowdb_vertex_t*)left)->vertex >
	    ((nowdb_vertex_t*)right)->vertex) return NOWDB_SORT_GREATER;

	if (((nowdb_vertex_t*)left)->property <
	    ((nowdb_vertex_t*)right)->property) return NOWDB_SORT_LESS;
	if (((nowdb_vertex_t*)left)->property >
	    ((nowdb_vertex_t*)right)->property) return NOWDB_SORT_GREATER;

	return NOWDB_SORT_EQUAL;
}

/* ------------------------------------------------------------------------
 * Standard compare (desc) for vertices
 * ------------------------------------------------------------------------
 */
nowdb_cmp_t nowdb_sort_vertex_compareD(const void *left,
                                       const void *right,
                                       void      *ignore)
{
	return nowdb_sort_vertex_compare(right, left, ignore);
}

/* ------------------------------------------------------------------------
 * Helper: sorting blocks
 * ------------------------------------------------------------------------
 */
static void sortBlocks(ts_algo_list_t  *blocks,
                       uint32_t        recsize,
                       nowdb_comprsc_t compare) {
	ts_algo_list_node_t *runner;
	nowdb_block_t *b;

	for (runner = blocks->head; runner!=NULL; runner=runner->nxt) {
		b = runner->cont;
		nowdb_mem_sort(b->block, b->sz/recsize,
		               recsize, compare, NULL);
	}
}

/* ------------------------------------------------------------------------
 * Helper: merging blocks
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t mergeN(ts_algo_list_t     *src,
                                 ts_algo_list_t     *trg,
                                 int                *off,
                                 nowdb_blist_t    *flist,
                                 int n, uint32_t recsize,
                                 nowdb_comprsc_t compare) {
	nowdb_err_t err = NOWDB_OK;
	ts_algo_list_node_t *runner;
	int idx = 0;
	nowdb_block_t *cb, *sb, *tb=NULL;
	char *mn;
	int i, j;

	/*
	fprintf(stderr, "merging %d %d-blocks from %p to %p\n",
	                src->len/n, n, src, trg);
	*/
	memset(off, 0, n*sizeof(int));
	while(src->len > 0) {
		if (tb == NULL || idx >= flist->sz) {
			// fprintf(stderr, "getting one (%d)\n",n);
			err = nowdb_blist_giva(flist, trg);
			if (err != NOWDB_OK) break;
			tb = trg->last->cont;
			idx = 0;
		}
		i = -1; j=0;
		cb = NULL;
		for(runner=src->head; runner!=NULL; runner=runner->nxt) {
			i++; if (i >= n) break;
			sb = runner->cont;
			if (off[i] >= sb->sz) {
				// fprintf(stderr, "%d exhausted: %d/%d\n", i, off[i], sb->sz);
				continue;
			}
			if (cb == NULL || compare(sb->block+off[i],
			              mn, NULL) == NOWDB_SORT_LESS) {
				cb = sb;
				mn = cb->block+off[i];
				j=i;
			}
		}
		if (cb != NULL) {
			// fprintf(stderr, "\tcopying %d.%d to %d\n", j, off[j], idx);
			memcpy(tb->block+idx, cb->block+off[j], recsize);
			idx+=recsize; off[j]+=recsize;
			tb->sz+=recsize; continue;
		}
		// fprintf(stderr, "\tfreeing %d blocks\n", n);
		for(i=0; i<n; i++) {
			// fprintf(stderr, "off %d: %d\n", i, off[i]);
			if (src->head == NULL) break;
			err = nowdb_blist_take(flist, src);
			if (err != NOWDB_OK) break;
		}
		if (err != NOWDB_OK) break;
		idx = flist->sz;
		memset(off, 0, n*sizeof(int));
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Helper: how many rounds
 * ------------------------------------------------------------------------
 */
static inline int crounds(int n) {
	int l=1;
	for(int i=n; i>0; i>>=1) l++;
	return l;
}

/* ------------------------------------------------------------------------
 * Helper: merging blocks
 * ------------------------------------------------------------------------
 */
static nowdb_err_t mergeBlocks(ts_algo_list_t  *blocks,
                               nowdb_blist_t    *flist,
                               uint32_t        recsize,
                               nowdb_comprsc_t compare)  {
	nowdb_err_t err;
	ts_algo_list_t tmp;
	ts_algo_list_t *src=NULL, *trg=NULL;
	int *hlp;
	int k = 2;
	int rounds = crounds((blocks->len)/recsize)+1;

	ts_algo_list_init(&tmp);

	hlp = calloc(2 << rounds, sizeof(int));
	if (hlp == NULL) {
		return nowdb_err_get(nowdb_err_no_mem,
		                        FALSE, OBJECT,
		           "allocating helper buffer");
	}
	for(int i=0; i<rounds; i++) {
		if (i%2 == 0) {
			src = blocks; trg = &tmp;
		} else {
			src = &tmp; trg = blocks;
		}
		err = mergeN(src, trg, hlp, flist, k, recsize, compare);
		if (err != NOWDB_OK) break;
		k <<= 1;
	}
	free(hlp);
	nowdb_blist_destroyBlockList(src, flist);
	if (trg == &tmp) {
		blocks->head = tmp.head;
		blocks->last = tmp.last;
		blocks->len  = tmp.len;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Sorting list of blocks
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_block_sort(ts_algo_list_t  *blocks,
                             nowdb_blist_t    *flist,
                             uint32_t        recsize,
                             nowdb_comprsc_t compare) 
{
	sortBlocks(blocks, recsize, compare);
	return mergeBlocks(blocks, flist, recsize, compare);
}


