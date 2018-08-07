/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Comparing, searching and sorting
 * ========================================================================
 *
 * ========================================================================
 */
#ifndef nowdb_sort_decl
#define nowdb_sort_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/mem/blist.h>

#include <beet/types.h>

typedef int nowdb_cmp_t;

#define NOWDB_SORT_LESS   -1
#define NOWDB_SORT_EQUAL   0
#define NOWDB_SORT_GREATER 1

/* ------------------------------------------------------------------------
 * ascending/descending
 * ------------------------------------------------------------------------
 */
#define NOWDB_ORD_NO    0
#define NOWDB_ORD_ASC   1
#define NOWDB_ORD_DESC  2

typedef char nowdb_ord_t;

/* ------------------------------------------------------------------------
 * Generic compare
 * ------------------------------------------------------------------------
 */
typedef nowdb_cmp_t (*nowdb_compare_t)(const void*, const void*);
typedef nowdb_cmp_t (*nowdb_comprsc_t)(const void*,const void*, void*);

/* ------------------------------------------------------------------------
 * Generic edge compare using index keys (asc)
 * ------------------------------------------------------------------------
 */
nowdb_cmp_t nowdb_sort_edge_keys_compare(const void *left,
                                         const void *right,
                                         void       *keys);

/* ------------------------------------------------------------------------
 * Generic edge compare using index keys (desc)
 * ------------------------------------------------------------------------
 */
nowdb_cmp_t nowdb_sort_edge_keys_compareD(const void *left,
                                          const void *right,
                                          void       *keys);

/* ------------------------------------------------------------------------
 * Generic vertex compare using index keys (asc)
 * ------------------------------------------------------------------------
 */
nowdb_cmp_t nowdb_sort_vertex_keys_compare(const void *left,
                                           const void *right,
                                           void       *keys);

/* ------------------------------------------------------------------------
 * Generic vertex compare using index keys (desc)
 * ------------------------------------------------------------------------
 */
nowdb_cmp_t nowdb_sort_vertex_keys_compareD(const void *left,
                                            const void *right,
                                            void       *keys);

/* ------------------------------------------------------------------------
 * Standard compare for edges (asc)
 * ------------------------------------------------------------------------
 */
nowdb_cmp_t nowdb_sort_edge_compare(const void *left,
                                    const void *right,
                                    void      *ignore);

/* ------------------------------------------------------------------------
 * Standard compare for edges (desc)
 * ------------------------------------------------------------------------
 */
nowdb_cmp_t nowdb_sort_edge_compareD(const void *left,
                                     const void *right,
                                     void      *ignore);

/* ------------------------------------------------------------------------
 * Standard compare for vertices (asc)
 * ------------------------------------------------------------------------
 */
nowdb_cmp_t nowdb_sort_vertex_compare(const void *left,
                                      const void *right,
                                      void      *ignore);

/* ------------------------------------------------------------------------
 * Standard compare for vertices (desc)
 * ------------------------------------------------------------------------
 */
nowdb_cmp_t nowdb_sort_vertex_compareD(const void *left,
                                       const void *right,
                                       void      *ignore);

/* ------------------------------------------------------------------------
 * Wrap beet compare
 * ------------------------------------------------------------------------
 */
typedef struct {
	beet_compare_t cmp;
	void          *rsc;
} nowdb_sort_wrapper_t;

nowdb_cmp_t nowdb_sort_wrapBeet(const void *left,
                                const void *right,
                                void      *ignore);

/* ------------------------------------------------------------------------
 * Generic sorting
 * ------------------------------------------------------------------------
 */
void nowdb_mem_sort(char *buf, uint32_t size, uint32_t recsize,
                           nowdb_comprsc_t compare, void *args);

/* ------------------------------------------------------------------------
 * Sorting edges
 * ------------------------------------------------------------------------
 */
void nowdb_mem_sort_edge(nowdb_edge_t *buf,
                         uint32_t      num,
                         nowdb_ord_t   ord);

/* ------------------------------------------------------------------------
 * Sorting vertices
 * ------------------------------------------------------------------------
 */
void nowdb_mem_sort_vertex(nowdb_vertex_t *buf,
                           uint32_t        num,
                           nowdb_ord_t     ord);

/* ------------------------------------------------------------------------
 * Sorting list of blocks
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_block_sort(ts_algo_list_t  *blocks,
                             nowdb_blist_t    *flist,
                             uint32_t        recsize,
                             nowdb_comprsc_t compare);
#endif
