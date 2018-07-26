/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Comparing, searching and sorting
 * ========================================================================
 */

#include <nowdb/sort/sort.h>

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
	if (((nowdb_edge_t*)left)->origin >  
	    ((nowdb_edge_t*)right)->origin) return NOWDB_SORT_LESS;
	if (((nowdb_edge_t*)left)->origin <
	    ((nowdb_edge_t*)right)->origin) return NOWDB_SORT_GREATER;

	if (((nowdb_edge_t*)left)->edge   >
	    ((nowdb_edge_t*)right)->edge) return NOWDB_SORT_LESS;
	if (((nowdb_edge_t*)left)->edge   <
	    ((nowdb_edge_t*)right)->edge) return NOWDB_SORT_GREATER;

	if (((nowdb_edge_t*)left)->destin >
	    ((nowdb_edge_t*)right)->destin) return NOWDB_SORT_LESS;
	if (((nowdb_edge_t*)left)->destin <
	    ((nowdb_edge_t*)right)->destin) return NOWDB_SORT_GREATER;

	if (((nowdb_edge_t*)left)->label  >
	    ((nowdb_edge_t*)right)->label) return NOWDB_SORT_LESS;
	if (((nowdb_edge_t*)left)->label <
	    ((nowdb_edge_t*)right)->label) return NOWDB_SORT_GREATER;

	if (((nowdb_edge_t*)left)->timestamp >
	    ((nowdb_edge_t*)right)->timestamp) return NOWDB_SORT_LESS;
	if (((nowdb_edge_t*)left)->timestamp <
	    ((nowdb_edge_t*)right)->timestamp) return NOWDB_SORT_GREATER;

	return NOWDB_SORT_EQUAL;
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
	if (((nowdb_vertex_t*)left)->role >
	    ((nowdb_vertex_t*)right)->role) return NOWDB_SORT_LESS;
	if (((nowdb_vertex_t*)left)->role <
	    ((nowdb_vertex_t*)right)->role) return NOWDB_SORT_GREATER;

	if (((nowdb_vertex_t*)left)->vertex >
	    ((nowdb_vertex_t*)right)->vertex) return NOWDB_SORT_LESS;
	if (((nowdb_vertex_t*)left)->vertex <
	    ((nowdb_vertex_t*)right)->vertex) return NOWDB_SORT_GREATER;

	if (((nowdb_vertex_t*)left)->property >
	    ((nowdb_vertex_t*)right)->property) return NOWDB_SORT_LESS;
	if (((nowdb_vertex_t*)left)->property <
	    ((nowdb_vertex_t*)right)->property) return NOWDB_SORT_GREATER;

	return NOWDB_SORT_EQUAL;
}

