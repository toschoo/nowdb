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

#include <tsalgo/types.h>

typedef int nowdb_cmp_t;

#define NOWDB_SORT_LESS   -1
#define NOWDB_SORT_EQUAL   0
#define NOWDB_SORT_GREATER 1

typedef nowdb_cmp_t (*nowdb_compare_t)(const void*, const void*);
typedef nowdb_cmp_t (*nowdb_comprsc_t)(const void*,const void*, void*);

void nowdb_mem_sort(char *buf, uint32_t recsize, uint32_t size,
                           nowdb_comprsc_t compare, void *args);

#endif
