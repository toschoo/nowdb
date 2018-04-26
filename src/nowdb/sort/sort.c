/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Comparing, searching and sorting
 * ========================================================================
 */

#include <nowdb/sort/sort.h>

void nowdb_mem_sort(char *buf, uint32_t size, uint32_t recsize,
                           nowdb_comprsc_t compare, void *args) {
	qsort_r(buf, (size_t)size, (size_t)recsize, compare, args);
}

