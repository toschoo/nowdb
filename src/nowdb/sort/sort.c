/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Comparing, searching and sorting
 * ========================================================================
 */

#include <nowdb/sort/sort.h>

void nowdb_mem_sort(char *buf, uint32_t recsize, uint32_t size,
                           nowdb_comprsc_t compare, void *args) {
	qsort_r(buf, (size_t)recsize, (size_t)size, compare, args);
}

