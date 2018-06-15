/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Standard compares for Index
 * ========================================================================
 */
#include <nowdb/index/index.h>
#include <beet/types.h>

#define PID(x) \
	(*(nowdb_pageid_t*)x)

char nowdb_index_pageid_compare(const void *left,
                                const void *right,
                                void      *ignore)
{
	if (PID(left) < PID(right)) return BEET_CMP_LESS;
	if (PID(left) > PID(right)) return BEET_CMP_GREATER;
	return BEET_CMP_EQUAL;
}

beet_err_t nowdb_index_keysinit(void **keys) {
	return BEET_OK;
}

void nowdb_index_keysdestroy(void **keys) {}


