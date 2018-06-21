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

#define KEYS(x) \
	((nowdb_index_keys_t*)x)

#define KEYCMP(left,right,o) \
	if ((*(nowdb_key_t*)((char*)left+o)) < \
	    (*(nowdb_key_t*)((char*)right+o))) return BEET_CMP_LESS; \
	if ((*(nowdb_key_t*)((char*)left+o)) > \
	    (*(nowdb_key_t*)((char*)right+o))) return BEET_CMP_GREATER;

/* this should be with either rounding or tolerance */
#define WEIGHTCMP(left,right,o) \
	if ((*(nowdb_value_t*)((char*)left+o)) < \
	    (*(nowdb_value_t*)((char*)right+o))) return BEET_CMP_LESS; \
	if ((*(nowdb_value_t*)((char*)left+o)) > \
	    (*(nowdb_value_t*)((char*)right+o))) return BEET_CMP_GREATER;

/* this should be with rounding */
#define TMSTMPCMP(left,right) \
	if ((*(nowdb_time_t*)((char*)left+NOWDB_OFF_TMSTMP)) < \
	    (*(nowdb_time_t*)((char*)right+NOWDB_OFF_TMSTMP))) \
		return BEET_CMP_LESS; \
	if ((*(nowdb_time_t*)((char*)left+NOWDB_OFF_TMSTMP)) > \
	    (*(nowdb_time_t*)((char*)right+NOWDB_OFF_TMSTMP))) \
		return BEET_CMP_GREATER;

#define ROLECMP(left,right) \
	if ((*(nowdb_roleid_t*)((char*)left+NOWDB_OFF_TMSTMP)) < \
	    (*(nowdb_roleid_t*)((char*)right+NOWDB_OFF_TMSTMP))) \
		return BEET_CMP_LESS; \
	if ((*(nowdb_roleid_t*)((char*)left+NOWDB_OFF_TMSTMP)) > \
	    (*(nowdb_roleid_t*)((char*)right+NOWDB_OFF_TMSTMP))) \
		return BEET_CMP_GREATER;

#define EDGECMP(left, right, keys) \
	if (KEYS(keys)->off[i] <= NOWDB_OFF_LABEL) \
		KEYCMP(left, right, KEYS(keys)->off[i]) \
	else if (KEYS(keys)->off[i] == NOWDB_OFF_TMSTMP) \
		TMSTMPCMP(left, right) \
	else if (KEYS(keys)->off[i] <= NOWDB_OFF_WEIGHT2) \
		WEIGHTCMP(left, right, KEYS(keys)->off[i])

#define VERTEXCMP(left, right, keys) \
	if (KEYS(keys)->off[i] <= NOWDB_OFF_PROP) \
		KEYCMP(left, right, KEYS(keys)->off[i]) \
	else if (KEYS(keys)->off[i] == NOWDB_OFF_VALUE) \
		WEIGHTCMP(left, right, KEYS(keys)->off[i]) \
	else if (KEYS(keys)->off[i] == NOWDB_OFF_ROLE) \
		ROLECMP(left,right); 

char nowdb_index_pageid_compare(const void *left,
                                const void *right,
                                void      *ignore)
{
	if (PID(left) < PID(right)) return BEET_CMP_LESS;
	if (PID(left) > PID(right)) return BEET_CMP_GREATER;
	return BEET_CMP_EQUAL;
}

beet_err_t nowdb_index_keysinit(void **keys, void *mykeys) {
	*keys = mykeys;
	return BEET_OK;
}

void nowdb_index_keysdestroy(void **keys) {}

char nowdb_index_edge_compare(const void *left,
                              const void *right,
                              void       *keys) {
	for(int i=0;i<KEYS(keys)->sz;i++) {
		EDGECMP(left, right, keys);
	}
	return BEET_CMP_EQUAL;
}

char nowdb_index_vertex_compare(const void *left,
                                const void *right,
                                void       *keys) {
	for(int i=0;i<KEYS(keys)->sz;i++) {
		VERTEXCMP(left, right, keys);
	}
	return BEET_CMP_EQUAL;
}
