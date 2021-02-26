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
// float compare?
#define WEIGHTCMP(left,right,o) \
	if ((*(nowdb_value_t*)((char*)left+o)) < \
	    (*(nowdb_value_t*)((char*)right+o))) return BEET_CMP_LESS; \
	if ((*(nowdb_value_t*)((char*)left+o)) > \
	    (*(nowdb_value_t*)((char*)right+o))) return BEET_CMP_GREATER;

/* this should be with rounding */
#define TMSTMPCMP(left,right,o) \
	if ((*(nowdb_time_t*)((char*)left+o)) < \
	    (*(nowdb_time_t*)((char*)right+o))) \
		return BEET_CMP_LESS; \
	if ((*(nowdb_time_t*)((char*)left+o)) > \
	    (*(nowdb_time_t*)((char*)right+o))) \
		return BEET_CMP_GREATER;

#define ROLECMP(left,right,o) \
	if ((*(nowdb_roleid_t*)((char*)left+o)) < \
	    (*(nowdb_roleid_t*)((char*)right+o))) \
		return BEET_CMP_LESS; \
	if ((*(nowdb_roleid_t*)((char*)left+o)) > \
	    (*(nowdb_roleid_t*)((char*)right+o))) \
		return BEET_CMP_GREATER;

#define EDGECMP(left, right, k) \
	if (KEYS(keys)->off[i] < NOWDB_OFF_STAMP) \
		KEYCMP(left, right, k) \
	else if (KEYS(keys)->off[i] == NOWDB_OFF_STAMP) \
		TMSTMPCMP(left, right, k) \
	else \
		WEIGHTCMP(left, right, k) \
	k+=8;

#define VERTEXCMP(left, right, k) \
	if (KEYS(keys)->off[i] <= NOWDB_OFF_PROP) { \
		KEYCMP(left, right, k) \
		k += 8; \
	} else if (KEYS(keys)->off[i] == NOWDB_OFF_VALUE) { \
		WEIGHTCMP(left, right, k) \
		k += 8; \
	} else if (KEYS(keys)->off[i] == NOWDB_OFF_ROLE) { \
		ROLECMP(left,right,k); \
		k += 4; \
	}

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
	int k=0;
	for(int i=0;i<KEYS(keys)->sz;i++) {
		EDGECMP(left, right, k);
	}
	return BEET_CMP_EQUAL;
}

char nowdb_index_vertex_compare(const void *left,
                                const void *right,
                                void       *keys) {
	int k=0;
	for(int i=0;i<KEYS(keys)->sz;i++) {
		VERTEXCMP(left, right, k);
	}
	return BEET_CMP_EQUAL;
}

void nowdb_index_grabKeys(nowdb_index_keys_t *k,
                          const char      *node,
                          char            *keys) {
	int off=0, sz=8;
	for(int i=0; i<k->sz; i++) {
		memcpy(keys+off, node+k->off[i], sz); off+=sz;
	}
}


void nowdb_index_grabEdgeKeys(nowdb_index_keys_t *k,
                              const char      *edge,
                              char            *keys) {
	int off=0, sz=8;
	for(int i=0; i<k->sz; i++) {
		memcpy(keys+off, edge+k->off[i], sz); off+=sz;
	}
}

void nowdb_index_grabVertexKeys(nowdb_index_keys_t *k,
                                const char    *vertex,
                                char            *keys) {
	int off=0, sz;
	for(int i=0; i<k->sz; i++) {
		if (k->off[i] < NOWDB_OFF_VTYPE) sz=8; else sz=4;
		memcpy(keys+off, vertex+k->off[i], sz); off+=sz;
	}
}
