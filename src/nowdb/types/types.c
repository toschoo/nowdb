/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Fundamental Types
 * ========================================================================
 */
#include <nowdb/types/types.h>

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

static inline void setValue(nowdb_edge_t *e, nowdb_type_t typ,
                                       void *value, char what) 
{
	switch(typ) {
	case NOWDB_TYP_NOTHING:
		if (what == 0) e->weight = 0; else e->weight2 = 0;
		break;

	case NOWDB_TYP_FLOAT:
	case NOWDB_TYP_INT:
	case NOWDB_TYP_UINT:
	case NOWDB_TYP_TIME:
	case NOWDB_TYP_TEXT:
	case NOWDB_TYP_DATE:
	case NOWDB_TYP_COMPLEX:
	case NOWDB_TYP_LONGTEXT:
		if (what == 0) memcpy(&e->weight, value, 8);
		else memcpy(&e->weight2, value, 8);
		break;
	default:
		if (what == 0) e->weight = 0;  else e->weight2 = 0;
	}
}

static inline int strtow(nowdb_edge_t *e, nowdb_type_t typ,
                                    char *value, char what) 
{
	char *tmp;
	uint64_t u;
	int64_t  i;
	double   d;

	switch(typ) {

	case NOWDB_TYP_FLOAT:
		d = strtod(value, &tmp);
		if (*tmp != 0) {
			e->weight = 0; return -1;
		}
		if (what == 0) memcpy(&e->weight, &d, 8);
		else memcpy(&e->weight2, &d, 8);
		break;

	case NOWDB_TYP_INT:
	case NOWDB_TYP_TIME:
	case NOWDB_TYP_DATE:
		i = strtol(value, &tmp, 10);
		if (*tmp != 0) {
			e->weight = 0; return -1;
		}
		if (what == 0) memcpy(&e->weight, &i, 8);
		else memcpy(&e->weight2, &i, 8);
		break;

	case NOWDB_TYP_UINT:
	case NOWDB_TYP_TEXT:
	case NOWDB_TYP_LONGTEXT:
		u = strtoul(value, &tmp, 10);
		if (*tmp != 0) {
			fprintf(stderr, "value: %s\n", value);
			e->weight = 0; return -1;
		}
		if (what == 0) memcpy(&e->weight, &u, 8);
		else memcpy(&e->weight2, &u, 8);
		break;

	case NOWDB_TYP_COMPLEX:
	case NOWDB_TYP_NOTHING:
	default:
		if (what == 0) e->weight = 0;  else e->weight2 = 0;
	}
	return 0;
}

void nowdb_edge_writeWeight(nowdb_edge_t *e, nowdb_type_t typ, void *value) {
	setValue(e, typ, value, 0);
}

void nowdb_edge_writeWeight2(nowdb_edge_t *e, nowdb_type_t typ, void *value) {
	setValue(e, typ, value, 1);
}

void nowdb_edge_readWeight(nowdb_edge_t *e, nowdb_type_t typ, void *value) {
}

void nowdb_edge_readWeight2(nowdb_edge_t *e, nowdb_type_t typ, void *value) {
}

int nowdb_edge_strtow(nowdb_edge_t *e, nowdb_type_t typ, char *value) {
	return strtow(e, typ, value, 0);
}

int nowdb_edge_strtow2(nowdb_edge_t *e, nowdb_type_t typ, char *value) {
	return strtow(e, typ, value, 1);
}
