/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Fundamental Types
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/errman.h>

#include <beet/config.h>

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

char nowdb_nullrec[64] = {0,0,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0};

void *nowdb_global_handle = NULL;

nowdb_bool_t nowdb_init() {
	if (!nowdb_errman_init()) return FALSE;
	nowdb_global_handle = beet_lib_init(NULL);
	if (nowdb_global_handle == NULL) return FALSE;
	return TRUE;
}

void *nowdb_lib() {
	return nowdb_global_handle;
}

void nowdb_close() {
	if (nowdb_global_handle != NULL) beet_lib_close(nowdb_global_handle);
	nowdb_errman_destroy();
}

static inline void setWeight(nowdb_edge_t *e, nowdb_type_t typ,
                                       void *value, char what) 
{
	switch(typ) {
	case NOWDB_TYP_FLOAT:
	case NOWDB_TYP_INT:
	case NOWDB_TYP_UINT:
	case NOWDB_TYP_TIME:
	case NOWDB_TYP_TEXT:
	case NOWDB_TYP_DATE:
	case NOWDB_TYP_LONGTEXT:
		if (what == 0) memcpy(&e->weight, value, 8);
		else memcpy(&e->weight2, value, 8);
		break;

	case NOWDB_TYP_COMPLEX:
	case NOWDB_TYP_NOTHING:
	default:
		if (what == 0) e->weight = 0;  else e->weight2 = 0;
	}
}

static inline void setValue(nowdb_vertex_t *v, nowdb_type_t typ,
                                                    void *value) {
	switch(typ) {
	case NOWDB_TYP_FLOAT:
	case NOWDB_TYP_INT:
	case NOWDB_TYP_UINT:
	case NOWDB_TYP_TIME:
	case NOWDB_TYP_TEXT:
	case NOWDB_TYP_DATE:
	case NOWDB_TYP_LONGTEXT:
		memcpy(&v->value, value, 8);
		break;

	case NOWDB_TYP_COMPLEX:
	case NOWDB_TYP_NOTHING:
	default:
		v->value = 0;
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
			if (what == 0) e->weight = 0; else e->weight2 = 0;
			return -1;
		}
		if (what == 0) memcpy(&e->weight, &d, 8);
		else memcpy(&e->weight2, &d, 8);
		break;

	case NOWDB_TYP_INT:
	case NOWDB_TYP_TIME:
	case NOWDB_TYP_DATE:
		i = strtol(value, &tmp, 10);
		if (*tmp != 0) {
			if (what == 0) e->weight = 0; else e->weight2 = 0;
			return -1;
		}
		if (what == 0) memcpy(&e->weight, &i, 8);
		else memcpy(&e->weight2, &i, 8);
		break;

	case NOWDB_TYP_UINT:
	case NOWDB_TYP_TEXT:
	case NOWDB_TYP_LONGTEXT:
		u = strtoul(value, &tmp, 10);
		if (*tmp != 0) {
			if (what == 0) e->weight = 0; else e->weight2 = 0;
			return -1;
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

static inline int strtov(nowdb_vertex_t *v, nowdb_type_t typ, char *value)
{
	char  *tmp;
	uint64_t u;
	int64_t  i;
	double   d;

	switch(typ) {

	case NOWDB_TYP_FLOAT:
		d = strtod(value, &tmp);
		if (*tmp != 0) {
			v->value = 0; return -1;
		}
		memcpy(&v->value, &d, 8);
		break;

	case NOWDB_TYP_INT:
	case NOWDB_TYP_TIME:
	case NOWDB_TYP_DATE:
		i = strtol(value, &tmp, 10);
		if (*tmp != 0) {
			v->value = 0; return -1;
		}
		memcpy(&v->value, &i, 8);
		break;

	case NOWDB_TYP_UINT:
	case NOWDB_TYP_TEXT:
	case NOWDB_TYP_LONGTEXT:
		u = strtoul(value, &tmp, 10);
		if (*tmp != 0) {
			v->value = 0; return -1;
		}
		memcpy(&v->value, &u, 8);
		break;

	case NOWDB_TYP_COMPLEX:
	case NOWDB_TYP_NOTHING:
	default:
		v->value = 0;
	}
	return 0;
}

void nowdb_edge_writeWeight(nowdb_edge_t *e, nowdb_type_t typ, void *value) {
	setWeight(e, typ, value, 0);
}

void nowdb_edge_writeWeight2(nowdb_edge_t *e, nowdb_type_t typ, void *value) {
	setWeight(e, typ, value, 1);
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

void nowdb_vertex_writeValue(nowdb_vertex_t *v, nowdb_type_t typ, void *value) {
	setValue(v, typ, value);
}

void nowdb_vertex_readValue(nowdb_vertex_t *v, nowdb_type_t typ, void *value);

int nowdb_vertex_strtov(nowdb_vertex_t *v, nowdb_type_t typ, char *value) {
	return strtov(v, typ, value);
}

