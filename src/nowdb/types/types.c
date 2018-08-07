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
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <sys/resource.h>

char nowdb_nullrec[64] = {0,0,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0,
                          0,0,0,0,0,0,0,0};

void *nowdb_global_handle = NULL;

static int setMaxFiles() {
	struct rlimit rl;
	if (getrlimit(RLIMIT_NOFILE,&rl) != 0) {
		fprintf(stderr, "cannot get max files limit: %d\n", errno);
		return -1;
	}

	rl.rlim_cur = rl.rlim_max;

	if (setrlimit(RLIMIT_NOFILE,&rl) != 0) {
		fprintf(stderr,"max files open: %d\n", errno);
		return -1;
	}
	return 0;
}

nowdb_bool_t nowdb_init() {
	if (!nowdb_errman_init()) return FALSE;
	nowdb_global_handle = beet_lib_init(NULL);
	if (nowdb_global_handle == NULL) return FALSE;
	if (setMaxFiles() != 0) return FALSE;
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

static inline void getWeight(nowdb_edge_t *e, nowdb_type_t typ,
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
	getWeight(e, typ, value, 0);
}

void nowdb_edge_readWeight2(nowdb_edge_t *e, nowdb_type_t typ, void *value) {
	getWeight(e, typ, value, 1);
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

int nowdb_vertex_offByName(char *fld) {
	if (fld == NULL) return -1;
	if (strcasecmp(fld, "vid") == 0) return NOWDB_OFF_VERTEX;
	if (strcasecmp(fld, "vertexid") == 0) return NOWDB_OFF_VERTEX;
	if (strcasecmp(fld, "vertex") == 0) return NOWDB_OFF_VERTEX;
	if (strcasecmp(fld, "prop") == 0) return NOWDB_OFF_PROP;
	if (strcasecmp(fld, "property") == 0) return NOWDB_OFF_PROP;
	if (strcasecmp(fld, "value") == 0) return NOWDB_OFF_VALUE;
	if (strcasecmp(fld, "vtype") == 0) return NOWDB_OFF_VTYPE;
	if (strcasecmp(fld, "type") == 0) return NOWDB_OFF_VTYPE;
	if (strcasecmp(fld, "role") == 0) return NOWDB_OFF_ROLE;
	return -1;
}

int nowdb_edge_offByName(char *fld) {
	if (fld == NULL) return -1;
	if (strcasecmp(fld, "edge") == 0) return NOWDB_OFF_EDGE;
	if (strcasecmp(fld, "origin") == 0) return NOWDB_OFF_ORIGIN;
	if (strcasecmp(fld, "destin") == 0) return NOWDB_OFF_DESTIN;
	if (strcasecmp(fld, "destination") == 0) return NOWDB_OFF_DESTIN;
	if (strcasecmp(fld, "label") == 0) return NOWDB_OFF_LABEL;
	if (strcasecmp(fld, "timestamp") == 0) return NOWDB_OFF_TMSTMP;
	if (strcasecmp(fld, "tmstmp") == 0) return NOWDB_OFF_TMSTMP;
	if (strcasecmp(fld, "time") == 0) return NOWDB_OFF_TMSTMP;
	if (strcasecmp(fld, "datetime") == 0) return NOWDB_OFF_TMSTMP;
	if (strcasecmp(fld, "date") == 0) return NOWDB_OFF_TMSTMP;
	if (strcasecmp(fld, "weight") == 0) return NOWDB_OFF_WEIGHT;
	if (strcasecmp(fld, "weight1") == 0) return NOWDB_OFF_WEIGHT;
	if (strcasecmp(fld, "value") == 0) return NOWDB_OFF_WEIGHT;
	if (strcasecmp(fld, "value2") == 0) return NOWDB_OFF_WEIGHT;
	if (strcasecmp(fld, "weight2") == 0) return NOWDB_OFF_WEIGHT2;
	if (strcasecmp(fld, "value2") == 0) return NOWDB_OFF_WEIGHT2;
	if (strcasecmp(fld, "wtype") == 0) return NOWDB_OFF_WTYPE;
	if (strcasecmp(fld, "type") == 0) return NOWDB_OFF_WTYPE;
	if (strcasecmp(fld, "wtype2") == 0) return NOWDB_OFF_WTYPE2;
	if (strcasecmp(fld, "type2") == 0) return NOWDB_OFF_WTYPE2;
	return -1;
}

int nowdb_sizeByOff(uint32_t recsize, uint16_t off) {
	if (recsize == 32) {
		switch(off) {
		case NOWDB_OFF_VTYPE:
		case NOWDB_OFF_ROLE: return 4;
		default: return 8;
		
		}
	} else {
		switch(off) {
		case NOWDB_OFF_WTYPE:
		case NOWDB_OFF_WTYPE2: return 4;
		default: return 8;
		}
	}
}
