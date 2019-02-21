/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Fundamental Types
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/errman.h>
#include <nowdb/types/time.h>

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <limits.h>
#include <fcntl.h>
#include <sys/resource.h>

char nowdb_nullrec[1024] = {0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                            0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};

char nowdb_client_init() {
	if (!nowdb_errman_init()) return 0;
	return 1;
}

void nowdb_client_close() {
	nowdb_errman_destroy();
}

int nowdb_strtoval(char *str, nowdb_type_t t, void *value) {
	char *tmp;
	double d;
	uint64_t u;
	int64_t i;

	switch(t) {
	case NOWDB_TYP_FLOAT:
		d = strtod(str, &tmp);
		if (*tmp != 0) return -1;
		memcpy(value, &d, 8);
		break;

	case NOWDB_TYP_INT:
	case NOWDB_TYP_TIME:
	case NOWDB_TYP_DATE:
		i = strtol(str, &tmp, 10);
		if (*tmp != 0) return -1;
		memcpy(value, &i, 8);
		break;

	case NOWDB_TYP_UINT:
		u = strtoul(str, &tmp, 10);
		if (*tmp != 0) return -1;
		memcpy(value, &u, 8);
		break;

	case NOWDB_TYP_BOOL:
		if (strcasecmp(str, "true") == 0) u=1;
		else if (strcasecmp(str, "false") == 0) u=0;
		else return -1;
		memcpy(value, &u, 8);
		break;

	case NOWDB_TYP_TEXT:
	case NOWDB_TYP_LONGTEXT:
		return 1;

	case NOWDB_TYP_COMPLEX:
	case NOWDB_TYP_NOTHING:
	default:
		return -1;
	}
	return 0;
}

// correct Type
// str2type

int nowdb_correctType(nowdb_type_t good,
                      nowdb_type_t *bad,
                      void       *value) 
{
	if (good == *bad) return 0;

	if (good == NOWDB_TYP_TIME ||
            good == NOWDB_TYP_DATE) {
		if (*bad == NOWDB_TYP_INT) {
			*bad = good;
			return 0;
		}
		if (*bad == NOWDB_TYP_UINT) {
			int64_t tmp;
			tmp = *(int64_t*)value;
			*bad = good; memcpy(value, &tmp, 8);
			return 0;
		}
		if (*bad == NOWDB_TYP_TEXT) {
			int rc;
			char *str = value;
			int64_t tmp;
			size_t s;

			s = strnlen(str, 4096);

			if (s < 10) {
				return -1;

			} else if (s == 10) {
				rc = nowdb_time_fromString(str,
				      NOWDB_DATE_FORMAT, &tmp);
			} else {
				rc = nowdb_time_fromString(str,
				      NOWDB_TIME_FORMAT, &tmp);
			}
			if (rc != 0) return -1;
			*bad = good; memcpy(value, &tmp, 8);
			return 0;
		}
	}
	if (good == NOWDB_TYP_FLOAT) {
		if (*bad == NOWDB_TYP_INT || *bad == NOWDB_TYP_UINT) {
			*bad = good; return 0;
		}
	}
	if (good == NOWDB_TYP_INT) {
		if (*bad == NOWDB_TYP_UINT) {
			*bad = good; return 0;
		}
	}
	return -1;
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
	if (strcasecmp(fld, "origin") == 0) return NOWDB_OFF_ORIGIN;
	if (strcasecmp(fld, "destin") == 0) return NOWDB_OFF_DESTIN;
	if (strcasecmp(fld, "destination") == 0) return NOWDB_OFF_DESTIN;
	if (strcasecmp(fld, "timestamp") == 0) return NOWDB_OFF_STAMP;
	if (strcasecmp(fld, "stamp") == 0) return NOWDB_OFF_STAMP;
	if (strcasecmp(fld, "time") == 0) return NOWDB_OFF_STAMP;
	if (strcasecmp(fld, "datetime") == 0) return NOWDB_OFF_STAMP;
	if (strcasecmp(fld, "date") == 0) return NOWDB_OFF_STAMP;
	return -1;
}

int nowdb_sizeByOff(nowdb_content_t content, uint16_t off) {
	if (content == NOWDB_CONT_VERTEX) {
		switch(off) {
		case NOWDB_OFF_VTYPE:
		case NOWDB_OFF_ROLE: return 4;
		default: return 8;
		}
	}
	return 8;
}

int nowdb_strtotype(char *str) {
	if (str == NULL) return -1;
	if (strnlen(str, 32) > 32) return -1;
	if (strcasecmp(str, "uint")     == 0 ||
	    strcasecmp(str, "uinteger") == 0) return NOWDB_TYP_UINT;
	if (strcasecmp(str, "int")     == 0 ||
	    strcasecmp(str, "integer") == 0) return NOWDB_TYP_INT;
	if (strcasecmp(str, "date") == 0 ||
	    strcasecmp(str, "time") == 0) return NOWDB_TYP_TIME;
	if (strcasecmp(str, "float")   == 0) return NOWDB_TYP_FLOAT;
	if (strcasecmp(str, "text")    == 0) return NOWDB_TYP_TEXT;
	if (strcasecmp(str, "bool")    == 0) return NOWDB_TYP_BOOL;
	return -1;
}

char *nowdb_typename(nowdb_type_t typ) {
	switch(typ) {
	case NOWDB_TYP_FLOAT: return "float";
	case NOWDB_TYP_INT: return "int";
	case NOWDB_TYP_UINT: return "uint";
	case NOWDB_TYP_DATE: return "date";
	case NOWDB_TYP_TIME: return "time";
	case NOWDB_TYP_TEXT: return "text";
	case NOWDB_TYP_NOTHING: return "NULL";
	default: return "unknown";
	}
}

static inline uint32_t edgeAttctrlSize(uint16_t atts) {
	return (atts%8==0?atts/8:atts/8+1);
}

static inline uint32_t edgeRecSize(char stamped, uint16_t atts) {
	if (atts == 0) {
		if (stamped) return 24; else return 16;
	}
	uint16_t xb = edgeAttctrlSize(atts);
	return (24+xb+8*atts);
}

uint32_t nowdb_edge_recSize(char stamped, uint16_t atts) {
	return edgeRecSize(stamped, atts);
}

static inline uint32_t recordsPerPage(uint32_t recsz) {
	return (NOWDB_IDX_PAGE/recsz);
}

uint32_t nowdb_recordsPerPage(uint32_t recsize) {
	return recordsPerPage(recsize);
}

static inline uint32_t pagectrlSize(uint32_t recsz) {
	if (recsz <= 24) return 0;
	uint32_t rp = recordsPerPage(recsz);
	return (rp%8==0?rp/8:rp/8+1);
}

uint32_t nowdb_pagectrlSize(uint32_t recsz) {
	return pagectrlSize(recsz);
}

uint32_t nowdb_edge_attctrlSize(uint16_t atts) {
	return edgeAttctrlSize(atts);
}

// get attribute control bit and byte for specific offset
void nowdb_edge_getCtrl(uint16_t atts, uint32_t off,
                        uint8_t  *bit, uint16_t *byte) {
	uint32_t xb = edgeAttctrlSize(atts);
	uint32_t o = (off - NOWDB_OFF_USER - xb)/8;
	*byte = o/8;
	*bit  = o%8;
	/*
	fprintf(stderr, "xb: %u, off: %u, o: %u, byte: %hu, bit: %u\n",
	                 xb, off, o, *byte, *bit);
	*/
}

