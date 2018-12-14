/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Uitilities to ease working with result rows
 * ========================================================================
 */

#include <nowdb/types/time.h>
#include <nowdb/types/error.h>
#include <nowdb/query/rowutl.h>

#define MAXROW 0x1000

/* ------------------------------------------------------------------------
 * write (e.g. print) buffer
 * ------------------------------------------------------------------------
 */
int nowdb_row_print(char *buf, uint32_t sz, FILE *stream) {
	char tmp[32];
	uint32_t i=0;
	char t;
	char x=0;
	int rc;

	if (buf == NULL) return 0;
	if (sz == 0) return 0;

	while(i<sz) {
		t = buf[i]; i++;
		if (t == '\n') {
			fprintf(stream, "\n");
			x=0; continue;
		} else if (x) {
			fprintf(stream, ";");
		}
		switch((nowdb_type_t)t) {
		case NOWDB_TYP_TEXT:
			fprintf(stream, "%s", buf+i);
			i+=strlen(buf+i)+1; break;

		case NOWDB_TYP_DATE: 
		case NOWDB_TYP_TIME: 
			rc = nowdb_time_toString(*(nowdb_time_t*)(buf+i),
		                                       NOWDB_TIME_FORMAT,
		                                                tmp, 32);
			if (rc != 0) return rc;
			fprintf(stream, "%s", tmp); i+=8; break;

		case NOWDB_TYP_INT: 
			fprintf(stream, "%ld", *(int64_t*)(buf+i));
			i+=8; break;

		case NOWDB_TYP_UINT: 
			fprintf(stream, "%lu", *(uint64_t*)(buf+i));
			i+=8; break;

		case NOWDB_TYP_FLOAT: 
			fprintf(stream, "%.4f", *(double*)(buf+i));
			i+=8; break;

		case NOWDB_TYP_BOOL: 
			if (*(char*)(buf+i) == 0) {
				fprintf(stream, "false");
			} else {
				fprintf(stream, "true");
			}
			i++; break;

		case NOWDB_TYP_NOTHING: 
			fprintf(stream, "NULL");
			i++; break;

		default:
			fprintf(stderr, "unknown type (%d): %d\n", i, (int)t);
			return nowdb_err_invalid;
		}
		x=1;
	}
	fflush(stream);
	return 0;
}

/* ------------------------------------------------------------------------
 * Create a row holding one value
 * ------------------------------------------------------------------------
 */
char *nowdb_row_fromValue(nowdb_type_t t, void *value, uint32_t *sz) {
	return nowdb_row_addValue(NULL, t, value, sz);
}

/* ------------------------------------------------------------------------
 * End of string
 * ------------------------------------------------------------------------
 */
static inline int findEndOfStr(char *buf, int sz, int idx) {
	int z;
	int x = strnlen(buf+idx, 4097);
	if (x > 4096) return -1;
	z = idx+x;
	if (z>=sz) return -1;
	return (z+1);
}

/* ------------------------------------------------------------------------
 * End of string (public interface)
 * ------------------------------------------------------------------------
 */
int nowdb_row_findEndOfStr(char *buf, int sz, int idx) {
	return findEndOfStr(buf, sz, idx);
}

/* ------------------------------------------------------------------------
 * End of row
 * ------------------------------------------------------------------------
 */
int nowdb_row_findEOR(char *buf, uint32_t sz, int idx) {
	int i;
	for(i=idx; i<sz;) {
		if (buf[i] == NOWDB_EOR) break;
		if (buf[i] == NOWDB_TYP_TEXT) {
			i = findEndOfStr(buf,sz,i);
			if (i < 0) return -1;
			continue;
		} else if (buf[i] == NOWDB_TYP_BOOL ||
		           buf[i] == NOWDB_TYP_NOTHING) {
			i+=2;
			continue;
		}
		i+=9;
	}
	if (i >= sz) return -1;
	return (i+1);
}

/* ------------------------------------------------------------------------
 * Find last complete row in a bunch of rows
 * ------------------------------------------------------------------------
 */
int nowdb_row_findLastRow(char *buf, int sz) {
	int i;
	int l=sz;
	
	if (sz == 0) return 0;
	for(i=0; i<sz;) {
		if (buf[i] == NOWDB_EOR) {
			l=i; i++;
			if (i == sz) break;
		}
		if (buf[i] == NOWDB_TYP_TEXT) {
			i++;
			i = findEndOfStr(buf, sz, i);
			if (i < 0) return i;
			continue;
		} else if (buf[i] == NOWDB_TYP_BOOL ||
		           buf[i] == NOWDB_TYP_NOTHING) {
			i+=2;
			continue;
		}
		i+=9;
	}
	l++;
	return l;
}

/* ------------------------------------------------------------------------
 * Compute length of row
 * ------------------------------------------------------------------------
 */
int nowdb_row_len(char *row) {
	int i;

	if (row == NULL) return 0;
	for(i=0;i<MAXROW;) {
		if (row[i] == NOWDB_EOR) break;
		if (row[i] == NOWDB_TYP_TEXT) {
			i = findEndOfStr(row,MAXROW,i);
			if (i < 0) return -1;
			continue;

		} else if (row[i] == NOWDB_TYP_BOOL ||
		           row[i] == NOWDB_TYP_NOTHING) {
			i+=2; continue;
		}
		i+=9;
	}
	if (i >= MAXROW) return -1;
	return i;
}

/* ------------------------------------------------------------------------
 * Add EOR to row
 * ------------------------------------------------------------------------
 */
void nowdb_row_addEOR(char *row, uint32_t *sz) {
	if (*sz > 0) (*sz)++;
}

/* ------------------------------------------------------------------------
 * Add a value to a row
 * ------------------------------------------------------------------------
 */
char *nowdb_row_addValue(char *row, nowdb_type_t t,
                         void *value, uint32_t *sz) {
	char *row2;
	size_t s;
	size_t m;
	int  l=0;

	l = *sz; // nowdb_row_len(row);
	if (l < 0) return NULL;

	switch(t) {
	case NOWDB_TYP_TEXT:
	case NOWDB_TYP_LONGTEXT:
		s = strnlen((char*)value, 4097);
		if (s > 4096) return NULL;

		s+=2;

		if (row == NULL) {
			row2 = calloc(1,s+1);
		} else {
			row2 = realloc(row, s+l+1);
		}
		if (row2 == NULL) return NULL;

		row2[l] = (char)t; row2[s+l-1] = 0; row2[s+l] = 10;
		memcpy(row2+l+1, value, s-2);

		// *sz = s+l+1;
		*sz = s+l;
		return row2;
	
	case NOWDB_TYP_UINT:
	case NOWDB_TYP_INT:
	case NOWDB_TYP_TIME:
	case NOWDB_TYP_DATE:
	case NOWDB_TYP_FLOAT:
	case NOWDB_TYP_BOOL:
	case NOWDB_TYP_NOTHING:
		s = t==NOWDB_TYP_BOOL||t==NOWDB_TYP_NOTHING?2:9;
		m = s-1;

		if (row == NULL) {
			row2 = calloc(1,s+1);
		} else {
			row2 = realloc(row, s+l+1);
		}
		if (row2 == NULL) return NULL;

		row2[l] = (char)t; // if (l>0) l++;
		memcpy(row2+l+1, value, m);
		row2[s+l] = 10;

		// *sz = s+l+1;
		*sz = s+l;
		return row2;

	default:
		return NULL;
	}
}
