/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Loader: CSV Loader for the canonical formal (context and vertex)
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/scope/loader.h>

#include <csv.h>

#define INBUFSIZE 131072
#define BLOCK       8192
#define BUFSIZE    64000
#define FEEDBACK  100000

static char *OBJECT = "ldr";

struct nowdb_csv_st {
	char       rejected;
	uint32_t        cur;
	uint64_t      total;
	uint64_t      fbcnt;
	char         *inbuf;
	char           *buf;
	uint32_t        pos;
	uint32_t    recsize;
	char        hlp[96];
	struct csv_parser p;
};

/* ------------------------------------------------------------------------
 * Whitspace
 * ------------------------------------------------------------------------
 */
static int is_space(unsigned char c) {
  if (c == CSV_SPACE || c == CSV_TAB) return 1;
  return 0;
}

/* ------------------------------------------------------------------------
 * EOL
 * ------------------------------------------------------------------------
 */
static int is_term(unsigned char c) {
  if (c == CSV_CR || c == CSV_LF) return 1;
  return 0;
}

nowdb_err_t nowdb_loader_init(nowdb_loader_t    *ldr,
                              FILE           *stream,
                              FILE          *ostream,
                              nowdb_store_t   *store,
                              nowdb_bitmap32_t flags) {
	nowdb_err_t err;

	ldr->err   = NOWDB_OK;
	ldr->flags = flags;
	ldr->stream = stream==NULL?stdin:stream;
	ldr->ostream = ostream==NULL?stderr:ostream;
	ldr->store  = store;
	ldr->fproc = NULL;
	ldr->rproc = NULL;
	ldr->csv   = NULL;
	ldr->delim = ';';
	ldr->loaded = 0;
	ldr->errors = 0;
	ldr->runtime = 0;

	if (flags & NOWDB_CSV_VERTEX) {
		ldr->fproc = &nowdb_csv_field_vertex;
		ldr->rproc = &nowdb_csv_row_vertex;
	} else {
		ldr->fproc = &nowdb_csv_field_context;
		ldr->rproc = &nowdb_csv_row_context;
	}

	ldr->csv = malloc(sizeof(nowdb_csv_t));
	if (ldr->csv == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                    FALSE, OBJECT, "allocating csv parser");

	ldr->csv->rejected = 0;
	ldr->csv->cur = 0;
	ldr->csv->pos = 0;
	ldr->csv->total = 0;
	ldr->csv->fbcnt = 0;
	ldr->csv->buf = NULL;

	if (flags & NOWDB_CSV_VERTEX) {
		ldr->csv->recsize = sizeof(nowdb_vertex_t);
	} else {
		ldr->csv->recsize = sizeof(nowdb_edge_t);
	}

	if (csv_init(&ldr->csv->p, 0) != 0) {
		err = nowdb_err_get(nowdb_err_no_mem,
		    FALSE, OBJECT, "csv parser init");
		free(ldr->csv); ldr->csv = NULL;
		return err;
	}

	csv_set_space_func(&ldr->csv->p, is_space);
  	csv_set_term_func(&ldr->csv->p, is_term);
	csv_set_delim(&ldr->csv->p,ldr->delim); 

	ldr->csv->inbuf = malloc(INBUFSIZE);
	if (ldr->csv->inbuf == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem,
		 FALSE, OBJECT, "allocating buffer");
		free(ldr->csv); ldr->csv = NULL;
		return err;
	}

	ldr->csv->buf = malloc(BUFSIZE);
	if (ldr->csv->buf == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem,
		 FALSE, OBJECT, "allocating buffer");
		free(ldr->csv->inbuf);
		free(ldr->csv); ldr->csv = NULL;
		return err;
	}
	return NOWDB_OK;
}

void nowdb_loader_destroy(nowdb_loader_t *ldr) {
	if (ldr->csv != NULL) {
		csv_free(&ldr->csv->p);
		if (ldr->csv->buf != NULL) {
			free(ldr->csv->buf);
			ldr->csv->buf = NULL;
		}
		if (ldr->csv->inbuf != NULL) {
			free(ldr->csv->inbuf);
			ldr->csv->inbuf = NULL;
		}
		free(ldr->csv);
		ldr->csv = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Read file to fill buffer
 * ------------------------------------------------------------------------
 */
nowdb_err_t fillbuf(FILE   *stream, 
                     char  *buf,
                     size_t bufsize,
                     size_t *outsize)
{
	size_t j,s,n,k;

	s=bufsize;j=0;
	while(s>0) {
		if (s>=BLOCK) {
			s-=BLOCK;n=BLOCK;
		} else {
			n=s;s=0;
		}
		k = fread(buf+j,1,n,stream);
		j+=k;
		if (k != n) {
			if (feof(stream)) break;
			return nowdb_err_get(nowdb_err_read,
			     TRUE, OBJECT, "loader stream");
		}
	}
	*outsize=j;
	return NOWDB_OK;
}

static inline void insertBuf(nowdb_loader_t *ldr) {

	if (ldr->err != NOWDB_OK) return;

	/* consider insertBulk! */
	for(int i=0; i<ldr->csv->pos; i+=ldr->csv->recsize) {
		/* fprintf(stderr, "inserting %d\n", i/64); */
		ldr->err = nowdb_store_insert(ldr->store, ldr->csv->buf+i);
		if (ldr->err != NOWDB_OK) return;
	}
}

static inline void cleanBuf(nowdb_loader_t *ldr) {
	if (ldr->err != NOWDB_OK) return;
	if (ldr->csv->pos == 0) return;
	insertBuf(ldr);
}

nowdb_err_t nowdb_loader_run(nowdb_loader_t *ldr) {
	size_t r, sz, off=0;

	do {
		ldr->err = fillbuf(ldr->stream, ldr->csv->inbuf+off,
		                                INBUFSIZE-off, &sz);
		if (ldr->err != NOWDB_OK) return ldr->err;
		r = csv_parse(&ldr->csv->p, ldr->csv->inbuf, sz,
		                   ldr->fproc, ldr->rproc, ldr);
		if (ldr->err != NOWDB_OK) return ldr->err;
		off = sz - r;
		if (off > 0) {
			// memcpy(ldr->csv->inbuf, ldr->csv->inbuf+r, off);
			fprintf(stderr, "CANNOT PARSE\n");
		}
	} while(sz == INBUFSIZE);

	csv_fini(&ldr->csv->p, ldr->fproc, ldr->rproc, ldr);
	if (ldr->err != NOWDB_OK) return ldr->err;

	cleanBuf(ldr);
	if (ldr->err != NOWDB_OK) return ldr->err;
	
	return NOWDB_OK;
}

#define EDGE    0
#define ORIGIN  8
#define DESTIN 16
#define LABEL  24
#define TMSTMP 32
#define WEIGHT 40
#define WEIGH2 48
#define WTYPE  56
#define WTYPE2 60

static inline int tohlp(nowdb_csv_t *csv, char *data,
                            size_t len, uint32_t off) {
	if (len > 31) return -1;
	memcpy(csv->hlp+off, data, len);
	csv->hlp[off+len] = 0;
	return 0;
}

static inline int toUInt(nowdb_csv_t *csv, char *data,
                             size_t len, void *target) {
	uint64_t n=0;
	char    *tmp;
	if (len == 0) {
		memcpy(target, &n, sizeof(uint64_t)); return 0;
	}
	if (tohlp(csv, data, len, 0) != 0) return -1;
	n = strtoul(csv->hlp, &tmp, 10);
	if (*tmp != 0) return -1;
	memcpy(target, &n, sizeof(uint64_t));
	return 0;
}

static inline int toInt(nowdb_csv_t *csv, char *data,
                            size_t len, void *target) {
	uint64_t n=0;
	char    *tmp;
	if (len == 0) {
		memcpy(target, &n, sizeof(uint64_t)); return 0;
	}
	if (tohlp(csv, data, len, 0) != 0) return -1;
	n = strtol(csv->hlp, &tmp, 10);
	if (*tmp != 0) return -1;
	memcpy(target, &n, sizeof(uint64_t));
	return 0;
}

static inline int toUInt32(nowdb_csv_t *csv, char *data,
                               size_t len, void *target) {
	uint32_t n=0;
	char    *tmp;
	if (len == 0) {
		memcpy(target, &n, sizeof(uint64_t)); return 0;
	}
	if (tohlp(csv, data, len, 64) != 0) return -1;
	n = (uint32_t)strtol(csv->hlp, &tmp, 10);
	if (*tmp != 0) return -1;
	memcpy(target, &n, sizeof(uint32_t));
	return 0;
}

#define REJECT(n,s) \
	ldr->csv->rejected = 1; \
	fprintf(stderr, "%s %s\n", n, s)

#define GETKEY(d, l, name, fld) \
	if (toUInt(ldr->csv, data, l,\
	    ldr->csv->buf+ldr->csv->pos+fld) != 0) \
	{ \
		REJECT(name, "invalid key"); \
	}

#define GETTMSTMP(d, l, name, fld) \
	if (toInt(ldr->csv, data, l,\
	    ldr->csv->buf+ldr->csv->pos+fld) != 0) \
	{ \
		REJECT(name, "invalid timestamp"); \
	}

#define GETVALUE(d, l, name, fld) \
	if (tohlp(ldr->csv, data, l, 4*(fld-WEIGHT)) != 0) \
	{ \
		REJECT(name, "invalid value"); \
	}

#define GETTYPE(d, l, name, fld) \
	if (toUInt32(ldr->csv, data, l,\
	    ldr->csv->buf+ldr->csv->pos+fld) != 0) \
	{ \
		REJECT(name, "invalid type"); \
	}

void nowdb_csv_field_context(void *data, size_t len, void *rsc) {
	nowdb_loader_t *ldr = rsc;

	switch(ldr->csv->cur) {
	case NOWDB_FIELD_EDGE:
		GETKEY(data, len, "EDGE", EDGE);
		ldr->csv->cur++; break;
	case NOWDB_FIELD_ORIGIN:
		GETKEY(data, len, "ORIGIN", ORIGIN);
		ldr->csv->cur++; break;
	case NOWDB_FIELD_DESTIN:
		GETKEY(data, len, "DESTIN", DESTIN);
		ldr->csv->cur++; break;
	case NOWDB_FIELD_LABEL:
		GETKEY(data, len, "LABEL", LABEL);
		ldr->csv->cur++; break;
	case NOWDB_FIELD_TIMESTAMP:
		GETTMSTMP(data, len, "TIMESTAMP", TMSTMP);
		ldr->csv->cur++; break;
	case NOWDB_FIELD_WEIGHT:
		GETVALUE(data, len, "WEIGHT", WEIGHT);
		ldr->csv->cur++; break;
	case NOWDB_FIELD_WEIGHT2:
		GETVALUE(data, len, "WEIGHT2", WEIGH2);
		ldr->csv->cur++; break;
	case NOWDB_FIELD_WTYPE:
		GETTYPE(data, len, "WTYPE", WTYPE);
		ldr->csv->cur++; break;
	case NOWDB_FIELD_WTYPE2:
		GETTYPE(data, len, "WTYPE2", WTYPE2);
		ldr->csv->cur = 0;
	}
}

/* consider implementing this as writeValue */
static inline void computeValues(nowdb_csv_t *csv) {
	uint64_t n = 0;

	nowdb_type_t type = *(nowdb_type_t*)(csv->buf+csv->pos+WTYPE);

	switch(type) {
	case NOWDB_TYP_NOTHING:

	case NOWDB_TYP_FLOAT:

	case NOWDB_TYP_INT:
	case NOWDB_TYP_UINT:

	/* ignored... */
	case NOWDB_TYP_TIME:
	case NOWDB_TYP_TEXT:
	case NOWDB_TYP_DATE:
	case NOWDB_TYP_COMPLEX:
	case NOWDB_TYP_LONGTEXT:
	default:
		memcpy(csv->buf+csv->pos+WEIGHT, &n, sizeof(uint64_t)); break;
	}
}

void nowdb_csv_row_context(int c, void *rsc) {
	nowdb_loader_t *ldr = rsc;

	if (ldr->csv->rejected) {
		ldr->csv->rejected = 0;
		/* count errors */
		return;
	}

	computeValues(ldr->csv);

	ldr->csv->pos+=ldr->csv->recsize;
	ldr->csv->fbcnt++;
	ldr->csv->total++;

	if (ldr->csv->pos >= BUFSIZE) {
		insertBuf(ldr);
		ldr->csv->pos = 0;
		if (ldr->csv->fbcnt >= FEEDBACK) {
			ldr->csv->fbcnt = 0;
			/* give feedback */
		}
	}
}

void nowdb_csv_field_vertex(void *data, size_t len, void *rsc) {
}

void nowdb_csv_row_vertex(int c, void *rsc) {
}


