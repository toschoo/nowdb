/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Loader: CSV Loader for vertex and edge
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/scope/loader.h>
#include <nowdb/scope/scope.h>

#include <tsalgo/list.h>

#include <csv.h>

#include <time.h>

#define INBUFSIZE 131072
#define BLOCK       8192
#define BUFSIZE    64000
#define FEEDBACK  100000

static char *OBJECT = "ldr";

/* ------------------------------------------------------------------------
 * csv state
 * ------------------------------------------------------------------------
 */
struct nowdb_csv_st {
	char                rejected; /* current row was rejected       */
	uint32_t                 cur; /* current field                  */
	char                   first; /* first line                     */
	uint64_t               total; /* total number of rows processed */
	uint64_t               fbcnt; /* counter for feedback           */
	char                  *inbuf; /* input buffer                   */
	char                    *buf; /* result buffer                  */
	uint32_t                 pos; /* position in the result buffer  */
	uint32_t                 old; /* remember previous position     */
	uint32_t             recsize; /* size of record                 */
	char                 hlp[96]; /* three 32 byte 'registers'      */
	char               txt[1024]; /* text buffer                    */
	struct csv_parser          p; /* the csv parser                 */
	ts_algo_list_t         *list; /* temporary store for headers    */
	nowdb_key_t              vid; /* edge model  : vertex id        */
	nowdb_model_prop_t    *props; /* vertex model: array of props   */
	nowdb_model_pedge_t   *pedge; /* edge   model: array of props   */
	uint8_t                  *xb; /* control block for edge model   */
	uint16_t                atts; /* number of atts in edge         */
	uint32_t             ctlSize; /* size of control block          */
	uint32_t                 psz; /* size of prop/pedge array       */
	uint32_t               pkidx; /* vertex model: index of pk      */
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

/* ------------------------------------------------------------------------
 * initialise the loader
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_loader_init(nowdb_loader_t    *ldr,
                              FILE           *stream,
                              FILE          *ostream,
                              void            *scope,
                              nowdb_context_t   *ctx,
                              nowdb_model_t   *model,
                              nowdb_text_t     *text,
                              char             *type,
                              nowdb_bitmap32_t flags) {
	nowdb_err_t err;

	ldr->err   = NOWDB_OK;
	ldr->flags = flags;
	ldr->stream = stream==NULL?stdin:stream;
	ldr->ostream = ostream==NULL?stderr:ostream;
	ldr->scope  = scope;
	ldr->ctx = ctx;
	ldr->model  = model;
	ldr->text   = text;
	ldr->type   = type;
	ldr->timefrm = NULL;
	ldr->datefrm = NULL;
	ldr->fproc = NULL;
	ldr->rproc = NULL;
	ldr->csv   = NULL;
	ldr->delim = ';';
	ldr->loaded = 0;
	ldr->errors = 0;
	ldr->runtime = 0;

	/* nice feature would be to pass
	 * date and time format in from SQL.
	 * Currently, this is hard-coded! */
	ldr->datefrm = NOWDB_DATE_FORMAT;
	ldr->timefrm = NOWDB_TIME_FORMAT;

	if (flags & NOWDB_CSV_MODEL) {
		ldr->rproc = &nowdb_csv_row;
		if (flags & NOWDB_CSV_VERTEX) {
			ldr->fproc = &nowdb_csv_field_type;
		} else {
			ldr->fproc = &nowdb_csv_field_edge;
		}
	} else {
		nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		      "loading without model is DEPRECATED!\n");
	}

	ldr->csv = malloc(sizeof(nowdb_csv_t));
	if (ldr->csv == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                    FALSE, OBJECT, "allocating csv parser");

	ldr->csv->first = 1;
	ldr->csv->rejected = 0;
	ldr->csv->cur = 0;
	ldr->csv->pos = 0;
	ldr->csv->old = 0;
	ldr->csv->total = 0;
	ldr->csv->fbcnt = 0;
	ldr->csv->buf = NULL;
	ldr->csv->list = NULL;
	ldr->csv->props = NULL;
	ldr->csv->pedge = NULL;
	ldr->csv->psz = 0;
	ldr->csv->pkidx = 0;
	ldr->csv->recsize = 0;
	ldr->csv->xb = 0;

	if (csv_init(&ldr->csv->p, 0) != 0) {
		err = nowdb_err_get(nowdb_err_loader, FALSE, OBJECT, 
		      (char*)csv_strerror(csv_error(&ldr->csv->p)));
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

/* ------------------------------------------------------------------------
 * Destroy prop list
 * ------------------------------------------------------------------------
 */
static void destroyPropList(ts_algo_list_t *list) {
	ts_algo_list_node_t *runner, *tmp;

	runner = list->head;
	while(runner!=NULL) {
		free(runner->cont); tmp = runner->nxt;
		ts_algo_list_remove(list, runner);
		free(runner); runner = tmp;
	}
	ts_algo_list_destroy(list);
}

/* ------------------------------------------------------------------------
 * Destroy the loader
 * ------------------------------------------------------------------------
 */
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
		if (ldr->csv->props != NULL) {
			for(int i=0;i<ldr->csv->psz;i++) {
				if (ldr->csv->props[i].name != NULL) {
					free(ldr->csv->props[i].name);
				}
			}
			free(ldr->csv->props); ldr->csv->props = NULL;
		}
		if (ldr->csv->pedge != NULL) {
			for(int i=0;i<ldr->csv->psz;i++) {
				if (ldr->csv->pedge[i].name != NULL) {
					free(ldr->csv->pedge[i].name);
				}
			}
			free(ldr->csv->pedge); ldr->csv->pedge = NULL;
		}
		if (ldr->csv->xb != NULL) {
			free(ldr->csv->xb); ldr->csv->xb = NULL;
		}
		if (ldr->csv->list != NULL) {
			destroyPropList(ldr->csv->list);
			free(ldr->csv->list);
			ldr->csv->list = NULL;
		}
		free(ldr->csv);
		ldr->csv = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Read file to fill input buffer
 * ------------------------------------------------------------------------
 */
nowdb_err_t fillbuf(FILE   *stream, 
                    char   *buf,
                    size_t  bufsize,
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

/* ------------------------------------------------------------------------
 * Insert records (into vertex or context) from output buffer
 * ------------------------------------------------------------------------
 */
static inline void insertBuf(nowdb_loader_t *ldr) {

	if (ldr->err != NOWDB_OK) return;

	/* consider insertBulk! */
	for(int i=0; i<ldr->csv->pos; i+=ldr->csv->recsize) {
		/* fprintf(stderr, "inserting %d\n", i/64); */
		ldr->err = nowdb_store_insert(&ldr->ctx->store,
		                               ldr->csv->buf+i);
		if (ldr->err != NOWDB_OK) return;
	}
}

/* ------------------------------------------------------------------------
 * Clean the buffer (insert remainder)
 * ------------------------------------------------------------------------
 */
static inline void cleanBuf(nowdb_loader_t *ldr) {
	if (ldr->err != NOWDB_OK) return;
	if (ldr->csv->pos == 0) return;
	insertBuf(ldr);
}

/* ------------------------------------------------------------------------
 * Run the loader
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_loader_run(nowdb_loader_t *ldr) {
	nowdb_err_t err;
	size_t r, sz=0, off=0;
	struct timespec t1, t2;

	ldr->loaded = 0;
	ldr->errors = 0;

	nowdb_timestamp(&t1);

	/* we lock the model, so nobody can change
	 * or remove types while we are working */
	if (ldr->model != NULL) {
		err = nowdb_lock_read(&ldr->model->lock);
		if (err != NOWDB_OK) return err;
	}
	do {
		err = fillbuf(ldr->stream, ldr->csv->inbuf+off,
		                           INBUFSIZE-off, &sz);
		if (err != NOWDB_OK) {
			if (ldr->model != NULL) {
				NOWDB_IGNORE(nowdb_unlock_read(
				            &ldr->model->lock));
			}
			return err;
		}
		r = csv_parse(&ldr->csv->p, ldr->csv->inbuf, sz,
		                   ldr->fproc, ldr->rproc, ldr);
		if (ldr->err != NOWDB_OK) {
			if (ldr->model != NULL) {
				NOWDB_IGNORE(nowdb_unlock_read(
				            &ldr->model->lock));
			}
			err = ldr->err; ldr->err = NOWDB_OK;
			return err;
		}
		off = sz - r;
		if (off > 0) {
			if (ldr->model != NULL) {
				NOWDB_IGNORE(nowdb_unlock_read(
				            &ldr->model->lock));
			}
			return nowdb_err_get(nowdb_err_loader, FALSE, OBJECT,
			       (char*)csv_strerror(csv_error(&ldr->csv->p)));
		}
	} while(sz == INBUFSIZE);

	csv_fini(&ldr->csv->p, ldr->fproc, ldr->rproc, ldr);
	if (ldr->err != NOWDB_OK) {
		if (ldr->model != NULL) {
			NOWDB_IGNORE(nowdb_unlock_read(
				    &ldr->model->lock));
		}
		err = ldr->err; ldr->err = NOWDB_OK;
		return err;
	}
	if (ldr->model != NULL) {
		err = nowdb_unlock_read(&ldr->model->lock);
		if (err != NOWDB_OK) return err;
	}

	cleanBuf(ldr);
	if (ldr->err != NOWDB_OK) {
		err = ldr->err; ldr->err = NOWDB_OK;
		return err;
	}

	nowdb_timestamp(&t2);

	ldr->runtime = nowdb_time_minus(&t2, &t1)/1000;
	
	return NOWDB_OK;
}

#define NOMEM(s) \
	ldr->err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, s);

/* ------------------------------------------------------------------------
 * Row header with vertex model
 * ------------------------------------------------------------------------
 */
static inline void rowTypeHeader(nowdb_loader_t *ldr) {
	ts_algo_list_node_t *runner, *tmp;
	nowdb_roleid_t roleid;
	nowdb_model_vertex_t *v;
	nowdb_model_prop_t *p;
	nowdb_err_t err;
	int i=0;

	if (ldr->csv->list == NULL) return;
	if (ldr->err != NOWDB_OK) return;

	err = nowdb_model_getVertexByName(ldr->model, ldr->type, &v);
	if (err != NOWDB_OK) {
		ldr->err = err;
		return;
	}

	roleid = v->roleid;

	ldr->csv->recsize = v->size;
	ldr->csv->atts    = v->num;

	if (ldr->csv->atts > 0) {
		ldr->csv->ctlSize = nowdb_attctrlSize(v->num);

		ldr->csv->xb = calloc(1,ldr->csv->ctlSize);
		if (ldr->csv->xb == NULL) {
			ldr->err = nowdb_err_get(nowdb_err_no_mem,
			                            FALSE, OBJECT,
			     "allocating attribute control block");
			return;
		}
	}

	ldr->csv->props = calloc(ldr->csv->psz, sizeof(nowdb_model_prop_t));
	if (ldr->csv->props == NULL) {
		NOMEM("allocating props");
		return;
	}
	runner=ldr->csv->list->head; 
	for(i=0; runner!=NULL; i++) {
		/*
		fprintf(stderr, "processing %d. field '%s'\n",
		                       i, (char*)runner->cont);
		*/
		err = nowdb_model_getPropByName(ldr->model, roleid,
		                                runner->cont,  &p);
		if (err != NOWDB_OK) {
			ldr->err = err;
			return;
		}
		ldr->csv->props[i].roleid = roleid;
		ldr->csv->props[i].propid = p->propid;
		ldr->csv->props[i].value = p->value;
		ldr->csv->props[i].off = p->off;
		ldr->csv->props[i].pk = p->pk;
		if (p->pk) ldr->csv->pkidx = i;

		ldr->csv->props[i].name = runner->cont;

		tmp = runner->nxt;
		ts_algo_list_remove(ldr->csv->list, runner);
		free(runner); runner = tmp;
	}
	ts_algo_list_destroy(ldr->csv->list);
	free(ldr->csv->list); ldr->csv->list = NULL;
}

/* ------------------------------------------------------------------------
 * Row header with edge model
 * ------------------------------------------------------------------------
 */
static inline void rowEdgeHeader(nowdb_loader_t *ldr) {
	ts_algo_list_node_t *runner, *tmp;
	nowdb_key_t     edgeid;
	nowdb_model_edge_t  *e;
	nowdb_model_pedge_t *p;
	nowdb_err_t err;
	int i=0;

	if (ldr->csv->list == NULL) return;
	if (ldr->err != NOWDB_OK) return;

	err = nowdb_model_getEdgeByName(ldr->model, ldr->type, &e);
	if (err != NOWDB_OK) {
		ldr->err = err;
		return;
	}

	edgeid = e->edgeid;
	ldr->csv->recsize = e->size;

	ldr->csv->atts = e->num;

	if (ldr->csv->atts > 0) {
		ldr->csv->ctlSize = nowdb_attctrlSize(e->num);

		ldr->csv->xb = calloc(1,ldr->csv->ctlSize);
		if (ldr->csv->xb == NULL) {
			ldr->err = nowdb_err_get(nowdb_err_no_mem,
			                            FALSE, OBJECT,
			     "allocating attribute control block");
			return;
		}
	}

	ldr->csv->pedge = calloc(ldr->csv->psz, sizeof(nowdb_model_pedge_t));
	if (ldr->csv->pedge == NULL) {
		NOMEM("allocating props");
		return;
	}
	runner=ldr->csv->list->head; 
	for(i=0; runner!=NULL; i++) {
		err = nowdb_model_getPedgeByName(ldr->model, edgeid,
		                                 runner->cont,  &p);
		if (err != NOWDB_OK) {
			ldr->err = err;
			return;
		}
		/*
		fprintf(stderr, "Getting %d. Pedge: '%s'\n",
		                     i, (char*)runner->cont);
		*/

		ldr->csv->pedge[i].edgeid = edgeid;
		ldr->csv->pedge[i].propid = p->propid;
		ldr->csv->pedge[i].value  = p->value;
		ldr->csv->pedge[i].off    = p->off;

		ldr->csv->pedge[i].name = runner->cont;

		tmp = runner->nxt;
		ts_algo_list_remove(ldr->csv->list, runner);
		free(runner); runner = tmp;
	}
	ts_algo_list_destroy(ldr->csv->list);
	free(ldr->csv->list); ldr->csv->list = NULL;
}

/* ------------------------------------------------------------------------
 * Field header
 * ------------------------------------------------------------------------
 */
static inline void fieldHeader(void *data, size_t len,
                               nowdb_loader_t    *ldr) {
	char *nm;

	if (ldr->csv->list == NULL) {
		ldr->csv->list = calloc(1, sizeof(ts_algo_list_t));
		if (ldr->csv->list == NULL) {
			NOMEM("allocating list");
			return;
		}
		ts_algo_list_init(ldr->csv->list);
	}

	ldr->csv->psz++;

	nm = malloc(len+1);
	if (nm == NULL) {
		NOMEM("allocating field name");
		return;
	}
	memcpy(nm, data, len); nm[len] = 0;
	if (ts_algo_list_append(ldr->csv->list, nm) != TS_ALGO_OK) {
		free(nm);
		NOMEM("list.append");
		return;
	}
}

/* ------------------------------------------------------------------------
 * Convert Loader
 * ------------------------------------------------------------------------
 */
#define LDR(x) \
	((nowdb_loader_t*)x)

/* ------------------------------------------------------------------------
 * Row callback
 * ------------------------------------------------------------------------
 */
void nowdb_csv_row(int c, void *ldr) {

	if (LDR(ldr)->err != NOWDB_OK) return;

	if (LDR(ldr)->csv->first) {
		LDR(ldr)->csv->first = FALSE;

		/* we have a header */
		if (LDR(ldr)->flags & NOWDB_CSV_HAS_HEADER) {

			/* ... and we actually use it */
			if (LDR(ldr)->flags & NOWDB_CSV_USE_HEADER) {

				/* ... we have a model */
				if (LDR(ldr)->flags & NOWDB_CSV_MODEL) {
					if (LDR(ldr)->flags &
					    NOWDB_CSV_VERTEX) {
						rowTypeHeader(ldr);
					} else {
						rowEdgeHeader(ldr);
					}
					return;
				}

			/* ... or we do not use the header */
			} 
			if ((LDR(ldr)->flags & NOWDB_CSV_MODEL) &&
			    (LDR(ldr)->flags & NOWDB_CSV_VERTEX)) {
				LDR(ldr)->err =  
					nowdb_err_get(nowdb_err_invalid,
					                  FALSE, OBJECT,
					  "vertex model without header");
			}
			return; /* header processed */

		/* if we do not have a header */
		} else if ((LDR(ldr)->flags & NOWDB_CSV_MODEL) &&
		           (LDR(ldr)->flags & NOWDB_CSV_VERTEX)) {
			LDR(ldr)->err =nowdb_err_get(
			               nowdb_err_invalid,
				           FALSE, OBJECT,
			   "vertex model without header");
			// stamped edge needs header too
			return;
		}
	}

	/* current row was regjected */
	if (LDR(ldr)->csv->rejected) {
		LDR(ldr)->csv->rejected = 0;
		LDR(ldr)->errors++;
		LDR(ldr)->csv->total++;

		LDR(ldr)->csv->pos=LDR(ldr)->csv->old;

		LDR(ldr)->csv->cur = 0;
		return;
	}

	uint32_t x = nowdb_ctrlStart(LDR(ldr)->csv->atts);

	if (LDR(ldr)->csv->xb != NULL) {
		memcpy(LDR(ldr)->csv->buf + 
		       LDR(ldr)->csv->pos + x,
		       LDR(ldr)->csv->xb,
		       LDR(ldr)->csv->ctlSize);
		memset(LDR(ldr)->csv->xb, 0, LDR(ldr)->csv->ctlSize);
	}

	/* finally: the normal processing */
	LDR(ldr)->csv->old=LDR(ldr)->csv->pos;
	LDR(ldr)->csv->pos+=LDR(ldr)->csv->recsize;
	LDR(ldr)->csv->fbcnt++;
	LDR(ldr)->csv->total++;

	/*
	fprintf(stderr, "%u + %u * %u == %u (%u)\n",
			LDR(ldr)->csv->pos, x,
			LDR(ldr)->csv->recsize,
	                LDR(ldr)->csv->pos + x *
	                LDR(ldr)->csv->recsize, BUFSIZE);
	*/

	if (LDR(ldr)->csv->pos +
	    LDR(ldr)->csv->recsize >= BUFSIZE) {
		insertBuf(ldr);
		LDR(ldr)->csv->pos = 0;
		LDR(ldr)->csv->old = 0;
		if (LDR(ldr)->csv->fbcnt >= FEEDBACK) {
			LDR(ldr)->csv->fbcnt = 0;
			/* give feedback */
		}
	}
	if (LDR(ldr)->err == NOWDB_OK) LDR(ldr)->loaded++;
}

/* ------------------------------------------------------------------------
 * Edge offsets
 * ------------------------------------------------------------------------
 */
#define ORIGIN  NOWDB_OFF_ORIGIN
#define DESTIN  NOWDB_OFF_DESTIN
#define STAMP   NOWDB_OFF_STAMP

/* ------------------------------------------------------------------------
 * Copy data to helper
 * ------------------------------------------------------------------------
 */
static inline int tohlp(nowdb_csv_t *csv, char *data,
                            size_t len, uint32_t off) {
	if (len > 31) return -1;
	memcpy(csv->hlp+off, data, len);
	csv->hlp[off+len] = 0;
	return 0;
}

/* ------------------------------------------------------------------------
 * Convert data to unsigned integer
 * ------------------------------------------------------------------------
 */
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

/* ------------------------------------------------------------------------
 * Convert data to integer
 * ------------------------------------------------------------------------
 */
static inline int toInt(nowdb_csv_t *csv, char *data,
                            size_t len, void *target) {
	uint64_t n=0;
	char    *tmp;
	if (len == 0) {
		memcpy(target, &n, sizeof(uint64_t)); return 0;
	}
	if (tohlp(csv, data, len, 0) != 0) return -1;
	n = strtoull(csv->hlp, &tmp, 10);
	if (*tmp != 0) return -1;
	memcpy(target, &n, sizeof(uint64_t));
	return 0;
}

/* ------------------------------------------------------------------------
 * Convert data to small uinteger
 * ------------------------------------------------------------------------
 */
static inline int toUInt32(nowdb_csv_t *csv, char *data,
                               size_t len, void *target) {
	uint32_t n=0;
	char    *tmp;
	if (len == 0) {
		memcpy(target, &n, sizeof(uint64_t)); return 0;
	}
	if (tohlp(csv, data, len, 64) != 0) return -1;
	n = (uint32_t)strtoul(csv->hlp+64, &tmp, 10);
	if (*tmp != 0) return -1;
	memcpy(target, &n, sizeof(uint32_t));
	return 0;
}

/* ------------------------------------------------------------------------
 * Macro to reject rows
 * ------------------------------------------------------------------------
 */
#define REJECT(n,s) \
	LDR(ldr)->csv->rejected = 1; \
	fprintf(LDR(ldr)->ostream, "[%09lu] %s %s\n", \
	                   LDR(ldr)->csv->total, n, s);

/* ------------------------------------------------------------------------
 * Macro to obtain a key field (edge, origin, destin, label, etc.)
 * ------------------------------------------------------------------------
 */
#define GETKEY(d, l, name, fld) \
	if (toUInt(LDR(ldr)->csv, data, l,\
	    LDR(ldr)->csv->buf+LDR(ldr)->csv->pos+fld) != 0) \
	{ \
		REJECT(name, "invalid key"); \
	}

/* ------------------------------------------------------------------------
 * Macro to obtain a timestamp field
 * ------------------------------------------------------------------------
 */
#define GETSTAMP(d, l, name, fld) \
	if (toInt(LDR(ldr)->csv, data, l,\
	    LDR(ldr)->csv->buf+LDR(ldr)->csv->pos+fld) != 0) \
	{ \
		REJECT(name, "invalid timestamp"); \
	}

/* ------------------------------------------------------------------------
 * Vertex offsets
 * ------------------------------------------------------------------------
 */

#define VERTEX  NOWDB_OFF_VERTEX

/* ------------------------------------------------------------------------
 * Handle errors
 * ------------------------------------------------------------------------
 */
#define HANDLEERR(ldr, err) \
	nowdb_err_send(err, fileno(ldr->ostream)); \
	nowdb_err_release(err); err = NOWDB_OK;

/* ------------------------------------------------------------------------
 * Convert text to key
 * ------------------------------------------------------------------------
 */
static inline char getKeyFromText(nowdb_loader_t *ldr,
                                  void *data, size_t len,
                                  void *target) {
	nowdb_err_t err;
	char *txt = ldr->csv->txt;

	memcpy(txt, data, len); txt[len] = 0;

	err = nowdb_text_insert(ldr->text, txt, target);
	if (err != NOWDB_OK) {
		HANDLEERR(ldr, err);
		return -1;
	}
	return 0;
}

/* ------------------------------------------------------------------------
 * String to int or uint
 * ------------------------------------------------------------------------
 */
#define STROX(t,to) \
	if (len > 255) { \
		return -1; \
	} \
	memcpy(ldr->csv->txt, data, len); \
	ldr->csv->txt[len] = 0; \
	*((t*)target) = to(ldr->csv->txt, &hlp, 10); \
	if (*hlp != 0) {  \
		return -1; \
	}

/* ------------------------------------------------------------------------
 * String to double
 * ------------------------------------------------------------------------
 */
#define STROD() \
	if (len > 255) { \
		return -1; \
	} \
	memcpy(ldr->csv->txt, data, len); \
	ldr->csv->txt[len] = 0; \
	*((double*)target) = strtod(ldr->csv->txt, &hlp); \
	if (*hlp != 0) {  \
		return -1; \
	}

/* ------------------------------------------------------------------------
 * String to time
 * ------------------------------------------------------------------------
 */
#define STRTOTIME() \
	if (len > 255) { \
		return -1; \
	} \
	memcpy(ldr->csv->txt, data, len); \
	ldr->csv->txt[len] = 0; \
	if (len == 10) { \
		rc = nowdb_time_fromString(ldr->csv->txt, \
		                    ldr->datefrm,target); \
	} else { \
		rc = nowdb_time_fromString(ldr->csv->txt, \
		                    ldr->timefrm,target); \
	} \
	if (rc != 0) { \
		err = nowdb_err_get(rc, FALSE, OBJECT, "time to string"); \
		HANDLEERR(ldr, err); \
		return -1; \
	}
	
/* ------------------------------------------------------------------------
 * Convert property value from string
 * ------------------------------------------------------------------------
 */
static inline char getValueAsType(nowdb_loader_t    *ldr,
                                  void *data, size_t len,
                                  nowdb_type_t       typ,
                                  void           *target) {
	nowdb_err_t err;
	char *hlp=NULL;
	int rc;

	switch(typ) {
	case NOWDB_TYP_NOTHING:
		memset(target, 0, sizeof(nowdb_key_t)); break;

	case NOWDB_TYP_TEXT:
		return getKeyFromText(ldr, data, len, target);

	case NOWDB_TYP_DATE:
	case NOWDB_TYP_TIME:
		STRTOTIME();
		break;

	case NOWDB_TYP_FLOAT:
		STROD();
		break;

	case NOWDB_TYP_INT:
		STROX(int64_t, strtoll);
		break;
		
	case NOWDB_TYP_UINT:
		STROX(uint64_t, strtoull);
		break;

	default: return -1;
	}
	return 0;
}

/* ------------------------------------------------------------------------
 * Vertex field callback with model
 * ------------------------------------------------------------------------
 */
void nowdb_csv_field_type(void *data, size_t len, void *ldr) {

	if (LDR(ldr)->err != NOWDB_OK) return;
	if (LDR(ldr)->csv->rejected) return;
	if (LDR(ldr)->csv->first) {
		fieldHeader(data, len, ldr);
		return;
	}
	if (len >= 1023) {
		REJECT("VERTEX", "value too big");
		return;
	}
	if (LDR(ldr)->csv->props == NULL) {
		LDR(ldr)->err = nowdb_err_get(nowdb_err_loader,
		                    FALSE, OBJECT, "no model");
		return;
	}

	int i = LDR(ldr)->csv->cur;
	int off = LDR(ldr)->csv->props[i].off;

	/* get PK (= vid) */
	if (i >= LDR(ldr)->csv->psz) {
		fprintf(stderr, "field %d!!!\n", i);
	}
	if (LDR(ldr)->csv->props[i].pk) {
		if (len == 0) {
			REJECT(LDR(ldr)->csv->props[i].name,
				      "primary key is NULL");
			return;
		}
		if (LDR(ldr)->csv->props[i].value == NOWDB_TYP_TEXT) {
			if (getKeyFromText(LDR(ldr), data, len,
			             &LDR(ldr)->csv->vid) != 0) {
				REJECT(LDR(ldr)->csv->props[i].name,
				                      "invalid key");
				return;
			}
		} else {
			if (toUInt(LDR(ldr)->csv, data, len,
			    &LDR(ldr)->csv->vid) != 0) {
				REJECT(LDR(ldr)->csv->props[i].name,
				                      "invalid key");
				return;
			}
		}
	}

	/* value according to type */
	if (len == 0) {
		memset(LDR(ldr)->csv->buf  + 
	               LDR(ldr)->csv->pos  + off, 0, 8);
	} else {
		if (getValueAsType(ldr, data, len,
	            LDR(ldr)->csv->props[i].value,
	            LDR(ldr)->csv->buf  +
	            LDR(ldr)->csv->pos  + off) != 0) {
			REJECT(LDR(ldr)->csv->props[i].name, "invalid value");
			return;
		}
		uint8_t  k;
		uint16_t d;
		nowdb_getCtrl(off, &k, &d);
		LDR(ldr)->csv->xb[d] |= 1<<k;
	}

	/* increment field counter */
	LDR(ldr)->csv->cur++;

	/* last field */
	if (LDR(ldr)->csv->cur >= LDR(ldr)->csv->psz) {
		LDR(ldr)->csv->cur = 0;

		nowdb_err_t err =
		nowdb_scope_registerVertex(LDR(ldr)->scope,
		                           LDR(ldr)->ctx,
		                           LDR(ldr)->csv->vid);
		if (err != NOWDB_OK) {
			char freemsg=1;
			char *msg = nowdb_err_describe(err, ';');
			if (msg == NULL) {
				msg = "out-of-mem"; freemsg = 0;
			}
			REJECT("cannot preregister vertex", msg);
			if (freemsg) free(msg);
			nowdb_err_release(err);
			return;
		}

	/* otherwise increase position */
	/*
	} else {
		LDR(ldr)->csv->pos += LDR(ldr)->csv->recsize;
	*/
	}
}

/* ------------------------------------------------------------------------
 * Edge field callback with model
 * ------------------------------------------------------------------------
 */
void nowdb_csv_field_edge(void *data, size_t len, void *ldr) {

	if (LDR(ldr)->err != NOWDB_OK) return;
	if (LDR(ldr)->csv->rejected) return;
	if (LDR(ldr)->csv->first) {
		fieldHeader(data, len, ldr);
		return;
	}
	if (len >= 1023) {
		REJECT("EDGE", "value too big");
		return;
	}
	if (LDR(ldr)->csv->pedge == NULL) {
		LDR(ldr)->err = nowdb_err_get(nowdb_err_loader,
		                    FALSE, OBJECT, "no model");
		return;
	}

	int i = LDR(ldr)->csv->cur;
	int off = LDR(ldr)->csv->pedge[i].off;

	if (len == 0) {
		// what if off is origin or destin or stamp?

		memset(LDR(ldr)->csv->buf + 
		       LDR(ldr)->csv->pos + off, 0, 8);

	} else {
		if (getValueAsType(ldr, data, len,
	            LDR(ldr)->csv->pedge[i].value,
	            LDR(ldr)->csv->buf  +
	            LDR(ldr)->csv->pos  + off) != 0) 
		{
			REJECT(LDR(ldr)->csv->pedge[i].name, "invalid value");
			return;
		}
		uint8_t  k;
		uint16_t d;
		nowdb_getCtrl(off, &k, &d);
		LDR(ldr)->csv->xb[d] |= 1<<k;
	}

	LDR(ldr)->csv->cur++;

	if (LDR(ldr)->csv->cur >= LDR(ldr)->csv->psz)
		LDR(ldr)->csv->cur = 0;
}
