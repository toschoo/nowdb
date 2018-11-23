/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Projection services
 * ========================================================================
 */

#include <nowdb/query/row.h>

static char *OBJECT = "row";

#define NOMEM(m) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, m);

#define ROWNULL() \
	if (row == NULL) return nowdb_err_get(nowdb_err_invalid, \
	                          FALSE, OBJECT, "row is NULL");

/* ------------------------------------------------------------------------
 * initialise the row
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_row_init(nowdb_row_t       *row,
                           nowdb_scope_t   *scope,
                           ts_algo_list_t *fields) {
	nowdb_err_t err=NOWDB_OK;
	ts_algo_list_node_t *runner;
	nowdb_expr_t tmp;
	int i=0;

	ROWNULL();

	if (fields == NULL) return nowdb_err_get(nowdb_err_invalid,
	                           FALSE, OBJECT, "fields is NULL");
	if (scope == NULL) return nowdb_err_get(nowdb_err_invalid,
	                           FALSE, OBJECT, "scope is NULL");

	row->eval.model = scope->model;
	row->eval.text  = scope->text;
	row->eval.tlru = NULL;
	row->eval.ce = NULL;
	row->eval.cv = NULL;
	row->eval.needtxt = 1;
	row->cur = 0;
	row->fur = 0;
	row->dirty = 0;

	row->fields = NULL;

	row->sz = fields->len;
	if (row->sz == 0) return NOWDB_OK;

	ts_algo_list_init(&row->eval.em);
	ts_algo_list_init(&row->eval.vm);

	row->fields = calloc(row->sz,sizeof(nowdb_expr_t));
	if (row->fields == NULL) {
		NOMEM("allocating fields");
		return err;
	}
	row->eval.tlru = calloc(1, sizeof(nowdb_ptlru_t));
	if (row->eval.tlru == NULL) {
		NOMEM("allocating text lru");
		free(row->fields); row->fields = NULL;
		return err;
	}
	err = nowdb_ptlru_init(row->eval.tlru, 100000);
	if (err != NOWDB_OK) {
		nowdb_row_destroy(row);
		return err;
	}
	for(runner=fields->head; runner!=NULL; runner=runner->nxt) {
		tmp = runner->cont;
		err = nowdb_expr_copy(tmp, row->fields+i);
		if (err != NOWDB_OK) break;
		i++;
	}
	if (err != NOWDB_OK) {
		nowdb_row_destroy(row); 
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * destroy the row
 * ------------------------------------------------------------------------
 */
void nowdb_row_destroy(nowdb_row_t *row) {
	if (row == NULL) return;
	if (row->fields != NULL) {
		for(int i=0;i<row->sz;i++) {
			if (row->fields[i] != NULL) {
				nowdb_expr_destroy(row->fields[i]);
				free(row->fields[i]);
				row->fields[i] = NULL;
			}
		}
		free(row->fields); row->fields = NULL;
	}
	nowdb_eval_destroy(&row->eval);
	
}

/* ------------------------------------------------------------------------
 * Compute size necessary to store type
 * ------------------------------------------------------------------------
 */
static inline uint32_t getSize(nowdb_type_t t, void *val) {
	switch(t) {
	case NOWDB_TYP_TEXT: return (uint32_t)strlen(val)+1;
	case NOWDB_TYP_BOOL: return 1;
	case NOWDB_TYP_NOTHING: return 0;
	default: return 8;
	}
}

/* ------------------------------------------------------------------------
 * raw project
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t copybuf(char *src, uint32_t recsz,
                                  char *buf, uint32_t sz,
                                             uint32_t *osz,
                                  char *cnt,
                                  char *full, char *ok)
{
	if (*osz+recsz >= sz) {
		*full=1; return NOWDB_OK;
	}
	memcpy(buf+(*osz), src, recsz);
	(*osz)+=recsz; (*cnt)++; *ok=1;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * project
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_row_project(nowdb_row_t *row,
                              char *src, uint32_t recsz,
                              uint64_t pmap,
                              char *buf, uint32_t sz,
                              uint32_t *osz, char *full,
                              char *cnt, char *ok) {
	nowdb_err_t err;
	void *val=NULL;
	nowdb_type_t typ;
	char t;
	uint32_t s=0;

	*full=0;
	*ok  =0;
	*cnt =0;

	// fprintf(stderr, "my pmap: %lu\n", pmap);

	if (row == NULL) return copybuf(src, recsz,
	                                buf,    sz,
	                                osz,   cnt,
	                                full,  ok);

	// we still need to write the linebreak
	// of the previous row
	if (row->cur == 0 && row->dirty) {
		if ((*osz)+1 < sz) {
			buf[*osz] = '\n';
			(*osz)++;
			row->dirty = 0;
			(*cnt)++; *ok=1;
			return NOWDB_OK;
		}
	}

	// project the fields
	for(int i=row->cur; i<row->sz; i++) {
		err = nowdb_expr_eval(row->fields[i],
	                             &row->eval, pmap,
		                      src, &typ, &val);
		if (err != NOWDB_OK) return err;
		t = (char)typ;
		s = getSize(t, val);
		if (*osz + s + 1 >= sz) {
			row->cur = i; *full=1;
			return NOWDB_OK;
		}
		memcpy(buf+(*osz), &t, 1); (*osz)++;
		if (s > 0) {
			memcpy(buf+(*osz), val, s); *osz+=s;
		}
		row->dirty = 1;

		// fprintf(stderr, "%d: %u\n", i, *osz);
	}

	// terminate line
	if ((*osz)+1 < sz) {
		buf[*osz] = '\n';
		(*osz)++;
		*ok = 1;
		(*cnt)++;
		row->dirty = 0;
	}
	row->cur = 0;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * transform projected buffer to string
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_row_toString(char  *buf,
                               char **str);

/* ------------------------------------------------------------------------
 * Extract a row from the buffer
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_row_extractRow(char    *buf, uint32_t   sz,
                                 uint32_t row, uint32_t *idx) {
	char t;
	uint32_t i=0;
	uint32_t r=0;

	while(i<sz) {
		if (r == row) break;
		t = buf[i]; i++;
		if (t == '\n') {
			r++; continue;
		}
		switch((nowdb_type_t)t) {
		case NOWDB_TYP_TEXT:
			i+=strlen(buf+i)+1; break;

		case NOWDB_TYP_DATE: 
		case NOWDB_TYP_TIME: 
		case NOWDB_TYP_UINT: 
		case NOWDB_TYP_INT: 
			i+=8; break;

		case NOWDB_TYP_BOOL: 
			i+=1; break;

		default:
			return nowdb_err_get(nowdb_err_invalid,
			  FALSE, OBJECT, "unknown type in row");
		}
	}
	if (r < row) {
		return nowdb_err_get(nowdb_err_not_found,
		       FALSE, OBJECT, "not enough rows");
	}
	*idx = i;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Extract a field from the buffer
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_row_extractField(char      *buf, uint32_t   sz,
                                   uint32_t field, uint32_t *idx) {
	char t;
	uint32_t i=0;
	uint32_t f=0;

	if (f == field) {
		*idx = 1; return NOWDB_OK;
	}
	while(i<sz) {
		t = buf[i]; i++;
		if (t == '\n') {
			return nowdb_err_get(nowdb_err_not_found,
			      FALSE, OBJECT, "not enough fields");
		}
		if (f == field) break;

		switch((nowdb_type_t)t) {
		case NOWDB_TYP_TEXT:
			// fprintf(stderr, "text at %u (%u)\n", i, f);
			i+=strlen(buf+i)+1; break;

		case NOWDB_TYP_DATE: 
		case NOWDB_TYP_TIME: 
		case NOWDB_TYP_UINT: 
		case NOWDB_TYP_INT: 
			// fprintf(stderr, "number at %u (%u)\n", i, f);
			i+=8; break;

		case NOWDB_TYP_BOOL: 
			// fprintf(stderr, "bool at %u (%u)\n", i, f);
			i+=1; break;

		default:
			return nowdb_err_get(nowdb_err_invalid,
			  FALSE, OBJECT, "unknown type in row");
		}
		f++;
	}
	if (f != field) {
		return nowdb_err_get(nowdb_err_not_found,
		      FALSE, OBJECT, "not enough fields");
	}
	*idx=i;
	return NOWDB_OK;
}
