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
	nowdb_field_t *tmp;
	int i=0;

	ROWNULL();

	if (fields == NULL) return nowdb_err_get(nowdb_err_invalid,
	                           FALSE, OBJECT, "fields is NULL");
	if (scope == NULL) return nowdb_err_get(nowdb_err_invalid,
	                           FALSE, OBJECT, "scope is NULL");

	row->model = scope->model;
	row->text  = scope->text;
	row->tlru = NULL;
	row->v = NULL;
	row->p = NULL;
	row->e = NULL;
	row->o = NULL;
	row->d = NULL;
	row->cur = 0;
	row->vcur = 0;
	row->dirty = 0;
	row->vrtx = NULL;

	row->fields = NULL;

	row->sz = fields->len;
	if (row->sz == 0) return NOWDB_OK;

	row->fields = calloc(row->sz,sizeof(nowdb_field_t));
	if (row->fields == NULL) {
		NOMEM("allocating fields");
		return err;
	}
	row->vrtx = calloc(row->sz, sizeof(nowdb_vertex_t));
	if (row->vrtx == NULL) {
		NOMEM("allocating vertex memory");
		free(row->fields); row->fields = NULL;
		return err;
	}
	row->tlru = calloc(1, sizeof(nowdb_ptlru_t));
	if (row->tlru == NULL) {
		NOMEM("allocating text lru");
		free(row->fields); row->fields = NULL;
		free(row->vrtx); row->vrtx = NULL;
		return err;
	}
	err = nowdb_ptlru_init(row->tlru, 100000);
	if (err != NOWDB_OK) {
		nowdb_row_destroy(row);
		return err;
	}
	for(runner=fields->head; runner!=NULL; runner=runner->nxt) {
		tmp = runner->cont;
		memcpy(row->fields+i, tmp, sizeof(nowdb_field_t));
		row->fields[i].name = NULL;
		if (tmp->name != NULL) {
			row->fields[i].name = strdup(tmp->name);
			if (row->fields[i].name == NULL) {
				NOMEM("allocating field name");
				row->sz = i; break;
			}
			err = nowdb_text_getKey(row->text,
			              row->fields[i].name,
			           &row->fields[i].propid);
			if (err != NOWDB_OK) break;
		}
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
			if (row->fields[i].name != NULL) {
				free(row->fields[i].name);
				row->fields[i].name = NULL;
			}
		}
		free(row->fields); row->fields = NULL;
	}
	if (row->vrtx != NULL) {
		free(row->vrtx); row->vrtx = NULL;
	}
	if (row->p != NULL) {
		free(row->p); row->p = NULL;
	}
	if (row->tlru != NULL) {
		nowdb_ptlru_destroy(row->tlru);
		free(row->tlru); row->tlru = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Helper: get text and cache it
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getText(nowdb_row_t *row,
                                  nowdb_key_t  key,
                                  char       **str) {
	nowdb_err_t err;

	err = nowdb_ptlru_get(row->tlru, key, str);
	if (err != NOWDB_OK) return err;

	if (*str != NULL) return NOWDB_OK;

	err = nowdb_text_getText(row->text, key, str);
	if (err != NOWDB_OK) return err;

	return nowdb_ptlru_add(row->tlru, key, *str);
}

/* ------------------------------------------------------------------------
 * save a lot of code
 * ------------------------------------------------------------------------
 */
#define HANDLETEXT(what) \
	t = (char)NOWDB_TYP_TEXT; \
	err = getText(row, what, &str); \
	if (err != NOWDB_OK) return err; \
	s = strlen(str); \
	if ((*osz)+s+2 >= sz) { \
		*nsp=1; \
		return NOWDB_OK; \
	} \
	memcpy(buf+*osz, &t, 1); (*osz)++; \
	memcpy(buf+*osz, str, s+1); (*osz)+=s+1;

#define HANDLEBINARY(what, how) \
	t = (char)how; \
	if ((*osz)+9 >= sz) { \
		*nsp=1; return NOWDB_OK; \
	} \
	memcpy(buf+*osz, &t, 1); (*osz)++; \
	memcpy(buf+*osz, what, 8); (*osz)+=8;

/* ------------------------------------------------------------------------
 * Helper: edge projection
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t projectEdgeField(nowdb_row_t *row,
                                           nowdb_edge_t *src,
                                           int i,
                                           char *buf,
                                           uint32_t sz,
                                           uint32_t *osz,
                                           char *nsp) {
	nowdb_value_t *w=NULL;
	nowdb_type_t typ;
	nowdb_err_t err;
	size_t s;
	char   t;
	char *str;

	*nsp = 0;
	if (row->e == NULL || row->e->edgeid != src->edge) {
		err = nowdb_model_getEdgeById(row->model,
		                      src->edge, &row->e);
		if (err != NOWDB_OK) return err;

		err = nowdb_model_getVertexById(row->model,
		                  row->e->origin, &row->o);
		if (err != NOWDB_OK) return err;

		err = nowdb_model_getVertexById(row->model,
		                   row->e->destin, &row->d);
		if (err != NOWDB_OK) return err;
	}
	switch(row->fields[i].off) {

	case NOWDB_OFF_EDGE:
		t = (char)NOWDB_TYP_TEXT;
		s = strlen(row->e->name);
		if ((*osz)+s+2 >= sz) {
			*nsp = 1; return NOWDB_OK;
		}
		memcpy(buf+*osz, &t, 1); (*osz)++;
		memcpy(buf+*osz, row->e->name, s+1); (*osz)+=s+1;
		break;

	case NOWDB_OFF_ORIGIN:
		if (row->o->vid == NOWDB_MODEL_TEXT) {
			HANDLETEXT(src->origin);
		} else {
			HANDLEBINARY(&src->origin, NOWDB_TYP_UINT);
		}
		break;

	case NOWDB_OFF_DESTIN:
		if (row->d->vid == NOWDB_MODEL_TEXT) {
			HANDLETEXT(src->destin);
		} else {
			HANDLEBINARY(&src->destin, NOWDB_TYP_UINT);
		}
		break;

	case NOWDB_OFF_LABEL:
		if (row->e->label == NOWDB_MODEL_TEXT) {
			HANDLETEXT(src->label);
		} else {
			HANDLEBINARY(&src->label, NOWDB_TYP_UINT);
		}
		break;

	case NOWDB_OFF_TMSTMP:
		HANDLEBINARY(&src->timestamp, NOWDB_TYP_TIME);
		break;

	case NOWDB_OFF_WEIGHT:
		w = &src->weight;
		typ = row->e->weight;

	case NOWDB_OFF_WEIGHT2:
		if (w == NULL) {
			w = &src->weight2;
			typ = row->e->weight2;
		}
		switch(typ) {
		case NOWDB_TYP_TEXT:
			HANDLETEXT(*w); break;

		case NOWDB_TYP_DATE:
		case NOWDB_TYP_TIME:
		case NOWDB_TYP_FLOAT:
		case NOWDB_TYP_INT:
		case NOWDB_TYP_UINT:
			HANDLEBINARY(w, typ); break;

		default: break;
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: vertex projection
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t projectVertex(nowdb_row_t *row,
                                        char *buf,
                                        uint32_t sz,
                                        uint32_t *osz,
                                        char *nsp) {
	nowdb_err_t err;
	size_t s;
	char   t;
	char *str;

	*nsp = 0;
	if (row->v == NULL) {
		err = nowdb_model_getVertexById(row->model,
		                row->vrtx[0].role, &row->v);
		if (err != NOWDB_OK) return err;
	}
	if (row->p == NULL) {
		row->p = calloc(row->sz, sizeof(nowdb_model_prop_t*));
		if (row->p == NULL) {
			NOMEM("allocating props");
			return err;
		}
		for(int i=0;i<row->sz;i++) {
			err = nowdb_model_getPropByName(row->model,
			                            row->v->roleid,
			                       row->fields[i].name,
                                                         row->p+i);
			if (err != NOWDB_OK) return err;
		}
	}
	for(int i=row->vcur; i<row->sz; i++) {
		switch(row->p[i]->value) {
		case NOWDB_TYP_TEXT:
			HANDLETEXT(row->vrtx[i].value); break;

		case NOWDB_TYP_DATE:
		case NOWDB_TYP_TIME:
		case NOWDB_TYP_FLOAT:
		case NOWDB_TYP_INT:
		case NOWDB_TYP_UINT:
			HANDLEBINARY(&row->vrtx[i].value,
			                row->p[i]->value);
			break;

		default:
			break;
		}
		row->vcur++;
	}
	row->vcur = 0;
	return NOWDB_OK;
}

static inline int findProp(nowdb_row_t *row, nowdb_vertex_t *v) {
	for(int i=0; i<row->sz; i++) {
		/*
		fprintf(stderr, "%s: %lu == %lu\n",
			row->fields[i].name,
			row->fields[i].propid,
			v->property);
		*/
		if (row->fields[i].propid == v->property) return i;
	}
	return -1;
}

/* ------------------------------------------------------------------------
 * project
 * TODO:
 * we may have more than one src buffer!
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_row_project(nowdb_row_t *row,
                              char *src, uint32_t recsz,
                              char *buf, uint32_t sz,
                              uint32_t *osz,
                              char *cnt, char *ok) {
	nowdb_err_t err;
	char nsp = 0;

	ROWNULL();

	*ok = 0;
	*cnt = 0;
	if (row->cur == 0 && row->dirty) {
		if ((*osz)+1 < sz) {
			buf[*osz] = '\n';
			(*osz)++;
			row->dirty = 0;
			(*cnt)++;
		}
	}
	for(int i=row->cur; i<row->sz; i++) {
		if (row->fields[i].target == NOWDB_TARGET_EDGE) {
			err = projectEdgeField(row, (nowdb_edge_t*)src, i,
			                              buf, sz, osz, &nsp);
			if (err != NOWDB_OK) return err;
			if (nsp) return NOWDB_OK;

		} else {
			*ok = 1;
			int k = findProp(row, (nowdb_vertex_t*)src);
			if (k < 0) return NOWDB_OK;
			if (!row->dirty) row->dirty = 1;
			memcpy(row->vrtx+k, src, sizeof(nowdb_vertex_t));
			row->cur++;
			if (row->cur == row->sz) break;
			return NOWDB_OK;
		}
		if (!row->dirty) row->dirty = 1;
	}
	if (recsz == sizeof(nowdb_vertex_t)) {
		err = projectVertex(row, buf, sz, osz, &nsp);
		if (err != NOWDB_OK) return err;
		if (nsp) return NOWDB_OK;
	}
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
 * switch group
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_row_group(nowdb_row_t *row);

/* ------------------------------------------------------------------------
 * transform projected buffer to string
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_row_toString(char  *buf,
                               char **str);

/* ------------------------------------------------------------------------
 * write (e.g. print) buffer
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_row_write(char *buf, uint32_t sz, FILE *stream) {
	nowdb_err_t err;
	char tmp[32];
	uint32_t i=0;
	char t;
	while(i<sz) {
		t = buf[i]; i++;
		if (t == '\n') {
			fprintf(stream, "\n"); continue;
		}
		switch((nowdb_type_t)t) {
		case NOWDB_TYP_TEXT:
			fprintf(stream, "%s;", buf+i);
			i+=strlen(buf+i)+1; break;

		case NOWDB_TYP_DATE: 
		case NOWDB_TYP_TIME: 
			err = nowdb_time_toString(*(nowdb_time_t*)(buf+i),
		                                        NOWDB_TIME_FORMAT,
		                                                  tmp, 32);
			if (err != NOWDB_OK) return err;
			fprintf(stream, "%s;", tmp); i+=8; break;

		case NOWDB_TYP_INT: 
			fprintf(stream, "%ld;", *(int64_t*)(buf+i));
			i+=8; break;

		case NOWDB_TYP_UINT: 
			fprintf(stream, "%lu;", *(uint64_t*)(buf+i));
			i+=8; break;

		case NOWDB_TYP_FLOAT: 
			fprintf(stream, "%.4f;", *(double*)(buf+i));
			i+=8; break;

		default:
			return nowdb_err_get(nowdb_err_invalid,
			  FALSE, OBJECT, "unknown type in row");
		}
	}
	return NOWDB_OK;
}
