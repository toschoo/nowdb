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
	nowdb_err_t err;
	ts_algo_list_node_t *runner;
	int i=0;

	ROWNULL();

	if (fields == NULL) return nowdb_err_get(nowdb_err_invalid,
	                           FALSE, OBJECT, "fields is NULL");
	if (scope == NULL) return nowdb_err_get(nowdb_err_invalid,
	                           FALSE, OBJECT, "scope is NULL");

	row->model = scope->model;
	row->text  = scope->text;
	row->v = NULL;
	row->p = NULL;
	row->e = NULL;
	row->o = NULL;
	row->d = NULL;

	row->fields = NULL;

	row->sz = fields->len;
	if (row->sz == 0) return NOWDB_OK;

	row->fields = calloc(row->sz,sizeof(nowdb_field_t));
	if (fields == NULL) {
		NOMEM("allocating fields");
		return err;
	}
	for(runner=fields->head; runner!=NULL; runner=runner->nxt) {
		// field names!
		memcpy(row->fields+i, runner->cont, sizeof(nowdb_field_t));i++;
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
		// field names!
		free(row->fields); row->fields = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Helper: edge projection
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t projectEdge(nowdb_row_t *row,
                                      nowdb_edge_t *src,
                                      int i,
                                      char *buf,
                                      uint32_t sz,
                                      uint32_t *osz) {
	nowdb_err_t err;
	size_t s;
	char   t;
	char *str;
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
		if ((*osz)+s+2 >= sz) return NOWDB_OK;
		memcpy(buf+*osz, &t, 1); (*osz)++;
		memcpy(buf+*osz, row->e->name, s+1); (*osz)+=s+1;
		break;

	case NOWDB_OFF_ORIGIN:
		if (row->o->vid == NOWDB_MODEL_TEXT) {
			t = (char)NOWDB_TYP_TEXT;
			err = nowdb_text_getText(row->text,
			                src->origin, &str);
			if (err != NOWDB_OK) return err;
			s = strlen(str);
			if ((*osz)+s+2 >= sz) return NOWDB_OK;
			memcpy(buf+*osz, &t, 1); (*osz)++;
			memcpy(buf+*osz, str, s+1); (*osz)+=s+1;
		} else {
			t = (char)NOWDB_TYP_UINT;
			if ((*osz)+9 >= sz) return NOWDB_OK;
			memcpy(buf+*osz, &t, 1); (*osz)++;
			memcpy(buf+*osz, &src->origin, 8); (*osz)+=8;
		}
		break;

	case NOWDB_OFF_DESTIN:
		if (row->d->vid == NOWDB_MODEL_TEXT) {
			t = (char)NOWDB_TYP_TEXT;
			err = nowdb_text_getText(row->text,
			                 src->destin, &str);
			if (err != NOWDB_OK) return err;
			s = strlen(str);
			if ((*osz)+s+2 >= sz) return NOWDB_OK;
			memcpy(buf+*osz, &t, 1); (*osz)++;
			memcpy(buf+*osz, str, s+1); (*osz)+=s+1;
		} else {
			t = (char)NOWDB_TYP_UINT;
			if ((*osz)+9 >= sz) return NOWDB_OK;
			memcpy(buf+*osz, &t, 1); (*osz)++;
			memcpy(buf+*osz, &src->destin, 8); (*osz)+=8;
		}
		break;

	case NOWDB_OFF_LABEL:
		if (row->e->label == NOWDB_MODEL_TEXT) {
			t = (char)NOWDB_TYP_TEXT;
			err = nowdb_text_getText(row->text,
			                  src->label, &str);
			if (err != NOWDB_OK) return err;
			s = strlen(str);
			if ((*osz)+s+2 >= sz) return NOWDB_OK;
			memcpy(buf+*osz, &t, 1); (*osz)++;
			memcpy(buf+*osz, str, s+1); (*osz)+=s+1;
		} else {
			t = (char)NOWDB_TYP_UINT;
			if ((*osz)+9 >= sz) return NOWDB_OK;
			memcpy(buf+*osz, &t, 1); (*osz)++;
			memcpy(buf+*osz, &src->label, 8); (*osz)+=8;
		}
		break;

	case NOWDB_OFF_TMSTMP:
		t = (char)NOWDB_TYP_TEXT;
		if ((*osz)+33 >= sz) return NOWDB_OK;
		memcpy(buf+(*osz), &t, 1); (*osz)++;
		err = nowdb_time_toString(src->timestamp,
		                       NOWDB_TIME_FORMAT,
		                       buf+*osz, 32);
		if (err != NOWDB_OK) return err;
		(*osz)+=strlen(buf+*osz)+1;
		break;

	case NOWDB_OFF_WEIGHT:
		break;

	case NOWDB_OFF_WEIGHT2:
		break;
	}

	return NOWDB_OK;
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
                              uint32_t *osz) {
	nowdb_err_t err;

	ROWNULL();

	for(int i=0; i<row->sz; i++) {
		if (row->fields[i].target == NOWDB_TARGET_EDGE) {
			err = projectEdge(row, (nowdb_edge_t*)src, i,
			                               buf, sz, osz);
			if (err != NOWDB_OK) return err;

		} else {
			/* TODO */
			fprintf(stderr, "not yet working...\n");
		}
	}
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
nowdb_err_t nowdb_row_write(char *buf, int fd);
