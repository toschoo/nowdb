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
	row->vcur = 0;
	row->dirty = 0;
	row->vrtx = NULL;

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
	row->vrtx = calloc(row->sz, sizeof(nowdb_vertex_t));
	if (row->vrtx == NULL) {
		NOMEM("allocating vertex memory");
		free(row->fields); row->fields = NULL;
		return err;
	}
	row->eval.tlru = calloc(1, sizeof(nowdb_ptlru_t));
	if (row->eval.tlru == NULL) {
		NOMEM("allocating text lru");
		free(row->fields); row->fields = NULL;
		free(row->vrtx); row->vrtx = NULL;
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
		if (err != NOWDB_OK) {
			// destroy what we created
			break;
		}

		/*
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
		*/
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
	if (row->vrtx != NULL) {
		free(row->vrtx); row->vrtx = NULL;
	}
	nowdb_eval_destroy(&row->eval);
	
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
 * Helper: function projection
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t projectFun(nowdb_row_t *row,
                                     nowdb_group_t *group,
                                     char *buf, uint32_t sz,
                                     uint32_t *osz, char *nsp) {
	nowdb_err_t err;
	nowdb_type_t s;
	char t;

	*nsp = 0;

	err = nowdb_group_getType(group, row->fur, &s);
	if (err != NOWDB_OK) return err;
	if ((*osz)+8+1 >= sz) {
		*nsp = 1; return NOWDB_OK;
	}
	t = (char)s;
	memcpy(buf+*osz, &t, 1); (*osz)++;
	err = nowdb_group_result(group, row->fur, buf+*osz);
	if (err != NOWDB_OK) return err;
	(*osz)+=8; row->fur++;

	// fprintf(stderr, "%d, %0.4f\n", t, *(double*)(buf+*osz-8));

	if (row->fur >= group->lst) {
		nowdb_group_reset(group);
		row->fur = 0;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: constant projection
 * ------------------------------------------------------------------------
 */
/*
static inline nowdb_err_t projectConst(nowdb_field_t *field,
                                       char *buf, uint32_t sz,
                                       uint32_t *osz, char *nsp) {
	uint32_t s;
	char t=0;

	switch(field->typ) {
	case NOWDB_TYP_TEXT:
		s = strlen(field->value);
		if ((*osz)+s+2 > sz) {
			*nsp = 1; return NOWDB_OK;
		}
		t = (char)NOWDB_TYP_TEXT;
		memcpy(buf+(*osz), &t, 1); (*osz)++;
		memcpy(buf+(*osz), field->value, s+1); (*osz)+=s+1;
		break;

	case NOWDB_TYP_DATE:
	case NOWDB_TYP_TIME:
	case NOWDB_TYP_INT:
	case NOWDB_TYP_UINT:
	case NOWDB_TYP_FLOAT:
		HANDLEBINARY(field->value, field->typ);
		break;

	case NOWDB_TYP_BOOL:
		if ((*osz)+2 >= sz) {
			*nsp = 1; return NOWDB_OK;
		}
		t = (char)field->typ;
		memcpy(buf+(*osz), &t, 1); (*osz)++;
		memcpy(buf+(*osz), field->value, 1); (*osz)++;
		break;

	default: break;
	
	}
	return NOWDB_OK;
}
*/

/* ------------------------------------------------------------------------
 * Helper: edge projection
 * ------------------------------------------------------------------------
 */
/*
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

	// evaluate expression

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
*/

/* ------------------------------------------------------------------------
 * Helper: get vertex model
 * ------------------------------------------------------------------------
 */
/*
static inline nowdb_err_t getVertexModel(nowdb_row_t *row) {
	nowdb_err_t err;

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
			if (row->fields[i].name == NULL) continue;
			err = nowdb_model_getPropByName(row->model,
			                            row->v->roleid,
			                       row->fields[i].name,
                                                         row->p+i);
			if (err != NOWDB_OK) return err;
		}
	}
	return NOWDB_OK;
}
*/

/* ------------------------------------------------------------------------
 * Helper: vertex projection
 * ------------------------------------------------------------------------
 */
/*
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

	err = getVertexModel(row);
	if (err != NOWDB_OK) return err;

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
*/

static inline int findProp(nowdb_row_t *row, nowdb_vertex_t *v) {
	int k=-1;
	for(int i=0; i<row->sz; i++) {

		/*
		fprintf(stderr, "%s: %lu (%d) == %lu\n",
			row->fields[i].name,
			row->fields[i].propid,
		        row->p[i]->pk,
			v->property);
		*/


		/*
		if (row->fields[i].target != NOWDB_TARGET_VERTEX) continue;
		if (row->fields[i].propid == v->property) return i;
		*/

		/* this is stupid */
		/*
		if (row->sz == 1   &&
                    row->p != NULL && row->p[i]->pk &&
		    row->p[i]->roleid == v->role) k=i;
		*/
	}
	return k;
}

static inline uint32_t getSize(nowdb_type_t t, void *val) {
	switch(t) {
	case NOWDB_TYP_TEXT: return (uint32_t)strlen(val);
	case NOWDB_TYP_BOOL: return 1;
	case NOWDB_TYP_NOTHING: return 0;
	default: return 8;
	}
}

/* ------------------------------------------------------------------------
 * project
 * TODO:
 * we may have more than one src buffer!
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_row_project(nowdb_row_t *row,
                              nowdb_group_t *group,
                              char *src, uint32_t recsz,
                              char *buf, uint32_t sz,
                              uint32_t *osz, char *full,
                              char *cnt, char *ok) {
	nowdb_err_t err;
	void *val=NULL;
	nowdb_type_t t;
	uint32_t s=0;

	ROWNULL();

	*full=0;
	*ok = 0;
	*cnt = 0;
	if (row->cur == 0 && row->dirty) {
		if ((*osz)+1 < sz) {
			buf[*osz] = '\n';
			(*osz)++;
			row->dirty = 0;
			// fprintf(stderr, "dirty love\n");
			(*cnt)++; *ok=1;
			return NOWDB_OK;
		}
	}
	for(int i=row->cur; i<row->sz; i++) {
		err = nowdb_expr_eval(row->fields[i], &row->eval,
		                                   src, &t, &val);
		if (err != NOWDB_OK) return err;

		if (*osz + 1 >= sz) {
			row->cur = i; *full=1;
			return NOWDB_OK;
		}
		memcpy(buf+(*osz), &t, 1); (*osz)++;
		s = getSize(t, val);
		if (*osz + s >= sz) {
			row->cur = i; *full=1;
			return NOWDB_OK;
		}
		memcpy(buf+(*osz), val, s); *osz += s;

		/*
		if (row->fields[i].target == NOWDB_TARGET_NULL) {
			fprintf(stderr, "projecting constant\n");
			if (row->fields[i].value == NULL) continue;
			err = projectConst(row->fields+i, buf, sz, osz, &nsp);
			if (err != NOWDB_OK) return err;
			if (nsp) {
				row->cur = i; *full=1;
				return NOWDB_OK;
			}

		} else if (row->fields[i].target == NOWDB_TARGET_EDGE) {
			if (row->fields[i].flags & NOWDB_FIELD_AGG) {
				err = projectFun(row, group, buf, sz,
				                          osz, &nsp);
			} else {
				err = projectEdgeField(row,
				     (nowdb_edge_t*)src, i,
			               buf, sz, osz, &nsp);
			}
			if (err != NOWDB_OK) return err;
			if (nsp) {
				row->cur = i; *full=1;
				return NOWDB_OK;
			}

		} else {
			// this is all quite disgusting.
			// we urgently need a clean solution
			// vor vertices! 
			*ok = 1;

			// we need the vertex model to decide
			// what is primary key
			if (row->v == NULL) {
				memcpy(row->vrtx, src,
				  sizeof(nowdb_vertex_t));
				err = getVertexModel(row);
				if (err != NOWDB_OK) return err;
			}
			int k = findProp(row, (nowdb_vertex_t*)src);
			if (k < 0) return NOWDB_OK;
			if (!row->dirty) row->dirty = 1;
			memcpy(row->vrtx+k, src, sizeof(nowdb_vertex_t));

			// if we have the primary key, force
			// the primary key vertex into value
			if (row->p != NULL && row->p[k]->pk) {
				memcpy(&row->vrtx[k].value,
				       &row->vrtx[k].vertex,8);
			}
			row->cur++;
			if (row->cur == row->sz) break;
			return NOWDB_OK;
		}
		if (!row->dirty) row->dirty = 1;
		*/
	}
	/*
	if (*ok && recsz == sizeof(nowdb_vertex_t)) {
		err = projectVertex(row, buf, sz, osz, &nsp);
		if (err != NOWDB_OK) return err;
		if (nsp) {
			*full=1;
			return NOWDB_OK;
		}
	}
	*/
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
