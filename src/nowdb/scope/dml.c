/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Scope DML services
 * ========================================================================
 */
#include <nowdb/scope/dml.h>

static char *OBJECT = "dml";

#define INVALID(m) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, m);

#define INVALIDVAL(m) \
	err = nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, m);

#define NOMEM(m) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, m);

/* ------------------------------------------------------------------------
 * Initialise dml helper
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_dml_init(nowdb_dml_t   *dml, 
                           nowdb_scope_t *scope,
                           char       withCache) {
	nowdb_err_t err;

	if (dml ==NULL) INVALID("dml is NULL");

	dml->trgname = NULL;
	dml->scope = scope;
	dml->store = NULL;
	dml->tlru  = NULL;
	dml->v = NULL;
	dml->p = NULL;
	dml->pe = NULL;
	dml->e = NULL;
	dml->o = NULL;
	dml->d = NULL;
	dml->propn = 0;
	dml->pedgen = 0;

	if (withCache) {
		dml->tlru = calloc(1,sizeof(nowdb_pklru_t));
		if (dml->tlru == NULL) {
			NOMEM("allocating lru cache");
			return err;
		}
		err = nowdb_pklru_init(dml->tlru, 1000000);
		if (err != NOWDB_OK) {
			free(dml->tlru); dml->tlru = NULL;
			return err;
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Destroy dml helper
 * ------------------------------------------------------------------------
 */
void nowdb_dml_destroy(nowdb_dml_t *dml) {
	if (dml == NULL) return;

	if (dml->trgname != NULL) {
		free(dml->trgname); dml->trgname = NULL;
	}
	if (dml->tlru != NULL) {
		nowdb_pklru_destroy(dml->tlru);
		free(dml->tlru ); dml->tlru = NULL;
	}
	if (dml->p != NULL) {
		free(dml->p); dml->p = NULL; dml->propn = 0;
	}
	if (dml->pe != NULL) {
		free(dml->pe); dml->pe = NULL; dml->pedgen = 0;
	}
}

/* ------------------------------------------------------------------------
 * Get context and edge
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getEdge(nowdb_dml_t *dml,
                                  char    *trgname,
                                  char      *found) {
	nowdb_err_t err;
	nowdb_context_t *ctx;

	err = nowdb_scope_getContext(dml->scope, trgname, &ctx);
	if (err != NOWDB_OK) {
		if (nowdb_err_contains(err, nowdb_err_key_not_found)) {
			nowdb_err_release(err);
			return NOWDB_OK;
		}
	}
	dml->content = NOWDB_CONT_EDGE;
	dml->store = &ctx->store;

	err = nowdb_model_getEdgeByName(dml->scope->model,
	                                 trgname, &dml->e);
	if (err != NOWDB_OK) return err;

	*found=1;

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Are we context or vertex?
 * ------------------------------------------------------------------------
 */
static nowdb_err_t getContextOrVertex(nowdb_dml_t *dml,
                                      char    *trgname) {
	nowdb_err_t err;
	char x=0;

	err = getEdge(dml, trgname, &x);
	if (err != NOWDB_OK) return err;
	if (x) return NOWDB_OK;

	err = nowdb_model_getVertexByName(dml->scope->model,trgname,&dml->v);
	if (err != NOWDB_OK) return err;

	dml->content = NOWDB_CONT_VERTEX;
	dml->store = &dml->scope->vertices;

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Get vertex properties from model
 * ------------------------------------------------------------------------
 */
static nowdb_err_t getVertexProperties(nowdb_dml_t *dml,
                                 ts_algo_list_t *fields) {
	nowdb_err_t err;
	char havePK = fields==NULL;
	ts_algo_list_t props;
	ts_algo_list_node_t *run;
	int len;
	int i;

	if (dml->p != NULL) {
		free(dml->p); dml->p = NULL; dml->propn = 0;
	}
	if (fields == NULL) {
		ts_algo_list_init(&props);
		err = nowdb_model_getProperties(dml->scope->model,
			                        dml->v->roleid,
			                        &props);
		if (err != NOWDB_OK) {
			ts_algo_list_destroy(&props);
			return err;
		}
		len = props.len;
	} else {
		len = fields->len;
	}

	dml->p = calloc(len, sizeof(nowdb_model_prop_t*));
	if (dml->p == NULL) {
		NOMEM("allocating properties");
		if (fields == NULL) ts_algo_list_destroy(&props);
		return err;
	}
	dml->propn = len;
	i = 0;
	for(run=fields==NULL?props.head:fields->head;
	    run!=NULL; run=run->nxt) {
		if (fields == NULL) {
			dml->p[i] = run->cont;
		} else {
			err = nowdb_model_getPropByName(dml->scope->model,
			                                dml->v->roleid,
			                                run->cont,
			                                dml->p+i);
			if (err != NOWDB_OK) break;
			if (dml->p[i]->pk) havePK = 1;
		}
		i++;
	}
	if (fields == NULL) ts_algo_list_destroy(&props);
	if (err != NOWDB_OK) {
		free(dml->p); dml->p = NULL; dml->propn = 0;
		return err;
	}
	if (!havePK) {
		free(dml->p); dml->p = NULL; dml->propn = 0;
		INVALIDVAL("PK not in list");
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Is our vertex representation in canonical order?
 * ------------------------------------------------------------------------
 */
static char propsSorted(nowdb_dml_t *dml) {
	for(int i=0; i<dml->propn; i++) {
		if (dml->p[i]->pos != i) return 0;
	}
	return 1;
}

/* ------------------------------------------------------------------------
 * Get edge properties from model
 * ------------------------------------------------------------------------
 */
static nowdb_err_t getEdgeProperties(nowdb_dml_t *dml,
                               ts_algo_list_t *fields) {
	nowdb_err_t err;
	ts_algo_list_t props;
	ts_algo_list_node_t *run;
	int len;
	int i;

	if (dml->pe != NULL) {
		free(dml->pe); dml->pe = NULL; dml->pedgen = 0;
	}
	if (fields == NULL) {
		ts_algo_list_init(&props);
		err = nowdb_model_getPedges(dml->scope->model,
			                    dml->e->edgeid,&props);
		if (err != NOWDB_OK) {
			ts_algo_list_destroy(&props);
			return err;
		}
		len = props.len;
	} else {
		len = fields->len;
	}

	dml->pe = calloc(len, sizeof(nowdb_model_pedge_t*));
	if (dml->pe == NULL) {
		NOMEM("allocating properties");
		if (fields == NULL) ts_algo_list_destroy(&props);
		return err;
	}
	dml->pedgen = len;
	i = 0;
	for(run=fields==NULL?props.head:fields->head;
	    run!=NULL; run=run->nxt) {
		if (fields == NULL) {
			dml->pe[i] = run->cont;
		} else {
			err = nowdb_model_getPedgeByName(dml->scope->model,
			                                 dml->e->edgeid,
			                                 run->cont,
			                                 dml->pe+i);
			if (err != NOWDB_OK) break;
		}
		i++;
	}
	if (fields == NULL) ts_algo_list_destroy(&props);
	if (err != NOWDB_OK) {
		free(dml->pe); dml->pe = NULL; dml->pedgen = 0;
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Do we insert the complete vertex?
 * ------------------------------------------------------------------------
 */
static char vertexComplete(nowdb_dml_t *dml,
                     ts_algo_list_t *fields, 
                     ts_algo_list_t *values) 
{
	ts_algo_list_node_t *run;
	int i=0;

	if (dml->p == NULL) return 0;
	if (fields == NULL) {
		if (values->len != dml->propn) return 0;
		return propsSorted(dml);
	}
	if (dml->propn != fields->len) return 0;
	for(run=fields->head; run!=NULL; run=run->nxt) {
		if (strcasecmp(dml->p[i]->name, (char*)run->cont) != 0) {
			return 0;
		}
		i++;
	}
	return 1;
}

/* ------------------------------------------------------------------------
 * Do we insert the complete edge?
 * ------------------------------------------------------------------------
 */
static char edgeComplete(nowdb_dml_t *dml,
                   ts_algo_list_t *fields, 
                   ts_algo_list_t *values) 
{
	ts_algo_list_node_t *run;
	int i=0;

	if (dml->pe == NULL) return 0;
	if (fields == NULL) {
		if (values->len != dml->pedgen) return 0;
		// return pedgesSorted(dml);
		return 1;
	}
	if (dml->pedgen != fields->len) return 0;
	// origin / destin / timestamp
	for(run=fields->head; run!=NULL; run=run->nxt) {
		if (strcasecmp(dml->p[i]->name, (char*)run->cont) != 0) {
			return 0;
		}
		i++;
	}
	return 1;
}

/* ------------------------------------------------------------------------
 * Set target according to name
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_dml_setTarget(nowdb_dml_t *dml,
                                char    *trgname,
                          ts_algo_list_t *fields,
                          ts_algo_list_t *values) {
	nowdb_err_t err;

	if (dml == NULL) INVALID("no dml descriptor");
	if (dml->scope == NULL) INVALID("no scope in dml descriptor");
	if (trgname == NULL) INVALID("no target name");

	// fprintf(stderr, "SETTING CONTENT %s\n", trgname);

	// check fields == values
	if (fields != NULL && values != NULL) {
		if (fields->len != values->len) {
			INVALID("field list and value list differ");
		}
	}
	if (dml->trgname != NULL) {
		if (strcasecmp(dml->trgname,
	                            trgname) == 0) {
			if (dml->content == NOWDB_CONT_EDGE) {
				if (edgeComplete(dml, fields, values)) {
					return NOWDB_OK;
				} else {
					return getEdgeProperties(
					              dml,fields);
				}
			} else {
				if (vertexComplete(dml, fields, values)) {
					return NOWDB_OK;
				} else {
					return getVertexProperties(
					                dml,fields);
				}
			}
		}
		free(dml->trgname); dml->trgname = NULL;
	}

	dml->trgname = strdup(trgname);
	if (dml->trgname == NULL) {
		NOMEM("allocating target name");
		return err;
	}

	// get context or vertex
	err = getContextOrVertex(dml, trgname);
	if (err != NOWDB_OK) return err;

	if (dml->content == NOWDB_CONT_EDGE) {
		return getEdgeProperties(dml,fields);
	} else {
		return getVertexProperties(dml, fields);
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Insert row in row format
 * TODO!
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_dml_insertRow(nowdb_dml_t *dml,
                                char        *row,
                                uint32_t     sz);

/* ------------------------------------------------------------------------
 * Helper: get key and cache it
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getKey(nowdb_dml_t *dml,
                                 char        *str,
                                 nowdb_key_t *key) {
	nowdb_err_t err;
	char *mystr;
	char found=0;

	err = nowdb_pklru_get(dml->tlru, str, key, &found);
	if (err != NOWDB_OK) return err;

	if (found) return NOWDB_OK;

	err = nowdb_text_insert(dml->scope->text, str, key);
	if (err != NOWDB_OK) return err;

	mystr = strdup(str);
	if (mystr == NULL) {
		NOMEM("allocating string");
		return err;
	}
	return nowdb_pklru_add(dml->tlru, mystr, *key);
}

/* ------------------------------------------------------------------------
 * Set edge model
 * ------------------------------------------------------------------------
 */
/*
static inline nowdb_err_t setEdgeModel(nowdb_dml_t *dml) {
	nowdb_err_t err;
	nowdb_key_t eid;

	err = getKey(dml, edge, &eid);
	if (err != NOWDB_OK) return err;

	if (dml->e != NULL) {
		if (dml->e->edgeid == eid) return NOWDB_OK;
	}

	err = nowdb_model_getEdgeById(dml->scope->model, eid, &dml->e);
	if (err != NOWDB_OK) return err;

	err = nowdb_model_getVertexById(dml->scope->model,
	                          dml->e->origin, &dml->o);
	if (err != NOWDB_OK) return err;

	err = nowdb_model_getVertexById(dml->scope->model,
	                          dml->e->destin, &dml->d);
	if (err != NOWDB_OK) return err;

	return NOWDB_OK;
}
*/

/* ------------------------------------------------------------------------
 * Get edge model
 * ------------------------------------------------------------------------
 */
/*
static inline nowdb_err_t getEdgeModel(nowdb_dml_t *dml,
                                 ts_algo_list_t *fields,
                                 ts_algo_list_t *values) {
	ts_algo_list_node_t *frun, *vrun, *edge=NULL;
	nowdb_simple_value_t *val;
	char tp=0;
	int off=-1;

	vrun = values->head;
	if (fields != NULL) {
		for(frun=fields->head; frun!=NULL; frun=frun->nxt) {
			off = nowdb_edge_offByName(frun->cont);
			if (off == NOWDB_OFF_EDGE) {
				edge=vrun; if (tp) break;
			} else if (off == NOWDB_OFF_TMSTMP) {
				tp=1; if (edge != NULL) break;
			}
			vrun = vrun->nxt;
		}
		if (edge == NULL) {
			INVALID("edge field missing in insert");
		}
		if (!tp) {
			INVALID("timestamp missing in insert");
		}
		val = edge->cont;
	} else {
		if (values == NULL) INVALID("no values");
	}
	// fprintf(stderr, "%s\n", (char*)val->value);
	return setEdgeModel(dml);
}
*/

/* ------------------------------------------------------------------------
 * Check edge model
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t checkEdgeValue(nowdb_dml_t          *dml,
                                         uint32_t off,       int i,
                                         char               *fname,
                                         nowdb_simple_value_t *val) {
	nowdb_model_pedge_t *pe;
	nowdb_err_t err;

	switch(off) {
	case NOWDB_OFF_ORIGIN:
	case NOWDB_OFF_DESTIN:
		if (fname == NULL) {
			pe = dml->pe[i];
		} else {
			err = nowdb_model_getPedgeByName(dml->scope->model,
			                                 dml->e->edgeid,
			                                 fname, &pe);
			if (err != NOWDB_OK) return err;
		}
		if  (pe->value == NOWDB_TYP_TEXT &&
		     val->type != NOWDB_TYP_TEXT) {
			if (off == NOWDB_OFF_ORIGIN) {
				INVALID("origin must be text");
			} else {
				INVALID("destin must be text");
			}
		} else if (pe->value != NOWDB_MODEL_TEXT &&
		           val->type != NOWDB_TYP_UINT) {
			if (off == NOWDB_OFF_ORIGIN) {
				INVALID("origin must be unsigned integer");
			} else {
				INVALID("destin must be unsigned integer");
			}
		}
		break;

	case NOWDB_OFF_STAMP:
		if (val->type == NOWDB_TYP_TEXT) {
			val->type = NOWDB_TYP_TIME; break;
		}
		if (val->type != NOWDB_TYP_INT  &&
		    val->type != NOWDB_TYP_UINT) {
			INVALID("not a valid timestamp");
		}
		break;

	default:
		if (fname == NULL) {
			pe = dml->pe[i];
		} else {
			err = nowdb_model_getPedgeByName(dml->scope->model,
			                                 dml->e->edgeid,
			                                 fname, &pe);
			if (err != NOWDB_OK) return err;
		}
		if (nowdb_correctType(pe->value,
		                    &val->type,
		                     val->value) != 0) {
			INVALID("invalid type");
		}

	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * String to int or uint
 * ------------------------------------------------------------------------
 */
#define STROX(t,to) \
	*((t*)target) = to(val->value, &hlp, 10); \
	if (*hlp != 0) {  \
		err = nowdb_err_get(nowdb_err_invalid, \
		      FALSE, OBJECT, "string to int"); \
	}

/* ------------------------------------------------------------------------
 * String to double
 * ------------------------------------------------------------------------
 */
#define STROD() \
	*((double*)target) = strtod(val->value, &hlp); \
	if (*hlp != 0) {  \
		err = nowdb_err_get(nowdb_err_invalid, \
		      FALSE, OBJECT, "string to double"); \
	}

/* ------------------------------------------------------------------------
 * String to time
 * ------------------------------------------------------------------------
 */
#define STRTOTIME() \
	if (strnlen(val->value, 4096) == 10) { \
		rc = nowdb_time_fromString(val->value, \
		             NOWDB_DATE_FORMAT,target);\
	} else { \
		rc = nowdb_time_fromString(val->value,\
		            NOWDB_TIME_FORMAT,target);\
	} \
	if (rc != 0) { \
		err = nowdb_err_get(rc, FALSE, OBJECT, "string to time"); \
	}
	
/* ------------------------------------------------------------------------
 * Convert property value from string
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getValueAsType(nowdb_dml_t *dml,
                                nowdb_simple_value_t *val,
                                         void     *target) {
	nowdb_err_t err=NOWDB_OK;
	char *hlp=NULL;
	int rc;

	switch(val->type) {
	case NOWDB_TYP_NOTHING:
		memset(target, 0, sizeof(nowdb_key_t)); break;

	case NOWDB_TYP_TEXT:
		// fprintf(stderr, "TEXT: %s\n", (char*)val->value);
		return getKey(dml, val->value, target);

	case NOWDB_TYP_DATE:
	case NOWDB_TYP_TIME:
		// fprintf(stderr, "TIME: %s\n", (char*)val->value);
		STRTOTIME();
		break;

	case NOWDB_TYP_FLOAT:
		// fprintf(stderr, "FLOAT: %s\n", (char*)val->value);
		STROD();
		break;

	case NOWDB_TYP_INT:
		// fprintf(stderr, "INT: %s\n", (char*)val->value);
		STROX(int64_t, strtol);
		break;

	case NOWDB_TYP_UINT:
		// fprintf(stderr, "UINT: %s\n", (char*)val->value);
		STROX(uint64_t, strtoul);
		break;

	// bool

	default: INVALID("unknown type");
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Copy value to edge descriptor
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t copyEdgeValue(nowdb_dml_t   *dml,
                                        char         *edge,
                                        uint32_t       off,
                                 nowdb_simple_value_t *val) {
	nowdb_err_t err;
	err = getValueAsType(dml, val, edge+off);
	if (err != NOWDB_OK) return err;

	if (off < NOWDB_OFF_USER) return NOWDB_OK;

	uint8_t bit;
	uint16_t byte;

	nowdb_edge_getCtrl(dml->e->num, off, &bit, &byte);

	char *xbyte = edge+NOWDB_OFF_USER+byte;
	(*xbyte) |= 1<<bit;

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Get off by name
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getEdgeOffByName(nowdb_dml_t *dml,
                                           char       *name,
                                           uint32_t    *off) {
	nowdb_err_t err;
	nowdb_model_pedge_t *p;

	err = nowdb_model_getPedgeByName(dml->scope->model,
	                                 dml->e->edgeid,
	                                 name, &p);
	if (err != NOWDB_OK) return err;

	*off = p->off;

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Insert edge in fields/values format
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t insertEdgeFields(nowdb_dml_t *dml,
                                     ts_algo_list_t *fields,
                                     ts_algo_list_t *values) 
{
	nowdb_err_t err = NOWDB_OK;
	ts_algo_list_node_t *vrun, *frun=NULL;
	nowdb_simple_value_t *val;
	char *edge;
	uint32_t off=0;
	int i=0;

	edge = calloc(1, dml->e->size);
	if (edge == NULL) {
		NOMEM("allocating edge");
		return err;
	}
	if (fields != NULL) {
		frun = fields->head;
	}
	for(vrun=values->head; vrun!=NULL; vrun=vrun->nxt) {
		char *nm=NULL;

		// get offset from name
		if (fields != NULL) {
			err = getEdgeOffByName(dml, frun->cont, &off);
			if (err != NOWDB_OK) break;
			nm = frun->cont;
		} else {
			off = dml->pe[i]->off;
		}

		val = vrun->cont;
		if (val == NULL) {
			INVALIDVAL("value descriptor is NULL");
			break;
		}

		// check and correct value type
		err = checkEdgeValue(dml, off, i, nm, val);
		if (err != NOWDB_OK) break;
		
		// write to edge (get edge model like in row)
		err = copyEdgeValue(dml, edge, off, val);
		if (err != NOWDB_OK) break;

		if (fields != NULL) {
			frun = frun->nxt;
		} else {
			i++;
		}

	}
	if (err != NOWDB_OK) {
		free(edge); return err;
	}
	if (fields == NULL && i < dml->pedgen) {
		INVALIDVAL("not enough fields");
		free(edge); return err;
	}

	/*
	fprintf(stderr, "INSERTING %lu-%lu @%ld\n",
	                 edge.origin,
	                 edge.destin,
	                 edge.timestamp);
	*/

	err = nowdb_store_insert(dml->store, edge);
	free(edge); return err;
}

/* ------------------------------------------------------------------------
 * Correct type:
 * This is chaotic! We should once and for all define the correction
 * in types and use it everywhere...
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t correctType(uint32_t typ,
                         nowdb_simple_value_t *val) {
	nowdb_err_t err;
	// this is all googledemoogle
	if (typ == NOWDB_TYP_TIME ||
	    typ == NOWDB_TYP_DATE) {
		if (val->type == NOWDB_TYP_TEXT) {
			val->type = typ;
			return NOWDB_OK;
		}
		if (val->type == NOWDB_TYP_UINT) {
			val->type = NOWDB_TYP_INT;
			return NOWDB_OK;
		}
		if (val->type == NOWDB_TYP_INT) {
			return NOWDB_OK;
		}
	}
	/* this is as it should be!!!
	if (nowdb_correctType(typ, &val->type, val->value) != 0) {
		INVALIDVAL("cannot correct value");
		return err;
	}
	*/
	INVALIDVAL("types differ");
	return err;
}

/* ------------------------------------------------------------------------
 * Insert vertex in fields/value format
 * NOTE: this relies on setTarget
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t insertVertexFields(nowdb_dml_t *dml,
                                       ts_algo_list_t *fields,
                                       ts_algo_list_t *values) {
	nowdb_err_t err=NOWDB_OK;
	ts_algo_list_node_t *vrun;
	nowdb_simple_value_t *val;
	nowdb_vertex_t vrtx;
	int i=0;
	int pkfound=0;

	memset(&vrtx, 0, sizeof(nowdb_vertex_t));
	for(vrun=values->head; vrun!=NULL; vrun=vrun->nxt) {
		val = vrun->cont;
		if (dml->p[i]->value != val->type) {
			err = correctType(dml->p[i]->value, val);
			if (err != NOWDB_OK) return err;
		}
		// store primary key in vertex
		if (dml->p[i]->pk) {
			err = getValueAsType(dml, val, &vrtx.vertex);
			if (err != NOWDB_OK) return err;
			pkfound = 1;

		// check all other values
		} else {
			err = getValueAsType(dml, val, &vrtx.value);
			if (err != NOWDB_OK) return err;
		}
		i++;
	}
	if (!pkfound) {
		INVALIDVAL("no pk in vertex");
		return err;
	}

	// now we have role and vertex
	i=0;
	vrtx.role = dml->v->roleid;

	// now register the vertex...
	// (we should lock here and keep the lock)
	err = nowdb_scope_registerVertex(dml->scope,
	                                 vrtx.role,
	                                 vrtx.vertex);
	if (err != NOWDB_OK) return err;

	// ... and store all attributes
	for(vrun=values->head; vrun!=NULL; vrun=vrun->nxt) {
		val = vrun->cont;
		vrtx.property = dml->p[i]->propid;
		vrtx.vtype = dml->p[i]->value;

		// no error shall occur, we already checked it
		err = getValueAsType(dml, val, &vrtx.value);
		if (err != NOWDB_OK) break;

		/*
		fprintf(stderr, "inserting vertex %u / %lu / %lu / %u\n",
		                 vrtx.role, vrtx.vertex,
		                 vrtx.property, vrtx.vtype);
		*/

		err = nowdb_store_insert(dml->store, &vrtx);
		if (err != NOWDB_OK) break;
		i++;
	}
	/*
	 * if things fail here,
	 * we have already registered the vertex
	 * and we have inserted parts of it.
	 * we need to:
	 * - delete everything we inserted
	 * - and unregister
	 */
	return err;
}

/* ------------------------------------------------------------------------
 * Insert in fields/value format
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_dml_insertFields(nowdb_dml_t *dml,
                             ts_algo_list_t *fields,
                             ts_algo_list_t *values) 
{
	if (dml->content == NOWDB_CONT_EDGE) {
		return insertEdgeFields(dml, fields, values);
	} else {
		return insertVertexFields(dml, fields, values);
	}
}
