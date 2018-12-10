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
	dml->e = NULL;
	dml->o = NULL;
	dml->d = NULL;
	dml->num = 0;

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
		free(dml->p); dml->p = NULL; dml->num = 0;
	}
}

/* ------------------------------------------------------------------------
 * Are we context or vertex?
 * ------------------------------------------------------------------------
 */
static nowdb_err_t getContextOrVertex(nowdb_dml_t *dml,
                                      char    *trgname) {
	nowdb_err_t err1, err2;
	nowdb_context_t *ctx;

	err1 = nowdb_scope_getContext(dml->scope, trgname, &ctx);
	if (err1 == NOWDB_OK) {
		dml->target = NOWDB_TARGET_EDGE;
		dml->store = &ctx->store;
		return NOWDB_OK;
	}

	if (!nowdb_err_contains(err1, nowdb_err_key_not_found)) return err1;

	err2 = nowdb_model_getVertexByName(dml->scope->model,trgname,&dml->v);
	if (err2 != NOWDB_OK) {
		err2->cause = err1; return err2;
	}

	nowdb_err_release(err1);

	dml->target = NOWDB_TARGET_VERTEX;
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
		free(dml->p); dml->p = NULL; dml->num = 0;
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
	dml->num = len;
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
		free(dml->p); dml->p = NULL; dml->num = 0;
		return err;
	}
	if (!havePK) {
		free(dml->p); dml->p = NULL; dml->num = 0;
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
	for(int i=0; i<dml->num; i++) {
		if (dml->p[i]->pos != i) return 0;
	}
	return 1;
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
		if (values->len != dml->num) return 0;
		return propsSorted(dml);
	}
	if (dml->num != fields->len) return 0;
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

	// fprintf(stderr, "SETTING TARGET %s\n", trgname);

	// check fields == values
	if (fields != NULL && values != NULL) {
		if (fields->len != values->len) {
			INVALID("field list and value list differ");
		}
	}
	if (dml->trgname != NULL) {
		if (strcasecmp(dml->trgname,
	                            trgname) == 0) {
			if (dml->target == NOWDB_TARGET_EDGE) {
				return NOWDB_OK;
			} else if (vertexComplete(dml, fields, values)) {
				return NOWDB_OK;
			} else {
				return getVertexProperties(dml, fields);
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

	if (dml->target == NOWDB_TARGET_VERTEX) {
		err = getVertexProperties(dml, fields);
		if (err != NOWDB_OK) return err;
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
static inline nowdb_err_t setEdgeModel(nowdb_dml_t *dml,
                                       char       *edge) {
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

/* ------------------------------------------------------------------------
 * Get edge model
 * ------------------------------------------------------------------------
 */
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
		if (values->len < 7) INVALID("incomplete value list");
		if (values->len > 7) INVALID("too many values");
		val = values->head->cont;
	}
	// fprintf(stderr, "%s\n", (char*)val->value);
	return setEdgeModel(dml, val->value);
}

/* ------------------------------------------------------------------------
 * Check edge model
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t checkEdgeValue(nowdb_dml_t          *dml,
                                         uint32_t              off,
                                         nowdb_simple_value_t *val) {
	switch(off) {
	case NOWDB_OFF_EDGE:
		if (val->type != NOWDB_TYP_TEXT) {
			INVALID("edge must be text");
		}
		break;

	case NOWDB_OFF_ORIGIN:
		if (dml->o->vid == NOWDB_MODEL_TEXT &&
		    val->type != NOWDB_TYP_TEXT) {
			INVALID("origin must be text");
		} else if (dml->o->vid != NOWDB_MODEL_TEXT &&
		           val->type != NOWDB_TYP_UINT) {
			INVALID("origin must be unsigned integer");
		}
		break;

	case NOWDB_OFF_DESTIN:
		if (dml->d->vid == NOWDB_MODEL_TEXT &&
		    val->type != NOWDB_TYP_TEXT) {
			INVALID("destination must be text");
		} else if (dml->d->vid != NOWDB_MODEL_TEXT &&
		           val->type != NOWDB_TYP_UINT) {
			INVALID("destination must be unsigned integer");
		}
		break;

	case NOWDB_OFF_LABEL:
		if (dml->e->label == NOWDB_MODEL_TEXT &&
		    val->type != NOWDB_TYP_TEXT) {
			INVALID("label must be text");
		} else if (dml->e->label != NOWDB_MODEL_TEXT &&
		           val->type != NOWDB_TYP_UINT) {
			INVALID("label must be unsigned integer");
		}
		break;

	case NOWDB_OFF_TMSTMP:
		if (val->type == NOWDB_TYP_TEXT) {
			val->type = NOWDB_TYP_TIME; break;
		}
		if (val->type != NOWDB_TYP_INT  &&
		    val->type != NOWDB_TYP_UINT) {
			INVALID("not a valid timestamp");
		}
		break;

	case NOWDB_OFF_WEIGHT:
		if (dml->e->weight == NOWDB_TYP_INT &&
		    val->type == NOWDB_TYP_UINT) {
			val->type = NOWDB_TYP_INT;
		} else if (dml->e->weight == NOWDB_TYP_TIME &&
		        (val->type == NOWDB_TYP_UINT      ||
                         val->type == NOWDB_TYP_INT)) {
			break;
		} else if (dml->e->weight == NOWDB_TYP_TIME &&
		           val->type == NOWDB_TYP_TEXT) {
			val->type = NOWDB_TYP_TIME;
		} else if (dml->e->weight != val->type) {
			INVALID("wrong type in weight");
		}
		break;
		

	case NOWDB_OFF_WEIGHT2:
		if (dml->e->weight2 == NOWDB_TYP_INT &&
		    val->type == NOWDB_TYP_UINT) {
			val->type = NOWDB_TYP_INT;
		} else if (dml->e->weight2 == NOWDB_TYP_TIME &&
		        (val->type == NOWDB_TYP_UINT      ||
                         val->type == NOWDB_TYP_INT)) {
			break;
		} else if (dml->e->weight2 == NOWDB_TYP_TIME &&
		           val->type == NOWDB_TYP_TEXT) {
			val->type = NOWDB_TYP_TIME;
		} else if (dml->e->weight2 != val->type) {
			INVALID("wrong type in weight");
		}
		break;

	default:
		INVALID("unknown type");

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
                                        nowdb_edge_t *edge,
                                        uint32_t       off,
                                 nowdb_simple_value_t *val) {
	return getValueAsType(dml, val, ((char*)edge)+off);
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
	nowdb_edge_t edge;
	int off=0;

	err = getEdgeModel(dml, fields, values);
	if (err != NOWDB_OK) return err;

	memset(&edge, 0, NOWDB_EDGE_SIZE);

	if (fields != NULL) {
		frun = fields->head;
	}
	for(vrun=values->head; vrun!=NULL; vrun=vrun->nxt) {

		// get offset from name
		if (fields != NULL) {
			off = nowdb_edge_offByName(frun->cont);
			if (off < 0) {
				INVALIDVAL("unknown field name");
				break;
			}
		}

		if (off > NOWDB_OFF_WEIGHT2) {
			INVALIDVAL("too many fields");
			break;
		}

		val = vrun->cont;
		if (val == NULL) {
			INVALIDVAL("value descriptor is NULL");
			break;
		}

		// check and correct value type
		err = checkEdgeValue(dml, off, val);
		if (err != NOWDB_OK) break;
		
		// write to edge (get edge model like in row)
		err = copyEdgeValue(dml, &edge, off, val);
		if (err != NOWDB_OK) break;

		if (off == NOWDB_OFF_WEIGHT) {
			edge.wtype[0] = val->type;
		} else if (off == NOWDB_OFF_WEIGHT2) {
			edge.wtype[1] = val->type;
		}
		
		if (fields != NULL) {
			frun = frun->nxt;
		} else {
			off += 8;
		}
	}
	if (err != NOWDB_OK) return err;
	if (fields == NULL && off < NOWDB_OFF_WTYPE) {
		INVALIDVAL("not enough fields");
		return err;
	}

	/*
	fprintf(stderr, "INSERTING %lu-%lu-%lu @%ld\n",
	                 edge.edge,
	                 edge.origin,
	                 edge.destin,
	                 edge.timestamp);
	*/

	return nowdb_store_insert(dml->store, &edge);
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
	if (dml->target == NOWDB_TARGET_EDGE) {
		return insertEdgeFields(dml, fields, values);
	} else {
		return insertVertexFields(dml, fields, values);
	}
}
