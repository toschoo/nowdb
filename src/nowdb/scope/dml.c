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
	dml->ctx = NULL;
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
 * Are we context or vertex?
 * ------------------------------------------------------------------------
 */
static nowdb_err_t getEdgeOrVertex(nowdb_dml_t *dml,
                                   char    *trgname) {
	nowdb_err_t err;
	nowdb_context_t *ctx;

	err = nowdb_scope_getContext(dml->scope, trgname, &ctx);
	if (err != NOWDB_OK) return NOWDB_OK;

	dml->ctx     = ctx;

	if (ctx->store.cont == NOWDB_CONT_EDGE) {
		err = nowdb_model_getEdgeByName(dml->scope->model,
		                                 trgname,&dml->e);
		dml->content = NOWDB_CONT_EDGE;
		if (err != NOWDB_OK) return err;
	} else {
		dml->content = NOWDB_CONT_VERTEX;
		err = nowdb_model_getVertexByName(dml->scope->model,
		                                   trgname,&dml->v);
	}
	if (err != NOWDB_OK) return err;
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
		if (strcasecmp(dml->pe[i]->name, (char*)run->cont) != 0) {
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

	// get edge or vertex
	err = getEdgeOrVertex(dml, trgname);
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

	/*
	case NOWDB_OFF_STAMP:
		if (val->type == NOWDB_TYP_DATE ||
                    val->type == NOWDB_TYP_TIME) break;

		if (val->type != NOWDB_TYP_INT  &&
		    val->type != NOWDB_TYP_UINT) {
			INVALID("not a valid timestamp");
		}
		break;
	*/

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
 * Convert property value from string
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getValueAsType(nowdb_dml_t *dml,
                                nowdb_simple_value_t *val,
                                         void     *target) {
	nowdb_err_t err=NOWDB_OK;

	switch(val->type) {
	case NOWDB_TYP_NOTHING:
		memset(target, 0, sizeof(nowdb_key_t)); break;

	case NOWDB_TYP_TEXT:
		// fprintf(stderr, "TEXT: %s\n", (char*)val->value);
		return getKey(dml, val->value, target);
		
	case NOWDB_TYP_DATE:
	case NOWDB_TYP_TIME:
	case NOWDB_TYP_FLOAT:
	case NOWDB_TYP_INT:
	case NOWDB_TYP_UINT:
		memcpy(target, val->value, sizeof(nowdb_key_t)); break;

	case NOWDB_TYP_BOOL:
		// error: we cannot insert booleans
		INVALID("boolean cannot be inserted");
	
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
	if (val->type == NOWDB_TYP_NOTHING) return NOWDB_OK;

	uint8_t bit;
	uint16_t byte;

	nowdb_edge_getCtrl(dml->e->num, off, &bit, &byte);

	char *xbyte = edge+NOWDB_OFF_USER+byte;
	(*xbyte) |= 1<<bit;

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Get off by name (vertex)
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getVertexOffByName(nowdb_dml_t *dml,
                                             char       *name,
                                             uint32_t    *off) {
	nowdb_err_t err;
	nowdb_model_prop_t *p;

	err = nowdb_model_getPropByName(dml->scope->model,
	                                dml->v->roleid,
	                                name, &p);
	if (err != NOWDB_OK) return err;

	*off = p->off;

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Get off by name (edge)
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
	char stamped;
	char o=0,d=0,t=0;
	int i=0;

	stamped = dml->e->size > NOWDB_OFF_STAMP;
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

		if (off >= dml->e->size) {
			INVALIDVAL("unknown field"); break;
		}

		switch(off) {
		case NOWDB_OFF_ORIGIN: o++; break;
		case NOWDB_OFF_DESTIN: d++; break;
		// case NOWDB_OFF_STAMP:  t++; break;
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
	if (o < 1) {
		INVALIDVAL("no origin in edge");
	} else if (o > 1) {
		INVALIDVAL("multiple values for origin");
	} else if (d < 1) {
		INVALIDVAL("no destin in edge");
	} else if (d > 1) {
		INVALIDVAL("multiple values for destin");
	} else if (stamped) {
		if (t < 1) {
			INVALIDVAL("no timestamp in stamped edge");
		} else if (t > 1) {
			INVALIDVAL("multiple value for timestamp");
		}
	} else {
		if (t != 0) INVALIDVAL("timestamp in unstamped edge");
	}
	if (err != NOWDB_OK) {
		free(edge); return err;
	}

	/*
	fprintf(stderr, "INSERTING %lu-%lu @%ld\n",
	                 edge.origin,
	                 edge.destin,
	                 edge.timestamp);
	*/

	err = nowdb_store_insert(&dml->ctx->store, edge);
	free(edge); return err;
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
	char *vrtx, *ctrl;
	int i=0;
	int pkfound=0;

	vrtx = calloc(1, dml->v->size);
 	if (vrtx == NULL) {
		NOMEM("allocating vertex");
		return err;
	}
	ctrl = vrtx+nowdb_vrtx_ctrlStart(dml->v->num);
	for(vrun=values->head; vrun!=NULL; vrun=vrun->nxt) {
		val = vrun->cont;
		if (val == NULL) {
			INVALIDVAL("value descriptor is NULL"); break;
		}
		if (dml->p[i]->value != val->type) {
			if (nowdb_correctType(dml->p[i]->value,
			                      &val->type,
			                      val->value) != 0) {
				INVALIDVAL("types differ"); break;
			}
		}
		// store primary key in vertex
		if (dml->p[i]->pk) {
			err = getValueAsType(dml, val, vrtx);
			if (err != NOWDB_OK) return err;
			if (val->type == NOWDB_TYP_NOTHING) {
				INVALIDVAL("pk is NULL"); break;
			}
			pkfound = 1;

		// check all other values
		} else {
			err = getValueAsType(dml, val, vrtx+dml->p[i]->off);
			if (err != NOWDB_OK) return err;
		}
		// set control block
		if (val->type != NOWDB_TYP_NOTHING) {
			uint8_t bit;
			uint16_t byte;
			nowdb_vrtx_getCtrl(dml->p[i]->off,
                                           &bit, &byte);
			char *xbyte = ctrl+byte;
			(*xbyte) |= 1<<bit;
		}
		i++;
	}
	if (!pkfound) {
		INVALIDVAL("no pk in vertex");
	}
	// stamp found || not required

	if (err != NOWDB_OK) {
		free(vrtx); return err;
	}

	// now we have role and vertex
	i=0;

	// now register the vertex...
	// (we should lock here and keep the lock)
	err = nowdb_scope_registerVertex(dml->scope,
	                                 dml->ctx,0,
	                                 *(uint64_t*)vrtx);
	if (err != NOWDB_OK) {
		free(vrtx); return err;
	}

	err = nowdb_store_insert(&dml->ctx->store, vrtx);
	/*
	 * We need unregister vertex!
	if (err != NOWDB_OK) {
		nowdb_scope_unregisterVertex(dml->scope,
                                             dml->ctx,0,
                                             vrtx);
	}
	*/

	free(vrtx);
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
