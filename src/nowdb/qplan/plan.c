/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Execution Plan
 * ========================================================================
 */
#include <nowdb/qplan/plan.h>
#include <nowdb/reader/filter.h>
#include <nowdb/query/row.h>
#include <nowdb/fun/fun.h>

#include <string.h>

static char *OBJECT = "plan";

/* ------------------------------------------------------------------------
 * Macro to wrap the frequent error "invalid ast"
 * ------------------------------------------------------------------------
 */
#define INVALIDAST(s) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, s)

/* ------------------------------------------------------------------------
 * Macro to wrap the frequent error "no memory"
 * ------------------------------------------------------------------------
 */
#define NOMEM(s) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, s)

/* ------------------------------------------------------------------------
 * Get field:
 * ----------
 * - sets the offset into the structure
 * - the size of the field
 * - the type of the field (if known!)
 * TODO:
 * => use offByName instead and unify!!!
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getField(char            *name,
                                   nowdb_ast_t      *trg,
                                   uint32_t         *off,
                                   uint32_t          *sz,
                                   char            isstr,
                                   nowdb_type_t    *type) {
	if (strcasecmp(name, "VID") == 0) {
		*off = NOWDB_OFF_VERTEX; *sz = 8;
		*type = isstr?NOWDB_TYP_TEXT:NOWDB_TYP_UINT;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "PROPERTY") == 0) {
		*off = NOWDB_OFF_PROP; *sz = 8;
		*type = isstr?NOWDB_TYP_TEXT:NOWDB_TYP_UINT;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "VALUE") == 0) {
		*off = NOWDB_OFF_VALUE; *sz = 8;
		*type = isstr?NOWDB_TYP_TEXT:NOWDB_TYP_UINT;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "VTYPE") == 0) {
		*off = NOWDB_OFF_VTYPE; *sz = 4;
		*type = NOWDB_TYP_INT;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "ROLE") == 0) {
		*off = NOWDB_OFF_ROLE; *sz = 4;
		*type = isstr?NOWDB_TYP_TEXT:NOWDB_TYP_UINT;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "EDGE") == 0) {
		*off = NOWDB_OFF_EDGE; *sz = 8;
		*type = isstr?NOWDB_TYP_TEXT:NOWDB_TYP_UINT;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "ORIGIN") == 0) {
		*off = NOWDB_OFF_ORIGIN; *sz = 8;
		*type = isstr?NOWDB_TYP_TEXT:NOWDB_TYP_UINT;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "DESTIN")      == 0 ||
	    strcasecmp(name, "DEST")        == 0 ||
	    strcasecmp(name, "DST")         == 0 ||
	    strcasecmp(name, "DESTINATION") == 0) {
		*off = NOWDB_OFF_DESTIN; *sz = 8;
		*type = isstr?NOWDB_TYP_TEXT:NOWDB_TYP_UINT;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "LABEL") == 0) {
		*off = NOWDB_OFF_LABEL; *sz = 8;
		*type = isstr?NOWDB_TYP_TEXT:NOWDB_TYP_UINT;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "STAMP")     == 0 ||
	    strcasecmp(name, "TIMESTAMP") == 0) {
		*off = NOWDB_OFF_TMSTMP; *sz = 8; 
		*type = isstr?NOWDB_TYP_TEXT:NOWDB_TYP_TIME;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "WEIGHT") == 0) {
		*off = NOWDB_OFF_WEIGHT; *sz = 8; 
		*type = isstr?NOWDB_TYP_TEXT:0;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "WEIGHT2") == 0) {
		*off = NOWDB_OFF_WEIGHT2; *sz = 8; 
		*type = isstr?NOWDB_TYP_TEXT:0;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "WTYPE") == 0) {
		*off = NOWDB_OFF_WTYPE; *sz = 4; 
		*type = NOWDB_TYP_INT;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "WTYPE2") == 0) {
		*off = NOWDB_OFF_WTYPE2; *sz = 4; 
		*type = NOWDB_TYP_INT;
		return NOWDB_OK;
	}
	*off = 0; *sz = 0; *type = 0;
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                     "unknown field");
}

/* ------------------------------------------------------------------------
 * Get edge field:
 * ---------------
 * - sets the offset into the structure
 * - the size of the field
 * - the type of the field (if known!)
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getEdgeField(char              *name,
                                       nowdb_model_edge_t   *e,
                                       nowdb_model_vertex_t *o,
                                       nowdb_model_vertex_t *d,
                                       nowdb_ast_t        *trg,
                                       uint32_t           *off,
                                       uint32_t            *sz,
                                       nowdb_type_t      *type) {

	if (strcasecmp(name, "EDGE") == 0) {
		*off = NOWDB_OFF_EDGE; *sz = 8;
		*type = NOWDB_TYP_TEXT;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "ORIGIN") == 0) {
		*off = NOWDB_OFF_ORIGIN; *sz = 8;
		if (o != NULL && o->vid == NOWDB_MODEL_TEXT) {
			*type = NOWDB_TYP_TEXT;
		} else {
			*type = NOWDB_TYP_UINT;
		}
		return NOWDB_OK;
	}
	if (strcasecmp(name, "DESTIN")      == 0 ||
	    strcasecmp(name, "DEST")        == 0 ||
	    strcasecmp(name, "DST")         == 0 ||
	    strcasecmp(name, "DESTINATION") == 0) {
		*off = NOWDB_OFF_DESTIN; *sz = 8;
		if (d != NULL && d->vid == NOWDB_MODEL_TEXT) {
			*type = NOWDB_TYP_TEXT;
		} else {
			*type = NOWDB_TYP_UINT;
		}
		return NOWDB_OK;
	}
	if (strcasecmp(name, "LABEL") == 0) {
		*off = NOWDB_OFF_LABEL; *sz = 8;
		if (e != NULL && e->edge == NOWDB_MODEL_TEXT) {
			*type = NOWDB_TYP_TEXT;
		} else {
			*type = NOWDB_TYP_UINT;
		}
		return NOWDB_OK;
	}
	if (strcasecmp(name, "STAMP")     == 0 ||
	    strcasecmp(name, "TIMESTAMP") == 0) {
		*off = NOWDB_OFF_TMSTMP; *sz = 8; 
		*type = NOWDB_TYP_TIME;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "WEIGHT") == 0) {
		*off = NOWDB_OFF_WEIGHT; *sz = 8; 
		*type = e!=NULL?e->weight:0;
		return NOWDB_OK;
	}
	if (strcasecmp(name, "WEIGHT2") == 0) {
		*off = NOWDB_OFF_WEIGHT2; *sz = 8; 
		*type = e!=NULL?e->weight2:0;
		return NOWDB_OK;
	}
	*off = 0; *sz = 0; *type = 0;
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                     "unknown field");
}

/* ------------------------------------------------------------------------
 * Get Value:
 * ----------
 * The type of the value is inferred either from
 * - the type of the field (then *typ is set) or
 * - from the explicit type coming from the ast
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getValue(nowdb_scope_t *scope,
                                   char            *str,
				   uint32_t         off,
                                   uint32_t          sz,
                                   nowdb_type_t    *typ,
                                   int            stype,
                                   void         **value) {
	char *tmp=NULL;
	nowdb_err_t err=NOWDB_OK;
	int rc;

	// check this branch! value is never allocated or set!
	if (*typ == 0) {
		fprintf(stderr, "IN THAT BRANCH\n");
		switch(stype) {
		case NOWDB_AST_FLOAT: *typ = NOWDB_TYP_FLOAT; break;
		case NOWDB_AST_UINT: *typ = NOWDB_TYP_UINT; break;
		case NOWDB_AST_INT: *typ = NOWDB_TYP_INT; break;
		case NOWDB_AST_DATE:
		case NOWDB_AST_TIME: *typ = NOWDB_TYP_INT; break;
		case NOWDB_AST_TEXT: *typ = NOWDB_TYP_TEXT; break;
		case NOWDB_AST_BOOL: *typ = NOWDB_TYP_BOOL; break;
		default: 
			*value = NULL;
			INVALIDAST("unknown type in AST");
		}
	}

	*value = malloc(sz);
	if (*value == NULL) return nowdb_err_get(nowdb_err_no_mem,
		              FALSE, OBJECT, "allocating buffer");

	switch(*typ) {
	case NOWDB_TYP_FLOAT:
		**(double**)value = (double)strtod(str, &tmp);
		break;

	case NOWDB_TYP_UINT:
		if (sz == 4) 
			**(uint32_t**)value = (uint32_t)strtoul(str, &tmp, 10);
		else
			**(uint64_t**)value = (uint64_t)strtoul(str, &tmp, 10);
		break;

	case NOWDB_TYP_DATE:
	case NOWDB_TYP_TIME:
		if (stype == NOWDB_AST_TEXT) {
			if (strnlen(str, 4096) == 10) {
				rc = nowdb_time_fromString(str,
				    NOWDB_DATE_FORMAT, *value);
			} else {
				rc = nowdb_time_fromString(str,
				    NOWDB_TIME_FORMAT, *value);
			}
			if (rc != 0) err = nowdb_err_get(rc, FALSE,
		                   OBJECT, "timestamp from string");
			break;
		}
		
	case NOWDB_TYP_INT:
		if (sz == 4) 
			**(int32_t**)value = (int32_t)strtol(str, &tmp, 10);
		else 
			**(int64_t**)value = (int64_t)strtol(str, &tmp, 10);
		break;

	case NOWDB_TYP_TEXT:
		if (off == NOWDB_OFF_TMSTMP) {
			*typ = NOWDB_TYP_TIME;
			if (strnlen(str, 4096) == 10) {
				rc = nowdb_time_fromString(str,
				    NOWDB_DATE_FORMAT, *value);
			} else {
				rc = nowdb_time_fromString(str,
				    NOWDB_TIME_FORMAT, *value);
			}
			if (rc != 0) err = nowdb_err_get(rc, FALSE,
		                   OBJECT, "timestamp from string");
		} else {
			err = nowdb_text_getKey(scope->text, str, *value);
			if (err != NOWDB_OK && err->errcode ==
			            nowdb_err_key_not_found) 
			{
				**(uint64_t**)value = NOWDB_TEXT_UNKNOWN;
				nowdb_err_release(err); err = NOWDB_OK;
			}
		}
		break;

	default:
		if (*value != NULL) {
			free(*value); *value = NULL;
		}
		return nowdb_err_get(nowdb_err_panic, FALSE, OBJECT,
	                                          "unexpected type");
	}
	if (err != NOWDB_OK) {
		if (*value != NULL) {
			free(*value); *value=NULL;
		}
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Get Values:
 * -----------
 * get all values in list of options
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getValues(nowdb_scope_t *scope,
                                    nowdb_ast_t     *ast,
				    uint32_t         off,
                                    uint32_t          sz,
                                    nowdb_type_t    *typ,
                                    int            stype,
                                    ts_algo_list_t *vals) {
	nowdb_err_t err=NOWDB_OK;
	nowdb_ast_t  *v;
	void     *value;

	v = ast;
	while (v!=NULL) {
		// fprintf(stderr, "value: %s\n", (char*)v->value);
		err = getValue(scope, v->value, off, sz, typ, stype, &value);
		if (err != NOWDB_OK) break;
		if (ts_algo_list_append(vals, value) != TS_ALGO_OK) {
			if (value != NULL) free(value);
			NOMEM("list.append"); break;
		}
		v = nowdb_ast_value(v);
	}
	if (err != NOWDB_OK) {
		ts_algo_list_node_t *run;
		for(run=vals->head; run!=NULL; run=run->nxt) {
			free(run->cont);
		}
		ts_algo_list_destroy(vals);
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Get typed Value:
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getTypedValue(nowdb_scope_t     *scope,
                                        char                *str,
                                        nowdb_model_prop_t *prop,
                                        void             **value) {
	nowdb_err_t err;
	char *tmp;

	*value = malloc(sizeof(nowdb_key_t));
	if (value == NULL) return nowdb_err_get(nowdb_err_no_mem,
		             FALSE, OBJECT, "allocating buffer");
	switch(prop->value) {
	case NOWDB_TYP_FLOAT:
		**(double**)value = (double)strtod(str, &tmp);
		break;

	case NOWDB_TYP_UINT:
		**(uint64_t**)value = (uint64_t)strtoul(str, &tmp, 10);
		break;

	case NOWDB_TYP_DATE:
	case NOWDB_TYP_TIME:
	case NOWDB_TYP_INT:
		**(int64_t**)value = (int64_t)strtol(str, &tmp, 10);
		break;

	case NOWDB_TYP_TEXT:
		err = nowdb_text_getKey(scope->text, str, *value);
		if (err != NOWDB_OK) {
			free(*value); *value = NULL;
		}
		return err;

	default: return nowdb_err_get(nowdb_err_panic, FALSE, OBJECT,
	                                          "unexpected type");
	}
	if (*tmp != 0) {
		free(*value); *value = NULL;
		return nowdb_err_get(nowdb_err_invalid,FALSE,OBJECT,
	                                 "typed conversion failed");
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Create an edge filter comparison node from an ast comparison node 
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getEdgeCompare(nowdb_scope_t   *scope,
                                          nowdb_ast_t      *trg,
                                          nowdb_model_edge_t *e,
                                          int          operator,
                                          nowdb_ast_t      *op1,
                                          nowdb_ast_t      *op2,
                                          nowdb_filter_t **comp) {
	nowdb_err_t err;
	nowdb_model_vertex_t *o;
	nowdb_model_vertex_t *d;
	nowdb_ast_t *op;
	void *value;
	uint32_t off,sz;
	nowdb_type_t typ;

	err = nowdb_model_getVertexById(scope->model, e->origin, &o);
	if (err != NOWDB_OK) return err;

	err = nowdb_model_getVertexById(scope->model, e->destin, &d);
	if (err != NOWDB_OK) return err;

	op = op1->ntype == NOWDB_AST_FIELD?op1:op2;

	err = getEdgeField(op->value, e, o, d, trg, &off, &sz, &typ);
	if (err != NOWDB_OK) return err;

	op = op1->ntype == NOWDB_AST_FIELD?op2:op1;

	if (operator == NOWDB_FILTER_IN) {
		ts_algo_list_t vals;
		ts_algo_list_init(&vals);

		err = getValues(scope, op, off, sizeof(nowdb_key_t),
	                                            &typ, 0, &vals);
		if (err != NOWDB_OK) return err;
		
		err = nowdb_filter_newCompare(comp, operator,
		                                off, sz, typ,
			                         NULL, &vals);
		if (err != NOWDB_OK) {
			ts_algo_list_node_t *run;
			for(run=vals.head; run!=NULL; run=run->nxt) {
				free(run->cont);
			}
			ts_algo_list_destroy(&vals);
			return err;
		}
	} else {
		err = getValue(scope, op->value, off, sz, &typ, 0, &value);
		if (err != NOWDB_OK) return err;

		err = nowdb_filter_newCompare(comp, operator,
		                   off, sz, typ, value, NULL);
		if (err != NOWDB_OK) {
			free(value); return err;
		}
		nowdb_filter_own(*comp);
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Create a typed filter comparison node from an ast comparison node 
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getTypedCompare(nowdb_scope_t    *scope,
                                          nowdb_ast_t        *trg,
                                          nowdb_model_vertex_t *v,
                                          int            operator,
                                          nowdb_ast_t        *op1,
                                          nowdb_ast_t        *op2,
                                          nowdb_filter_t   **comp) {
	nowdb_err_t err;
	nowdb_filter_t *pid=NULL;
	nowdb_filter_t *val=NULL;
	nowdb_filter_t *and;
	nowdb_model_prop_t *p;
	nowdb_ast_t *op;
	void *value=NULL;
	uint32_t off=0;

	op = op1->ntype == NOWDB_AST_FIELD?op1:op2;

	// get propid
	err = nowdb_model_getPropByName(scope->model,
	                   v->roleid, op->value, &p);
	if (err != NOWDB_OK) return err;

	// make comp propid == 'propid'
	if (!p->pk) {
		err = nowdb_filter_newCompare(&pid, NOWDB_FILTER_EQ,
		                                     NOWDB_OFF_PROP,
		                                sizeof(nowdb_key_t),
		                                     NOWDB_TYP_UINT,
	                                     	  &p->propid, NULL);
		if (err != NOWDB_OK) return err;
	}

	// if pk, it is just vid!
	off = p->pk?NOWDB_OFF_VERTEX:NOWDB_OFF_VALUE;

	op = op1->ntype == NOWDB_AST_FIELD?op2:op1;

	// we have 'in' instead of a single value
	if (operator == NOWDB_FILTER_IN) {
		ts_algo_list_t vals;

		ts_algo_list_init(&vals);
		err = getValues(scope, op, off, sizeof(nowdb_key_t),
	                                       &p->value, 0, &vals);
		if (err != NOWDB_OK) {
			nowdb_filter_destroy(pid);
			free(pid); return err;
		}
		err = nowdb_filter_newCompare(&val, operator, off,
			                      sizeof(nowdb_key_t),
			                                 p->value,
			                             NULL, &vals);
		if (err != NOWDB_OK) {
			ts_algo_list_node_t *run;
			nowdb_filter_destroy(pid); free(pid); 
			for(run=vals.head; run!=NULL; run=run->nxt) {
				free(run->cont);
			}
			ts_algo_list_destroy(&vals);
			return err;
		}
		ts_algo_list_destroy(&vals);

	} else {
		err = getValue(scope, op->value, off, sizeof(nowdb_key_t),
	                                    &p->value, op->stype, &value);
		if (err != NOWDB_OK) {
			nowdb_filter_destroy(pid);
			free(pid); return err;
		}
		err = nowdb_filter_newCompare(&val, operator, off,
			                      sizeof(nowdb_key_t),
			                                 p->value,
			                               value,NULL);
		if (err != NOWDB_OK) {
			nowdb_filter_destroy(pid); free(pid); 
			if (value != NULL) free(value);
			return err;
		}
	}
	nowdb_filter_own(val);

	if (p->pk) {
		*comp = val; return NOWDB_OK;
	}

	err = nowdb_filter_newBool(&and, NOWDB_FILTER_AND);
	if (err != NOWDB_OK) {
		nowdb_filter_destroy(pid); free(pid); 
		nowdb_filter_destroy(val); free(val); 
		free(value); return err;
	}
	and->left = pid;
	and->right = val;
	*comp = and;

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Create a filter comparison node from an ast comparison node 
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getCompare(nowdb_scope_t    *scope,
                                     nowdb_ast_t        *trg,
                                     nowdb_model_edge_t   *e,
                                     nowdb_model_vertex_t *v,
                                     nowdb_filter_t   **comp,
                                     nowdb_ast_t        *ast) {
	nowdb_err_t err;
	nowdb_ast_t *op1, *op2;
	uint32_t off, sz;
	void *conv;
	nowdb_type_t typ;

	op1 = nowdb_ast_operand(ast, 1);
	if (op1 == NULL) INVALIDAST("no first operand in compare");

	op2 = nowdb_ast_operand(ast, 2);
	if (op2 == NULL) INVALIDAST("no second operand in compare");

	if (op1->value == NULL) INVALIDAST("first operand in compare is NULL");
	if (op2->value == NULL) INVALIDAST("second operand in compare is NULL");

	if (trg->stype == NOWDB_AST_TYPE) {
		return getTypedCompare(scope, trg, v,
		          ast->stype, op1, op2, comp);
	} else if (e != NULL) {
		return getEdgeCompare(scope, trg, e,
		         ast->stype, op1, op2, comp);
	}

	// fprintf(stderr, "beyond type and edge\n");

	// we have 'in' instead of a single value
	if (ast->stype == NOWDB_FILTER_IN) {
		ts_algo_list_t vals;

		ts_algo_list_init(&vals);

		err = getField(op1->value, trg, &off,
			       &sz, op2->isstr, &typ);
		if (err != NOWDB_OK) return err;

		err = getValues(scope, op2, off, sizeof(nowdb_key_t),
	                                             &typ, 0, &vals);
		if (err != NOWDB_OK) return err;
		
		err = nowdb_filter_newCompare(comp, ast->stype,
		                                  off, sz, typ,
			                           NULL, &vals);
		if (err != NOWDB_OK) {
			ts_algo_list_node_t *run;
			for(run=vals.head; run!=NULL; run=run->nxt) {
				free(run->cont);
			}
			ts_algo_list_destroy(&vals);
			return err;
		}
		ts_algo_list_destroy(&vals);

	/* check whether op1 is field or value */
	} else {
		if (op1->ntype == NOWDB_AST_FIELD) {
			err = getField(op1->value, trg, &off,
			               &sz, op2->isstr, &typ);
			if (err != NOWDB_OK) return err;
			err = getValue(scope, op2->value, off, sz,
			                 &typ, op2->stype, &conv);
		} else {
			err = getField(op2->value, trg, &off,
			               &sz, op1->isstr, &typ);
			if (err != NOWDB_OK) return err;
			err = getValue(scope, op1->value, off, sz,
			                 &typ, op1->stype, &conv);
		}
		if (err != NOWDB_OK) return err;

		err = nowdb_filter_newCompare(comp, ast->stype,
		                      off, sz, typ, conv, NULL);
		if (err != NOWDB_OK) return err;
		nowdb_filter_own(*comp);
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Get filter condition (boolean or comparison) from ast condition node
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getCondition(nowdb_scope_t    *scope,
                                       nowdb_ast_t        *trg,
                                       nowdb_model_edge_t   *e,
                                       nowdb_model_vertex_t *v,
                                       nowdb_filter_t      **b,
                                       nowdb_ast_t        *ast) {
	int op;
	nowdb_err_t err;

	switch(ast->ntype) {
	case NOWDB_AST_COMPARE: return getCompare(scope, trg, e, v, b, ast);
	case NOWDB_AST_JUST: return getCondition(scope, trg, e, v, b,
	                                   nowdb_ast_operand(ast,1));

	case NOWDB_AST_NOT:
		err = nowdb_filter_newBool(b, NOWDB_FILTER_NOT);
		if (err != NOWDB_OK) return err;
		err = getCondition(scope, trg, e, v, &(*b)->left,
		                      nowdb_ast_operand(ast, 1));
		if (err != NOWDB_OK) {
			nowdb_filter_destroy(*b); free(*b); *b=NULL;
			return err;
		}
		return NOWDB_OK;

	case NOWDB_AST_AND:
	case NOWDB_AST_OR:
		op = ast->ntype==NOWDB_AST_AND?NOWDB_FILTER_AND:
		                               NOWDB_FILTER_OR;
		err = nowdb_filter_newBool(b,op);
		if (err != NOWDB_OK) return err;
		err = getCondition(scope, trg, e, v, &(*b)->left,
		                       nowdb_ast_operand(ast,1));
		if (err != NOWDB_OK) {
			nowdb_filter_destroy(*b); free(*b); *b=NULL;
			return err;
		}
		err = getCondition(scope, trg, e, v, &(*b)->right,
		                         nowdb_ast_operand(ast,2));
		if (err != NOWDB_OK) {
			nowdb_filter_destroy(*b); free(*b); *b=NULL;
			return err;
		}
		return NOWDB_OK;

	default:
		INVALIDAST("unknown condition type in ast");
	}
}

/* ------------------------------------------------------------------------
 * Identify index candidates in filter
 * ------------------------------------------------------------------------
 */
static nowdb_err_t idxFromFilter(nowdb_filter_t *filter,
                                 ts_algo_list_t *cands) {
	nowdb_err_t err;

	if (filter == NULL) return NOWDB_OK;

	if (filter->ntype == NOWDB_FILTER_COMPARE) {
		if (filter->op == NOWDB_FILTER_EQ) { // IN as well
			if (ts_algo_list_append(cands,
			        filter) != TS_ALGO_OK) {
				return nowdb_err_get(nowdb_err_no_mem,
				        FALSE, OBJECT, "list.append");
			}
			return NOWDB_OK;
		}
	}
	if (filter->ntype == NOWDB_FILTER_BOOL) {
		switch(filter->op) {
		case NOWDB_FILTER_AND:
			err = idxFromFilter(filter->left, cands);
			if (err != NOWDB_OK) return err;
			err = idxFromFilter(filter->right, cands);
			if (err != NOWDB_OK) return err;
			return NOWDB_OK;

		case NOWDB_FILTER_JUST:
			err = idxFromFilter(filter->left, cands);
			if (err != NOWDB_OK) return err;
			return NOWDB_OK;

		/* or is multi-index */
		default: return NOWDB_OK;
		}

	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Check whether candidates cover keys
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t cover(ts_algo_list_t     *cands,
                                nowdb_index_t      *idx,
                                nowdb_index_keys_t *keys,
                                ts_algo_list_t     *res,
                                char               *ok) {
	nowdb_filter_t *node;
	ts_algo_list_node_t *runner;

	/* we need a result idx/key */
	/* we need to find it by searching for all keys */

	*ok = 0;
	for(int i=0;i<keys->sz;i++) {
		for(runner=cands->head;runner!=NULL;runner=runner->nxt) {
			node = runner->cont;
			if (keys->off[i] == node->off) {
				if (ts_algo_list_append(res,node)
				                   != TS_ALGO_OK) {
					return nowdb_err_get(nowdb_err_no_mem,
				                FALSE, OBJECT, "list.append");
				}
			}
		}
	}
	if (res->len == keys->sz) *ok = 1;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Compare indexes (descending)
 * ------------------------------------------------------------------------
 */
#define KEY(x) \
	((nowdb_index_keys_t*)x)

static ts_algo_cmp_t comparekeysz(void *one, void *two) {
	if (KEY(one)->sz < KEY(two)->sz) return ts_algo_cmp_greater;
	if (KEY(one)->sz > KEY(two)->sz) return ts_algo_cmp_less;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * make idx+keys
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t makeIndexAndKeys(nowdb_index_t  *idx,
                                           ts_algo_list_t *nodes,
                                           ts_algo_list_t *res) {
	nowdb_plan_idx_t *pidx;
	ts_algo_list_node_t *runner;
	nowdb_filter_t    *node;
	int i=0;

	if (nodes == NULL || nodes->len == 0) {
		INVALIDAST("no nodes");
	}
	pidx = calloc(1, sizeof(nowdb_plan_idx_t));
	if (pidx == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                  FALSE, OBJECT, "allocating plan idx");
	pidx->idx = idx;
	pidx->keys = malloc(nodes->len*8);
	if (pidx->keys == NULL) {
		free(pidx);
		return nowdb_err_get(nowdb_err_no_mem,
	            FALSE, OBJECT, "allocating keys");
	}
	for(runner=nodes->head;runner!=NULL;runner=runner->nxt) {
		node = runner->cont;
		memcpy(pidx->keys+i, node->val, node->size);
		i+=node->size;
	}
	if (ts_algo_list_append(res, pidx) != TS_ALGO_OK) {
		free(pidx->keys); free(pidx);
		return nowdb_err_get(nowdb_err_no_mem,
	                FALSE, OBJECT, "list.append");
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Intersect candidates and indices
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t intersect(ts_algo_list_t *cands,
                                    ts_algo_list_t *idxes,
                                    ts_algo_list_t *res) {
	nowdb_err_t err = NOWDB_OK;
	ts_algo_list_node_t *runner;
	nowdb_index_t *idx;
	nowdb_index_keys_t *keys;
	ts_algo_list_t *xes;
	ts_algo_list_t  nodes;
	char x;

	/* sort idxes by keysize */
	xes = ts_algo_list_sort(idxes, &comparekeysz);
	if (xes == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                           FALSE, OBJECT, "list.sort");

	ts_algo_list_init(&nodes);
	for(runner=xes->head;runner!=NULL;runner=runner->nxt) {
		idx = ((nowdb_index_desc_t*)runner->cont)->idx;
		keys = nowdb_index_getResource(idx);
		err = cover(cands, idx, keys, &nodes, &x);
		if (err != NOWDB_OK) break;
		if (x) {
			err = makeIndexAndKeys(idx, &nodes, res);
			break;
		}
		ts_algo_list_destroy(&nodes);
		ts_algo_list_init(&nodes);
	}
	ts_algo_list_destroy(&nodes);
	ts_algo_list_destroy(xes); free(xes);
	return err;
}

/* ------------------------------------------------------------------------
 * fields to keys
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t fields2keys(ts_algo_list_t      *fields,
                                      nowdb_index_keys_t **keys) {
	nowdb_err_t err;
	nowdb_field_t *f;
	ts_algo_list_node_t *runner;
	int i=0;

	if (fields->len == 0) return NOWDB_OK;

	*keys = calloc(1,sizeof(nowdb_index_keys_t));
	if (*keys == NULL) {
		NOMEM("allocating keys");
		return err;
	}
	(*keys)->sz = fields->len;
	(*keys)->off = calloc(fields->len, sizeof(uint16_t));
	if ((*keys)->off == NULL) {
		NOMEM("allocating key offsets");
		free(*keys);
		return err;
	}
	for(runner=fields->head; runner!=NULL; runner=runner->nxt) {
		f = runner->cont;
		if (f->name != NULL) {
			// for the moment no properties!
			free((*keys)->off); free(*keys);
			INVALIDAST("cannot handle property fields");
		}
		(*keys)->off[i] = (uint16_t)f->off; i++;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Find indices for group
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getGroupOrderIndex(nowdb_scope_t  *scope,
                                             int             stype,
                                             char           *context,
                                             ts_algo_list_t *fields, 
                                             ts_algo_list_t *res) {
	nowdb_index_keys_t *keys=NULL;
	nowdb_index_desc_t *desc=NULL;
	nowdb_context_t    *ctx=NULL;
	nowdb_plan_idx_t   *idx=NULL;
	nowdb_err_t err;

	err = fields2keys(fields, &keys);
	if (err != NOWDB_OK) return err;
	if (keys == NULL) return NOWDB_OK;

	if (context != NULL && stype == NOWDB_AST_CONTEXT) {
		err = nowdb_scope_getContext(scope, context, &ctx);
		if (err != NOWDB_OK) {
			free(keys->off); free(keys);
			return err;
		}
	}

	err = nowdb_index_man_getByKeys(scope->iman, ctx, keys, &desc);
	if (err != NOWDB_OK) {
		free(keys->off); free(keys);
		return err;
	}

	idx = calloc(1,sizeof(nowdb_plan_idx_t));
	if (idx == NULL) {
		free(keys->off); free(keys);
		NOMEM("allocating plan index");
		return err;
	}

	idx->idx = desc->idx;
	idx->keys = NULL;

	free(keys->off); free(keys);

	if (ts_algo_list_append(res, idx) != TS_ALGO_OK) {
		NOMEM("list.append");
		free(idx); return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Find indices for filter
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getIndices(nowdb_scope_t  *scope,
                                     int             stype,
                                     char           *context,
                                     nowdb_filter_t *filter, 
                                     ts_algo_list_t *res) {
	ts_algo_list_t cands;
	ts_algo_list_t idxes;
	nowdb_err_t err;
	nowdb_context_t *ctx=NULL;

	if (filter == NULL) return NOWDB_OK;

	if (context != NULL && stype == NOWDB_AST_CONTEXT) {
		err = nowdb_scope_getContext(scope, context, &ctx);
		if (err != NOWDB_OK) return err;
	}

	ts_algo_list_init(&cands);
	ts_algo_list_init(&idxes);

	err = nowdb_index_man_getAllOf(scope->iman, ctx, &idxes);
	if (err != NOWDB_OK || idxes.len == 0) {
		ts_algo_list_destroy(&cands);
		ts_algo_list_destroy(&idxes);
		return err;
	}
	
	err = idxFromFilter(filter, &cands);
	if (err != NOWDB_OK || cands.len == 0) {
		ts_algo_list_destroy(&cands);
		ts_algo_list_destroy(&idxes);
		return err;
	}

	err = intersect(&cands, &idxes, res);

	ts_algo_list_destroy(&cands);
	ts_algo_list_destroy(&idxes);

	return err;
}

/* ------------------------------------------------------------------------
 * Add vertex type to filter
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getType(nowdb_scope_t    *scope,
                                  nowdb_ast_t      *trg,
                                  nowdb_filter_t  **filter,
                                  nowdb_model_vertex_t **v) {
	nowdb_err_t err;

	err = nowdb_model_getVertexByName(scope->model, trg->value, v);
	if (err != NOWDB_OK) return err;

	err = nowdb_filter_newCompare(filter, NOWDB_FILTER_EQ,
	               NOWDB_OFF_ROLE, sizeof(nowdb_roleid_t),
	                 NOWDB_TYP_UINT, &(*v)->roleid, NULL);
	if (err != NOWDB_OK) return err;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Get filter
 * ----------
 * this is way to simple. we need to get the where *per target* and
 * we have to consider aliases. Also, the target may be derived, so
 * we must consider fields that we do not know beforehand.
 * Furthermore, not every where condition goes into a filter condition.
 * A filter condition is always of the form: 
 * field = value.
 * However, a where condition may assume the forms:
 * field = field (join!)
 * value = value (constant)
 * TRUE, FALSE
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getFilter(nowdb_scope_t   *scope,
                                    nowdb_ast_t     *trg,
                                    nowdb_ast_t     *ast,
                                    nowdb_filter_t **filter) {
	nowdb_err_t     err;
	nowdb_ast_t    *cond;
	nowdb_filter_t *w = NULL;
	nowdb_filter_t *t = NULL;
	nowdb_filter_t *and;
	nowdb_model_vertex_t *v=NULL;
	nowdb_model_edge_t   *e=NULL;

	*filter = NULL;

	if (trg->stype == NOWDB_AST_TYPE) {
		err = getType(scope, trg, &t, &v);
		if (err != NOWDB_OK) return err;
	}

	if (ast != NULL) {
		cond = nowdb_ast_operand(ast,1);
		if (cond == NULL) INVALIDAST("no first operand in where");

		/*
		fprintf(stderr, "where: %d->%d\n", ast->ntype, cond->ntype);
		*/

		/* get condition creates a filter */
		err = getCondition(scope, trg, e, v, &w, cond);
		if (err != NOWDB_OK) {
			if (t!=NULL) {
				nowdb_filter_destroy(t);free(t);
			}
			if (*filter != NULL) {
				nowdb_filter_destroy(*filter); 
				free(*filter); *filter = NULL;
				return err;
			}
			return err;
		}
	}
	if (t != NULL && w != NULL) {
		err = nowdb_filter_newBool(&and, NOWDB_FILTER_AND);
		if (err != NOWDB_OK) {
			nowdb_filter_destroy(t);
			nowdb_filter_destroy(w);
			free(t); t = NULL;
			free(w); w = NULL;
			return err;
		}
		and->left = t;
		and->right = w;
		*filter = and;

	} else if (t != NULL) *filter = t; else *filter = w;

	// nowdb_filter_show(*filter, stderr);

	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Destroy list of fields
 * -----------------------------------------------------------------------
 */
static inline void destroyFieldList(ts_algo_list_t *list) {
	ts_algo_list_node_t *runner, *tmp;
	nowdb_expr_t exp;

	if (list == NULL) return;
	runner=list->head;
	while(runner!=NULL) {
		exp = runner->cont;
		nowdb_expr_destroy(exp); free(exp);
		tmp = runner->nxt;
		ts_algo_list_remove(list, runner);
		free(runner); runner=tmp;
	}
}

/* -----------------------------------------------------------------------
 * Destroy list of aggregates
 * -----------------------------------------------------------------------
 */
static inline void destroyFunList(ts_algo_list_t *list) {
	ts_algo_list_node_t *runner, *tmp;
	nowdb_fun_t *f;

	if (list == NULL) return;
	runner=list->head;
	while(runner!=NULL) {
		f = runner->cont;
		nowdb_fun_destroy(f);
		free(f);
		tmp = runner->nxt;
		ts_algo_list_remove(list, runner);
		free(runner); runner=tmp;
	}
}

/* ------------------------------------------------------------------------
 * Get const value
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getConstValue(uint16_t typ,
                                        char    *str,
                                        void **value) {
	nowdb_err_t err;
	char x;
	char *tmp;

	if (typ != NOWDB_TYP_TEXT) {
		*value = malloc(sizeof(nowdb_key_t));
		if (value == NULL) {
			NOMEM("allocatin value");
			return err;
		}
	}
	switch(typ) {
	case NOWDB_TYP_TEXT:
		*value = strdup(str);
		if (*value == NULL) {
			NOMEM("allocating value");
			return err;
		}
		return NOWDB_OK;

	case NOWDB_TYP_BOOL:
		if (strcasecmp(str, "true") == 0) x=1;
		else if (strcasecmp(str, "false") == 0) x=0;
		else {
			free(*value); *value = NULL;
			return nowdb_err_get(nowdb_err_invalid,
			                         FALSE, OBJECT,
			                     "invalid boolean");
		}
		memcpy(*value, &x, 1);
		return NOWDB_OK;

	case NOWDB_TYP_FLOAT:
		**(double**)value = (double)strtod(str, &tmp);
		break;

	case NOWDB_TYP_UINT:
		**(uint64_t**)value = (uint64_t)strtoul(str, &tmp, 10);
		break;

	case NOWDB_TYP_TIME:
	case NOWDB_TYP_INT:
		**(int64_t**)value = (int64_t)strtol(str, &tmp, 10);
		break;

	default:
		if (*value != NULL) {
			free(*value); *value = NULL;
		}
		return nowdb_err_get(nowdb_err_panic, FALSE, OBJECT,
	                                          "unexpected type");
	}
	if (*tmp != 0) {
		free(*value); *value = NULL;
		return nowdb_err_get(nowdb_err_invalid,FALSE,OBJECT,
	                            "conversion of constant failed");
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Get vertex field
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getVertexField(nowdb_scope_t    *scope,
                                         nowdb_expr_t       *exp,
                                         nowdb_model_vertex_t *v,
                                         nowdb_ast_t      *field) {
	nowdb_err_t err;
	nowdb_model_prop_t *p;

	if (v == NULL) {
		INVALIDAST("vertex not allowed here");
	}
	err = nowdb_model_getPropByName(scope->model,
	                                   v->roleid,
	                            field->value, &p);
	if (err != NOWDB_OK) return err;

	err = nowdb_expr_newVertexField(exp, field->value,
	                             v->roleid, p->propid);
	if (err != NOWDB_OK) return err;
	NOWDB_EXPR_TOFIELD(*exp)->type = p->value;
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Predeclaration for recursive call
 * -----------------------------------------------------------------------
 */
static nowdb_err_t getExpr(nowdb_scope_t    *scope,
                           nowdb_model_vertex_t *v,
                           nowdb_ast_t        *trg,
                           nowdb_ast_t      *field,
                           nowdb_expr_t      *expr,
                           char               *agg);

/* -----------------------------------------------------------------------
 * Make agg function
 * shall be recursive (for future use with expressions)
 * -----------------------------------------------------------------------
 */
static nowdb_err_t makeAgg(nowdb_scope_t *scope,
                           nowdb_model_vertex_t *v,
                           nowdb_ast_t   *trg,
                           nowdb_ast_t   *fun,
                           int            op,
                           nowdb_expr_t  *expr) {
	nowdb_ast_t *param;
	nowdb_err_t err;
	nowdb_content_t cont;
	nowdb_fun_t *f;
	nowdb_expr_t myx=NULL;
	char dummy;

	cont = trg->stype == NOWDB_AST_CONTEXT?NOWDB_CONT_EDGE:
	                                       NOWDB_CONT_VERTEX;
	param = nowdb_ast_param(fun);
	if (param != NULL) {
		err = getExpr(scope, v, trg, param, &myx, &dummy);
		if (err != NOWDB_OK) return err;
	}
	err = nowdb_fun_new(&f, op, cont, myx, NULL);
	if (err != NOWDB_OK) return err;

	err = nowdb_expr_newAgg(expr, NOWDB_FUN_AS_AGG(f));
	if (err != NOWDB_OK) {
		nowdb_fun_destroy(f); free(f);
		return err;
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Helper for makeOp
 * -----------------------------------------------------------------------
 */
#define DESTROYLIST(l) \
	ts_algo_list_node_t *run, *tmp; \
	run = l.head; \
	while(run!=NULL) { \
		nowdb_expr_destroy(run->cont); \
		free(run->cont); \
		tmp = run->nxt; \
		free(run); \
		run=tmp; \
	}

/* -----------------------------------------------------------------------
 * Recursively get operator
 * -----------------------------------------------------------------------
 */
static nowdb_err_t makeOp(nowdb_scope_t *scope,
                          nowdb_model_vertex_t *v,
                          nowdb_ast_t   *trg,
                          nowdb_ast_t   *field,
                          int            op,
                          nowdb_expr_t  *expr,
                          char          *agg) {
	ts_algo_list_t ops;
	nowdb_expr_t exp;
	nowdb_err_t err;
	nowdb_ast_t *o;

	// get operators
	ts_algo_list_init(&ops);
	o = nowdb_ast_param(field);
	while (o != NULL) {
		/*
		fprintf(stderr, "%s param %s\n", (char*)field->value,
		                                 (char*)o->value);
		*/
		err = getExpr(scope, v, trg, o, &exp, agg);
		if (err != NOWDB_OK) {
			// destroy list and values, etc.
			DESTROYLIST(ops);
			return err;
		}
		if (ts_algo_list_append(&ops, exp) != TS_ALGO_OK) {
			DESTROYLIST(ops);
			nowdb_expr_destroy(exp); free(exp);
			NOMEM("list.append");
			return err;
		}
		o = nowdb_ast_nextParam(o);
	}

	err = nowdb_expr_newOpL(expr, op, &ops);
	if (err != NOWDB_OK) {
		DESTROYLIST(ops);
		return err;
	}
	ts_algo_list_destroy(&ops);
	return NOWDB_OK;
}
#undef DESTROYLIST

/* -----------------------------------------------------------------------
 * Make function
 * -----------------------------------------------------------------------
 */
static nowdb_err_t makeFun(nowdb_scope_t *scope,
                           nowdb_model_vertex_t *v,
                           nowdb_ast_t   *trg,
                           nowdb_ast_t   *field,
                           nowdb_expr_t  *expr,
                           char          *agg) {
	int op;
	char x;

	op = nowdb_op_fromName(field->value, &x);
	if (op < 0) {
		INVALIDAST("unknown operator");
	}
	if (x) {
		*agg=1;
		return makeAgg(scope, v, trg, field, op, expr);
	}
	return makeOp(scope, v, trg, field, op, expr, agg);
}

/* -----------------------------------------------------------------------
 * Recursively get expression
 * -----------------------------------------------------------------------
 */
static nowdb_err_t getExpr(nowdb_scope_t    *scope,
                           nowdb_model_vertex_t *v,
                           nowdb_ast_t        *trg,
                           nowdb_ast_t      *field,
                           nowdb_expr_t      *expr,
                           char               *agg) {
	nowdb_err_t err;
	nowdb_type_t typ;
	int off=-1;
	void *value;

	/* expression */
	if (field->ntype == NOWDB_AST_FUN ||
	    field->ntype == NOWDB_AST_OP) {

		// fprintf(stderr, "FUN: %s\n", (char*)field->value);

		// flags = NOWDB_FIELD_AGG;

		// distinguish here between agg and ordinary function
		// err = makeFun(trg, field, expr);

		err = makeFun(scope, v, trg, field, expr, agg);
		if (err != NOWDB_OK) return err;

	/*
	} else if (field->ntype == NOWDB_AST_OP) {

		fprintf(stderr, "OP: %s\n", (char*)field->value);

		err = makeOp(scope, v, trg, field, expr, agg);
		if (err != NOWDB_OK) return err;
	*/

	} else if (field->ntype == NOWDB_AST_VALUE) {
		typ = nowdb_ast_type(field->stype);
		err = getConstValue(typ, field->value, &value);
		if (err != NOWDB_OK) return err;

		err = nowdb_expr_newConstant(expr, value, typ);
		if (err != NOWDB_OK) return err;

		// fprintf(stderr, "type: %d / %d\n",
		//        field->vtype, field->stype);

		free(value);

	} else {
		/* we need to distinguish the target! */
		if (trg->stype == NOWDB_AST_CONTEXT) {

			off = nowdb_edge_offByName(field->value);
			if (off < 0) {
				INVALIDAST("unknown field name");
			}
			err = nowdb_expr_newEdgeField(expr, off);
			if (err != NOWDB_OK) return err;

		} else if (trg->stype == NOWDB_AST_TYPE) {
			// get roleid and propid 
			err = getVertexField(scope, expr, v, field);
			if (err != NOWDB_OK) return err;
		} else {
			/*
			fprintf(stderr, "STYPE: %d\n",
			trg->stype);
			*/
			INVALIDAST("unknown target");
		}

		/* name or offset */
		/*
		if (flags == 0 && off < 0) {
			f->name = strdup(field->value);
			if (f->name == NULL) {
				NOMEM("allocating field name");
				free(f); break;
			}
			if (v != NULL) f->roleid = v->roleid;
			f->pk = 0;
		} else {
			f->off = (uint32_t)off;
			f->name = NULL;
		}
		*/
	}

	// f->flags = flags | NOWDB_FIELD_SELECT;
	// f->agg    = 0;

	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Get fields for projection, grouping and ordering 
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t filterAgg(nowdb_expr_t expr,
                                    ts_algo_list_t *l) {
	nowdb_err_t          err;
	ts_algo_list_t       tmp;
	ts_algo_list_node_t *run;
	nowdb_agg_t         *agg;

	ts_algo_list_init(&tmp);
	err = nowdb_expr_filter(expr, NOWDB_EXPR_AGG, &tmp);
	if (err != NOWDB_OK) return err;

	for(run=tmp.head; run!=NULL; run=run->nxt) {
		agg = run->cont;
		if (ts_algo_list_append(l, agg->agg) != TS_ALGO_OK) {
			ts_algo_list_destroy(&tmp);
			NOMEM("list.append");
			return err;
		}
	}
	ts_algo_list_destroy(&tmp);
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Get fields for projection, grouping and ordering 
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t getFields(nowdb_scope_t   *scope,
                                    nowdb_ast_t     *trg,
                                    nowdb_ast_t     *ast,
                                    ts_algo_list_t **fields, 
                                    ts_algo_list_t **aggs) {
	// nowdb_bitmap8_t flags;
	nowdb_err_t err = NOWDB_OK;
	nowdb_model_vertex_t *v=NULL;
	nowdb_expr_t exp;
	nowdb_ast_t *field;
	char agg;
	char dagg=0;

	// get vertex type
	if (trg->stype == NOWDB_AST_TYPE && trg->value != NULL) {
		err = nowdb_model_getVertexByName(scope->model,
		                               trg->value, &v);
		if (err != NOWDB_OK) return err;
	}

	*fields = calloc(1, sizeof(ts_algo_list_t));
	if (*fields == NULL) {
		NOMEM("allocating list");
		return err;
	}
	ts_algo_list_init(*fields);

	field = nowdb_ast_field(ast);
	while (field != NULL) {

		// fprintf(stderr, "%s\n", (char*)field->value);

		agg = 0;
		err = getExpr(scope, v, trg, field, &exp, &agg);
		if (err != NOWDB_OK) break;
		
		if (agg) {
			if (aggs == NULL) {
				INVALIDAST("aggregates not allowed here");
			}
			if (*aggs == NULL) {
				*aggs = calloc(1, sizeof(ts_algo_list_t));
				if (*aggs == NULL) {
					NOMEM("allocating list");
					break;
				}
				ts_algo_list_init(*aggs);
				dagg=1;
			}
			err = filterAgg(exp, *aggs);
			if (err != NOWDB_OK) {
				nowdb_expr_destroy(exp); free(exp);
				break;
			}
		}
		
		/* testing only! */
		if (ts_algo_list_append(*fields, exp) != TS_ALGO_OK) {
			nowdb_expr_destroy(exp); free(exp);
			NOMEM("list.append");
			break;
		}
		field = nowdb_ast_field(field);
	}
	if (err != NOWDB_OK) {
		fprintf(stderr, "ERROR: "); nowdb_err_print(err);

		destroyFieldList(*fields);
		free(*fields); *fields=NULL;

		if (dagg) {
			destroyFunList(*aggs);
			free(*aggs); *aggs=NULL;
		}
	}
	return err;
}

/* -----------------------------------------------------------------------
 * grouping and projection must be equivalent
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t compareForGrouping(ts_algo_list_t *grp,
                                             ts_algo_list_t *sel) {
	ts_algo_list_node_t *grun, *srun;
	nowdb_expr_t gf, sf;

	srun = sel->head;
	for(grun=grp->head; grun != NULL; grun=grun->nxt) {
		if (srun == NULL) INVALIDAST("projection incomplete");
		gf = grun->cont;
		sf = srun->cont;
		if (!nowdb_expr_equal(gf, sf)) {
			INVALIDAST("projection and grouping differ");
		}
		srun = srun->nxt;
	}
	for (;srun != NULL; srun=srun->nxt) {
		if (nowdb_expr_has(srun->cont, NOWDB_EXPR_AGG)) continue;
		INVALIDAST("projection and grouping differ");
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Adjust target to what it s according to model
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t adjustTarget(nowdb_scope_t *scope,
                                       nowdb_ast_t   *trg) {
	nowdb_err_t  err;
	nowdb_target_t t;

	err = nowdb_model_whatIs(scope->model, trg->value, &t);
	if (err != NOWDB_OK) {
		if (err->errcode == nowdb_err_key_not_found) {
			nowdb_err_release(err);
			trg->stype = NOWDB_AST_CONTEXT;
			return NOWDB_OK;
		}
		return err;
	}

	trg->stype = t==NOWDB_TARGET_VERTEX?NOWDB_AST_TYPE:
	                                 NOWDB_AST_CONTEXT;
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Over-simplistic to get it going:
 * - we assume an ast with a simple target object
 * - we create 1 reader fullscan+ or indexsearch 
 *   on either vertex or context
 * - that's it
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_plan_fromAst(nowdb_scope_t  *scope,
                               nowdb_ast_t    *ast,
                               ts_algo_list_t *plan) {
	nowdb_filter_t *filter = NULL;
	ts_algo_list_t *pj, *grp=NULL, *agg=NULL, *ord=NULL;
	ts_algo_list_t idxes;
	nowdb_err_t   err;
	nowdb_ast_t  *trg, *from, *sel, *group=NULL, *order=NULL;
	nowdb_ast_t  *field;
	nowdb_plan_t *stp;
	char hasAgg=0;

	from = nowdb_ast_from(ast);
	if (from == NULL) INVALIDAST("no 'from' in DQL");

	trg = nowdb_ast_target(from);
	if (trg == NULL) INVALIDAST("no target in from");

	err = adjustTarget(scope, trg);
	if (err != NOWDB_OK) return err;

	/* create summary node */
	stp = malloc(sizeof(nowdb_plan_t));
	if (stp == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                      FALSE, OBJECT, "allocating plan");
	stp->ntype = NOWDB_PLAN_SUMMARY;
	stp->stype = 0;
	stp->helper = 1; /* number of targets */
	stp->name = NULL;
	stp->load = NULL;

	/* add summary node */
	if (ts_algo_list_append(plan, stp) != TS_ALGO_OK) {
		return nowdb_err_get(nowdb_err_no_mem,
	                FALSE, OBJECT, "list append");
	}

	/* create filter from where */
	err = getFilter(scope, trg, nowdb_ast_where(ast), &filter);
	if (err != NOWDB_OK) {
		nowdb_plan_destroy(plan, FALSE); return err;
	}

	/* init index list */
	ts_algo_list_init(&idxes);

	/* the selection of indices follows the precedence
	 * group > order > where,
	 * i.e. if we have grouping, use the index for grouping
	 *      if we have ordering, use the index for ordering
	 * only if we don't have grouping nor ordering,
	 *         we use the indices for the where.
	 *
	 * This is, because we currently don't have a way
	 * to order or to group without indices.
	 * At the end, when we have such means,
	 * the precedence should be the contrary:
	 * whenever we can use an index for filtering,
	 * do so! It is better to sort some 1000 data points,
	 * then to scan the whole database -- even when it is
	 * by means of an index.
	 * Exception: when we have a small range! */

	/* check for aggregates */
	sel = nowdb_ast_select(ast);
	if (sel != NULL) {
		field = nowdb_ast_field(sel);
		while(field != NULL) {
			if (field->ntype == NOWDB_AST_FUN) {
				hasAgg = 1; break;
			}
			field = nowdb_ast_field(field);
		}
	}

	/* get group by */
	group = nowdb_ast_group(ast);
	if (group != NULL) {
		err = getFields(scope, trg, group, &grp, NULL);
		if (err != NOWDB_OK) {
			if (filter != NULL) {
				nowdb_filter_destroy(filter); free(filter);
			}
			nowdb_plan_destroy(plan, FALSE); return err;
		}
		/* find index for group by */
		err = getGroupOrderIndex(scope, trg->stype,
		                  trg->value, grp, &idxes);
		if (err != NOWDB_OK) {
			if (filter != NULL) {
				nowdb_filter_destroy(filter); free(filter);
			}
			if (grp != NULL) {
				destroyFieldList(grp); free(grp);
			}
			nowdb_plan_destroy(plan, FALSE); return err;
		}
	}

	/* get order by */
	order = nowdb_ast_order(ast);
	if (order != NULL && idxes.len == 0) {
		err = getFields(scope, trg, order, &ord, NULL);
		if (err != NOWDB_OK) {
			if (filter != NULL) {
				nowdb_filter_destroy(filter); free(filter);
			}
			nowdb_plan_destroy(plan, FALSE); return err;
		}
		/* find index for order by */
		err = getGroupOrderIndex(scope, trg->stype,
		                  trg->value, ord, &idxes);
		if (err != NOWDB_OK) {
			if (filter != NULL) {
				nowdb_filter_destroy(filter); free(filter);
			}
			if (grp != NULL) {
				destroyFieldList(grp); free(grp);
			}
			if (ord != NULL) {
				destroyFieldList(ord); free(ord);
			}
			nowdb_plan_destroy(plan, FALSE); return err;
		}
	}

	/* find indices for filter */
	if (idxes.len == 0) {
		err = getIndices(scope, trg->stype,
		                        trg->value, filter, &idxes);
		if (err != NOWDB_OK) {
			if (filter != NULL) {
				nowdb_filter_destroy(filter); free(filter);
			}
			if (grp != NULL) {
				destroyFieldList(grp); free(grp);
			}
			if (ord != NULL) {
				destroyFieldList(ord); free(ord);
			}
			nowdb_plan_destroy(plan, FALSE); return err;
		}
	}

	/* create reader from target or index */
	stp = malloc(sizeof(nowdb_plan_t));
	if (stp == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
	                                         "allocating plan");
		if (filter != NULL) {
			nowdb_filter_destroy(filter); free(filter);
		}
		if (grp != NULL) {
			destroyFieldList(grp); free(grp);
		}
		if (ord != NULL) {
			destroyFieldList(ord); free(ord);
		}
		ts_algo_list_destroy(&idxes);
		nowdb_plan_destroy(plan, FALSE); return err;
	}

	stp->ntype = NOWDB_PLAN_READER;
	if (idxes.len == 1 && grp == NULL && ord == NULL) {
		stp->stype = NOWDB_PLAN_SEARCH_;
		stp->helper = trg->stype;
		stp->name = trg->value;
		stp->load = idxes.head->cont;

 	/* this is for group without aggregates and without filter */
	} else if (idxes.len == 1 &&
	           order  == NULL &&
	           filter == NULL &&
	           hasAgg == 0) {
		stp->stype = NOWDB_PLAN_KRANGE_;
		stp->helper = trg->stype;
		stp->name = trg->value;
		stp->load = idxes.head->cont;

 	/* this is for order and group with aggregates */
	} else if (idxes.len == 1) {
		stp->stype = NOWDB_PLAN_FRANGE_;
		stp->helper = trg->stype;
		stp->name = trg->value;
		stp->load = idxes.head->cont;
	
	} else {
		stp->stype = NOWDB_PLAN_FS_; /* default is fullscan+ */
		stp->helper = trg->stype;
		stp->name = trg->value;
		stp->load = NULL;
	}

	/* we don't need this anymore */
	ts_algo_list_destroy(&idxes);

	/* add target node */
	if (ts_algo_list_append(plan, stp) != TS_ALGO_OK) {
		err = nowdb_err_get(nowdb_err_no_mem,
	               FALSE, OBJECT, "list append");
		if (filter != NULL) {
			nowdb_filter_destroy(filter); free(filter);
		}
		if (grp != NULL) {
			destroyFieldList(grp); free(grp);
		}
		if (ord != NULL) {
			destroyFieldList(ord); free(ord);
		}
		nowdb_plan_destroy(plan, FALSE); return err;
	}

	/* add filter */
	if (filter != NULL) {
		stp = malloc(sizeof(nowdb_plan_t));
		if (stp == NULL) {
			err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
			                                 "allocating plan");
			if (filter != NULL) {
				nowdb_filter_destroy(filter); free(filter);
			}
			if (grp != NULL) {
				destroyFieldList(grp); free(grp);
			}
			if (ord != NULL) {
				destroyFieldList(ord); free(ord);
			}
			nowdb_plan_destroy(plan, FALSE); return err;
		}

		stp->ntype = NOWDB_PLAN_FILTER;
		stp->stype = 0;
		stp->helper = 0;
		stp->name = NULL;
		stp->load = filter; /* the filter is stored directly here */

		if (ts_algo_list_append(plan, stp) != TS_ALGO_OK) {
			err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                             "list.append");
			if (filter != NULL) {
				nowdb_filter_destroy(filter); free(filter);
			}
			if (grp != NULL) {
				destroyFieldList(grp); free(grp);
			}
			if (ord != NULL) {
				destroyFieldList(ord); free(ord);
			}
			free(stp); return err;
		}
	}

	/* add group by */
	if (group != NULL) {
		stp = malloc(sizeof(nowdb_plan_t));
		if (stp == NULL) {
			NOMEM("allocating plan");
			if (grp != NULL) {
				destroyFieldList(grp); free(grp);
			}
			if (ord != NULL) {
				destroyFieldList(ord); free(ord);
			}
			nowdb_plan_destroy(plan, FALSE); return err;
		}

		stp->ntype = NOWDB_PLAN_GROUPING;
		stp->stype = 0;
		stp->helper = 0;
		stp->name = NULL;
		stp->load = grp; 
	
		if (ts_algo_list_append(plan, stp) != TS_ALGO_OK) {
			NOMEM("list.append");
			if (grp != NULL) {
				destroyFieldList(grp); free(grp);
			}
			if (agg != NULL) {
				destroyFunList(agg); free(agg);
			}
			if (ord != NULL) {
				destroyFieldList(ord); free(ord);
			}
			nowdb_plan_destroy(plan, FALSE);
			free(stp); return err;
		}


	/* add order by */
	} else  if (order != NULL) {
		stp = malloc(sizeof(nowdb_plan_t));
		if (stp == NULL) {
			NOMEM("allocating plan");
			if (ord != NULL) {
				destroyFieldList(ord); free(ord);
			}
			nowdb_plan_destroy(plan, FALSE); return err;
		}

		stp->ntype = NOWDB_PLAN_ORDERING;
		stp->stype = 0;
		stp->helper = 0;
		stp->name = NULL;
		stp->load = ord; 
	
		if (ts_algo_list_append(plan, stp) != TS_ALGO_OK) {
			NOMEM("list.append");
			if (ord != NULL) {
				destroyFieldList(ord); free(ord);
			}
			nowdb_plan_destroy(plan, FALSE);
			free(stp); return err;
		}
	}

	/* add projection */
	if (sel == NULL) return NOWDB_OK;
	if (sel->stype == NOWDB_AST_STAR) return NOWDB_OK;

	err = getFields(scope, trg, sel, &pj, &agg);
	if (err != NOWDB_OK) {
		if (agg != NULL) {
			destroyFunList(agg); free(agg);
		}
		nowdb_plan_destroy(plan, FALSE); return err;
	}

	if (grp != NULL) {
		err = compareForGrouping(grp, pj);
		if (err != NOWDB_OK) {
			if (agg != NULL) {
				destroyFunList(agg); free(agg);
			}
			destroyFieldList(pj); free(pj);
			nowdb_plan_destroy(plan, FALSE); return err;
		}
	}

	stp = malloc(sizeof(nowdb_plan_t));
	if (stp == NULL) {
		NOMEM("allocating plan");
		if (agg != NULL) {
			destroyFunList(agg); free(agg);
		}
		destroyFieldList(pj); free(pj);
		nowdb_plan_destroy(plan, FALSE); return err;
	}

	stp->ntype = NOWDB_PLAN_PROJECTION;
	stp->stype = 0;
	stp->helper = 0;
	stp->name = NULL;
	stp->load = pj; 

	if (ts_algo_list_append(plan, stp) != TS_ALGO_OK) {
		NOMEM("list.append");
		if (agg != NULL) {
			destroyFunList(agg); free(agg);
		}
		destroyFieldList(pj); free(pj);
		nowdb_plan_destroy(plan, FALSE);
		free(stp); return err;
	}
	if (agg != NULL) {
		stp = malloc(sizeof(nowdb_plan_t));
		if (stp == NULL) {
			NOMEM("allocating plan");
			if (agg != NULL) {
				destroyFunList(agg); free(agg);
			}
			nowdb_plan_destroy(plan, FALSE);
			return err;
		}

		stp->ntype = NOWDB_PLAN_AGGREGATES;
		stp->stype = 0;
		stp->helper = 0;
		stp->name = NULL;
		stp->load = agg; 
	
		if (ts_algo_list_append(plan, stp) != TS_ALGO_OK) {
			NOMEM("list.append");
			if (agg != NULL) {
				destroyFunList(agg); free(agg);
			}
			nowdb_plan_destroy(plan, FALSE);
			free(stp); return err;
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Destroy plan
 * ------------------------------------------------------------------------
 */
void nowdb_plan_destroy(ts_algo_list_t *plan, char cont) {
	ts_algo_list_node_t *runner, *tmp;
	nowdb_plan_t *node;

	if (plan == NULL) return;
	runner = plan->head;
	while(runner!=NULL) {
		node = runner->cont;
		// fprintf(stderr, "destroying node [%d]\n", node->ntype);
		if (node->load != NULL) {
			if (cont && node->ntype == NOWDB_PLAN_FILTER) {
				nowdb_filter_destroy(node->load);
				free(node->load);
			}
			if (node->ntype == NOWDB_PLAN_PROJECTION) {
				destroyFieldList(node->load);
				free(node->load);
			}
			if (node->ntype == NOWDB_PLAN_GROUPING) {
				destroyFieldList(node->load);
				free(node->load);
			}
			if (node->ntype == NOWDB_PLAN_AGGREGATES) {
				destroyFunList(node->load);
				free(node->load);
			}
			if (node->ntype == NOWDB_PLAN_ORDERING) {
				destroyFieldList(node->load);
				free(node->load);
			}
			if (node->ntype == NOWDB_PLAN_READER) {
				if (node->stype == NOWDB_PLAN_SEARCH_ ||
				    node->stype == NOWDB_PLAN_FRANGE_ ||
				    node->stype == NOWDB_PLAN_KRANGE_ ||
				    node->stype == NOWDB_PLAN_CRANGE_) 
				{
					nowdb_plan_idx_t *pidx = node->load;
					free(pidx->keys); free(pidx);
				}
			}
		}
		free(node); tmp = runner->nxt;
		ts_algo_list_remove(plan, runner);
		free(runner); runner = tmp;
	}
}
