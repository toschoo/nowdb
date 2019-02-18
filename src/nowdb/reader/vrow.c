/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Vertex row: representation of all vertex properties
 *             needed to evaluate a filter as one single row
 * ========================================================================
 */
#include <nowdb/reader/vrow.h>
#include <nowdb/fun/fun.h>

static char *OBJECT = "vrow";

#define INVALID(m) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, m);

#define NOMEM(m) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, m);

#define ROLESZ sizeof(nowdb_roleid_t)
#define KEYSZ  sizeof(nowdb_key_t)

/* ------------------------------------------------------------------------
 * Represent all properties as a row
 * TODO:
 * pattern shall be list of patterns
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_key_t vertex; /* the vertex              */
	uint64_t      pmap; /* bitmap: seen props      */
	char          *row; /* the row                 */
	int             np; /* current number of props */
	char       hasvrtx; /* needs and has pk        */
} vrow_t;

/* ------------------------------------------------------------------------
 * Mapping properties to offsets (needed?)
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_key_t propid; /* the propid */
	uint32_t       off; /* the offset */
	uint32_t       pos; /* position   */
} propmap_t;

/* ------------------------------------------------------------------------
 * Callbacks for vrtx tree
 * ------------------------------------------------------------------------
 */
#define VROW(x) \
	((vrow_t*)x)

static ts_algo_cmp_t vrowcompare(void *ignore, void *one, void *two) {
	if (VROW(one)->vertex < VROW(two)->vertex) return ts_algo_cmp_less;
	if (VROW(one)->vertex > VROW(two)->vertex) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}
static void vrowdelete(void *ignore, void **n) {}
static void vrowdestroy(void *ignore, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	if (VROW(*n)->row != NULL) {
		free(VROW(*n)->row);
	}
	free(*n); *n=NULL;
}
static ts_algo_rc_t noupdate(void *ignore, void *o, void *n) {
	/* vrowdestroy(NULL, &n); */ return TS_ALGO_OK;
}

static inline void destroyVRow(vrow_t *vrow) {
	vrowdestroy(NULL, (void**)&vrow);
}

#define DESTROYREADY(v) \
	for(ts_algo_list_node_t *run=v->ready->head; \
	                    run!=NULL; run=run->nxt) \
	{ \
		vrowdestroy(NULL, &run->cont); \
	} \
	ts_algo_list_destroy(v->ready); \
	free(v->ready); v->ready=NULL;

/* ------------------------------------------------------------------------
 * Callbacks for propmap
 * ------------------------------------------------------------------------
 */
#define PROP(x) \
	((propmap_t*)x)

static ts_algo_cmp_t propcompare(void *ignore, void *one, void *two) {
	if (PROP(one)->propid < PROP(two)->propid) return ts_algo_cmp_less;
	if (PROP(one)->propid > PROP(two)->propid) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}
static void propdelete(void *ignore, void **n) {}
static void propdestroy(void *ignore, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	free(*n); *n=NULL;
}

#define DESTROYIN(l) \
	for(ts_algo_list_node_t *run=l->head; \
	             run!=NULL; run=run->nxt) { \
		free(run->cont); \
	} \
	ts_algo_list_destroy(l);

/* ------------------------------------------------------------------------
 * Copy in list
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t copyList(ts_algo_list_t  *l1,
                                   uint32_t         sz,
                                   ts_algo_list_t **l2) {
	ts_algo_list_node_t *run;
	nowdb_err_t err;

	*l2 = calloc(1,sizeof(ts_algo_list_t));
	if (*l2 == NULL) {
		NOMEM("allocating list");
		return err;
	}
	ts_algo_list_init(*l2);

	for(run = l1->head; run!=NULL; run=run->nxt) {
		void *val = malloc(sz);
		if (val == NULL) {
			NOMEM("allocating value");
			DESTROYIN((*l2));
			free(*l2); *l2 = NULL;
			return err;
		}
		memcpy(val, run->cont, sz);
		if (ts_algo_list_append(*l2, val) != TS_ALGO_OK) {
			NOMEM("list.append");
			DESTROYIN((*l2));
			free(*l2); *l2 = NULL;
			return err;
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Get/Insert a vrow
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getVRow(nowdb_vrow_t   *vrow,
                                  nowdb_vertex_t *vrtx,
                                  vrow_t         **v) { 
	nowdb_err_t err;
	vrow_t pattern;

	memcpy(&pattern.vertex, &vrtx->vertex, KEYSZ);
	*v = ts_algo_tree_find(vrow->vrtx, &pattern);
	if (*v == NULL) {
		*v = calloc(1, sizeof(vrow_t));
		if (*v == NULL) {
			NOMEM("allocating vrow");
			return err;
		}

		(*v)->vertex = vrtx->vertex;
		(*v)->np = 0;
		(*v)->hasvrtx = 0;
		(*v)->pmap = 0;

		(*v)->row = calloc(1,vrow->size);
		if ((*v)->row == NULL) {
			free(*v); *v=NULL;
			NOMEM("allocating vrow values");
			return err;
		}
		memcpy((*v)->row, &vrow->role, ROLESZ);

		if (ts_algo_tree_insert(vrow->vrtx, *v) != TS_ALGO_OK) {
			free((*v)->row);
			free(*v); *v=NULL;
			NOMEM("vertex.insert");
			return err;
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Insert a property into propmap
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t insertProperty(nowdb_vrow_t *vrow,
                                         void         *val,
                                         int          *cnt,
                                         int          *off) {
	nowdb_err_t err;
	propmap_t pattern, *res;

	// find offset
	// fprintf(stderr, "inserting property %lu\n", *(uint64_t*)src->val);
	memcpy(&pattern.propid, val, KEYSZ);
	res = ts_algo_tree_find(vrow->pspec, &pattern);
	if (res == NULL) {
		res = calloc(1, sizeof(propmap_t));
		if (res == NULL) {
			NOMEM("allocating propmap");
			return err;
		}
		memcpy(&res->propid, val, KEYSZ);
		res->pos = *cnt;
 		res->off = ROLESZ+KEYSZ*(*cnt);(*cnt)++;

		if (ts_algo_tree_insert(vrow->pspec, res) != TS_ALGO_OK) {
			NOMEM("allocating propmap");
			return err;
		}
	}
	*off = (int)res->off;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Insert vertex into propmap
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t insertVertex(nowdb_vrow_t *vrow,
                                       int          *cnt,
                                       int          *off) {
	nowdb_err_t err;
	propmap_t pattern, *res;

	// find offset
	// fprintf(stderr, "inserting vertex\n");
	pattern.propid=NOWDB_OFF_VERTEX;
	res = ts_algo_tree_find(vrow->pspec, &pattern);
	if (res == NULL) {
		res = calloc(1, sizeof(propmap_t));
		if (res == NULL) {
			NOMEM("allocating propmap");
			return err;
		}
		res->propid=NOWDB_OFF_VERTEX;
		res->pos = *cnt;
 		res->off = ROLESZ+KEYSZ*(*cnt);(*cnt)++;

		if (ts_algo_tree_insert(vrow->pspec, res) != TS_ALGO_OK) {
			NOMEM("allocating propmap");
			return err;
		}
	}
	*off = (int)res->off;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Get property offset from propmap
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getProperty(nowdb_vrow_t  *vrow,
                                      propmap_t  *pattern,
                                      propmap_t       **p) {
	// find offset
	*p = ts_algo_tree_find(vrow->pspec, pattern);
	return NOWDB_OK;
}

#define FIELD(x) \
	NOWDB_EXPR_TOFIELD(x)

#define OP(x) \
	NOWDB_EXPR_TOOP(x)

#define ARG(x,i) \
	NOWDB_EXPR_TOOP(x)->argv[i]

#define SETARG(x,i,n) \
	NOWDB_EXPR_TOOP(x)->argv[i] = n

/* ------------------------------------------------------------------------
 * substitute x by "and(=(key,key), x)
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t addKey(nowdb_expr_t       *x,
                                 nowdb_model_prop_t *p) {
	nowdb_err_t err;
	nowdb_expr_t f1, f2;
	nowdb_expr_t eq, n;

	err = nowdb_expr_newVertexField(&f1, p->name,
	                                     p->roleid,
	                                     p->propid,
	                                     p->value);
	if (err != NOWDB_OK) return err;
	nowdb_expr_usekey(f1);

	err = nowdb_expr_newVertexField(&f2, p->name,
	                                     p->roleid,
	                                     p->propid,
	                                     p->value);
	if (err != NOWDB_OK) {
		nowdb_expr_destroy(f1); free(f1);
		return err;
	}
	nowdb_expr_usekey(f2);

        err = nowdb_expr_newOp(&eq, NOWDB_EXPR_OP_EQ, f1, f2);
	if (err != NOWDB_OK) {
		nowdb_expr_destroy(f1); free(f1);
		nowdb_expr_destroy(f2); free(f2);
		return err;
	}

	err = nowdb_expr_newOp(&n, NOWDB_EXPR_OP_AND, eq, *x);
	if (err != NOWDB_OK) {
		nowdb_expr_destroy(eq); free(eq);
		return err;
	}
	*x = n;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * update a filter expression recursively
 * (can be merge with updXNode)
 * ------------------------------------------------------------------------
 */
static nowdb_err_t updFNode(nowdb_vrow_t *vrow,
                            int           *cnt,
                            int           *off, // we don't to pass this through
                            nowdb_expr_t  expr) {
	nowdb_err_t err;

	switch(nowdb_expr_type(expr)) {
	case NOWDB_EXPR_CONST: return NOWDB_OK;

	case NOWDB_EXPR_REF: return updFNode(vrow, cnt, off,
	                        NOWDB_EXPR_TOREF(expr)->ref);

	case NOWDB_EXPR_AGG:
		return updFNode(vrow, cnt, off,
	               ((nowdb_fun_t*)NOWDB_EXPR_TOAGG(
	                             expr)->agg)->expr);

	case NOWDB_EXPR_OP:
		for(int i=0; i<OP(expr)->args; i++) {
			err = updFNode(vrow, cnt, off,
			           OP(expr)->argv[i]);
			if (err != NOWDB_OK) return err;
		}
		return NOWDB_OK;

	case NOWDB_EXPR_FIELD: 

		switch(FIELD(expr)->off) {
		case NOWDB_OFF_ROLE:
			FIELD(expr)->off = 0;
			return NOWDB_OK;

		case NOWDB_OFF_VERTEX:
			err = insertVertex(vrow, cnt, off);
			if (err != NOWDB_OK) return err;
			vrow->wantvrtx = 1;
			FIELD(expr)->off = *off;
			return NOWDB_OK;

		case NOWDB_OFF_VALUE:
			err = insertProperty(vrow,
			     &FIELD(expr)->propid,
		                         cnt,off);
			if (err != NOWDB_OK) return err;
			FIELD(expr)->off = *off;
			return NOWDB_OK;
		default: return NOWDB_OK;
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Update expression recursively
 * ------------------------------------------------------------------------
 */
static nowdb_err_t updXNode(nowdb_vrow_t *vrow,
                            int          *cnt,
                            int          *off, // we don't need to pass this through
                            nowdb_expr_t expr) {
	nowdb_err_t err;

	switch(nowdb_expr_type(expr)) {
	case NOWDB_EXPR_CONST: return NOWDB_OK;

	case NOWDB_EXPR_REF: return updXNode(vrow, cnt, off,
	                        NOWDB_EXPR_TOREF(expr)->ref);

	case NOWDB_EXPR_AGG:
		return updXNode(vrow, cnt, off,
	               ((nowdb_fun_t*)NOWDB_EXPR_TOAGG(
	                             expr)->agg)->expr);

	case NOWDB_EXPR_OP: 
		for(int i=0; i<NOWDB_EXPR_TOOP(expr)->args; i++) {
			err = updXNode(vrow, cnt, off,
			  NOWDB_EXPR_TOOP(expr)->argv[i]);
			if (err != NOWDB_OK) return err;
		}
		return NOWDB_OK;

	case NOWDB_EXPR_FIELD: 
	
		err = insertProperty(vrow,&NOWDB_EXPR_TOFIELD(
			                expr)->propid,cnt,off);
		if (err != NOWDB_OK) return err;

		NOWDB_EXPR_TOFIELD(expr)->off = *off;
		return NOWDB_OK;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * filter -> and(=(key,key), filter)
 * ------------------------------------------------------------------------
 */
static nowdb_err_t addPK(nowdb_vrow_t *vrow,
                         nowdb_expr_t *x) {
	nowdb_err_t err;
	nowdb_model_prop_t *p;

	if (vrow->eval->model == NULL) return NOWDB_OK;

	err = nowdb_model_getPK(vrow->eval->model,
                                vrow->role,   &p);
	if (err != NOWDB_OK) return err;

	return addKey(x, p);
}

/* ------------------------------------------------------------------------
 * Copy/Change filter
 * ------------------------------------------------------------------------
 */
static nowdb_err_t copyFilter(nowdb_vrow_t *vrow,
                              nowdb_expr_t   fil) {
	nowdb_err_t err;
	int off=-1;
	int cnt=0;

	/*
	fprintf(stderr, "FILTER BEFORE:\n");
	nowdb_expr_show(fil, stderr); fprintf(stderr, "\n");
	*/

	err = nowdb_expr_copy(fil, &vrow->filter);
	if (err != NOWDB_OK) return err;

	err = addPK(vrow, &vrow->filter);
	if (err != NOWDB_OK) return err;

	err = updFNode(vrow, &cnt, &off, vrow->filter);
	if (err != NOWDB_OK) return err;

	vrow->np = (uint32_t)cnt;
	vrow->size = (uint32_t)ROLESZ+cnt*KEYSZ;

	// nowdb_expr_show(vrow->filter, stderr); fprintf(stderr, "\n");

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Create a vertex row for a given filter
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_vrow_fromFilter(nowdb_roleid_t role,
                                  nowdb_vrow_t **vrow,
                                  nowdb_expr_t    fil,
                                  nowdb_eval_t  *eval) {
	nowdb_err_t err;

	if (fil  == NULL) INVALID("filter is NULL");

	err = nowdb_vrow_new(role, vrow, eval);
	if (err != NOWDB_OK) return err;

	err = copyFilter(*vrow, fil);
	if (err != NOWDB_OK) {
		nowdb_vrow_destroy(*vrow);
		free(*vrow); *vrow = NULL;
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Create a vertex row for a given expression
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_vrow_new(nowdb_roleid_t role,
                           nowdb_vrow_t **vrow,
                           nowdb_eval_t  *eval) {
	nowdb_err_t err;

	if (vrow == NULL) INVALID("vrow pointer is NULL");

	*vrow = calloc(1, sizeof(nowdb_vrow_t));
	if (*vrow == NULL) {
		NOMEM("allocating vertex row");
		return err;
	}

	(*vrow)->role = role;
	(*vrow)->eval = eval;

	(*vrow)->ready = calloc(1, sizeof(ts_algo_list_t));
	if ((*vrow)->ready == NULL) {
		free(*vrow); *vrow = NULL;
		NOMEM("allocating ready list");
		return err;
	}
	ts_algo_list_init((*vrow)->ready);
	
	(*vrow)->vrtx = ts_algo_tree_new(&vrowcompare, NULL,
	                                 &noupdate,
	                                 &vrowdelete,
	                                 &vrowdestroy);
	if ((*vrow)->vrtx == NULL) {
		nowdb_vrow_destroy(*vrow);
		free(*vrow); *vrow = NULL;
		NOMEM("tree.new");
		return err;
	}

	(*vrow)->pspec = ts_algo_tree_new(&propcompare, NULL,
	                                  &noupdate,
	                                  &propdelete,
	                                  &propdestroy);
	if ((*vrow)->pspec == NULL) {
		nowdb_vrow_destroy(*vrow);
		free(*vrow); *vrow = NULL;
		NOMEM("tree.new");
		return err;
	}

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Add expression to an existing vrow
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_vrow_addExpr(nowdb_vrow_t *vrow, 
                               nowdb_expr_t  expr) {
	nowdb_err_t err;
	int off=-1;
	int cnt=0;

	cnt = (int)vrow->np;

	err = updXNode(vrow, &cnt, &off, expr);
	if (err != NOWDB_OK) return err;

	vrow->np = (uint32_t)cnt;
	vrow->size = (uint32_t)ROLESZ+cnt*KEYSZ;

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Destroy the vertex row
 * ------------------------------------------------------------------------
 */
void nowdb_vrow_destroy(nowdb_vrow_t *vrow) {
	if (vrow == NULL) return;
	if (vrow->vrtx != NULL) {
		ts_algo_tree_destroy(vrow->vrtx);
		free(vrow->vrtx); vrow->vrtx = NULL;
	}
	if (vrow->pspec != NULL) {
		ts_algo_tree_destroy(vrow->pspec);
		free(vrow->pspec); vrow->pspec = NULL;
	}
	if (vrow->ready != NULL) {
		DESTROYREADY(vrow);
	}
	if (vrow->filter != NULL) {
		nowdb_expr_destroy(vrow->filter);
		free(vrow->filter); vrow->filter = NULL;
	}
	if (vrow->prev != NULL) {
		free(vrow->prev); vrow->prev = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Set force completion
 * ------------------------------------------------------------------------
 */
void nowdb_vrow_autoComplete(nowdb_vrow_t *vrow) {
	vrow->forcecomp = 1;
}

/* ------------------------------------------------------------------------
 * Try to add a vertex property
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_vrow_add(nowdb_vrow_t   *vrow,
                           nowdb_vertex_t *vertex,
                           char           *added) {
	nowdb_err_t err;
	propmap_t pattern, *pm;
	vrow_t *v;

	*added = 0;

	// not our kind
	// be careful: for joins, we have to relax that
	if (vertex->role != vrow->role) return NOWDB_OK;

	// find offset in propmap
	pattern.propid = vertex->property;
	err = getProperty(vrow, &pattern, &pm);
	if (err != NOWDB_OK) return err;
	if (pm == NULL) {
		/*
		fprintf(stderr, "prop not found %lu/%lu/%lu\n",
	                vertex->vertex, vertex->property, vertex->value);
		*/
		return NOWDB_OK;
	}

	// get vrow
	err = getVRow(vrow, vertex, &v);
	if (err != NOWDB_OK) return err;

	// add according to offset

	/*
	fprintf(stderr, "property %lu/%lu/%lu into %u\n",
	  vertex->vertex, vertex->property, vertex->value, pm->off);
	*/
	
	memcpy(v->row+pm->off, &vertex->value, KEYSZ);
	// fprintf(stderr, "setting %d (%u/%u)\n", 1 << pm->pos, pm->pos, vrow->np);
	v->pmap |= (1 << pm->pos);
	v->np++;

	// filter requests vid
	if (vrow->wantvrtx && !v->hasvrtx) {
		pattern.propid = NOWDB_OFF_VERTEX;
		err = getProperty(vrow, &pattern, &pm);
		if (err != NOWDB_OK) return err;
		if (pm == NULL) return NOWDB_OK; // error
		/*
		fprintf(stderr, "vertex %lu/%lu/%lu into %u\n",
		        vertex->vertex, vertex->property,
		        vertex->value, pm->off);
		*/
		memcpy(v->row+pm->off, &vertex->vertex, KEYSZ);
		v->np++; v->hasvrtx = 1;
		v->pmap |= (1 << pm->pos);
	}

	// force completion of prev
	if (vrow->forcecomp) {
		if (vrow->prev == NULL) {
			vrow->prev = malloc(KEYSZ);
			if (vrow->prev == NULL) {
				NOMEM("allocating vid");
				return err;
			}
			memcpy(vrow->prev, &vertex->vertex, KEYSZ);

		} else if (*vrow->prev != vertex->vertex) {
			vrow_t *v2, vpattern;
			// fprintf(stderr, "FORCING MATTERS\n");
			vpattern.vertex = *vrow->prev;
			memcpy(vrow->prev, &vertex->vertex, KEYSZ);
			v2 = ts_algo_tree_find(vrow->vrtx, &vpattern);
			if (v2 != NULL) {
				// fprintf(stderr, "have old one\n");
				ts_algo_tree_delete(vrow->vrtx, v2);
				if (ts_algo_list_append(
				    vrow->ready, v2) != TS_ALGO_OK) {
					NOMEM("ready.append");
					return err;
				}
			}
		}
	}

	// if complete, delete and add to ready
	if (v->np >= vrow->np) {
		ts_algo_tree_delete(vrow->vrtx, v);
		if (ts_algo_list_append(vrow->ready, v) != TS_ALGO_OK) {
			NOMEM("ready.append");
			return err;
		}
		/*
		fprintf(stderr, "complete, have: %d / %d\n",
		                          vrow->vrtx->count,
		                          vrow->ready->len);
		*/
	}
	*added=1;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Filter vrow with np > 0
 * ------------------------------------------------------------------------
 */
static ts_algo_bool_t oneProp(void *ignore, const void *one,
                                            const void *two) 
{
	// this is redundant: if there is an entitiy,
	// its property counter is at least 1...
	return (VROW(two)->np > 0);
}

/* ------------------------------------------------------------------------
 * Force completion of all rows
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_vrow_force(nowdb_vrow_t *vrow) {
	nowdb_err_t err;
	ts_algo_list_node_t *run;

	if (vrow == NULL) return NOWDB_OK;

	// filter all with np > 0
	for(run=vrow->ready->head; run!=NULL;) {
		if (run->nxt == NULL) break;
		run = run->nxt;
	}
	if (ts_algo_tree_filter(vrow->vrtx, vrow->ready,
	                  NULL, &oneProp) != TS_ALGO_OK) 
	{
		NOMEM("tree.filter");
		return err;
	}
	if (run==NULL) run=vrow->ready->head; else run=run->nxt;
	for(;run!=NULL;run=run->nxt) {
		ts_algo_tree_delete(vrow->vrtx, run->cont);
	}
	// fprintf(stderr, "having %d\n", vrow->ready->len);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Check for complete vertex
 * ------------------------------------------------------------------------
 */
nowdb_bool_t nowdb_vrow_complete(nowdb_vrow_t *vrow,
                                 uint32_t     *size,
                                 uint64_t     *pmap,
                                 char         **row) {
	ts_algo_list_node_t *node;
	vrow_t *v;

	if (vrow->ready->len == 0) return 0;

	*size = vrow->size;
	node = vrow->ready->head;
	v = node->cont;
	*row = v->row;
	v->row = NULL;
	*pmap = v->pmap;
	ts_algo_list_remove(vrow->ready, node);
	free(node); destroyVRow(v);
		
	return 1;
}

/* ------------------------------------------------------------------------
 * Check if vrow is empty
 * ------------------------------------------------------------------------
 */
nowdb_bool_t nowdb_vrow_empty(nowdb_vrow_t *vrow) {
	return (vrow->ready->len == 0);
}

/* ------------------------------------------------------------------------
 * Try to evaluate
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_vrow_eval(nowdb_vrow_t *vrow,
                            nowdb_key_t   *vid,
                            nowdb_bool_t *found) {

	nowdb_err_t err;
	ts_algo_list_node_t *node;
	vrow_t *v;
	void *x;
	nowdb_type_t t;

	*found = 0;

	if (vrow->ready->len == 0) return NOWDB_OK;

	node = vrow->ready->head;
	v = node->cont;
	
	err = nowdb_expr_eval(vrow->filter,
	                        vrow->eval,
	                           v->pmap,
	                   v->row, &t, &x);
	if (err != NOWDB_OK) return err;
	if (t != NOWDB_TYP_NOTHING &&
	    *(nowdb_value_t*)x) {
		*found = 1;
		memcpy(vid, &v->vertex, 8);
	}
	ts_algo_list_remove(vrow->ready, node);
	free(node); destroyVRow(v);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Show current vrows and their state (debugging)
 * ------------------------------------------------------------------------
 */
static void showVRow(vrow_t *row, uint32_t np) {
	fprintf(stderr, "vid: %lu (%u/%u): ",
	                       row->vertex,
	                       row->np, np);
	if (row->np == np) {
		// show all properties
		fprintf(stderr, "|%u| ", row->row[0]);
		for(int i=0; i<row->np; i++) {
			fprintf(stderr, "%lu, ", *(uint64_t*)(row->row+4+i*8));
		}
	}
	fprintf(stderr, "\n");
}

/* ------------------------------------------------------------------------
 * Show current vrows and their state (debugging)
 * ------------------------------------------------------------------------
 */
void nowdb_vrow_show(nowdb_vrow_t *vrow) {
	ts_algo_list_node_t *run;

	fprintf(stderr, "having %d pending\n", vrow->vrtx->count);
	if (vrow->ready->len > 0) {
		for(run=vrow->ready->head;run!=NULL;run=run->nxt) {
			showVRow(run->cont, vrow->np);
		}
	}
}
