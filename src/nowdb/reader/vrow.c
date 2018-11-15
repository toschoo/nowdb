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
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_key_t vertex; /* the vertex              */
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
                                         int          *cnt) {
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
 		res->off = ROLESZ+KEYSZ*(*cnt);(*cnt)++;

		if (ts_algo_tree_insert(vrow->pspec, res) != TS_ALGO_OK) {
			NOMEM("allocating propmap");
			return err;
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Insert vertex into propmap
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t insertVertex(nowdb_vrow_t  *vrow,
                                       int            *cnt) {
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
 		res->off = ROLESZ+KEYSZ*(*cnt);(*cnt)++;

		if (ts_algo_tree_insert(vrow->pspec, res) != TS_ALGO_OK) {
			NOMEM("allocating propmap");
			return err;
		}
	}
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

/* ------------------------------------------------------------------------
 * Copy filter recursively
 * ------------------------------------------------------------------------
 */
static nowdb_err_t copyFNode(nowdb_vrow_t   *vrow,
                             int             *cnt,
                             int             *off,
                             nowdb_filter_t  *src,
                             nowdb_filter_t **trg) {
	nowdb_err_t err;
	ts_algo_list_t *in, *in2=NULL;

	if (src->ntype == NOWDB_FILTER_BOOL) {
		err = nowdb_filter_newBool(trg, src->op);
		if (err != NOWDB_OK) return err;

	} else if (src->off == NOWDB_OFF_PROP) {
		err = nowdb_filter_newBool(trg, NOWDB_FILTER_TRUE);
		if (err != NOWDB_OK) return err;

		err = insertProperty(vrow, src->val, cnt);
		if (err != NOWDB_OK) {
			nowdb_filter_destroy(*trg);
			free(*trg); *trg = NULL;
			return err;
		}
		*off=ROLESZ+KEYSZ*(*cnt-1);
		return NOWDB_OK;

	} else {
		// role is always the first
		if (src->off == NOWDB_OFF_ROLE) {
			*off = 0;

		// use the prop offset from the propmap (for property)
		} else if (src->off == NOWDB_OFF_VALUE) {
			// fprintf(stderr, "VALUE\n");
			if (*off < 0) {
				INVALID("no valid offset");
			}

		// use the prop offset from the propmap (for vertex)
		} else if (src->off == NOWDB_OFF_VERTEX) {
			// fprintf(stderr, "VERTEX\n");
			err = insertVertex(vrow, cnt);
			if (err != NOWDB_OK) return err;
			vrow->wantvrtx = 1;
			
			*off=ROLESZ+KEYSZ*(*cnt-1);
	
		// ignore anything else	
		} else {
			// fprintf(stderr, "IGNORE %u\n", src->off);
			err = nowdb_filter_newBool(trg, NOWDB_FILTER_TRUE);
			if (err != NOWDB_OK) return err;
			return NOWDB_OK;
		}

		// we don't own the value,
		// but we own the in-list...
		if (src->in != NULL) {
			in = ts_algo_tree_toList(src->in);
			if (in == NULL) {
				NOMEM("tree.toList");
				return err;
			}
			err = copyList(in, src->size, &in2);
			ts_algo_list_destroy(in); free(in);
			if (err != NOWDB_OK) return err;
		}
		err = nowdb_filter_newCompare(trg, src->op, *off,
                                                   src->size,  
                                                   src->typ,
		                                   src->val, in2);
		if (err != NOWDB_OK) {
			if (in2 != NULL) {
				DESTROYIN(in2); free(in2);
			}
			return err;
		}
		if (in2 != NULL) {
			ts_algo_list_destroy(in2); free(in2);
		}
		return NOWDB_OK;
	}
	// go left (here we expect the propid)
	if (src->left != NULL) {
		*off = -1;
		err = copyFNode(vrow, cnt, off, src->left, &(*trg)->left);
		if (err != NOWDB_OK) {
			nowdb_filter_destroy(*trg);
			free(*trg); *trg = NULL;
			return err;
		}
	}
	// go left (here we expect the value)
	if (src->right != NULL) {
		err = copyFNode(vrow, cnt, off, src->right, &(*trg)->right);
		if (err != NOWDB_OK) {
			nowdb_filter_destroy(*trg);
			free(*trg); *trg = NULL;
			return err;
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
                            int          *off,
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
		// if (NOWDB_EXPR_TOFIELD(expr)->off == NOWDB_OFF_VALUE) {
	
			err = insertProperty(vrow,&NOWDB_EXPR_TOFIELD(
			                            expr)->propid,cnt);
			if (err != NOWDB_OK) return err;

			*off=ROLESZ+KEYSZ*(*cnt-1);
			NOWDB_EXPR_TOFIELD(expr)->off = *off;
		// } // what else?
		break;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Copy/Change filter
 * ------------------------------------------------------------------------
 */
static nowdb_err_t copyFilter(nowdb_vrow_t  *vrow,
                              nowdb_filter_t *fil) {
	nowdb_err_t err;
	int off=-1;
	int cnt=0;

	/*
	fprintf(stderr, "FILTER BEFORE:\n");
	nowdb_filter_show(fil, stderr); fprintf(stderr, "\n");
	*/

	err = copyFNode(vrow, &cnt, &off, fil, &vrow->filter);
	if (err != NOWDB_OK) return err;

	vrow->np = (uint32_t)cnt;
	vrow->size = (uint32_t)ROLESZ+cnt*KEYSZ;

	// nowdb_filter_show(vrow->filter, stderr); fprintf(stderr, "\n");

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Copy/Change Expression
 * ------------------------------------------------------------------------
 */
/*
static nowdb_err_t copyExpr(nowdb_vrow_t *vrow,
                            nowdb_expr_t  expr) {
	nowdb_err_t err;
	int off=-1;
	int cnt=0;

	err = nowdb_expr_copy(expr, &vrow->expr);
	if (err != NOWDB_OK) return err;

	err = updXNode(vrow, &cnt, &off, vrow->expr);

	vrow->np = (uint32_t)cnt;
	vrow->size = (uint32_t)ROLESZ+cnt*KEYSZ;

	return NOWDB_OK;
}
*/

/* ------------------------------------------------------------------------
 * Create a vertex row for a given filter
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_vrow_fromFilter(nowdb_roleid_t role,
                                  nowdb_vrow_t **vrow,
                                  nowdb_filter_t *fil) {
	nowdb_err_t err;

	if (fil  == NULL) INVALID("filter is NULL");

	err = nowdb_vrow_new(role, vrow);
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
                           nowdb_vrow_t **vrow) {
	nowdb_err_t err;

	if (vrow == NULL) INVALID("vrow pointer is NULL");

	*vrow = calloc(1, sizeof(nowdb_vrow_t));
	if (*vrow == NULL) {
		NOMEM("allocating vertex row");
		return err;
	}

	(*vrow)->role = role;

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

	/*
	err = copyExpr(*vrow, expr);
	if (err != NOWDB_OK) {
		nowdb_vrow_destroy(*vrow);
		free(*vrow); *vrow = NULL;
		return err;
	}
	*/
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
		nowdb_filter_destroy(vrow->filter);
		free(vrow->filter); vrow->filter = NULL;
	} 
	if (vrow->expr != NULL) {
		nowdb_expr_destroy(vrow->expr);
		free(vrow->expr); vrow->expr = NULL;
	} 
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
	if (pm == NULL) return NOWDB_OK;

	// get vrow
	err = getVRow(vrow, vertex, &v);
	if (err != NOWDB_OK) return err;

	// add according to offset
	
	/*
	fprintf(stderr, "putting %lu/%lu/%lu into %u\n",
	  vertex->vertex, vertex->property, vertex->value, pm->off);
	*/
	
	memcpy(v->row+pm->off, &vertex->value, KEYSZ);
	v->np++;

	// filter requests vid
	if (vrow->wantvrtx && !v->hasvrtx) {
		pattern.propid = NOWDB_OFF_VERTEX;
		err = getProperty(vrow, &pattern, &pm);
		if (err != NOWDB_OK) return err;
		if (pm == NULL) return NOWDB_OK; // error
		memcpy(v->row+pm->off, &vertex->vertex, KEYSZ);
		v->np++; v->hasvrtx = 1;
	}

	// if complete, delete and add to ready
	if (v->np >= vrow->np) {
		ts_algo_tree_delete(vrow->vrtx, v);
		if (ts_algo_list_append(vrow->ready, v) != TS_ALGO_OK) {
			NOMEM("ready.append");
			return err;
		}
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
	return (VROW(two)->np > 0);
}

/* ------------------------------------------------------------------------
 * Force completion of all rows
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_vrow_force(nowdb_vrow_t *vrow) {
	nowdb_err_t err;
	ts_algo_list_node_t *run;

	// filter all with np > 0
	for(run=vrow->ready->head; run!=NULL;) {
		if (run->nxt == NULL) break;
	}
	if (ts_algo_tree_filter(vrow->vrtx, vrow->ready,
	                  NULL, &oneProp) != TS_ALGO_OK) 
	{
		NOMEM("tree.filter");
		return err;
	}
	if (run==NULL) run=vrow->ready->head;
	for(;run!=NULL;run=run->nxt) {
		ts_algo_tree_delete(vrow->vrtx, run->cont);
	}
	fprintf(stderr, "ready: %d\n", vrow->ready->len);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Check for complete vertex
 * ------------------------------------------------------------------------
 */
nowdb_bool_t nowdb_vrow_complete(nowdb_vrow_t *vrow,
                                 uint32_t     *size,
                                 char        **row) {
	ts_algo_list_node_t *node;
	vrow_t *v;

	if (vrow->ready->len == 0) return 0;

	*size = vrow->size;
	node = vrow->ready->head;
	v = node->cont;
	*row = v->row;
	v->row = NULL;
	ts_algo_list_remove(vrow->ready, node);
	free(node); destroyVRow(v);
		
	return 1;
}

/* ------------------------------------------------------------------------
 * Try to evaluate
 * ------------------------------------------------------------------------
 */
nowdb_bool_t nowdb_vrow_eval(nowdb_vrow_t *vrow,
                            nowdb_key_t   *vid) {
	ts_algo_list_node_t *node;
	char found = 0;
	vrow_t *v;

	if (vrow->ready->len == 0) return 0;

	node = vrow->ready->head;
	v = node->cont;
	
	if (nowdb_filter_eval(vrow->filter, v->row)) {
		found = 1;
		*vid = v->vertex;
	}
	ts_algo_list_remove(vrow->ready, node);
	free(node); destroyVRow(v);
	return found;
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
