/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Vertex row: representation of all vertex properties
 *             needed to evaluate a filter as one single row
 * ========================================================================
 */
#include <nowdb/reader/vrow.h>

static char *OBJECT = "vrow";

#define INVALID(m) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, m);

#define NOMEM(m) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, m);

/* ------------------------------------------------------------------------
 * Represent all properties as a row
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_key_t vertex; /* the vertex */
	nowdb_value_t *row; /* the row    */
	char         state; /* state      */
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
static ts_algo_rc_t noupdate(void *ignore, void *o, void *n) {
	free(n); return TS_ALGO_OK;
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
 * Insert a property into propmap
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t insertProperty(nowdb_vrow_t  *vrow,
                                         nowdb_filter_t *src,
                                         int            *cnt) {
	nowdb_err_t err;
	propmap_t pattern, *res;

	// find offset
	memcpy(&pattern.propid, src->val, 8);
	res = ts_algo_tree_find(vrow->pspec, &pattern);
	if (res == NULL) {
		res = calloc(1, sizeof(propmap_t));
		if (res == NULL) {
			NOMEM("allocating propmap");
			return err;
		}
		memcpy(&res->propid, src->val, 8);
		res->off = 8*(*cnt); (*cnt)++;

		if (ts_algo_tree_insert(vrow->pspec, res) != TS_ALGO_OK) {
			NOMEM("allocating propmap");
			return err;
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Copy filter recursively
 * ------------------------------------------------------------------------
 */
static nowdb_err_t copyNode(nowdb_vrow_t   *vrow,
                            int            count,
                            nowdb_filter_t  *src,
                            nowdb_filter_t **trg) {
	nowdb_err_t err;
	int cnt = count;
	ts_algo_list_t *in, *in2=NULL;

	if (src->ntype == NOWDB_FILTER_BOOL) {
		err = nowdb_filter_newBool(trg, src->op);
		if (err != NOWDB_OK) return err;

	// TODO:
	// if the src->off is propid,
	// ignore the propid (replace it by TRUE)
	// and insert the propid
	} else if (src->off == NOWDB_OFF_PROP) {
		err = nowdb_filter_newBool(trg, NOWDB_FILTER_TRUE);
		if (err != NOWDB_OK) return err;

		err = insertProperty(vrow, src, &cnt);
		if (err != NOWDB_OK) {
			nowdb_filter_destroy(*trg);
			free(*trg); *trg = NULL;
			return err;
		}
		return NOWDB_OK;

	} else {
		// we don't own the value,
		// but we own the in-list...
		if (src->in != NULL) {
			in = ts_algo_tree_toList(src->in);
			if (in == NULL) {
				NOMEM("tree.toList");
				return err;
			}
			err = copyList(in, src->size, &in2);
			ts_algo_list_destroy(in);
			if (err != NOWDB_OK) return err;
		}
		err = nowdb_filter_newCompare(trg, src->op,
                                       src->off, src->size,
                                       src->typ, src->val,in2);
		if (err != NOWDB_OK) {
			DESTROYIN(in2); free(in2);
			return err;
		}
		ts_algo_list_destroy(in2); free(in2);
		return NOWDB_OK;
	}
	if (src->left != NULL) {
		err = copyNode(vrow, cnt, src->left, &(*trg)->left);
		if (err != NOWDB_OK) {
			nowdb_filter_destroy(*trg);
			free(*trg); *trg = NULL;
			return err;
		}
	}
	if (src->right != NULL) {
		err = copyNode(vrow, cnt, src->right, &(*trg)->right);
		if (err != NOWDB_OK) {
			nowdb_filter_destroy(*trg);
			free(*trg); *trg = NULL;
			return err;
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Copy/Change filter
 * ------------------------------------------------------------------------
 */
static nowdb_err_t copyFilter(nowdb_vrow_t  *vrow,
                              nowdb_filter_t *fil) {
	return copyNode(vrow, 0, fil, &vrow->filter);
}

/* ------------------------------------------------------------------------
 * Create a vertex row for a given filter
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_vrow_create(nowdb_roleid_t role,
                              nowdb_vrow_t **vrow,
                              nowdb_filter_t *fil) {
	nowdb_err_t err;

	if (vrow == NULL) INVALID("vrow pointer is NULL");
	if (fil  == NULL) INVALID("filter is NULL");

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

	err = copyFilter(*vrow, fil);
	if (err != NOWDB_OK) {
		nowdb_vrow_destroy(*vrow);
		free(*vrow); *vrow = NULL;
		return err;
	}
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
}

/* ------------------------------------------------------------------------
 * Try to add a vertex property
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_vrow_add(nowdb_vrow_t   *vrow,
                           nowdb_vertex_t *vertex,
                           char           *added);

/* ------------------------------------------------------------------------
 * Try to evaluate
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_vrow_eval(nowdb_vrow_t    *vrow,
                            nowdb_vertex_t **vertex);

/* ------------------------------------------------------------------------
 * Show current vrows and their state (debugging)
 * ------------------------------------------------------------------------
 */
void nowdb_vrow_show(nowdb_vrow_t *vrow);
