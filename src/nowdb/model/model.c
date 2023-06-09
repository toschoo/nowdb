/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Model
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/model/model.h>
#include <nowdb/io/dir.h>

#include <tsalgo/tree.h>

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

static char *OBJECT = "model";

#define VMODEL "vertex.model"
#define PMODEL "property.model"
#define EMODEL "edge.model"
#define AMODEL "pedge.model"

#define V 0
#define P 1
#define E 2
#define A 3

#define MODELNULL() \
	if (model == NULL) return nowdb_err_get(nowdb_err_invalid, \
	                          FALSE, OBJECT, "model is NULL");

#define NOMEM(s) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, s);

#define INVALID(s) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, s);

#define PROP(x) \
	((nowdb_model_prop_t*)x)

#define PEDGE(x) \
	((nowdb_model_pedge_t*)x)

#define VRTX(x) \
	((nowdb_model_vertex_t*)x)

#define EDGE(x) \
	((nowdb_model_edge_t*)x)

#define ROLEID(x) \
	(*(nowdb_roleid_t*)x)

/* ------------------------------------------------------------------------
 * type or edge descriptor
 * ------------------------------------------------------------------------
 */
typedef struct {
	char *name; /* name of the thing    */
	void *ptr;  /* pointer to the thing */
	char  t;    /* type of the thing    */
} thing_t;

#define THING(x) \
	((thing_t*)x)

/* ------------------------------------------------------------------------
 * thing by name
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t thingnamecompare(void *ignore, void *one, void *two) {
	int x = strcasecmp(THING(one)->name, THING(two)->name);
	if (x < 0) return ts_algo_cmp_less;
	if (x > 0) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * destroy thing
 * ------------------------------------------------------------------------
 */
static void thingdestroy(void *ignore, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	if (THING(*n)->name != NULL) {
		free(THING(*n)->name);
		THING(*n)->name = NULL;
	}
	free(*n); *n = NULL;
}

/* ------------------------------------------------------------------------
 * compare properties by id (roleid, propid)
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t propidcompare(void *ignore, void *one, void *two) {
	if (PROP(one)->roleid < PROP(two)->roleid) return ts_algo_cmp_less;
	if (PROP(one)->roleid > PROP(two)->roleid) return ts_algo_cmp_greater;
	if (PROP(one)->propid < PROP(two)->propid) return ts_algo_cmp_less;
	if (PROP(one)->propid > PROP(two)->propid) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * compare properties by name (roleid, name)
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t propnamecompare(void *ignore, void *one, void *two) {
	if (PROP(one)->roleid < PROP(two)->roleid) return ts_algo_cmp_less;
	if (PROP(one)->roleid > PROP(two)->roleid) return ts_algo_cmp_greater;
	int x = strcasecmp(PROP(one)->name, PROP(two)->name);
	if (x < 0) return ts_algo_cmp_less;
	if (x > 0) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * Filter by roleid
 * ------------------------------------------------------------------------
 */
static ts_algo_bool_t propsByRoleid(void *ignore, const void *pattern,
                                                  const void *node) {
	if (PROP(pattern)->roleid == PROP(node)->roleid) return TRUE;
	return FALSE;
}

/* ------------------------------------------------------------------------
 * Filter by roleid and pk
 * ------------------------------------------------------------------------
 */
static ts_algo_bool_t pkByRoleid(void *ignore, const void *pattern,
                                                  const void *node) {
	if (PROP(pattern)->roleid == PROP(node)->roleid &&
	    PROP(node)->pk) return TRUE;
	return FALSE;
}

/* ------------------------------------------------------------------------
 * Sort by pos
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t sortByPos(void *one, void *two) {
	if (PROP(one)->pos < PROP(two)->pos) return ts_algo_cmp_less;
	if (PROP(one)->pos > PROP(two)->pos) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * Sort by off
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t sortByOff(void *one, void *two) {
	if (PEDGE(one)->off < PEDGE(two)->off) return ts_algo_cmp_less;
	if (PEDGE(one)->off > PEDGE(two)->off) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * compare edge properties by id (edgeid, propid)
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t pedgeidcompare(void *ignore, void *one, void *two) {
	if (PEDGE(one)->edgeid < PEDGE(two)->edgeid) return ts_algo_cmp_less;
	if (PEDGE(one)->edgeid > PEDGE(two)->edgeid) return ts_algo_cmp_greater;
	if (PEDGE(one)->propid < PEDGE(two)->propid) return ts_algo_cmp_less;
	if (PEDGE(one)->propid > PEDGE(two)->propid) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * compare properties by name (roleid, name)
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t pedgenamecompare(void *ignore, void *one, void *two) {
	if (PEDGE(one)->edgeid < PEDGE(two)->edgeid) return ts_algo_cmp_less;
	if (PEDGE(one)->edgeid > PEDGE(two)->edgeid) return ts_algo_cmp_greater;
	int x = strcasecmp(PEDGE(one)->name, PEDGE(two)->name);
	if (x < 0) return ts_algo_cmp_less;
	if (x > 0) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * Filter by edgeid
 * ------------------------------------------------------------------------
 */
static ts_algo_bool_t pedgesByEdgeid(void *ignore, const void *pattern,
                                                      const void *node) {
	if (PEDGE(pattern)->edgeid == PEDGE(node)->edgeid) return TRUE;
	return FALSE;
}

/* ------------------------------------------------------------------------
 * compare vertex by id 
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t vrtxidcompare(void *ignore, void *one, void *two) {
	if (VRTX(one)->roleid < VRTX(two)->roleid) return ts_algo_cmp_less;
	if (VRTX(one)->roleid > VRTX(two)->roleid) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * compare vertex by name
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t vrtxnamecompare(void *ignore, void *one, void *two) {
	int x = strcasecmp(VRTX(one)->name, VRTX(two)->name);
	if (x < 0) return ts_algo_cmp_less;
	if (x > 0) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * compare edge by name
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t edgeidcompare(void *ignore, void *one, void *two) {
	if (EDGE(one)->edgeid < EDGE(two)->edgeid) return ts_algo_cmp_less;
	if (EDGE(one)->edgeid > EDGE(two)->edgeid) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * compare edge by name
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t edgenamecompare(void *ignore, void *one, void *two) {
	int x = strcasecmp(EDGE(one)->name, EDGE(two)->name);
	if (x < 0) return ts_algo_cmp_less;
	if (x > 0) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * destroy property
 * ------------------------------------------------------------------------
 */
static void propdestroy(void *ignore, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	if (PROP(*n)->name != NULL) {
		free(PROP(*n)->name);
		PROP(*n)->name = NULL;
	}
	free(*n); *n = NULL;
}

/* ------------------------------------------------------------------------
 * destroy edge property
 * ------------------------------------------------------------------------
 */
static void pedgedestroy(void *ignore, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	if (PEDGE(*n)->name != NULL) {
		free(PEDGE(*n)->name);
		PEDGE(*n)->name = NULL;
	}
	free(*n); *n = NULL;
}

/* ------------------------------------------------------------------------
 * destroy vertex
 * ------------------------------------------------------------------------
 */
static void vrtxdestroy(void *ignore, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	if (VRTX(*n)->name != NULL) {
		free(VRTX(*n)->name);
		VRTX(*n)->name = NULL;
	}
	free(*n); *n = NULL;
}

/* ------------------------------------------------------------------------
 * destroy edge
 * ------------------------------------------------------------------------
 */
static void edgedestroy(void *ignore, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	if (EDGE(*n)->name != NULL) {
		free(EDGE(*n)->name);
		EDGE(*n)->name = NULL;
	}
	free(*n); *n = NULL;
}

/* ------------------------------------------------------------------------
 * do not destroy twice!
 * - the name tree is considered secondary
 * - the id tree is responsible for memory
 * ------------------------------------------------------------------------
 */
static void nodestroy(void *ignore, void **n) {}

/* ------------------------------------------------------------------------
 * currently no update
 * ------------------------------------------------------------------------
 */
static ts_algo_rc_t noupdate(void *ignore, void *o, void *n) {
	return TS_ALGO_OK;
}

/* ------------------------------------------------------------------------
 * Helper: create files if they do not exist
 * ------------------------------------------------------------------------
 */
static nowdb_err_t mkFiles(nowdb_model_t *model) {
	nowdb_err_t err;
	struct stat st;
	nowdb_path_t p;
	nowdb_path_t f;
	FILE *tmp;

	for(int i=0;i<4;i++) {
		switch(i) {
		case 0: f=VMODEL; break;
		case 1: f=PMODEL; break;
		case 2: f=EMODEL; break;
		case 3: f=AMODEL; break;
		}

		p = nowdb_path_append(model->path, f);
		if (p == NULL) {
			NOMEM("create model path"); return err;
		}

		if (stat(p, &st) != 0) {
			tmp = fopen(p, "wb");
			if (tmp == NULL) {
				err = nowdb_err_get(nowdb_err_open,
				                  TRUE, OBJECT, p);
				free(p); return err;
			}
			fclose(tmp);
		}
		free(p);
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: generic find
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t find(nowdb_model_t *model,
                               ts_algo_tree_t *tree,
                               void         *wanted,
                               void         **found) {

	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2 = NOWDB_OK;

	if (found == NULL) return nowdb_err_get(nowdb_err_invalid,
	                                 FALSE, OBJECT, "object");

	err = nowdb_lock_read(&model->lock);
	if (err != NOWDB_OK) return err;

	*found = ts_algo_tree_find(tree, wanted);

	err2 = nowdb_unlock_read(&model->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * init model
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_init(nowdb_model_t *model, char *path) {
	nowdb_err_t err;

	MODELNULL();

	model->path = NULL;
	model->thingByName = NULL;
	model->vrtxById = NULL;
	model->vrtxByName = NULL;
	model->edgeByName = NULL;
	model->edgeById = NULL;
	model->pedgeByName = NULL;
	model->pedgeById = NULL;

	if (path == NULL) return nowdb_err_get(nowdb_err_invalid,
	                          FALSE, OBJECT, "path is NULL");

	err = nowdb_rwlock_init(&model->lock);
	if (err != NOWDB_OK) return err;

	model->path = strdup(path);
	if (model->path == NULL) {
		NOMEM("allocating path");
		return err;
	}

	model->thingByName= ts_algo_tree_new(&thingnamecompare, NULL,
	                                     &noupdate,
	                                     &thingdestroy,
	                                     &thingdestroy);
	if (model->thingByName == NULL) {
		nowdb_model_destroy(model);
		NOMEM("tree.new");
		return err;
	}

	model->vrtxById = ts_algo_tree_new(&vrtxidcompare, NULL,
	                                   &noupdate,
	                                   &vrtxdestroy, /* primary! */
	                                   &vrtxdestroy);
	if (model->vrtxById == NULL) {
		nowdb_model_destroy(model);
		NOMEM("tree.new");
		return err;
	}

	model->vrtxByName = ts_algo_tree_new(&vrtxnamecompare, NULL,
	                                     &noupdate,
	                                     &nodestroy, /* secondary */
	                                     &nodestroy);
	if (model->vrtxByName == NULL) {
		nowdb_model_destroy(model);
		NOMEM("tree.new");
		return err;
	}

	model->propById = ts_algo_tree_new(&propidcompare, NULL,
	                                   &noupdate,
	                                   &propdestroy, /* primary! */
	                                   &propdestroy);
	if (model->propById == NULL) {
		nowdb_model_destroy(model);
		NOMEM("tree.new");
		return err;
	}

	model->propByName = ts_algo_tree_new(&propnamecompare, NULL,
	                                     &noupdate,
	                                     &nodestroy, /* secondary */
	                                     &nodestroy);
	if (model->propByName == NULL) {
		nowdb_model_destroy(model);
		NOMEM("tree.new");
		return err;
	}

	model->edgeById = ts_algo_tree_new(&edgeidcompare, NULL,
	                                   &noupdate,
	                                   &edgedestroy, /* primary! */
	                                   &edgedestroy);
	if (model->edgeById == NULL) {
		nowdb_model_destroy(model);
		NOMEM("tree.new");
		return err;
	}

	model->edgeByName = ts_algo_tree_new(&edgenamecompare, NULL,
	                                     &noupdate,
	                                     &nodestroy, /* secondary */
	                                     &nodestroy);
	if (model->edgeByName == NULL) {
		nowdb_model_destroy(model);
		NOMEM("tree.new");
		return err;
	}

	model->pedgeById = ts_algo_tree_new(&pedgeidcompare, NULL,
	                                    &noupdate,
	                                    &pedgedestroy, /* primary! */
	                                    &pedgedestroy);
	if (model->pedgeById == NULL) {
		nowdb_model_destroy(model);
		NOMEM("tree.new");
		return err;
	}

	model->pedgeByName = ts_algo_tree_new(&pedgenamecompare, NULL,
	                                      &noupdate,
	                                      &nodestroy, /* secondary */
	                                      &nodestroy);
	if (model->pedgeByName == NULL) {
		nowdb_model_destroy(model);
		NOMEM("tree.new");
		return err;
	}

	/* create the files if they do not exist */
	err = mkFiles(model);
	if (err != NOWDB_OK) {
		nowdb_model_destroy(model);
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * destroy model
 * ------------------------------------------------------------------------
 */
void nowdb_model_destroy(nowdb_model_t *model) {
	if (model == NULL) return;
	nowdb_rwlock_destroy(&model->lock);
	if (model->path != NULL) {
		free(model->path); model->path = NULL;
	}
	if (model->thingByName != NULL) {
		ts_algo_tree_destroy(model->thingByName);
		free(model->thingByName); model->thingByName = NULL;
	}
	if (model->vrtxByName != NULL) { /* destroy first the secondary! */
		ts_algo_tree_destroy(model->vrtxByName);
		free(model->vrtxByName); model->vrtxByName = NULL;
	}
	if (model->vrtxById != NULL) {
		ts_algo_tree_destroy(model->vrtxById);
		free(model->vrtxById); model->vrtxById = NULL;
	}
	if (model->propByName != NULL) { /* destroy first the secondary! */
		ts_algo_tree_destroy(model->propByName);
		free(model->propByName); model->propByName = NULL;
	}
	if (model->propById != NULL) {
		ts_algo_tree_destroy(model->propById);
		free(model->propById); model->propById = NULL;
	}
	if (model->edgeByName != NULL) { /* destroy first the secondary! */
		ts_algo_tree_destroy(model->edgeByName);
		free(model->edgeByName); model->edgeByName = NULL;
	}
	if (model->edgeById != NULL) {
		ts_algo_tree_destroy(model->edgeById);
		free(model->edgeById); model->edgeById = NULL;
	}
	if (model->pedgeByName != NULL) { /* destroy first the secondary! */
		ts_algo_tree_destroy(model->pedgeByName);
		free(model->pedgeByName); model->pedgeByName = NULL;
	}
	if (model->pedgeById != NULL) {
		ts_algo_tree_destroy(model->pedgeById);
		free(model->pedgeById); model->pedgeById = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Get all edges
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getEdges(nowdb_model_t   *model,
                                 ts_algo_list_t **edges) {
	nowdb_err_t err;

	MODELNULL();

	if (model->edgeById->count == 0) {
		return nowdb_err_get(nowdb_err_not_found, FALSE,OBJECT,
		                     "no edges in model");
	}
	*edges = ts_algo_tree_toList(model->edgeById);
	if (*edges == NULL) {
		NOMEM("tree.toList");
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Get all vertices
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getVertices(nowdb_model_t  *model,
                                   ts_algo_list_t **vrtxs) {
	nowdb_err_t err;

	MODELNULL();

	if (model->vrtxById->count == 0) {
		return nowdb_err_get(nowdb_err_not_found, FALSE,OBJECT,
		                     "no types in model");
	}
	*vrtxs = ts_algo_tree_toList(model->vrtxById);
	if (*vrtxs == NULL) {
		NOMEM("tree.toList");
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: compute vertex size
 * ------------------------------------------------------------------------
 */
static uint32_t computeVertexSize(ts_algo_list_t *list) {
	uint32_t sz = 0;
	ts_algo_list_node_t *runner;
	nowdb_model_vertex_t *v;

	// 0 + roleid + vid + stamped + num + size + ctrl
	for(runner=list->head;runner!=NULL;runner=runner->nxt) {
		v = runner->cont;
		sz+=strlen(v->name) + 1 + 4 + 1 + 1 + 2 + 4 + 4;
	}
	return sz;
}

/* ------------------------------------------------------------------------
 * Helper: compute property size
 * ------------------------------------------------------------------------
 */
static uint32_t computePropSize(ts_algo_list_t *list) {
	uint32_t sz = 0;
	ts_algo_list_node_t *runner;
	nowdb_model_prop_t *p;

	/* size(prop) = strlen(name) + '\0' + propid (8)
	 *            + roleid(4) + pos(4) + value(4)
                      + pk(1) + stamp(1) + inc(1) + offset (4) */
	for(runner=list->head;runner!=NULL;runner=runner->nxt) {
		p = runner->cont;
		sz+=strlen(p->name) + 1 + 8 + 4 + 4 + 4 + 1 + 1 + 1 + 4;
	}
	return sz;
}

/* ------------------------------------------------------------------------
 * Helper: compute edge property size
 * ------------------------------------------------------------------------
 */
static uint32_t computePedgeSize(ts_algo_list_t *list) {
	uint32_t sz = 0;
	ts_algo_list_node_t *runner;
	nowdb_model_pedge_t *p;

	/* size(prop) = strlen(name) + '\0' + propid (8)
                      + edgeid (8) + pos (4) + value (4)
                      + origin (1) + destin (1) + stamp (1)
                      + off (4) */
	for(runner=list->head;runner!=NULL;runner=runner->nxt) {
		p = runner->cont;
		sz+=strlen(p->name) + 1 + 8 + 8 + 4 + 4
		                    + 1 + 1 + 1 + 4;
	}
	return sz;
}

/* ------------------------------------------------------------------------
 * Helper: compute edge size
 * ------------------------------------------------------------------------
 */
static uint32_t computeEdgeSize(ts_algo_list_t *list) {
	uint32_t sz = 0;
	ts_algo_list_node_t *runner;
	nowdb_model_edge_t *e;

	/* size = strlen(name) + '\0' + edgeid (8)
                + origin (4) + destin (4) +
                + stamped (1) + num (2)
                + ctrl (4) + size (4) */
	for(runner=list->head;runner!=NULL;runner=runner->nxt) {
		e = runner->cont;
		sz+=strlen(e->name) + 1 + 8 + 4 + 4
                                    + 1 + 2 + 4 + 4;
	}
	return sz;
}

/* ------------------------------------------------------------------------
 * Helper: write vertex to buffer
 * ------------------------------------------------------------------------
 */
static void vertex2buf(char *buf, uint32_t mx, ts_algo_list_t *list) {
	uint32_t sz = 0;
	ts_algo_list_node_t *runner;
	nowdb_model_vertex_t *v;

	for(runner=list->head;runner!=NULL;runner=runner->nxt) {
		v = runner->cont;
		memcpy(buf+sz, &v->roleid, 4); sz+=4;
		memcpy(buf+sz, &v->vid, 1); sz+=1;
		memcpy(buf+sz, &v->stamped, 1); sz+=1;
		memcpy(buf+sz, &v->num, 2); sz+=2;
		memcpy(buf+sz, &v->ctrl, 4); sz+=4;
		memcpy(buf+sz, &v->size, 4); sz+=4;
		strcpy(buf+sz, v->name); sz+=strlen(v->name)+1;
	}
}

/* ------------------------------------------------------------------------
 * Helper: write property to buffer
 * ------------------------------------------------------------------------
 */
static void prop2buf(char *buf, uint32_t mx, ts_algo_list_t *list) {
	uint32_t sz = 0;
	ts_algo_list_node_t *runner;
	nowdb_model_prop_t *p;

	for(runner=list->head;runner!=NULL;runner=runner->nxt) {
		p = runner->cont;
		memcpy(buf+sz, &p->propid, 8); sz+=8;
		memcpy(buf+sz, &p->roleid, 4); sz+=4;
		memcpy(buf+sz, &p->pos, 4); sz+=4;
		memcpy(buf+sz, &p->value, 4); sz+=4;
		memcpy(buf+sz, &p->pk, 1); sz+=1;
		memcpy(buf+sz, &p->stamp, 1); sz+=1;
		memcpy(buf+sz, &p->inc, 1); sz+=1;
		memcpy(buf+sz, &p->off, 4); sz+=4;
		strcpy(buf+sz, p->name); sz+=strlen(p->name)+1;
	}
}

/* ------------------------------------------------------------------------
 * Helper: write pedge to buffer
 * ------------------------------------------------------------------------
 */
static void pedge2buf(char *buf, uint32_t mx, ts_algo_list_t *list) {
	uint32_t sz = 0;
	ts_algo_list_node_t *runner;
	nowdb_model_pedge_t *p;

	for(runner=list->head;runner!=NULL;runner=runner->nxt) {
		p = runner->cont;
		memcpy(buf+sz, &p->propid, 8); sz+=8;
		memcpy(buf+sz, &p->edgeid, 8); sz+=8;
		memcpy(buf+sz, &p->pos, 4); sz+=4;
		memcpy(buf+sz, &p->value, 4); sz+=4;
		memcpy(buf+sz, &p->origin, 1); sz+=1;
		memcpy(buf+sz, &p->destin, 1); sz+=1;
		memcpy(buf+sz, &p->stamp, 1); sz+=1;
		memcpy(buf+sz, &p->off, 4); sz+=4;
		strcpy(buf+sz, p->name); sz+=strlen(p->name)+1;
	}
}

/* ------------------------------------------------------------------------
 * Helper: write edge to buffer
 * ------------------------------------------------------------------------
 */
static void edge2buf(char *buf, uint32_t mx, ts_algo_list_t *list) {
	uint32_t sz = 0;
	ts_algo_list_node_t *runner;
	nowdb_model_edge_t *e;

	for(runner=list->head;runner!=NULL;runner=runner->nxt) {
		e = runner->cont;
		memcpy(buf+sz, &e->edgeid, 8); sz+=8;
		memcpy(buf+sz, &e->origin, 4); sz+=4;
		memcpy(buf+sz, &e->destin, 4); sz+=4;
		memcpy(buf+sz, &e->stamped, 1); sz+=1;
		memcpy(buf+sz, &e->num, 2); sz+=2;
		memcpy(buf+sz, &e->ctrl, 4); sz+=4;
		memcpy(buf+sz, &e->size, 4); sz+=4;
		strcpy(buf+sz, e->name); sz+=strlen(e->name)+1;
	}
}

/* ------------------------------------------------------------------------
 * Helper: store model
 * ------------------------------------------------------------------------
 */
static nowdb_err_t storeModel(nowdb_model_t *model, char what) {
	nowdb_err_t err = NOWDB_OK;
	ts_algo_list_t *list;
	nowdb_path_t p,f;
	char *buf=NULL;
	uint32_t sz;
	ts_algo_tree_t *tree;

	switch(what) {
	case V: tree = model->vrtxById; f=VMODEL; break;
	case P: tree = model->propById; f=PMODEL; break;
	case E: tree = model->edgeById; f=EMODEL; break;
	case A: tree = model->pedgeById; f=AMODEL; break;
	default: return nowdb_err_get(nowdb_err_panic,
	  FALSE, OBJECT, "impossible value in switch");
	}

	if (tree->count == 0) {
		return NOWDB_OK;
	}
	list = ts_algo_tree_toList(tree);
	if (list == NULL) {
		NOMEM("vrtxById.toList");
		return err;
	}

	switch(what) {
	case V: sz = computeVertexSize(list); break;
	case P: sz = computePropSize(list); break;
	case E: sz = computeEdgeSize(list); break;
	case A: sz = computePedgeSize(list); break;
	default: return nowdb_err_get(nowdb_err_panic,
	  FALSE, OBJECT, "impossible value in switch");
	}
	fprintf(stderr, "store size: %u\n", sz); 
	if (sz == 0) return NOWDB_OK;
	buf = malloc(sz);
	if (buf == NULL) {
		ts_algo_list_destroy(list); free(list);
		NOMEM("allocating buffer");
		return err;
	}

	switch(what) {
	case V: vertex2buf(buf,sz,list); break;
	case P: prop2buf(buf,sz,list); break;
	case E: edge2buf(buf,sz,list); break;
	case A: pedge2buf(buf,sz,list); break;
	default: return nowdb_err_get(nowdb_err_panic,
	  FALSE, OBJECT, "impossible value in switch");
	}

	ts_algo_list_destroy(list); free(list);

	p = nowdb_path_append(model->path, f);
	if (p == NULL) {
		free(buf);
		NOMEM("allocating path");
		return err;
	}

	err = nowdb_writeFileWithBkp(model->path, p, buf, sz);
	free(p); free(buf);

	return err;
}

/* ------------------------------------------------------------------------
 * Helper: get file size
 * ------------------------------------------------------------------------
 */
static nowdb_err_t readSize(nowdb_path_t   path,
                            uint32_t      *sz) {
	nowdb_err_t err;
	struct stat st;

	if (stat(path, &st) != 0) {
		err = nowdb_err_get(nowdb_err_stat, TRUE, OBJECT, path);
		return err;
	}
	*sz = (uint32_t)st.st_size;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: load vertex model
 * ------------------------------------------------------------------------
 */
static nowdb_err_t loadVertex(ts_algo_list_t *list,
                              char *buf, uint32_t sz) {
	nowdb_err_t err;
	uint32_t off = 0;
	nowdb_model_vertex_t *v;
	size_t s;

	while(off < sz) {
		v = calloc(1,sizeof(nowdb_model_vertex_t));
		if (v == NULL) {
			NOMEM("allocating vertex");
			return err;
		}
		memcpy(&v->roleid, buf+off, 4); off+=4;
		memcpy(&v->vid, buf+off, 1); off+=1;
		memcpy(&v->stamped, buf+off, 1); off+=1;
		memcpy(&v->num, buf+off, 2); off+=2;
		memcpy(&v->ctrl, buf+off, 4); off+=4;
		memcpy(&v->size, buf+off, 4); off+=4;
		s = strnlen(buf+off, 4097);
		if (s >= 4096) {
			free(v);
			return nowdb_err_get(nowdb_err_catalog,
		         FALSE, OBJECT, "vertex name too long");
		}
		v->name = malloc(s+1);
		if (v->name == NULL) {
			free(v);
			NOMEM("allocating vertex name");
			return err;
		}
		strcpy(v->name,buf+off); off+=s+1;
		if (ts_algo_list_append(list, v) != TS_ALGO_OK) {
			free(v);
			NOMEM("list.append");
			return err;
		}
		fprintf(stderr, "Vertex |%s| is stamped: %d\n", v->name, v->stamped);
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: load property model
 * ------------------------------------------------------------------------
 */
static nowdb_err_t loadProp(ts_algo_list_t   *list,
                            char *buf, uint32_t sz) {
	nowdb_err_t err;
	uint32_t off = 0;
	nowdb_model_prop_t *p;
	size_t s;

	while(off < sz) {
		p = calloc(1,sizeof(nowdb_model_prop_t));
		if (p == NULL) {
			NOMEM("allocating prop");
			return err;
		}
		memcpy(&p->propid, buf+off, 8); off+=8;
		memcpy(&p->roleid, buf+off, 4); off+=4;
		memcpy(&p->pos, buf+off, 4); off+=4;
		memcpy(&p->value, buf+off, 4); off+=4;
		memcpy(&p->pk, buf+off, 1); off+=1;
		memcpy(&p->stamp, buf+off, 1); off+=1;
		memcpy(&p->inc, buf+off, 1); off+=1;
		memcpy(&p->off, buf+off, 4); off+=4;
		s = strnlen(buf+off, 4097);
		if (s >= 4096) {
			free(p);
			return nowdb_err_get(nowdb_err_catalog,
		         FALSE, OBJECT, "property name too long");
		}
		p->name = malloc(s+1);
		if (p->name == NULL) {
			free(p);
			NOMEM("allocating property name");
			return err;
		}
		strcpy(p->name,buf+off); off+=s+1;
		if (ts_algo_list_append(list, p) != TS_ALGO_OK) {
			free(p);
			NOMEM("list.append");
			return err;
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: load pedge model
 * ------------------------------------------------------------------------
 */
static nowdb_err_t loadPedge(ts_algo_list_t   *list,
                             char *buf, uint32_t sz) {
	nowdb_err_t err;
	uint32_t off = 0;
	nowdb_model_pedge_t *p;
	size_t s;

	while(off < sz) {
		p = calloc(1,sizeof(nowdb_model_pedge_t));
		if (p == NULL) {
			NOMEM("allocating prop");
			return err;
		}
		memcpy(&p->propid, buf+off, 8); off+=8;
		memcpy(&p->edgeid, buf+off, 8); off+=8;
		memcpy(&p->pos, buf+off, 4); off+=4;
		memcpy(&p->value, buf+off, 4); off+=4;
		memcpy(&p->origin, buf+off, 1); off+=1;
		memcpy(&p->destin, buf+off, 1); off+=1;
		memcpy(&p->stamp, buf+off, 1); off+=1;
		memcpy(&p->off, buf+off, 4); off+=4;
		s = strnlen(buf+off, 4097);
		if (s >= 4096) {
			free(p);
			return nowdb_err_get(nowdb_err_catalog,
		         FALSE, OBJECT, "property name too long");
		}
		p->name = malloc(s+1);
		if (p->name == NULL) {
			free(p);
			NOMEM("allocating property name");
			return err;
		}
		strcpy(p->name,buf+off); off+=s+1;
		if (ts_algo_list_append(list, p) != TS_ALGO_OK) {
			free(p);
			NOMEM("list.append");
			return err;
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: initialise value according to vertex
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t updEdgePedge(nowdb_model_t   *model,
                                       nowdb_model_edge_t  *e,
                                       nowdb_model_pedge_t *o,
                                       nowdb_model_pedge_t *d) {
	nowdb_model_vertex_t pattern, *v;

	pattern.roleid = e->origin;
	v = ts_algo_tree_find(model->vrtxById, &pattern);
	if (v == NULL) return nowdb_err_get(nowdb_err_dup_key,
	        FALSE, OBJECT, "origin is not a known vertex");

	o->value = v->vid == NOWDB_MODEL_NUM?
	                      NOWDB_TYP_UINT:
	                      NOWDB_TYP_TEXT;

	fprintf(stderr, "searching destin: %u\n", e->destin);
	pattern.roleid = e->destin;
	v = ts_algo_tree_find(model->vrtxById, &pattern);
	if (v == NULL) return nowdb_err_get(nowdb_err_dup_key,
	        FALSE, OBJECT, "destin is not a known vertex");

	d->value = v->vid == NOWDB_MODEL_NUM?
	                      NOWDB_TYP_UINT:
	                      NOWDB_TYP_TEXT;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: load edge model
 * ------------------------------------------------------------------------
 */
static nowdb_err_t loadEdge(ts_algo_list_t   *list,
                            char *buf, uint32_t sz) {
	nowdb_err_t err;
	uint32_t off = 0;
	nowdb_model_edge_t *e;
	size_t s;

	while(off < sz) {
		e = calloc(1,sizeof(nowdb_model_edge_t));
		if (e == NULL) {
			NOMEM("allocating edge");
			return err;
		}
		memcpy(&e->edgeid, buf+off, 8); off+=8;
		memcpy(&e->origin, buf+off, 4); off+=4;
		memcpy(&e->destin, buf+off, 4); off+=4;
		memcpy(&e->stamped, buf+off, 1); off+=1;
		memcpy(&e->num, buf+off, 2); off+=2;
		memcpy(&e->ctrl, buf+off, 4); off+=4;
		memcpy(&e->size, buf+off, 4); off+=4;
		s = strnlen(buf+off, 4097);
		if (s >= 4096) {
			free(e);
			return nowdb_err_get(nowdb_err_catalog,
		         FALSE, OBJECT, "edge name too long");
		}
		e->name = malloc(s+1);
		if (e->name == NULL) {
			free(e);
			NOMEM("allocating edge name");
			return err;
		}
		strcpy(e->name,buf+off); off+=s+1;
		if (ts_algo_list_append(list, e) != TS_ALGO_OK) {
			free(e);
			NOMEM("list.append");
			return err;
		}

		// initEdgePedge(e);
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: load model
 * ------------------------------------------------------------------------
 */
static nowdb_err_t loadModel(nowdb_model_t *model, char what) {
	nowdb_err_t err = NOWDB_OK;
	ts_algo_list_t list;
	ts_algo_list_node_t *runner;
	nowdb_path_t p,f;
	char *buf=NULL;
	uint32_t sz=0;
	ts_algo_tree_t *byId, *byName;
	thing_t *thing;

	switch(what) {
	case V: byId = model->vrtxById; f=VMODEL;
	        byName = model->vrtxByName; break;
	case P: byId = model->propById; f=PMODEL;
	        byName = model->propByName; break;
	case E: byId = model->edgeById; f=EMODEL;
	        byName = model->edgeByName; break;
	case A: byId = model->pedgeById; f=AMODEL;
	        byName = model->pedgeByName; break;
	default: return nowdb_err_get(nowdb_err_panic,
	  FALSE, OBJECT, "impossible value in switch");
	}

	p = nowdb_path_append(model->path, f);
	if (p == NULL) {
		NOMEM("allocating path");
		return err;
	}

	err = readSize(p, &sz);
	if (err != NOWDB_OK) {
		free(p); return err;
	}

	if (sz == 0) {
		free(p); return NOWDB_OK;
	}
	buf = malloc(sz);
	if (buf == NULL) {
		free(p);
		NOMEM("allocating buffer");
		return err;
	}

	err = nowdb_readFile(p, buf, sz);
	if (err != NOWDB_OK) {
		free(p); free(buf); return err;
	}

	free(p);

	ts_algo_list_init(&list);

	switch(what) {
	case V: err = loadVertex(&list, buf, sz); break;
	case P: err = loadProp(&list, buf, sz); break;
	case E: err = loadEdge(&list, buf, sz); break;
	case A: err = loadPedge(&list, buf, sz); break;
	default: return nowdb_err_get(nowdb_err_panic,
	  FALSE, OBJECT, "impossible value in switch");
	}

	free(buf);

	/* NOTE: on error, it is not sufficient to
	 *       destroy the list; we also need to
	 *       destroy the content that was not
	 *       yet inserted into a tree */
	if (err != NOWDB_OK) {
		ts_algo_list_destroy(&list); return err;
	}
	for(runner=list.head;runner!=NULL;runner=runner->nxt) {

		if (ts_algo_tree_insert(byId, runner->cont) != TS_ALGO_OK) {
			ts_algo_list_destroy(&list);
			NOMEM("byId.insert");
			return err;
		}
		if (ts_algo_tree_insert(byName, runner->cont) != TS_ALGO_OK) {
			ts_algo_list_destroy(&list);
			NOMEM("byName.insert");
			return err;
		}
		if (what == V || what == E) {
			thing = calloc(1,sizeof(thing_t));
			if (thing == NULL) {
				ts_algo_list_destroy(&list);
				NOMEM("allocating thing descriptor");
				return err;
			}
			thing->name = strdup(what==V?
			    EDGE(runner->cont)->name:
			    VRTX(runner->cont)->name);
			if (thing->name == NULL) {
				free(thing);
				ts_algo_list_destroy(&list);
				NOMEM("allocating thing's name");
				return err;
			}
			thing->ptr = runner->cont;
			thing->t = what;
			if (ts_algo_tree_insert(model->thingByName,
			                      thing) != TS_ALGO_OK) {
				free(thing);
				ts_algo_list_destroy(&list);
				NOMEM("thing.insert");
				return err;
			}
		}
	}
	ts_algo_list_destroy(&list);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * load model from disk
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_load(nowdb_model_t *model) {
	nowdb_err_t err=NOWDB_OK;
	nowdb_err_t err2;

	MODELNULL();

	err = nowdb_lock_write(&model->lock);
	if (err != NOWDB_OK) return err;

	err = loadModel(model, V);
	if (err != NOWDB_OK) goto unlock;

	err = loadModel(model, P);
	if (err != NOWDB_OK) goto unlock;

	err = loadModel(model, E);
	if (err != NOWDB_OK) goto unlock;

	err = loadModel(model, A);
	if (err != NOWDB_OK) goto unlock;

unlock:
	err2 = nowdb_unlock_write(&model->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Helper: generic add without locking 
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t addNL(ts_algo_tree_t *byId,
                                ts_algo_tree_t *byName,
                                ts_algo_tree_t *three,
                                void           *entity,
                                char            what) {
	nowdb_err_t err;
	void *tmp;
	thing_t pattern, *thing=NULL;

	tmp = ts_algo_tree_find(byId, entity);
	if (tmp != NULL) {
		return nowdb_err_get(nowdb_err_dup_key,
		                   FALSE, OBJECT, "id");
	}
	if (what == E || what == V) {
		pattern.name = what==E?EDGE(entity)->name:
		                       VRTX(entity)->name;
		thing = ts_algo_tree_find(three, &pattern);
		if (thing != NULL) {
			return nowdb_err_get(nowdb_err_dup_key,
			          FALSE, OBJECT, pattern.name);
		}
	} else {
		tmp = ts_algo_tree_find(byName, entity);
		if (tmp != NULL) {
			return nowdb_err_get(nowdb_err_dup_key,
			                 FALSE, OBJECT, "name");
		}
	}
	if (ts_algo_tree_insert(byId, entity) != TS_ALGO_OK) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                      "tree.insert");
	}
	if (ts_algo_tree_insert(byName, entity) != TS_ALGO_OK) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                      "tree.insert");
	}

	// what is it?
	if (what == E || what == V) {
		thing = calloc(1,sizeof(thing_t));
		if (thing == NULL) {
			NOMEM("allocating thing");
			return err;
		}
		thing->name = strdup(what==E?EDGE(entity)->name:
		                             VRTX(entity)->name);
		if (thing->name == NULL) {
			free(thing);
			NOMEM("allocating thing's name");
			return err;
		}
		thing->ptr = entity;
		thing->t = what;

		if (ts_algo_tree_insert(three, thing) != TS_ALGO_OK) {
			return nowdb_err_get(nowdb_err_no_mem,
			         FALSE, OBJECT, "tree.insert");
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: generic add
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t add(nowdb_model_t  *model,
                              char            what,
                              void           *thing) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2 = NOWDB_OK;
	ts_algo_tree_t *byId;
	ts_algo_tree_t *byName;

	if (thing == NULL) return nowdb_err_get(nowdb_err_invalid,
	                         FALSE, OBJECT, "object is NULL");

	switch(what) {
	case V: byId = model->vrtxById;
	        byName = model->vrtxByName; break;
	case P: byId = model->propById;
	        byName = model->propByName; break;
	case E: byId = model->edgeById; 
	        byName = model->edgeByName; break;
	case A: byId = model->pedgeById; 
	        byName = model->pedgeByName; break;
	default: return nowdb_err_get(nowdb_err_panic,
	  FALSE, OBJECT, "impossible value in switch");
	}

	err = nowdb_lock_write(&model->lock);
	if (err != NOWDB_OK) return err;

	err = addNL(byId, byName, model->thingByName, thing, what);
	if (err != NOWDB_OK) goto unlock;

	err = storeModel(model, what);

unlock:
	err2 = nowdb_unlock_write(&model->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * add vertex
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_addVertex(nowdb_model_t        *model,
                                  nowdb_model_vertex_t *vrtx) {
	MODELNULL();
	return add(model, V, vrtx);
}

/* ------------------------------------------------------------------------
 * add property
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_addProperty(nowdb_model_t      *model,
                                    nowdb_model_prop_t *prop) {
	MODELNULL();
	return add(model, P, prop);
}

/* ------------------------------------------------------------------------
 * add pedge
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_addPedge(nowdb_model_t      *model,
                                 nowdb_model_pedge_t *prop) {
	MODELNULL();
	return add(model, A, prop);
}

/* ------------------------------------------------------------------------
 * add edge
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_addEdge(nowdb_model_t      *model,
                                nowdb_model_edge_t *edge) {
	MODELNULL();
	return add(model, E, edge);
}

/* ------------------------------------------------------------------------
 * Helper: find pk in list of properties (and make it first)
 * ------------------------------------------------------------------------
 */
static void findPK(ts_algo_list_t     *props,
                   nowdb_model_prop_t **prop) {
	ts_algo_list_node_t *runner;
	nowdb_model_prop_t  *tmp;
	nowdb_model_prop_t  *zero=NULL;

	*prop = NULL;
	for(runner=props->head; runner!=NULL; runner=runner->nxt) {
		tmp = runner->cont;
		if (tmp->pk) {
			if (tmp->pos != 0 && zero != NULL) {
				zero->pos = tmp->pos; tmp->pos = 0;
			}
			*prop = tmp; break;
		} else if (tmp->pos == 0) zero = tmp;
	}
}

/* ------------------------------------------------------------------------
 * Helper: find origin and destin in list of properties (and make it first)
 * ------------------------------------------------------------------------
 */
static void findOriDstStamp(ts_algo_list_t  *props,
                       nowdb_model_pedge_t **oprop,
                       nowdb_model_pedge_t **dprop,
                       nowdb_model_pedge_t **sprop) {
	ts_algo_list_node_t *runner;
	nowdb_model_pedge_t *tmp;
	nowdb_model_pedge_t *zero=NULL;
	nowdb_model_pedge_t *one=NULL;
	nowdb_model_pedge_t *two=NULL;

	*oprop = NULL;
	*dprop = NULL;
	for(runner=props->head; runner!=NULL; runner=runner->nxt) {
		tmp = runner->cont;
		if (tmp->origin) {
			if (tmp->pos != 0 && zero != NULL) {
				zero->pos = tmp->pos; tmp->pos = 0;
			}
			*oprop = tmp;
		} else if (tmp->destin) {
			if (tmp->pos != 0 && one != NULL) {
				one->pos = tmp->pos; tmp->pos = 0;
			}
			*dprop = tmp;
		} else if (tmp->stamp) {
			if (tmp->pos != 0 && two != NULL) {
				two->pos = tmp->pos; tmp->pos = 0;
			}
			*sprop = tmp;
		} else if (tmp->pos == 0) zero = tmp;
		  else if (tmp->pos == 1) one = tmp;
		  else if (tmp->pos == 2) two = tmp;
	}
}

/* ------------------------------------------------------------------------
 * Helper: find stamp in list of properties
 * ------------------------------------------------------------------------
 */
static void findStamp(ts_algo_list_t  *props,
                   nowdb_model_prop_t **prop) {
	ts_algo_list_node_t *runner;
	nowdb_model_prop_t  *tmp;
	nowdb_model_prop_t  *one=NULL;

	*prop = NULL;
	for(runner=props->head; runner!=NULL; runner=runner->nxt) {
		tmp = runner->cont;
		if (tmp->stamp) {
			if (tmp->pos > 1 && one != NULL) {
				one->pos = tmp->pos; tmp->pos = 1;
			}
			*prop = tmp; break;
		} else if (tmp->pos == 1 && !tmp->pk) one = tmp;
	}
}

static void resortProps(ts_algo_list_t *props) {
	ts_algo_list_node_t *runner;
	nowdb_model_prop_t  *cur;
	char clean = 0;

	do {
		int pos=0; clean = 1;

		for(runner=props->head; runner!=NULL; runner=runner->nxt) {
			cur = runner->cont;
			if (cur->pos > pos) {
				for(int i=pos; i<cur->pos; i++) {
					ts_algo_list_degrade(props, runner);
				}
				clean=0;break;
				
			} else if (cur->pos < pos) {
				for(int i=pos; i<cur->pos; i--) {
					ts_algo_list_promote(props, runner);
				}
				clean=0;break;
			}
			pos++;
		}
	} while (!clean);
}

static void resortPedges(ts_algo_list_t *props) {
	ts_algo_list_node_t *runner;
	nowdb_model_pedge_t *cur;
	char clean = 0;

	do {
		int pos=0; clean = 1;

		for(runner=props->head; runner!=NULL; runner=runner->nxt) {
			cur = runner->cont;
			if (cur->pos > pos) {
				for(int i=pos; i<cur->pos; i++) {
					ts_algo_list_degrade(props, runner);
				}
				clean=0;break;
				
			} else if (cur->pos < pos) {
				for(int i=pos; i<cur->pos; i--) {
					ts_algo_list_promote(props, runner);
				}
				clean=0;break;
			}
			pos++;
		}
	} while (!clean);
}

/* ------------------------------------------------------------------------
 * Helper: no duplicated properties properties in a list
 *         THIS IS NOT EFFICIENT!
 * ------------------------------------------------------------------------
 */
static char propUnique(ts_algo_list_t *props,
                       nowdb_model_prop_t *p) {
	ts_algo_list_node_t *runner;
	nowdb_model_prop_t *q;

	for(runner=props->head;runner!=NULL;runner=runner->nxt) {
		q = runner->cont;
		if (p == q) continue;
		if (strcmp(p->name,q->name) == 0) return 0;
	}
	return 1;
}

/* ------------------------------------------------------------------------
 * Helper: set roleid to properties in a list
 *         and check that they don't exist
 * ------------------------------------------------------------------------
 */
static nowdb_err_t propsUnique(nowdb_model_t  *model,
                               nowdb_roleid_t roleid,
                               ts_algo_list_t *props,
                               uint16_t         *num, 
                               uint32_t        *size,
                               uint32_t        *ctrl) {
	ts_algo_list_node_t *runner;
	nowdb_model_prop_t  *p, *tmp;
	uint32_t off=0, xb;

	xb  = nowdb_attctrlSize(props->len);
 
        *ctrl = xb;

	for(runner=props->head; runner!=NULL; runner=runner->nxt) {
		p = runner->cont;
		p->roleid = roleid;
		p->off = off; off+=8;

		if (p->pk) fprintf(stderr, "PK: %u\n", p->off);
		if (p->inc) fprintf(stderr, "INC: %u\n", p->off);

		(*num)++;

		tmp = ts_algo_tree_find(model->propById, p);
		if (tmp != NULL) return nowdb_err_get(nowdb_err_dup_key,
		                                 FALSE, OBJECT, p->name);
		tmp = ts_algo_tree_find(model->propByName, p);
		if (tmp != NULL) return nowdb_err_get(nowdb_err_dup_key,
		                                 FALSE, OBJECT, p->name);
		if (!propUnique(props, p)) {
			return nowdb_err_get(nowdb_err_dup_key,
		                        FALSE, OBJECT, p->name);
		}
	}
	(*size)=off+xb;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: no duplicated edge properties in a list
 *         THIS IS NOT EFFICIENT!
 * ------------------------------------------------------------------------
 */
static char pedgeUnique(ts_algo_list_t  *props,
                       nowdb_model_pedge_t *p) {
	ts_algo_list_node_t *runner;
	nowdb_model_pedge_t *q;

	for(runner=props->head;runner!=NULL;runner=runner->nxt) {
		q = runner->cont;
		if (p == q) continue;
		if (strcmp(p->name,q->name) == 0) return 0;
	}
	return 1;
}

/* ------------------------------------------------------------------------
 * Helper: set edgeid to edge properties in a list
 *         and check that they don't exist
 * ------------------------------------------------------------------------
 */
static nowdb_err_t pedgesUnique(nowdb_model_t  *model,
                                nowdb_key_t    edgeid,
                                ts_algo_list_t *props,
                                uint16_t         *num, 
                                uint32_t        *size,
                                uint32_t        *ctrl) {
	ts_algo_list_node_t *runner;
	nowdb_model_pedge_t *p, *tmp;
	uint32_t off=0, xb;
	
	xb = nowdb_attctrlSize(props->len);

	*ctrl = xb;

	for(runner=props->head; runner!=NULL; runner=runner->nxt) {
		p = runner->cont;

		p->edgeid = edgeid;
		p->off = off; off+=8;

		(*num)++;

		tmp = ts_algo_tree_find(model->pedgeById, p);
		if (tmp != NULL) return nowdb_err_get(nowdb_err_dup_key,
		                                 FALSE, OBJECT, p->name);
		tmp = ts_algo_tree_find(model->pedgeByName, p);
		if (tmp != NULL) return nowdb_err_get(nowdb_err_dup_key,
		                                 FALSE, OBJECT, p->name);
		if (!pedgeUnique(props, p)) {
			return nowdb_err_get(nowdb_err_dup_key,
		                        FALSE, OBJECT, p->name);
		}

		// get propid
	}
	(*size)=off+xb;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * min and max would be nice functions for ts_algo_tree
 * ------------------------------------------------------------------------
 */
static ts_algo_rc_t findMax(void *ignore, void *max, const void *node) {
	if (ROLEID(max) < VRTX(node)->roleid) {
		ROLEID(max) = VRTX(node)->roleid;
	}
	return TS_ALGO_OK;
}

static nowdb_roleid_t getNextRoleid(nowdb_model_t *model) {
	nowdb_roleid_t max = 0;
	ts_algo_tree_reduce(model->vrtxById, &max, &findMax);
	return max+1;
}

/* ------------------------------------------------------------------------
 * undo type
 * ------------------------------------------------------------------------
 */
#define UNDOTYPE() \
	{ \
	err2 = removeType(model, name);  \
	if (err2 != NOWDB_OK) {  \
		nowdb_err_release(err);  \
		err=err2; err2=NOWDB_OK;  \
		goto unlock;  \
	} \
	}

/* ------------------------------------------------------------------------
 * Predeclaration
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t removeType(nowdb_model_t  *model,
                                     char           *name);

/* ------------------------------------------------------------------------
 * add type
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_addType(nowdb_model_t  *model,
                                char           *name,
                                ts_algo_list_t *props) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;
	nowdb_model_prop_t *prop=NULL;
	nowdb_model_prop_t *stamp=NULL;
	nowdb_model_vertex_t *vrtx;
	ts_algo_list_node_t *runner;
	thing_t pattern, *thing;
	nowdb_bool_t stamped=0;

	MODELNULL();

	err = nowdb_lock_write(&model->lock);
	if (err != NOWDB_OK) return err;

	pattern.name = name;
	thing = ts_algo_tree_find(model->thingByName, &pattern);
	if (thing != NULL) {
		err = nowdb_err_get(nowdb_err_dup_key,
		                  FALSE, OBJECT, name);
		goto unlock;
	}

	if (props != NULL) {
		findPK(props, &prop);
		if (prop == NULL) {
			err = nowdb_err_get(nowdb_err_invalid,
			       FALSE, OBJECT, "no PK in type");
			goto unlock;
		}
		findStamp(props, &stamp);
		if (stamp != NULL) stamped = 1;
		resortProps(props);
	}

	vrtx = calloc(1, sizeof(nowdb_model_vertex_t));
	if (vrtx == NULL) {
		NOMEM("allocating vertex");
		goto unlock;
	}
	vrtx->name = strdup(name);
	if (vrtx->name == NULL) {
		NOMEM("allocating vertex name");
		free(vrtx); goto unlock;
	}

	vrtx->roleid  = getNextRoleid(model);
	vrtx->stamped = stamped;
	vrtx->num     = 0;
	vrtx->ctrl    = 0;
	vrtx->size    = 0;

	fprintf(stderr, "Vertex '%s' is stamped: %d\n", vrtx->name, vrtx->stamped);

	if (prop != NULL) {
		vrtx->vid = prop->value==NOWDB_TYP_TEXT?
		                       NOWDB_MODEL_TEXT:
		                        NOWDB_MODEL_NUM;
	} else {
		vrtx->vid = NOWDB_MODEL_NUM;
	}

	if (props != NULL) {
		err = propsUnique(model, vrtx->roleid, props,
		        &vrtx->num, &vrtx->size, &vrtx->ctrl);
		if (err != NOWDB_OK) {
			free(vrtx->name); free(vrtx);
			goto unlock;
		}
	}

	fprintf(stderr, "Vertex atts: %hu, size: %u\n", vrtx->num, vrtx->size);

	err = addNL(model->vrtxById,
	            model->vrtxByName,
	            model->thingByName, vrtx, V);
	if (err != NOWDB_OK) {
		free(vrtx->name); free(vrtx);
		goto unlock;
	}
	if (props != NULL) {
		// note that we remove the props from the list
		// so that the caller can destroy everything in
		// the list wether there was an error or not
		for(runner=props->head; runner!=NULL;) {
			err = addNL(model->propById,
			            model->propByName, NULL,
			            runner->cont, P);
			if (err != NOWDB_OK) break;
			ts_algo_list_node_t *tmp = runner->nxt;
			ts_algo_list_remove(props, runner);
			free(runner); runner=tmp;
		}
		if (err != NOWDB_OK) {
			UNDOTYPE();
		}
	
		err = storeModel(model, P);
		if (err != NOWDB_OK) {
			UNDOTYPE();
		}
	}

	err = storeModel(model, V);
	if (err != NOWDB_OK) {
		UNDOTYPE();
	}

unlock:
	err2 = nowdb_unlock_write(&model->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * undo edge
 * ------------------------------------------------------------------------
 */
#define UNDOEDGE() \
	err2 = removeEdgeType(model, name);  \
	if (err2 != NOWDB_OK) {  \
		nowdb_err_release(err);  \
		err=err2; err2=NOWDB_OK;  \
		goto unlock;  \
	}

/* ------------------------------------------------------------------------
 * predeclaration
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t removeEdgeType(nowdb_model_t *model,
                                         char          *name);

/* ------------------------------------------------------------------------
 * add edge
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_addEdgeType(nowdb_model_t  *model,
                                    char            *name,
                                    nowdb_key_t    edgeid,
                                    nowdb_roleid_t origin,
                                    nowdb_roleid_t destin,
                                    ts_algo_list_t *props) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;
	nowdb_model_edge_t  *edge;
	ts_algo_list_node_t *runner;
	thing_t pattern, *thing;
	nowdb_bool_t stamped=0;

	MODELNULL();

	err = nowdb_lock_write(&model->lock);
	if (err != NOWDB_OK) return err;

	pattern.name = name;
	thing = ts_algo_tree_find(model->thingByName, &pattern);
	if (thing != NULL) {
		err = nowdb_err_get(nowdb_err_dup_key,
		                  FALSE, OBJECT, name);
		goto unlock;
	}

	nowdb_model_pedge_t *ori = NULL;
	nowdb_model_pedge_t *dst = NULL;
	nowdb_model_pedge_t *stp = NULL;
	if (props != NULL) {
		findOriDstStamp(props, &ori, &dst, &stp);
		if (ori == NULL) {
			err = nowdb_err_get(nowdb_err_invalid,
			       FALSE, OBJECT, "no origin in edge");
			goto unlock;
		}
		if (dst == NULL) {
			err = nowdb_err_get(nowdb_err_invalid,
			       FALSE, OBJECT, "no destin in edge");
			goto unlock;
		}
		if (stp != NULL) stamped = 1;
		resortPedges(props);
	}

	edge = calloc(1, sizeof(nowdb_model_edge_t));
	if (edge == NULL) {
		NOMEM("allocating edge");
		goto unlock;
	}
	edge->name = strdup(name);
	if (edge->name == NULL) {
		NOMEM("allocating edge name");
		free(edge); goto unlock;
	}

	edge->edgeid  = edgeid;
	edge->origin  = origin;
	edge->destin  = destin;
	edge->stamped = stamped;
	edge->num     = 0;
	edge->ctrl    = 0;
	edge->size    = 0;

	err = updEdgePedge(model, edge, ori, dst);
	if (err != NOWDB_OK) {
		free(edge->name); free(edge);
		goto unlock;
	}

	if (props != NULL) {
		err = pedgesUnique(model, edge->edgeid, props,
		        &edge->num, &edge->size, &edge->ctrl);
		if (err != NOWDB_OK) {
			free(edge->name); free(edge);
			goto unlock;
		}
	}

	err = addNL(model->edgeById,
	            model->edgeByName,
	            model->thingByName, edge, E);
	if (err != NOWDB_OK) {
		free(edge->name); free(edge);
		goto unlock;
	}
	if (props != NULL) {
		// note that we remove the props from the list
		// so that the caller can destroy everything in
		// the list wether there was an error or not
		for(runner=props->head; runner!=NULL;) {
			err = addNL(model->pedgeById,
			            model->pedgeByName, NULL,
			            runner->cont, A);
			if (err != NOWDB_OK) break;
			ts_algo_list_node_t *tmp = runner->nxt;
			ts_algo_list_remove(props,runner);
			free(runner); runner=tmp;
		}
		if (err != NOWDB_OK) {
			UNDOEDGE();
		}
	
		err = storeModel(model, A);
		if (err != NOWDB_OK) {
			UNDOEDGE();
		}
	}

	err = storeModel(model, E);
	if (err != NOWDB_OK) {
		UNDOEDGE();
	}

unlock:
	err2 = nowdb_unlock_write(&model->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * remove vertex
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_removeVertex(nowdb_model_t *model,
                                     nowdb_roleid_t role) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2 = NOWDB_OK;
	nowdb_model_vertex_t *vrtx;
	nowdb_model_vertex_t  tmp;
	thing_t pattern;

	MODELNULL();

	err = nowdb_lock_write(&model->lock);
	if (err != NOWDB_OK) return err;

	tmp.roleid = role;
	vrtx = ts_algo_tree_find(model->vrtxById, &tmp);
	if (vrtx == NULL) {
		err = nowdb_err_get(nowdb_err_key_not_found,
		                    FALSE, OBJECT, "roleid");
		goto unlock;
	}

	pattern.name = vrtx->name;
	ts_algo_tree_delete(model->thingByName, &pattern);
	ts_algo_tree_delete(model->vrtxByName, vrtx);
	ts_algo_tree_delete(model->vrtxById, vrtx);

	err = storeModel(model, V);
unlock:
	err2 = nowdb_unlock_write(&model->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * remove property
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_removeProperty(nowdb_model_t *model,
                                       nowdb_roleid_t  role,
                                       nowdb_key_t   propid) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2 = NOWDB_OK;
	nowdb_model_prop_t *p;
	nowdb_model_prop_t  tmp;


	MODELNULL();

	err = nowdb_lock_write(&model->lock);
	if (err != NOWDB_OK) return err;

	tmp.roleid = role;
	tmp.propid = propid;

	p = ts_algo_tree_find(model->propById, &tmp);
	if (p == NULL) {
		err = nowdb_err_get(nowdb_err_key_not_found,
		                    FALSE, OBJECT, "propid");
		goto unlock;
	}

	ts_algo_tree_delete(model->propByName, p);
	ts_algo_tree_delete(model->propById, p);

	err = storeModel(model, P);
unlock:
	err2 = nowdb_unlock_write(&model->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * remove pedge
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_removePedge(nowdb_model_t *model,
                                    nowdb_key_t   edgeid,
                                    nowdb_key_t   propid) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2 = NOWDB_OK;
	nowdb_model_pedge_t *p;
	nowdb_model_pedge_t  tmp;

	MODELNULL();

	err = nowdb_lock_write(&model->lock);
	if (err != NOWDB_OK) return err;

	tmp.edgeid = edgeid;
	tmp.propid = propid;

	p = ts_algo_tree_find(model->pedgeById, &tmp);
	if (p == NULL) {
		err = nowdb_err_get(nowdb_err_key_not_found,
		                    FALSE, OBJECT, "propid");
		goto unlock;
	}

	ts_algo_tree_delete(model->pedgeByName, p);
	ts_algo_tree_delete(model->pedgeById, p);

	err = storeModel(model, A);
unlock:
	err2 = nowdb_unlock_write(&model->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * remove edge
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_removeEdge(nowdb_model_t *model,
                                   nowdb_key_t    edge) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2 = NOWDB_OK;
	nowdb_model_edge_t *e;
	nowdb_model_edge_t  tmp;
	thing_t pattern;

	MODELNULL();

	err = nowdb_lock_write(&model->lock);
	if (err != NOWDB_OK) return err;

	tmp.edgeid = edge;
	e = ts_algo_tree_find(model->edgeById, &tmp);
	if (e == NULL) {
		err = nowdb_err_get(nowdb_err_key_not_found,
		                    FALSE, OBJECT, "edgeid");
		goto unlock;
	}

	pattern.name = e->name;
	ts_algo_tree_delete(model->thingByName, &pattern);
	ts_algo_tree_delete(model->edgeByName, e);
	ts_algo_tree_delete(model->edgeById, e);

	err = storeModel(model, E);
unlock:
	err2 = nowdb_unlock_write(&model->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Helper: remove type in one go
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t removeType(nowdb_model_t  *model,
                                     char           *name) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_model_prop_t ptmp;
	nowdb_model_vertex_t *vrtx, vtmp;
	ts_algo_list_t props;
	ts_algo_list_node_t *runner;
	thing_t pattern;

	vtmp.name = name;

	vrtx = ts_algo_tree_find(model->vrtxByName, &vtmp);
	if (vrtx == NULL) {
		return nowdb_err_get(nowdb_err_key_not_found,
		                        FALSE, OBJECT, name);
	}

	// get all properties
	ptmp.roleid = vrtx->roleid;
	ts_algo_list_init(&props);

	if (ts_algo_tree_filter(model->propById, &props, &ptmp,
	                          propsByRoleid) != TS_ALGO_OK) {
		NOMEM("tree.filter");
		return err;
	}

	for(runner=props.head; runner!=NULL; runner=runner->nxt) {
		ts_algo_tree_delete(model->propByName, runner->cont);
		ts_algo_tree_delete(model->propById, runner->cont);
	}

	ts_algo_list_destroy(&props);

	pattern.name = vrtx->name;
	ts_algo_tree_delete(model->thingByName, &pattern);
	ts_algo_tree_delete(model->vrtxByName, vrtx);
	ts_algo_tree_delete(model->vrtxById, vrtx);

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Remove type in one go
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_removeType(nowdb_model_t  *model,
                                   char           *name) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2 = NOWDB_OK;

	MODELNULL();

	if (name == NULL) return nowdb_err_get(nowdb_err_invalid,
	                           FALSE, OBJECT, "name is NULL");

	err = nowdb_lock_write(&model->lock);
	if (err != NOWDB_OK) return err;

	err = removeType(model, name);
	if (err != NOWDB_OK) goto unlock;

	err = storeModel(model, P);
	if (err != NOWDB_OK) goto unlock;

	err = storeModel(model, V);
	if (err != NOWDB_OK) goto unlock;

unlock:
	err2 = nowdb_unlock_write(&model->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Helper: remove edge in one go
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t removeEdgeType(nowdb_model_t *model,
                                         char          *name) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_model_pedge_t ptmp;
	nowdb_model_edge_t *edge, etmp;
	ts_algo_list_t props;
	ts_algo_list_node_t *runner;
	thing_t pattern;
	etmp.name = name;

	edge = ts_algo_tree_find(model->edgeByName, &etmp);
	if (edge == NULL) {
		return nowdb_err_get(nowdb_err_key_not_found,
		                        FALSE, OBJECT, name);
	}

	// get all properties
	ptmp.edgeid = edge->edgeid;
	ts_algo_list_init(&props);

	if (ts_algo_tree_filter(model->pedgeById, &props, &ptmp,
	                          pedgesByEdgeid) != TS_ALGO_OK) {
		NOMEM("tree.filter");
		return err;
	}

	for(runner=props.head; runner!=NULL; runner=runner->nxt) {
		ts_algo_tree_delete(model->pedgeByName, runner->cont);
		ts_algo_tree_delete(model->pedgeById, runner->cont);
	}

	ts_algo_list_destroy(&props);

	pattern.name = edge->name;
	ts_algo_tree_delete(model->thingByName, &pattern);
	ts_algo_tree_delete(model->edgeByName, edge);
	ts_algo_tree_delete(model->edgeById, edge);

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Remove edge in one go
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_removeEdgeType(nowdb_model_t  *model,
                                       char           *name) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2 = NOWDB_OK;

	MODELNULL();

	if (name == NULL) return nowdb_err_get(nowdb_err_invalid,
	                           FALSE, OBJECT, "name is NULL");

	err = nowdb_lock_write(&model->lock);
	if (err != NOWDB_OK) return err;

	err = removeEdgeType(model, name);
	if (err != NOWDB_OK) goto unlock;

	err = storeModel(model, A);
	if (err != NOWDB_OK) goto unlock;

	err = storeModel(model, E);

unlock:
	err2 = nowdb_unlock_write(&model->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * find vertex by name
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getVertexByName(nowdb_model_t        *model,
                                        char                  *name,
                                        nowdb_model_vertex_t **vrtx) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_model_vertex_t  tmp;

	MODELNULL();

	tmp.name = name;
	err = find(model, model->vrtxByName, &tmp, (void**)vrtx);
	if (err != NOWDB_OK) return err;
	if (*vrtx == NULL) return nowdb_err_get(nowdb_err_key_not_found,
		                                   FALSE, OBJECT, name);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * find vertex by id
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getVertexById(nowdb_model_t        *model,
                                      nowdb_roleid_t       roleid,
                                      nowdb_model_vertex_t **vrtx) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_model_vertex_t  tmp;

	MODELNULL();

	tmp.roleid = roleid;
	err = find(model, model->vrtxById, &tmp, (void**)vrtx);
	if (err != NOWDB_OK) return err;
	if (*vrtx == NULL) return nowdb_err_get(nowdb_err_key_not_found,
		                                  FALSE, OBJECT, "vid");
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * find edge by name
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getEdgeByName(nowdb_model_t      *model,
                                      char                *name,
                                      nowdb_model_edge_t **edge) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_model_edge_t tmp;

	MODELNULL();

	tmp.name = name;
	err = find(model, model->edgeByName, &tmp, (void**)edge);
	if (err != NOWDB_OK) return err;
	if (*edge == NULL) return nowdb_err_get(nowdb_err_key_not_found,
		                                   FALSE, OBJECT, name);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * find edge by id
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getEdgeById(nowdb_model_t      *model,
                                    nowdb_key_t        edgeid,
                                    nowdb_model_edge_t **edge) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_model_edge_t tmp;

	MODELNULL();

	tmp.edgeid = edgeid;
	err = find(model, model->edgeById, &tmp, (void**)edge);
	if (err != NOWDB_OK) return err;
	if (*edge == NULL) return nowdb_err_get(nowdb_err_key_not_found,
		                                FALSE, OBJECT, "edgeid");
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * find property by id
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getPropById(nowdb_model_t      *model,
                                    nowdb_roleid_t     roleid,
                                    nowdb_key_t        propid,
                                    nowdb_model_prop_t **prop) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_model_prop_t tmp;

	MODELNULL();

	tmp.roleid = roleid;
	tmp.propid = propid;

	err = find(model, model->propById, &tmp, (void**)prop);
	if (err != NOWDB_OK) return err;
	if (*prop == NULL) return nowdb_err_get(nowdb_err_key_not_found,
		                                   FALSE, OBJECT, "id");
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * find property by name
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getPropByName(nowdb_model_t      *model,
                                      nowdb_roleid_t     roleid,
                                      char                *name,
                                      nowdb_model_prop_t **prop) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_model_prop_t tmp;

	MODELNULL();

	tmp.roleid = roleid;
	tmp.name   = name;

	err = find(model, model->propByName, &tmp, (void**)prop);
	if (err != NOWDB_OK) return err;
	if (*prop == NULL) return nowdb_err_get(nowdb_err_key_not_found,
		                                   FALSE, OBJECT, name);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Get pedge by id
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getPedgeById(nowdb_model_t       *model,
                                     nowdb_key_t         edgeid,
                                     nowdb_key_t         propid,
                                     nowdb_model_pedge_t **prop) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_model_pedge_t tmp;

	MODELNULL();

	tmp.edgeid = edgeid;
	tmp.propid = propid;

	err = find(model, model->pedgeById, &tmp, (void**)prop);
	if (err != NOWDB_OK) return err;
	if (*prop == NULL) return nowdb_err_get(nowdb_err_key_not_found,
		                                   FALSE, OBJECT, "id");
	return NOWDB_OK;
}

/*
static inline char getCanonicalPedge(nowdb_model_t       *model,
                                     nowdb_key_t         edgeid,
                                     char                 *name,
                                     nowdb_model_pedge_t **prop) {
	nowdb_err_t err;
	nowdb_model_edge_t *e, pattern;
	char x = 0;

	if (strcasecmp(name, "origin")         == 0) x=1;
	else if (strcasecmp(name, "destin")    == 0  ||
	         strcasecmp(name, "dest")      == 0  ||
	         strcasecmp(name, "dst")       == 0) x=2;
	else if (strcasecmp(name, "stamp")     == 0  ||
	         strcasecmp(name, "timestamp") == 0) x=3;
	else return 0;

	pattern.edgeid = edgeid;
	err = find(model, model->edgeById, &pattern, (void**)&e);
	if (err != NOWDB_OK) {
		nowdb_err_release(err); // we ignore it here
		return 0;
	}
	
	switch(x) {
	case 1: *prop = &e->op; break;
	case 2: *prop = &e->dp; break;
	case 3: *prop = &e->tp; break;
	}

	return 1;
}
*/

/* ------------------------------------------------------------------------
 * find pedge by name
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getPedgeByName(nowdb_model_t       *model,
                                       nowdb_key_t         edgeid,
                                       char                 *name,
                                       nowdb_model_pedge_t **prop) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_model_pedge_t tmp;

	MODELNULL();

	// if (getCanonicalPedge(model, edgeid, name, prop)) return NOWDB_OK;

	tmp.edgeid = edgeid;
	tmp.name   = name;

	err = find(model, model->pedgeByName, &tmp, (void**)prop);
	if (err != NOWDB_OK) return err;
	if (*prop == NULL) return nowdb_err_get(nowdb_err_key_not_found,
		                                   FALSE, OBJECT, name);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: copy list
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t copyList(ts_algo_list_t *src,
                                   ts_algo_list_t *trg) {
	nowdb_err_t err;
	ts_algo_list_node_t *run;

	for(run=src->head; run!=NULL; run=run->nxt) {
		if (ts_algo_list_append(trg, run->cont) != TS_ALGO_OK) {
			NOMEM("list.append");
			ts_algo_list_destroy(trg);
			return err;
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Get Primary key of that vertex (roleid)
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getPK(nowdb_model_t      *model,
                              nowdb_roleid_t     roleid,
                              nowdb_model_prop_t **prop) {
	nowdb_model_prop_t tmp;

	MODELNULL();

	if (prop == NULL) return nowdb_err_get(nowdb_err_invalid,
	                          FALSE, OBJECT, "prop is NULL");

	tmp.roleid = roleid;

	*prop = ts_algo_tree_search(model->propById, &tmp, pkByRoleid);
	if (*prop == NULL) INVALID("no PK found for this vertex");
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Get all properties of vertex (roleid)
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getProperties(nowdb_model_t  *model,
                                      nowdb_roleid_t roleid,
                                      ts_algo_list_t *props) {
	nowdb_err_t err;
	nowdb_model_prop_t tmp;
	ts_algo_list_t unsorted, *sorted;

	MODELNULL();
	if (props == NULL) return nowdb_err_get(nowdb_err_invalid,
	                           FALSE, OBJECT, "list is NULL");

	ts_algo_list_init(&unsorted);
	tmp.roleid = roleid;

	// filter properties using tmp (i.e. roleid)
	if (ts_algo_tree_filter(model->propById, &unsorted, &tmp,
	                        propsByRoleid) != TS_ALGO_OK) {
		NOMEM("tree.filter");
		return err;
	}

	if (unsorted.len == 0) return NOWDB_OK;

	// sort properties by pos
	sorted = ts_algo_list_sort(&unsorted, sortByPos);
	ts_algo_list_destroy(&unsorted);
	if (sorted == NULL) {
		NOMEM("list.sort");
		return err;
	}

	// copy sorted list to result list
	err = copyList(sorted, props);
	ts_algo_list_destroy(sorted); free(sorted);
	if (err != NOWDB_OK) {
		NOMEM("list.copy");
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Get pedges
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getPedges(nowdb_model_t  *model,
                                  nowdb_key_t    edgeid,
                                  ts_algo_list_t *props) {
	nowdb_err_t err;
	nowdb_model_edge_t *e, pattern;
	nowdb_model_pedge_t tmp;
	ts_algo_list_t unsorted, *sorted;

	MODELNULL();
	if (props == NULL) return nowdb_err_get(nowdb_err_invalid,
	                           FALSE, OBJECT, "list is NULL");

	pattern.edgeid = edgeid;
	e = ts_algo_tree_find(model->edgeById, &pattern);
	if (e == NULL) INVALID("no such edge");


	ts_algo_list_init(&unsorted);
	tmp.edgeid = edgeid;

	// filter properties using tmp (i.e. roleid)
	if (ts_algo_tree_filter(model->pedgeById, &unsorted, &tmp,
		                    pedgesByEdgeid) != TS_ALGO_OK) {
		NOMEM("tree.filter");
		return err;
	}
	
	// sort properties by off
	sorted = ts_algo_list_sort(&unsorted, sortByOff);
	ts_algo_list_destroy(&unsorted);
	if (sorted == NULL) {
		NOMEM("list.sort");
		return err;
	}

	// copy sorted list to result list
	err = copyList(sorted, props);
	ts_algo_list_destroy(sorted); free(sorted);
	if (err != NOWDB_OK) {
		NOMEM("list.copy");
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * What is by name
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_whatIs(nowdb_model_t *model,
                               char           *name,
                               nowdb_content_t *trg) {
	nowdb_err_t err=NOWDB_OK;
	nowdb_err_t err2;
	thing_t pattern, *thing;

	MODELNULL();
	if (name == NULL) return nowdb_err_get(nowdb_err_invalid,
	                           FALSE, OBJECT, "name is NULL");

	err = nowdb_lock_read(&model->lock);
	if (err != NOWDB_OK) return err;

	pattern.name = name;
	thing = ts_algo_tree_find(model->thingByName, &pattern);

	if (thing == NULL) {
		err = nowdb_err_get(nowdb_err_key_not_found,
	                               FALSE, OBJECT, name);
	} else {
		*trg = thing->t==E?NOWDB_CONT_EDGE:
		                   NOWDB_CONT_VERTEX;
	}

	err2 = nowdb_unlock_read(&model->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

