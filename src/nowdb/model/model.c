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

#define V 0
#define P 1
#define E 2

#define MODELNULL() \
	if (model == NULL) return nowdb_err_get(nowdb_err_invalid, \
	                          FALSE, OBJECT, "model is NULL");

#define NOMEM(s) \
	return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, s);

#define PROP(x) \
	((nowdb_model_prop_t*)x)

#define VRTX(x) \
	((nowdb_model_vertex_t*)x)

#define EDGE(x) \
	((nowdb_model_edge_t*)x)

static ts_algo_cmp_t propidcompare(void *ignore, void *one, void *two) {
	if (PROP(one)->roleid < PROP(two)->roleid) return ts_algo_cmp_less;
	if (PROP(one)->roleid > PROP(two)->roleid) return ts_algo_cmp_greater;
	if (PROP(one)->propid < PROP(two)->propid) return ts_algo_cmp_less;
	if (PROP(one)->propid > PROP(two)->propid) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

static ts_algo_cmp_t propnamecompare(void *ignore, void *one, void *two) {
	int x = strcmp(PROP(one)->name, PROP(two)->name);
	if (x < 0) return ts_algo_cmp_less;
	if (x > 0) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

static ts_algo_cmp_t vrtxidcompare(void *ignore, void *one, void *two) {
	if (VRTX(one)->roleid < VRTX(two)->roleid) return ts_algo_cmp_less;
	if (VRTX(one)->roleid > VRTX(two)->roleid) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

static ts_algo_cmp_t vrtxnamecompare(void *ignore, void *one, void *two) {
	int x = strcmp(VRTX(one)->name, VRTX(two)->name);
	if (x < 0) return ts_algo_cmp_less;
	if (x > 0) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

static ts_algo_cmp_t edgeidcompare(void *ignore, void *one, void *two) {
	if (EDGE(one)->edgeid < EDGE(two)->edgeid) return ts_algo_cmp_less;
	if (EDGE(one)->edgeid > EDGE(two)->edgeid) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

static ts_algo_cmp_t edgenamecompare(void *ignore, void *one, void *two) {
	int x = strcmp(EDGE(one)->name, EDGE(two)->name);
	if (x < 0) return ts_algo_cmp_less;
	if (x > 0) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

static void propdestroy(void *ignore, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	if (PROP(*n)->name != NULL) {
		free(PROP(*n)->name);
		PROP(*n)->name = NULL;
	}
	free(*n); *n = NULL;
}

static void vrtxdestroy(void *ignore, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	if (VRTX(*n)->name != NULL) {
		free(VRTX(*n)->name);
		VRTX(*n)->name = NULL;
	}
	free(*n); *n = NULL;
}

static void edgedestroy(void *ignore, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	if (EDGE(*n)->name != NULL) {
		free(EDGE(*n)->name);
		EDGE(*n)->name = NULL;
	}
	free(*n); *n = NULL;
}

static void nodestroy(void *ignore, void **n) {}

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

	for(int i=0;i<3;i++) {
		switch(i) {
		case 0: f=VMODEL; break;
		case 1: f=PMODEL; break;
		case 2: f=EMODEL; break;
		}

		p = nowdb_path_append(model->path, f);
		if (p == NULL) NOMEM("create model path");

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

nowdb_err_t nowdb_model_init(nowdb_model_t *model, char *path) {
	nowdb_err_t err;

	MODELNULL();

	model->path = NULL;
	model->vrtxById = NULL;
	model->vrtxByName = NULL;
	model->edgeByName = NULL;
	model->edgeById = NULL;

	if (path == NULL) return nowdb_err_get(nowdb_err_invalid,
	                          FALSE, OBJECT, "path is NULL");

	err = nowdb_rwlock_init(&model->lock);
	if (err != NOWDB_OK) return err;

	model->path = strdup(path);
	if (model->path == NULL) NOMEM("allocating path");

	model->vrtxById = ts_algo_tree_new(&vrtxidcompare, NULL,
	                                   &noupdate,
	                                   &vrtxdestroy,
	                                   &vrtxdestroy);
	if (model->vrtxById == NULL) {
		nowdb_model_destroy(model);
		NOMEM("tree.new");
	}

	model->vrtxByName = ts_algo_tree_new(&vrtxnamecompare, NULL,
	                                     &noupdate,
	                                     &nodestroy,
	                                     &nodestroy);
	if (model->vrtxByName == NULL) {
		nowdb_model_destroy(model);
		NOMEM("tree.new");
	}

	model->propById = ts_algo_tree_new(&propidcompare, NULL,
	                                   &noupdate,
	                                   &propdestroy,
	                                   &propdestroy);
	if (model->propById == NULL) {
		nowdb_model_destroy(model);
		NOMEM("tree.new");
	}

	model->propByName = ts_algo_tree_new(&propnamecompare, NULL,
	                                     &noupdate,
	                                     &nodestroy,
	                                     &nodestroy);
	if (model->propByName == NULL) {
		nowdb_model_destroy(model);
		NOMEM("tree.new");
	}

	model->edgeById = ts_algo_tree_new(&edgeidcompare, NULL,
	                                   &noupdate,
	                                   &edgedestroy,
	                                   &edgedestroy);
	if (model->edgeById == NULL) {
		nowdb_model_destroy(model);
		NOMEM("tree.new");
	}

	model->edgeByName = ts_algo_tree_new(&edgenamecompare, NULL,
	                                     &noupdate,
	                                     &nodestroy,
	                                     &nodestroy);
	if (model->edgeByName == NULL) {
		nowdb_model_destroy(model);
		NOMEM("tree.new");
	}

	err = mkFiles(model);
	if (err != NOWDB_OK) {
		nowdb_model_destroy(model);
		return err;
	}
	return NOWDB_OK;
}

void nowdb_model_destroy(nowdb_model_t *model) {
	if (model == NULL) return;
	nowdb_rwlock_destroy(&model->lock);
	if (model->path != NULL) {
		free(model->path); model->path = NULL;
	}
	if (model->vrtxByName != NULL) {
		ts_algo_tree_destroy(model->vrtxByName);
		free(model->vrtxByName); model->vrtxByName = NULL;
	}
	if (model->vrtxById != NULL) {
		ts_algo_tree_destroy(model->vrtxById);
		free(model->vrtxById); model->vrtxById = NULL;
	}
	if (model->propByName != NULL) {
		ts_algo_tree_destroy(model->propByName);
		free(model->propByName); model->propByName = NULL;
	}
	if (model->propById != NULL) {
		ts_algo_tree_destroy(model->propById);
		free(model->propById); model->propById = NULL;
	}
	if (model->edgeByName != NULL) {
		ts_algo_tree_destroy(model->edgeByName);
		free(model->edgeByName); model->edgeByName = NULL;
	}
	if (model->edgeById != NULL) {
		ts_algo_tree_destroy(model->edgeById);
		free(model->edgeById); model->edgeById = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Helper: compute vertex size
 * ------------------------------------------------------------------------
 */
static uint32_t computeVertexSize(ts_algo_list_t *list) {
	uint32_t sz = 0;
	ts_algo_list_node_t *runner;
	nowdb_model_vertex_t *v;

	for(runner=list->head;runner!=NULL;runner=runner->nxt) {
		v = runner->cont;
		sz+=strlen(v->name) + 1 + 4 + 1;
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

	for(runner=list->head;runner!=NULL;runner=runner->nxt) {
		p = runner->cont;
		sz+=strlen(p->name) + 1 + 8 + 8 + 1;
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

	for(runner=list->head;runner!=NULL;runner=runner->nxt) {
		e = runner->cont;
		sz+=strlen(e->name) + 1 + 26;
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
		memcpy(buf+sz, &p->value, 4); sz+=4;
		memcpy(buf+sz, &p->prop, 1); sz+=1;
		strcpy(buf+sz, p->name); sz+=strlen(p->name)+1;
	}
}

/* ------------------------------------------------------------------------
 * Helper: write property to buffer
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
		memcpy(buf+sz, &e->weight, 4); sz+=4;
		memcpy(buf+sz, &e->weight2, 4); sz+=4;
		memcpy(buf+sz, &e->edge, 1); sz+=1;
		memcpy(buf+sz, &e->label, 1); sz+=1;
		strcpy(buf+sz, e->name); sz+=strlen(e->name)+1;
	}
}

/* ------------------------------------------------------------------------
 * Helper: store vertex model
 * ------------------------------------------------------------------------
 */
static nowdb_err_t storeModel(nowdb_model_t *model, char what) {
	nowdb_err_t err = NOWDB_OK;
	ts_algo_list_t *list;
	nowdb_path_t p,f;
	char *buf;
	uint32_t sz;
	ts_algo_tree_t *tree;

	switch(what) {
	case V: tree = model->vrtxById; f=VMODEL; break;
	case P: tree = model->propById; f=PMODEL; break;
	case E: tree = model->edgeById; f=EMODEL; break;
	default: return nowdb_err_get(nowdb_err_panic,
	  FALSE, OBJECT, "impossible value in switch");
	}

	if (tree->count == 0) {
		fprintf(stderr, "write zero and ready\n");
		return NOWDB_OK;
	}
	list = ts_algo_tree_toList(tree);
	if (list == NULL) NOMEM("vrtxById.toList");

	switch(what) {
	case V: sz = computeVertexSize(list); break;
	case P: sz = computePropSize(list); break;
	case E: sz = computeEdgeSize(list); break;
	default: return nowdb_err_get(nowdb_err_panic,
	  FALSE, OBJECT, "impossible value in switch");
	}

	buf = malloc(sz);
	if (buf == NULL) {
		ts_algo_list_destroy(list); free(list);
		NOMEM("allocating buffer");
	}

	vertex2buf(buf, sz, list);
	switch(what) {
	case V: vertex2buf(buf,sz,list); break;
	case P: prop2buf(buf,sz,list); break;
	case E: edge2buf(buf,sz,list); break;
	default: return nowdb_err_get(nowdb_err_panic,
	  FALSE, OBJECT, "impossible value in switch");
	}

	ts_algo_list_destroy(list); free(list);

	p = nowdb_path_append(model->path, f);
	if (p == NULL) {
		free(buf);
		NOMEM("allocating path");
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
	uint32_t off = 0;
	nowdb_model_vertex_t *v;
	size_t s;

	while(off < sz) {
		v = calloc(1,sizeof(nowdb_model_vertex_t));
		if (v == NULL) NOMEM("allocating vertex");
		memcpy(&v->roleid, buf+off, 4); off+=4;
		memcpy(&v->vid, buf+off, 1); off+=1;
		s = strnlen(buf+off, 4097);
		if (s > 4096) {
			free(v);
			return nowdb_err_get(nowdb_err_catalog,
		         FALSE, OBJECT, "vertex name too long");
		}
		v->name = malloc(s+1);
		if (v->name == NULL) {
			free(v);
			NOMEM("allocating vertex name");
		}
		strcpy(v->name,buf+off); off+=s+1;
		if (ts_algo_list_append(list, v) != TS_ALGO_OK) {
			free(v);
			NOMEM("list.append");
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: load property model
 * ------------------------------------------------------------------------
 */
static nowdb_err_t loadProp(ts_algo_list_t   *list,
                            char *buf, uint32_t sz) {
	uint32_t off = 0;
	nowdb_model_prop_t *p;
	size_t s;

	while(off < sz) {
		p = calloc(1,sizeof(nowdb_model_prop_t));
		if (p == NULL) NOMEM("allocating prop");
		memcpy(&p->propid, buf+off, 8); off+=8;
		memcpy(&p->roleid, buf+off, 4); off+=4;
		memcpy(&p->value, buf+off, 4); off+=4;
		memcpy(&p->prop, buf+off, 1); off+=1;
		s = strnlen(buf+off, 4097);
		if (s > 4096) {
			free(p);
			return nowdb_err_get(nowdb_err_catalog,
		         FALSE, OBJECT, "property name too long");
		}
		p->name = malloc(s+1);
		if (p->name == NULL) {
			free(p);
			NOMEM("allocating property name");
		}
		strcpy(p->name,buf+off); off+=s+1;
		if (ts_algo_list_append(list, p) != TS_ALGO_OK) {
			free(p);
			NOMEM("list.append");
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: load edge model
 * ------------------------------------------------------------------------
 */
static nowdb_err_t loadEdge(ts_algo_list_t   *list,
                            char *buf, uint32_t sz) {
	uint32_t off = 0;
	nowdb_model_edge_t *e;
	size_t s;

	while(off < sz) {
		e = calloc(1,sizeof(nowdb_model_edge_t));
		if (e == NULL) NOMEM("allocating edge");
		memcpy(&e->edgeid, buf+off, 8); off+=8;
		memcpy(&e->origin, buf+off, 4); off+=4;
		memcpy(&e->destin, buf+off, 4); off+=4;
		memcpy(&e->weight, buf+off, 4); off+=4;
		memcpy(&e->weight2, buf+off, 4); off+=4;
		memcpy(&e->edge, buf+off, 1); off+=1;
		memcpy(&e->label, buf+off, 1); off+=1;
		s = strnlen(buf+off, 4097);
		if (s > 4096) {
			free(e);
			return nowdb_err_get(nowdb_err_catalog,
		         FALSE, OBJECT, "edge name too long");
		}
		e->name = malloc(s+1);
		if (e->name == NULL) {
			free(e);
			NOMEM("allocating edge name");
		}
		strcpy(e->name,buf+off); off+=s+1;
		if (ts_algo_list_append(list, e) != TS_ALGO_OK) {
			free(e);
			NOMEM("list.append");
		}
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
	char *buf;
	uint32_t sz=0;
	ts_algo_tree_t *byId, *byName;

	switch(what) {
	case V: byId = model->vrtxById; f=VMODEL;
	        byName = model->vrtxByName; break;
	case P: byId = model->propById; f=PMODEL;
	        byName = model->propByName; break;
	case E: byId = model->edgeById; f=EMODEL;
	        byName = model->edgeByName; break;
	default: return nowdb_err_get(nowdb_err_panic,
	  FALSE, OBJECT, "impossible value in switch");
	}

	p = nowdb_path_append(model->path, f);
	if (p == NULL) NOMEM("allocating path");

	err = readSize(p, &sz);
	if (err != NOWDB_OK) {
		free(p); return err;
	}

	buf = malloc(sz);
	if (buf == NULL) {
		free(p);
		NOMEM("allocating buffer");
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
	default: return nowdb_err_get(nowdb_err_panic,
	  FALSE, OBJECT, "impossible value in switch");
	}

	free(buf);

	if (err != NOWDB_OK) {
		ts_algo_list_destroy(&list); return err;
	}
	for(runner=list.head;runner!=NULL;runner=runner->nxt) {
		if (ts_algo_tree_insert(byId, runner->cont) != TS_ALGO_OK) {
			ts_algo_list_destroy(&list);
			NOMEM("byId.insert");
		}
		if (ts_algo_tree_insert(byName, runner->cont) != TS_ALGO_OK) {
			ts_algo_list_destroy(&list);
			NOMEM("byName.insert");
		}
	}
	ts_algo_list_destroy(&list);
	return NOWDB_OK;
}

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

unlock:
	err2 = nowdb_unlock_write(&model->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

nowdb_err_t nowdb_model_remove(nowdb_model_t *model);

static inline nowdb_err_t add(nowdb_model_t  *model,
                              char            what,
                              void           *thing) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2 = NOWDB_OK;
	ts_algo_tree_t *byId;
	ts_algo_tree_t *byName;
	void *tmp;

	if (thing == NULL) return nowdb_err_get(nowdb_err_invalid,
	                         FALSE, OBJECT, "object is NULL");

	switch(what) {
	case V: byId = model->vrtxById;
	        byName = model->vrtxByName; break;
	case P: byId = model->propById;
	        byName = model->propByName; break;
	case E: byId = model->edgeById; 
	        byName = model->edgeByName; break;
	default: return nowdb_err_get(nowdb_err_panic,
	  FALSE, OBJECT, "impossible value in switch");
	}

	err = nowdb_lock_write(&model->lock);
	if (err != NOWDB_OK) return err;

	tmp = ts_algo_tree_find(byId, thing);
	if (tmp != NULL) {
		err = nowdb_err_get(nowdb_err_dup_key, FALSE, OBJECT,
		                                               "id");
		goto unlock;
	}

	tmp = ts_algo_tree_find(byName, thing);
	if (tmp != NULL) {
		err = nowdb_err_get(nowdb_err_dup_key, FALSE, OBJECT,
		                                             "name");
		goto unlock;
	}
	if (ts_algo_tree_insert(byId, thing) != TS_ALGO_OK) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                     "tree.insert");
		goto unlock;
	}
	if (ts_algo_tree_insert(byName, thing) != TS_ALGO_OK) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                     "tree.insert");
		goto unlock;
	}

	err = storeModel(model, what);

unlock:
	err2 = nowdb_unlock_write(&model->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

nowdb_err_t nowdb_model_addVertex(nowdb_model_t        *model,
                                  nowdb_model_vertex_t *vrtx) {
	MODELNULL();
	return add(model, V, vrtx);
}

nowdb_err_t nowdb_model_addProperty(nowdb_model_t      *model,
                                    nowdb_model_prop_t *prop) {
	MODELNULL();
	return add(model, P, prop);
}

nowdb_err_t nowdb_model_addEdge(nowdb_model_t      *model,
                                nowdb_model_edge_t *edge) {
	MODELNULL();
	return add(model, E, edge);
}

nowdb_err_t nowdb_model_removeVertex(nowdb_model_t *model,
                                     nowdb_roleid_t role) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2 = NOWDB_OK;
	nowdb_model_vertex_t *vrtx;
	nowdb_model_vertex_t  tmp;

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

	ts_algo_tree_delete(model->vrtxByName, vrtx);
	ts_algo_tree_delete(model->vrtxById, vrtx);

unlock:
	err2 = nowdb_unlock_write(&model->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}


nowdb_err_t nowdb_model_removeProp(nowdb_model_t *model,
                                   nowdb_roleid_t role,
                                   nowdb_key_t  propid) {
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

unlock:
	err2 = nowdb_unlock_write(&model->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

nowdb_err_t nowdb_model_removeEdge(nowdb_model_t *model,
                                   nowdb_key_t    edge) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2 = NOWDB_OK;
	nowdb_model_edge_t *e;
	nowdb_model_edge_t  tmp;

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

	ts_algo_tree_delete(model->edgeByName, e);
	ts_algo_tree_delete(model->edgeById, e);

unlock:
	err2 = nowdb_unlock_write(&model->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

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
		                                 FALSE, OBJECT, "name");
	return NOWDB_OK;
}

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
		                                   FALSE, OBJECT, "id");
	return NOWDB_OK;
}

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
		                                  FALSE, OBJECT, "name");
	return NOWDB_OK;
}

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
		                                   FALSE, OBJECT, "id");
	return NOWDB_OK;
}

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
		                                 FALSE, OBJECT, "name");
	return NOWDB_OK;
}
