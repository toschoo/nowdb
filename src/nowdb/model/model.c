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

static ts_algo_rc_t noupdate(void *ignore, void *o, void *n) {
	return TS_ALGO_OK;
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
	                                     &vrtxdestroy,
	                                     &vrtxdestroy);
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
	                                     &propdestroy,
	                                     &propdestroy);
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
	                                     &edgedestroy,
	                                     &edgedestroy);
	if (model->edgeByName == NULL) {
		nowdb_model_destroy(model);
		NOMEM("tree.new");
	}

	return NOWDB_OK;
}

void nowdb_model_destroy(nowdb_model_t *model) {
	if (model == NULL) return;
	nowdb_rwlock_destroy(&model->lock);
	if (model->path != NULL) {
		free(model->path); model->path = NULL;
	}
	if (model->vrtxById != NULL) {
		free(model->vrtxById); model->vrtxById = NULL;
	}
	if (model->vrtxByName != NULL) {
		free(model->vrtxByName); model->vrtxByName = NULL;
	}
	if (model->propById != NULL) {
		free(model->propById); model->propById = NULL;
	}
	if (model->propByName != NULL) {
		free(model->propByName); model->propByName = NULL;
	}
	if (model->edgeById != NULL) {
		free(model->edgeById); model->edgeById = NULL;
	}
	if (model->edgeByName != NULL) {
		free(model->edgeByName); model->edgeByName = NULL;
	}
}

nowdb_err_t nowdb_model_load(nowdb_model_t *model);

nowdb_err_t nowdb_model_remove(nowdb_model_t *model);

static inline nowdb_err_t add(nowdb_model_t  *model,
                              ts_algo_tree_t *byId, 
                              ts_algo_tree_t *byName,
                              void           *thing) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2 = NOWDB_OK;
	void *tmp;

	if (thing == NULL) return nowdb_err_get(nowdb_err_invalid,
	                         FALSE, OBJECT, "object is NULL");

	err = nowdb_lock_write(&model->lock);
	if (err == NULL) return err;

	tmp = ts_algo_tree_find(byName, thing);
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

	/*
	 * write back to file
	 */

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
	return add(model, model->vrtxById, model->vrtxByName, vrtx);
}

nowdb_err_t nowdb_model_addProperty(nowdb_model_t      *model,
                                    nowdb_roleid_t     roleid,
                                    nowdb_model_prop_t *prop) {
	MODELNULL();
	return add(model, model->propById, model->propByName, prop);
}

nowdb_err_t nowdb_model_addEdge(nowdb_model_t      *model,
                                nowdb_model_edge_t *edge) {
	MODELNULL();
	return add(model, model->edgeById, model->edgeByName, edge);
}

nowdb_err_t nowdb_model_removeVertex(nowdb_model_t *model,
                                     nowdb_roleid_t role) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2 = NOWDB_OK;
	nowdb_model_vertex_t *vrtx;
	nowdb_model_vertex_t  tmp;

	MODELNULL();

	err = nowdb_lock_write(&model->lock);
	if (err == NULL) return err;

	tmp.roleid = role;
	vrtx = ts_algo_tree_find(model->vrtxById, &tmp);
	if (vrtx == NULL) {
		err = nowdb_err_get(nowdb_err_key_not_found,
		                    FALSE, OBJECT, "roleid");
		goto unlock;
	}

	ts_algo_tree_delete(model->vrtxById, vrtx);
	ts_algo_tree_delete(model->vrtxByName, vrtx);

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
	if (err == NULL) return err;

	tmp.roleid = role;
	tmp.propid = propid;

	p = ts_algo_tree_find(model->propById, &tmp);
	if (p == NULL) {
		err = nowdb_err_get(nowdb_err_key_not_found,
		                    FALSE, OBJECT, "propid");
		goto unlock;
	}

	ts_algo_tree_delete(model->propById, p);
	ts_algo_tree_delete(model->propByName, p);

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
	if (err == NULL) return err;

	tmp.edgeid = edge;
	e = ts_algo_tree_find(model->edgeById, &tmp);
	if (e == NULL) {
		err = nowdb_err_get(nowdb_err_key_not_found,
		                    FALSE, OBJECT, "edgeid");
		goto unlock;
	}

	ts_algo_tree_delete(model->edgeById, e);
	ts_algo_tree_delete(model->edgeByName, e);

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
	if (err == NULL) return err;

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
