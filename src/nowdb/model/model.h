/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Model
 * ========================================================================
 *
 * ========================================================================
 */
#ifndef nowdb_model_decl
#define nowdb_model_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/task/lock.h>
#include <nowdb/model/types.h>

#include <tsalgo/tree.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
	nowdb_rwlock_t  lock;
	char           *path;
	ts_algo_tree_t *vrtxById;
	ts_algo_tree_t *vrtxByName;
	ts_algo_tree_t *propById;
	ts_algo_tree_t *propByName;
	ts_algo_tree_t *edgeById;
	ts_algo_tree_t *edgeByName;
} nowdb_model_t;

nowdb_err_t nowdb_model_init(nowdb_model_t *model, char *path);

void nowdb_model_destroy(nowdb_model_t *model);

nowdb_err_t nowdb_model_load(nowdb_model_t *model);

nowdb_err_t nowdb_model_remove(nowdb_model_t *model);

nowdb_err_t nowdb_model_addVertex(nowdb_model_t        *model,
                                  nowdb_model_vertex_t *vrtx);

nowdb_err_t nowdb_model_removeVertex(nowdb_model_t *model,
                                     nowdb_roleid_t role); 

nowdb_err_t nowdb_model_addProperty(nowdb_model_t      *model,
                                    nowdb_roleid_t     roleid,
                                    nowdb_model_prop_t *prop);

nowdb_err_t nowdb_model_removeProperty(nowdb_model_t  *model,
                                       nowdb_roleid_t roleid,
                                       nowdb_key_t    propid);

nowdb_err_t nowdb_model_addEdge(nowdb_model_t      *model,
                                nowdb_model_edge_t *edge);

nowdb_err_t nowdb_model_removeEdge(nowdb_model_t *model,
                                   nowdb_key_t    edge);

nowdb_err_t nowdb_model_getVertexByName(nowdb_model_t        *model,
                                        char                  *name,
                                        nowdb_model_vertex_t **vrtx);

nowdb_err_t nowdb_model_getVertexById(nowdb_model_t        *model,
                                      nowdb_roleid_t       roleid,
                                      nowdb_model_vertex_t **vrtx);

nowdb_err_t nowdb_model_getPropById(nowdb_model_t      *model,
                                    nowdb_roleid_t     roleid,
                                    nowdb_key_t        propid,
                                    nowdb_model_prop_t **prop);

nowdb_err_t nowdb_model_getPropByName(nowdb_model_t      *model,
                                      nowdb_roleid_t     roleid,
                                      char                *name,
                                      nowdb_model_prop_t **prop);

nowdb_err_t nowdb_model_getEdgeByName(nowdb_model_t      *model,
                                      char                *name,
                                      nowdb_model_edge_t **edge);

nowdb_err_t nowdb_model_getEdgeById(nowdb_model_t      *model,
                                    nowdb_key_t        edgeid,
                                    nowdb_model_edge_t **edge);
#endif
