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

/* ------------------------------------------------------------------------
 * model consits of
 * - vertex model
 * - property model
 * - edge model
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_rwlock_t  lock;        /* model is threadsafe        */
	char           *path;        /* path to model on disk      */
	ts_algo_tree_t *thingByName; /* lookup what it is          */
	ts_algo_tree_t *vrtxById;    /* lookup vertex by id        */
	ts_algo_tree_t *vrtxByName;  /* lookup vertex by name      */
	ts_algo_tree_t *propById;    /* lookup prop.  by id        */
	ts_algo_tree_t *propByName;  /* lookup prop.  by name      */
	ts_algo_tree_t *edgeById;    /* lookup edge   by id        */
	ts_algo_tree_t *edgeByName;  /* lookup edge   by name      */
	ts_algo_tree_t *pedgeById;   /* lookup edge prop.  by id   */
	ts_algo_tree_t *pedgeByName; /* lookup edge prop.  by name */
} nowdb_model_t;

/* ------------------------------------------------------------------------
 * init the model
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_init(nowdb_model_t *model, char *path);

/* ------------------------------------------------------------------------
 * destroy the model
 * ------------------------------------------------------------------------
 */
void nowdb_model_destroy(nowdb_model_t *model);

/* ------------------------------------------------------------------------
 * load the model from disk
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_load(nowdb_model_t *model);

/* ------------------------------------------------------------------------
 * Get all edges
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getEdges(nowdb_model_t   *model,
                                 ts_algo_list_t **edges);

/* ------------------------------------------------------------------------
 * Get all vertices
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getVertices(nowdb_model_t  *model,
                                   ts_algo_list_t **vrtxs);

/* ------------------------------------------------------------------------
 * Add a vertex model
 * NOTE: vertex model must be unique in id and name!
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_addVertex(nowdb_model_t        *model,
                                  nowdb_model_vertex_t *vrtx);

/* ------------------------------------------------------------------------
 * Remove a vertex model
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_removeVertex(nowdb_model_t *model,
                                     nowdb_roleid_t role); 

/* ------------------------------------------------------------------------
 * Add a property model
 * NOTE: the model must be unique in (roleid, propid) and (roleid, name)
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_addProperty(nowdb_model_t      *model,
                                    nowdb_model_prop_t *prop);

/* ------------------------------------------------------------------------
 * Remove a property model
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_removeProperty(nowdb_model_t  *model,
                                       nowdb_roleid_t roleid,
                                       nowdb_key_t    propid);

/* ------------------------------------------------------------------------
 * Add type in one go
 * -------------------
 * - name is the name of the type
 * - props is a list of properties
 * The properties do not need to have a roleid.
 * The roleid is determined when adding.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_addType(nowdb_model_t  *model,
                                char           *name,
                                ts_algo_list_t *props);

/* ------------------------------------------------------------------------
 * Remove type in one go
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_removeType(nowdb_model_t  *model,
                                   char           *name);

/* ------------------------------------------------------------------------
 * Add an edge model
 * NOTE: the model must be unique in edgeid and name
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_addEdge(nowdb_model_t      *model,
                                nowdb_model_edge_t *edge);

/* ------------------------------------------------------------------------
 * Add edge with properties
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_addEdgeType(nowdb_model_t   *model,
                                    char             *name,
                                    nowdb_key_t     edgeid,
                                    nowdb_roleid_t  origin,
                                    nowdb_roleid_t  destin,
                                    ts_algo_list_t *props);

/* ------------------------------------------------------------------------
 * Remove an edge model
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_removeEdge(nowdb_model_t *model,
                                   nowdb_key_t    edge);

/* ------------------------------------------------------------------------
 * Remove edge with properties
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_removeEdgeType(nowdb_model_t *model,
                                       char          *name);

/* ------------------------------------------------------------------------
 * Get vertex by name
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getVertexByName(nowdb_model_t        *model,
                                        char                  *name,
                                        nowdb_model_vertex_t **vrtx);

/* ------------------------------------------------------------------------
 * Get vertex by id
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getVertexById(nowdb_model_t        *model,
                                      nowdb_roleid_t       roleid,
                                      nowdb_model_vertex_t **vrtx);

/* ------------------------------------------------------------------------
 * Get property by (roleid, propid)
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getPropById(nowdb_model_t      *model,
                                    nowdb_roleid_t     roleid,
                                    nowdb_key_t        propid,
                                    nowdb_model_prop_t **prop);

/* ------------------------------------------------------------------------
 * Get property by (roleid, name)
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getPropByName(nowdb_model_t      *model,
                                      nowdb_roleid_t     roleid,
                                      char                *name,
                                      nowdb_model_prop_t **prop);

/* ------------------------------------------------------------------------
 * Get Primary key of that vertex (roleid)
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getPK(nowdb_model_t      *model,
                              nowdb_roleid_t     roleid,
                              nowdb_model_prop_t **prop);

/* ------------------------------------------------------------------------
 * Get all properties of vertex (roleid)
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getProperties(nowdb_model_t  *model,
                                      nowdb_roleid_t roleid,
                                      ts_algo_list_t *props);

/* ------------------------------------------------------------------------
 * Get all edge properties of edge (edgeid)
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getPedges(nowdb_model_t  *model,
                                  nowdb_key_t    edgeid,
                                  ts_algo_list_t *props);

/* ------------------------------------------------------------------------
 * Get edge by name
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getEdgeByName(nowdb_model_t      *model,
                                      char                *name,
                                      nowdb_model_edge_t **edge);

/* ------------------------------------------------------------------------
 * Get edge by id
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getEdgeById(nowdb_model_t      *model,
                                    nowdb_key_t        edgeid,
                                    nowdb_model_edge_t **edge);

/* ------------------------------------------------------------------------
 * Get edge property by (edgeid, propid)
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getPedgeById(nowdb_model_t        *model,
                                     nowdb_key_t          edgeid,
                                     nowdb_key_t          propid,
                                     nowdb_model_pedge_t **pedge);

/* ------------------------------------------------------------------------
 * Get property by (edgeid, name)
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_getPedgeByName(nowdb_model_t        *model,
                                       nowdb_key_t          edgeid,
                                       char                  *name,
                                       nowdb_model_pedge_t **pedge);

/* ------------------------------------------------------------------------
 * What is by name
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_model_whatIs(nowdb_model_t  *model,
                               char            *name,
                               nowdb_content_t *trg);
#endif
