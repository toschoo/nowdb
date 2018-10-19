/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Index
 * ========================================================================
 */
#ifndef nowdb_index_decl
#define nowdb_index_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/task/lock.h>
#include <nowdb/scope/context.h>

#include <beet/index.h>

#include <stdlib.h>
#include <stdint.h>

/* ------------------------------------------------------------------------
 * How keys are represented in an index
 * ------------------------------------------------------------------------
 */
typedef struct {
	uint16_t   sz;
	uint16_t *off;
} nowdb_index_keys_t;

/* ------------------------------------------------------------------------
 * Create Index Keys
 * Note: probably not too useful
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_keys_create(nowdb_index_keys_t **keys,
                                            uint16_t sz, ...);

/* ------------------------------------------------------------------------
 * Copy  Index Keys
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_keys_copy(nowdb_index_keys_t *from,
                                  nowdb_index_keys_t **to);

/* ------------------------------------------------------------------------
 * Keysize vertex
 * ------------------------------------------------------------------------
 */
uint32_t nowdb_index_keySizeVertex(nowdb_index_keys_t *k);

/* ------------------------------------------------------------------------
 * Keysize edge
 * ------------------------------------------------------------------------
 */
uint32_t nowdb_index_keySizeEdge(nowdb_index_keys_t *k);

/* ------------------------------------------------------------------------
 * Grab keys from vertex
 * ------------------------------------------------------------------------
 */
void nowdb_index_grabVertexKeys(nowdb_index_keys_t *k,
                                const char    *vertex,
                                      char      *keys);

/* ------------------------------------------------------------------------
 * Grab keys from edge
 * ------------------------------------------------------------------------
 */
void nowdb_index_grabEdgeKeys(nowdb_index_keys_t *k,
                              const char      *edge,
                                    char      *keys);

/* ------------------------------------------------------------------------
 * Destroy Index Keys
 * ------------------------------------------------------------------------
 */
void nowdb_index_keys_destroy(nowdb_index_keys_t *keys);

/* ------------------------------------------------------------------------
 * Compare Edge Index Keys
 * ------------------------------------------------------------------------
 */
char nowdb_index_edge_compare(const void *left,
                              const void *right,
                              void       *keys);

/* ------------------------------------------------------------------------
 * Compare Vertex Index Keys
 * ------------------------------------------------------------------------
 */
char nowdb_index_vertex_compare(const void *left,
                                const void *right,
                                void       *keys);

/* ------------------------------------------------------------------------
 * Index
 * TODO: consider to add callbacks, e.g.:
 *       - keySize
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_rwlock_t lock;
	beet_index_t    idx;
} nowdb_index_t;

/* ------------------------------------------------------------------------
 * How we store index metadata
 * ------------------------------------------------------------------------
 */
typedef struct {
	char              *name;
	nowdb_context_t    *ctx;
	nowdb_index_keys_t *keys;
	nowdb_index_t      *idx;
} nowdb_index_desc_t;

/* -----------------------------------------------------------------------
 * Make safe index descriptor
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_desc_create(char                *name,
                                    nowdb_context_t     *ctx,
                                    nowdb_index_keys_t  *keys,
                                    nowdb_index_t       *idx,
                                    nowdb_index_desc_t **desc); 

/* -----------------------------------------------------------------------
 * Destroy index descriptor
 * -----------------------------------------------------------------------
 */
void nowdb_index_desc_destroy(nowdb_index_desc_t *desc); 

/* ------------------------------------------------------------------------
 * Create index
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_create(char *base,   char *path,
                               uint16_t            size,
                               nowdb_index_desc_t *desc);

/* ------------------------------------------------------------------------
 * Drop index
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_drop(char *base, char *path);

/* ------------------------------------------------------------------------
 * Open index
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_open(char *base,
                             char *path,
                             void *handle,
                             nowdb_index_desc_t *desc);

/* ------------------------------------------------------------------------
 * Close index
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_close(nowdb_index_t *idx);

/* ------------------------------------------------------------------------
 * Destroy index
 * ------------------------------------------------------------------------
 */
void nowdb_index_destroy(nowdb_index_t *idx);

/* ------------------------------------------------------------------------
 * Announce usage of this index
 * ----------------------------
 * The index is locked for reading.
 * The only procedure that would lock for writing is drop.
 * This way, we make sure that no index is dropped while it is used.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_use(nowdb_index_t *idx);

/* ------------------------------------------------------------------------
 * Announce end of usage of this index
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_enduse(nowdb_index_t *idx);

/* ------------------------------------------------------------------------
 * Insert into index 
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_insert(nowdb_index_t    *idx,
                               char             *keys,
                               nowdb_pageid_t    pge,
                               nowdb_bitmap64_t *map);

/* ------------------------------------------------------------------------
 * Get index 'compare' method
 * ------------------------------------------------------------------------
 */
beet_compare_t nowdb_index_getCompare(nowdb_index_t *idx);

/* ------------------------------------------------------------------------
 * Get index resource
 * ------------------------------------------------------------------------
 */
void *nowdb_index_getResource(nowdb_index_t *idx);

#endif
