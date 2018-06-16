/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Index Manager
 * ========================================================================
 */
#ifndef nowdb_index_man_decl
#define nowdb_index_man_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/task/lock.h>
#include <nowdb/scope/context.h>
#include <nowdb/index/index.h>

#include <tsalgo/tree.h>
#include <beet/index.h>

#include <stdlib.h>
#include <stdint.h>

/* ------------------------------------------------------------------------
 * Index manager
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_rwlock_t     lock; /* protect index manager       */
	FILE              *file; /* where we store metadata     */
	char              *path; /* path to that file           */
	char           *ctxpath; /* path to the contexts        */
	char            *vxpath; /* path to vertex              */
	void            *handle; /* handle to compare lib       */
	ts_algo_tree_t *context; /* context for name resolution */
	ts_algo_tree_t  *byname; /* map storing indices by name */
	ts_algo_tree_t   *bykey; /* map storing indices by key  */
} nowdb_index_man_t;

/* ------------------------------------------------------------------------
 * Initialise index manager
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_man_init(nowdb_index_man_t *iman,
                                 ts_algo_tree_t *context,
                                              char *path,
                                           char *ctxpath,
                                            char *vxpath,
                                            void *handle);

/* ------------------------------------------------------------------------
 * Destroy index manager
 * ------------------------------------------------------------------------
 */
void nowdb_index_man_destroy(nowdb_index_man_t *iman);

/* ------------------------------------------------------------------------
 * Register an index
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_man_register(nowdb_index_man_t  *iman,
                                     char               *name,
                                     nowdb_context_t    *ctx,
                                     nowdb_index_keys_t *keys,
                                     nowdb_index_t      *idx);

/* ------------------------------------------------------------------------
 * Unregister an index
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_man_unregister(nowdb_index_man_t *iman,
                                       char              *name);

/* ------------------------------------------------------------------------
 * Get index by name
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_man_getByName(nowdb_index_man_t *iman,
                                      char              *name,
                                      nowdb_index_t    **idx);

/* ------------------------------------------------------------------------
 * Get index by keys
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_man_getByKeys(nowdb_index_man_t  *iman,
                                      nowdb_context_t    *ctx,
                                      nowdb_index_keys_t *keys,
                                      nowdb_index_t     **idx);
#endif
