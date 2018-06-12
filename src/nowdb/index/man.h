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

#include <tsalgo/tree.h>
#include <beet/index.h>

#include <stdlib.h>
#include <stdint.h>

/* ------------------------------------------------------------------------
 * Index manager
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_rwlock_t  lock;   /* protect index manager       */
	ts_algo_tree_t *byname; /* map storing indices by name */
	ts_algo_tree_t *bykey;  /* map storing indices by key  */
} nowdb_index_man_t;

/* ------------------------------------------------------------------------
 * How keys are represented in an index
 * ------------------------------------------------------------------------
 */
typedef struct {
	uint16_t   sz;
	uint16_t *off;
} nowdb_index_keys_t;

/* ------------------------------------------------------------------------
 * Initialise index manager
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_man_init(nowdb_index_man_t *iman);

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
                                     beet_index_t        idx);

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
                                      beet_index_t      *idx);

/* ------------------------------------------------------------------------
 * Get index by keys
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_man_getByKeys(nowdb_index_man_t  *iman,
                                      nowdb_context_t    *ctx,
                                      nowdb_index_keys_t *keys,
                                      beet_index_t       *idx);
#endif
