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
 * Destroy Index Keys
 * ------------------------------------------------------------------------
 */
void nowdb_index_keys_destroy(nowdb_index_keys_t *keys);

/* ------------------------------------------------------------------------
 * Index
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
nowdb_err_t nowdb_index_create(char *path, uint16_t size,
                               nowdb_index_desc_t *desc);

/* ------------------------------------------------------------------------
 * Drop index
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_drop(char *path);

/* ------------------------------------------------------------------------
 * Open index
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_open(char *path, void *handle,
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

#endif
