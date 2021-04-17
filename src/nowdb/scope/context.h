/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Context is just a wrapper around store
 * ========================================================================
 *
 * ========================================================================
 */
#ifndef nowdb_context_decl
#define nowdb_context_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/task/lock.h>
#include <nowdb/io/dir.h>
#include <nowdb/store/store.h>

/* -----------------------------------------------------------------------
 * Context
 * -----------------------------------------------------------------------
 */
typedef struct {
	char             *name; // context name
        char         *strgname; // storage name
	nowdb_plru8r_t *evache; // external vertex cache (contains residents)
	nowdb_plru8r_t *ivache; // internal vertex cache
	nowdb_store_t    store; // the heart of the matter
} nowdb_context_t;

/* ------------------------------------------------------------------------
 * Destroy context
 * ------------------------------------------------------------------------
 */
void nowdb_context_destroy(nowdb_context_t *ctx);

/* ------------------------------------------------------------------------
 * Wrap error into context error
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_context_err(nowdb_context_t *ctx, nowdb_err_t err);

/* ------------------------------------------------------------------------
 * Insert one record
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_context_insert(nowdb_context_t *ctx,
                                 void          *data);

/* ------------------------------------------------------------------------
 * Insert n records
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_context_insertBulk(nowdb_context_t *ctx,
                                     void           *data,
                                     uint32_t      count);
#endif
