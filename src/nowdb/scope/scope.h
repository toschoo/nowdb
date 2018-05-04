/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Scope: a collection of contexts
 * ========================================================================
 *
 * ========================================================================
 */
#ifndef nowdb_scope_decl
#define nowdb_scope_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/task/lock.h>
#include <nowdb/io/dir.h>
#include <nowdb/store/store.h>

#include <tsalgo/tree.h>

/* -----------------------------------------------------------------------
 * Context
 * -----------------------------------------------------------------------
 */
typedef struct {
	char          *name;
	nowdb_store_t store;
} nowdb_context_t;

/* ------------------------------------------------------------------------
 * Destroy context
 * ------------------------------------------------------------------------
 */
void nowdb_context_destroy(nowdb_context_t *ctx);

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

/* -----------------------------------------------------------------------
 * Context configurator
 * -----------------------------------------------------------------------
 */
typedef struct {
	uint32_t allocsize;
	uint32_t largesize;
	uint32_t   sorters;
	nowdb_bool_t  sort;
	nowdb_comp_t  comp;
	nowdb_encp_t  encp;
} nowdb_ctx_config_t;

#define NOWDB_SCOPE_CLOSED 0
#define NOWDB_SCOPE_OPEN 1

/* -----------------------------------------------------------------------
 * Scope
 * -----------------------------------------------------------------------
 */
typedef struct {
	nowdb_rwlock_t lock;     /* read/write lock */
	uint32_t       state;    /* open or closed  */
	nowdb_path_t   path;     /* base path       */
	nowdb_path_t   catalog;  /* catalog path    */
	nowdb_version_t ver;     /* db version      */
	nowdb_store_t vertices;  /* vertices        */
	ts_algo_tree_t contexts; /* contexts        */
	                         /* index manager   */
	                         /* model           */
	                         /* strings         */
} nowdb_scope_t;

/* -----------------------------------------------------------------------
 * Allocate and initialise a new scope
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_new(nowdb_scope_t **scope,
                            nowdb_path_t     path,
                            nowdb_version_t  ver);

/* -----------------------------------------------------------------------
 * Initialise an already allocated scope
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_init(nowdb_scope_t *scope,
                             nowdb_path_t    path,
                             nowdb_version_t  ver);

/* -----------------------------------------------------------------------
 * Destroy scope
 * -----------------------------------------------------------------------
 */
void nowdb_scope_destroy(nowdb_scope_t *scope);

/* -----------------------------------------------------------------------
 * Create a scope physically on disk
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_create(nowdb_scope_t *scope);

/* -----------------------------------------------------------------------
 * Drop a scope physically from disk
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_drop(nowdb_scope_t *scope);

/* -----------------------------------------------------------------------
 * Open a scope
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_open(nowdb_scope_t *scope);

/* -----------------------------------------------------------------------
 * Close a scope
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_close(nowdb_scope_t *scope);

/* -----------------------------------------------------------------------
 * Create a context within that scope
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_createContext(nowdb_scope_t     *scope,
                                      char               *name,
                                      nowdb_ctx_config_t *cfg);

/* -----------------------------------------------------------------------
 * Drop a context within that scope
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_dropContext(nowdb_scope_t *scope,
                                    char          *name);

/* -----------------------------------------------------------------------
 * Get context within that scope
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_getContext(nowdb_scope_t   *scope,
                                   char             *name,
                                   nowdb_context_t **ctx);

/* -----------------------------------------------------------------------
 * Create index within that scope
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_createIndex(nowdb_scope_t *scope,
                                    char          *name);
                                    /* .... */

/* -----------------------------------------------------------------------
 * Drop index within that scope
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_dropIndex(nowdb_scope_t *scope,
                                  char          *name);

/* -----------------------------------------------------------------------
 * Get index within that scope by name
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_getIndexByName(nowdb_scope_t   *scope,
                                       char            *name);

/* -----------------------------------------------------------------------
 * Get index within that scope by definition
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_getIndex(nowdb_scope_t   *scope);
                                 /* ... */

/* ------------------------------------------------------------------------
 * Insert one record
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_insert(nowdb_scope_t *scope,
                               char        *context,
                               void          *data);
#endif
