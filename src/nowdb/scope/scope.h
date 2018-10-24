/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Scope: a database with 1 vertex table and n context tables
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
#include <nowdb/scope/context.h>
#include <nowdb/scope/loader.h>
#include <nowdb/index/man.h>
#include <nowdb/model/model.h>
#include <nowdb/mem/plru12.h>
#include <nowdb/text/text.h>
#include <nowdb/scope/procman.h>

#include <tsalgo/tree.h>

/* -----------------------------------------------------------------------
 * Scope
 * -----------------------------------------------------------------------
 */
typedef struct {
	nowdb_rwlock_t     lock; /* read/write lock   */
	uint32_t          state; /* open or closed    */
	nowdb_path_t       path; /* base path         */
	nowdb_path_t    catalog; /* catalog path      */
	nowdb_version_t     ver; /* db version        */
	nowdb_store_t  vertices; /* vertices          */
	nowdb_index_t   *vindex; /* index on vertices */
	nowdb_plru12_t   *vache; /* vertex cache      */
	ts_algo_tree_t contexts; /* contexts          */
	nowdb_index_man_t *iman; /* index manager     */
	nowdb_model_t    *model; /* model             */
	nowdb_text_t      *text; /* strings           */
	nowdb_procman_t   *pman; /* stored procedures */
} nowdb_scope_t;

/* -----------------------------------------------------------------------
 * Scope state (open/closed)
 * -----------------------------------------------------------------------
 */
#define NOWDB_SCOPE_CLOSED 0
#define NOWDB_SCOPE_OPEN 1

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
nowdb_err_t nowdb_scope_createIndex(nowdb_scope_t     *scope,
                                    char               *name,
                                    char            *context,
                                    nowdb_index_keys_t *keys,
                                    uint16_t         sizing);

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
                                       char            *name,
                                       nowdb_index_t   **idx);

/* -----------------------------------------------------------------------
 * Get index within that scope by definition
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_getIndex(nowdb_scope_t   *scope,
                                 char          *context,
                                 nowdb_index_keys_t  *k,
                                 nowdb_index_t    **idx);

/* -----------------------------------------------------------------------
 * Create new stored procedure/function
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_createProcedure(nowdb_scope_t  *scope,
                                        nowdb_proc_desc_t *pd);

/* -----------------------------------------------------------------------
 * Drop stored procedure/function
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_dropProcedure(nowdb_scope_t *scope,
                                      char          *name);

/* -----------------------------------------------------------------------
 * Get stored procedure/function
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_getProcedure(nowdb_scope_t   *scope,
                                     char             *name,
                                     nowdb_proc_desc_t **pd);

/* -----------------------------------------------------------------------
 * Create Type
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_createType(nowdb_scope_t  *scope,
                                   char           *name,
                                   ts_algo_list_t *props);

/* -----------------------------------------------------------------------
 * Drop Type
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_dropType(nowdb_scope_t *scope,
                                 char          *name);

/* -----------------------------------------------------------------------
 * Create Edge
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_createEdge(nowdb_scope_t  *scope,
                                   char           *name,
                                   char           *origin,
                                   char           *destin,
                                   uint32_t        label,
                                   uint32_t        weight,
                                   uint32_t        weight2);

/* -----------------------------------------------------------------------
 * Drop Edge
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_dropEdge(nowdb_scope_t *scope,
                                 char          *name);

/* ------------------------------------------------------------------------
 * Insert one record
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_insert(nowdb_scope_t *scope,
                               char        *context,
                               void          *data);

/* ------------------------------------------------------------------------
 * Load csv
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_scope_load(nowdb_scope_t *scope,
                             nowdb_context_t *ctx,
                             nowdb_path_t    path,
                             nowdb_loader_t *ldr);
#endif
