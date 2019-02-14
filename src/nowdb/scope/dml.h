/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Scope DML services
 * ========================================================================
 */
#ifndef nowdb_dml_decl
#define nowdb_dml_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/store/store.h>
#include <nowdb/model/model.h>
#include <nowdb/text/text.h>
#include <nowdb/mem/pklru.h>
#include <nowdb/scope/scope.h>

#include <tsalgo/list.h>

/* ------------------------------------------------------------------------
 * DML helper
 * ------------------------------------------------------------------------
 */
typedef struct {
	char            *trgname; /* name of least recently used target */
        nowdb_scope_t     *scope; /* current scope                      */
	nowdb_target_t    target; /* least recently used target         */
	nowdb_store_t     *store; /* least recently used store          */
	nowdb_pklru_t      *tlru; /* private text cache                 */
	nowdb_model_vertex_t  *v; /* least recently used vertex model   */
	nowdb_model_prop_t   **p; /* least recently used prop model     */
	nowdb_model_edge_t    *e; /* least recently used edge model     */
	nowdb_model_pedge_t **pe; /* least recently used pedge model    */
	nowdb_model_vertex_t  *o; /* least recently used origin         */
	nowdb_model_vertex_t  *d; /* least recently used destin         */
	int                propn; /* number of vertex properties        */
	int               pedgen; /* number of edge   properties        */
} nowdb_dml_t;

/* ------------------------------------------------------------------------
 * Simplified value structure
 * ------------------------------------------------------------------------
 */
typedef struct {
	void       *value;
	nowdb_type_t type;
} nowdb_simple_value_t;

/* ------------------------------------------------------------------------
 * Initialise an already allocated dml helper
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_dml_init(nowdb_dml_t   *dml, 
                           nowdb_scope_t *scope,
                           char       withCache);

/* ------------------------------------------------------------------------
 * Destroy a dml helper
 * ------------------------------------------------------------------------
 */
void nowdb_dml_destroy(nowdb_dml_t *dml);

/* ------------------------------------------------------------------------
 * Set target according to target name
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_dml_setTarget(nowdb_dml_t *dml,
                                char    *trgname,
                          ts_algo_list_t *fields,
                          ts_algo_list_t *values);

/* ------------------------------------------------------------------------
 * Insert one row (in row format)
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_dml_insertRow(nowdb_dml_t *dml,
                                char        *row,
                                uint32_t     sz);

/* ------------------------------------------------------------------------
 * Insert one row (in fields/value format)
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_dml_insertFields(nowdb_dml_t *dml,
                             ts_algo_list_t *fields,
                             ts_algo_list_t *values);

/* delete and update need either a rowid or a pointer to the row */
#endif
