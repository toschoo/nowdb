/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Stored Procedure Interface (internal)
 * ========================================================================
 */
#ifndef nowdb_proc_decl
#define nowdb_proc_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/scope/scope.h>
#include <nowdb/sql/parser.h>

#ifdef _NOWDB_WITH_PYTHON
#include NOWDB_INC_PYTHON
#endif

/* ------------------------------------------------------------------------
 * Stored procedure interface
 * ------------------------------------------------------------------------
 */
typedef struct {
	void              *lib;    /* server lib                */
	nowdb_scope_t     *scope;  /* current scope             */
	nowdbsql_parser_t *parser; /* string parser             */
#ifdef _NOWDB_WITH_PYTHON
	PyThreadState     *pyIntp; /* Python interpreter thread */
#endif
} nowdb_proc_t;

/* ------------------------------------------------------------------------
 * Create proc interface
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_proc_create(nowdb_proc_t **proc,
                              void          *lib,
                              nowdb_scope_t *scope);

/* ------------------------------------------------------------------------
 * Destroy proc interface
 * ------------------------------------------------------------------------
 */
void nowdb_proc_destroy(nowdb_proc_t *proc);

/* ------------------------------------------------------------------------
 * Set current scope to proc interface
 * ------------------------------------------------------------------------
 */
void nowdb_proc_setScope(nowdb_proc_t  *proc, 
                         nowdb_scope_t *scope);

/* ------------------------------------------------------------------------
 * Get scope from proc interface
 * ------------------------------------------------------------------------
 */
nowdb_scope_t *nowdb_proc_getScope(nowdb_proc_t *proc);

/* ------------------------------------------------------------------------
 * Get lib from proc interface
 * ------------------------------------------------------------------------
 */
void *nowdb_proc_getLib(nowdb_proc_t *proc);

/* ------------------------------------------------------------------------
 * Get PyThread from proc interface
 * ------------------------------------------------------------------------
 */
void *nowdb_proc_getInterpreter(nowdb_proc_t *proc);

/* ------------------------------------------------------------------------
 * Get PyThread from proc interface
 * ------------------------------------------------------------------------
 */
void nowdb_proc_updateInterpreter(nowdb_proc_t *proc, void *intp);

// execute sql statement
// result
// cursor


// execute sql statement
// result
// cursor

#endif
