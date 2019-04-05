/* ========================================================================
 * (c) Tobias Schoofs, 2018 -- 2019
 * ========================================================================
 * Stored Procedure Interface (internal)
 * ========================================================================
 * This is the session interface towards stored procedures.
 * It is used by one session (never by more than one session in parallel).
 * It is threfore *not* threadsafe and must never used by
 * concurrent threads!!!
 * ========================================================================
 */
#ifndef nowdb_proc_decl
#define nowdb_proc_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/scope/scope.h>
#include <nowdb/scope/dml.h>
#include <nowdb/sql/parser.h>

#include <tsalgo/tree.h>

#include <lualib.h>
#include <lauxlib.h>

#ifdef _NOWDB_WITH_PYTHON
#include NOWDB_INC_PYTHON
#endif

/* ------------------------------------------------------------------------
 * Stored procedure interface
 * ------------------------------------------------------------------------
 */
typedef struct nowdb_proc_t {
	void              *lib;    /* server lib                */
	nowdb_scope_t     *scope;  /* current scope             */
	nowdb_dml_t       *dml;    /* dml helper                */
	nowdbsql_parser_t *parser; /* string parser             */
	ts_algo_tree_t    *mods;   /* imported modules          */
	ts_algo_tree_t    *funs;   /* loaded functions          */
#ifdef _NOWDB_WITH_PYTHON
	PyThreadState     *pyIntp; /* Python interpreter thread */
	PyObject          *nowmod; /* the 'nowdb' module        */
#endif
	char              *luapath; // path where lua code resides
	char              *nowpath; // path to nowdb.lua
	lua_State         *luaIntp; // Lua interpreter
	                            // caller
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
nowdb_err_t nowdb_proc_setScope(nowdb_proc_t  *proc, 
                                nowdb_scope_t *scope);

/* ------------------------------------------------------------------------
 * Reinit interpreter
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_proc_reinit(nowdb_proc_t *proc);

/* ------------------------------------------------------------------------
 * Get scope from proc interface
 * ------------------------------------------------------------------------
 */
nowdb_scope_t *nowdb_proc_getScope(nowdb_proc_t *proc);

/* ------------------------------------------------------------------------
 * Get DML helper
 * ------------------------------------------------------------------------
 */
nowdb_dml_t *nowdb_proc_getDML(nowdb_proc_t *proc);

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
 * Update interpreter
 * ------------------------------------------------------------------------
 */
void nowdb_proc_updateInterpreter(nowdb_proc_t *proc);

/* ------------------------------------------------------------------------
 * Load callable function (and the caller)
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_proc_loadFun(nowdb_proc_t     *proc,
                               char            *fname,
                               nowdb_proc_desc_t **pd,
                               void             **fun,
                               void            **call);

/* ------------------------------------------------------------------------
 * Call the function
 * ------------------------------------------------------------------------
 */
void *nowdb_proc_call(nowdb_proc_t *proc, void *call, void *fun, void *args);
#endif
