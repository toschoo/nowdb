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

typedef struct {
	void              *lib;    /* server lib    */
	nowdb_scope_t     *scope;  /* current scope */
	nowdbsql_parser_t *parser; /* string parser */
} nowdb_proc_t;

nowdb_err_t nowdb_proc_create(nowdb_proc_t **proc,
                              void          *lib,
                              nowdb_scope_t *scope);

void nowdb_proc_destroy(nowdb_proc_t *proc);

void nowdb_proc_setScope(nowdb_proc_t  *proc, 
                         nowdb_scope_t *scope);

// execute sql statement
// result
// cursor

#endif
