/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Advanced testing with scopes
 * ========================================================================
 */
#ifndef COM_SCOPES_DECL
#define COM_SCOPES_DECL

#include <nowdb/scope/scope.h>
#include <nowdb/query/cursor.h>
#include <nowdb/io/dir.h>
#include <common/bench.h>

int initScopes();
void closeScopes();

int doesExistScope(nowdb_path_t path);

nowdb_scope_t *mkScope(nowdb_path_t path);

nowdb_scope_t *createScope(nowdb_path_t path);

int dropScope(nowdb_path_t path);

int openScope(nowdb_scope_t *scope);

int closeScope(nowdb_scope_t *scope);

int execStmt(nowdb_scope_t *scope, char *stmt);

nowdb_cursor_t *openCursor(nowdb_scope_t *scope, char *stmt);

void closeCursor(nowdb_cursor_t *cur);

int waitscope(nowdb_scope_t *scope, char *context);
#endif

