/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Session
 * TODO: should use embtls
 * ========================================================================
 */
#include <nowdb/sql/ast.h>
#include <nowdb/sql/parser.h>
#include <nowdb/query/stmt.h>
#include <nowdb/query/cursor.h>
#include <nowdb/ifc/session.h>

// run a session everything is done internally
nowdb_err_t nowdb_session_create(nowdb_t *lib,
                                 int  istream,
                                 int  ostream,
                                 int  estream);

// run a session everything is done internally
nowdb_err_t nowdb_session_run(nowdb_session_t  ses);

// destroy session
void nowdb_session_destroy(nowdb_session_t  ses);
