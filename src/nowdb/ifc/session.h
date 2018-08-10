/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Session
 * ========================================================================
 */
#ifndef nowdb_ses_decl
#define nowdb_ses_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/task/task.h>
#include <nowdb/scope/scope.h>
#include <nowdb/ifc/nowdb.h>

typedef struct {
	nowdb_t         *lib; /* from where we get scopes and threads */
	nowdb_scope_t *scope; /* on what we process                   */
	nowdb_task_t    task; /* this runs the session                */
	int          istream; /* incoming stream                      */
	int          ostream; /* outgoing stream (may be == istream   */
	int          estream; /* error stream (may be == ostream      */
} nowdb_session_t;

// run a session everything is done internally
nowdb_err_t nowdb_session_create(nowdb_t *lib,
                                 int  istream,
                                 int  ostream,
                                 int  estream);

// run a session everything is done internally
nowdb_err_t nowdb_session_run(nowdb_session_t  ses);

// destroy session
void nowdb_session_destroy(nowdb_session_t  ses);

#endif
