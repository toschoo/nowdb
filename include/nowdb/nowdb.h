/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Public interface
 * ========================================================================
 */
#ifndef NOWDB_DECL
#define NOWDB_DECL

// types should go to include
#include <nowdb/types/error.h>

#include <pthread.h>
typedef pthread_t nowdb_thread_t;

// structure to hold the opened scopes
typedef struct nowdb_t* nowdb_t;

// scopes currently used by a session
typedef struct nowdb_scope_t* nowdb_scope_t;

// result of a statement
typedef struct nowdb_result_t* nowdb_result_t;

// cursor
typedef struct nowdb_cursor_t* nowdb_cursor_t;

typedef struct nowdb_session_t* nowdb_session_t;

// init and close library
nowdb_err_t nowdb_library_init(nowdb_t *nowdb, char *base, int nthreads);
void nowdb_library_close(nowdb_t nowdb);
nowdb_err_t nowdb_library_shutdown(nowdb_t nowdb);

nowdb_err_t nowdb_getSession(nowdb_t lib,
                             nowdb_session_t *ses,
                             nowdb_thread_t master,           
                             int istream,
                             int ostream,
                             int estream);

// execute statement as string
nowdb_err_t nowdb_exec_statement(nowdb_t          nowdb,
                                 nowdb_scope_t    scope,
                                 char              *sql,
                                 nowdb_result_t *result);

// run a session everything is done internally
nowdb_err_t nowdb_session_run(nowdb_session_t  ses);

nowdb_err_t nowdb_session_stop(nowdb_session_t ses);

// destroy session
void nowdb_session_destroy(nowdb_session_t  ses);

// open close and fetch
nowdb_err_t nowdb_cursor_open(nowdb_cursor_t cursor);
void nowdb_cursor_close(nowdb_cursor_t cursor);
nowdb_err_t nowdb_cursor_fetch(nowdb_cursor_t   *cur,
                              char *buf, uint32_t sz,
                                       uint32_t *osz,
                                     uint32_t *count);
#endif

