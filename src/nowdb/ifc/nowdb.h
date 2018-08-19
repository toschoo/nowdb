/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Collection of scopes + threadpool
 * ========================================================================
 */
#ifndef nowdb_main_decl
#define nowdb_main_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/task/task.h>
#include <nowdb/task/lock.h>
#include <nowdb/scope/scope.h>
#include <nowdb/sql/parser.h>
#include <nowdb/query/cursor.h>

#include <tsalgo/tree.h>
#include <tsalgo/list.h>

typedef struct {
	char stype;
	char rtype;
        char ctype;
        int  opts;
} nowdb_ses_option_t;

#define NOWDB_SES_SQL 0
#define NOWDB_SES_LE  0
#define NOWDB_SES_TXT 1
#define NOWDB_SES_BE  2
#define NOWDB_SES_ACK 1
#define NOWDB_SES_NOACK 0

#define NOWDB_SES_TIMING 1

typedef struct {
	uint32_t curid;
	uint32_t off;
	uint32_t count;
	nowdb_cursor_t *cur;
} nowdb_ses_cursor_t;

typedef struct {
	nowdb_lock_t        *lock; /* protect the session                */
	void                 *lib; /* from where we get scopes           */
	nowdb_scope_t      *scope; /* on what we process                 */
	nowdb_task_t         task; /* this runs the session              */
	nowdb_task_t       master; /* the main thread                    */
	nowdbsql_parser_t *parser; /* the parser                         */
	nowdb_err_t           err; /* error                              */
	ts_algo_list_node_t *node; /* where to find us                   */
	FILE               *ifile; /* input  stream as file              */
	FILE               *ofile; /* output stream as file              */
	FILE               *efile; /* error  stream as file              */
	char                 *buf; /* result buffer                      */
	ts_algo_tree_t   *cursors; /* open cursors                       */
	uint32_t            bufsz; /* result buffer size                 */
	uint64_t            curid; /* next free cursorid                 */
	int               istream; /* incoming stream                    */
	int               ostream; /* outgoing stream (may be == istream */
	int               estream; /* error stream (may be == ostream    */
	char              running; /* running or waiting                 */
	char                 stop; /* terminate thread                   */
	char                alive; /* session alive                      */
        nowdb_ses_option_t    opt; /* session options                    */
} nowdb_session_t;

typedef struct {
	nowdb_rwlock_t     *lock;
	char               *base;
	ts_algo_tree_t   *scopes;
	ts_algo_list_t *fthreads;
	ts_algo_list_t *uthreads;
	int             nthreads;
} nowdb_t;

nowdb_err_t nowdb_library_init(nowdb_t **lib, char *base, int nthreads);
void nowdb_library_close(nowdb_t *lib);
nowdb_err_t nowdb_library_shutdown(nowdb_t *lib);

nowdb_err_t nowdb_getScope(nowdb_t *lib, char *name,
                           nowdb_scope_t    **scope);
nowdb_err_t nowdb_addScope(nowdb_t *lib, char *name,
                           nowdb_scope_t *scope); 

nowdb_err_t nowdb_getSession(nowdb_t *lib,
                             nowdb_session_t **ses,
                             nowdb_task_t master,
                             int istream,
                             int ostream,
                             int estream);

// run a session everything is done internally
nowdb_err_t nowdb_session_create(nowdb_session_t **ses,
                                 nowdb_t          *lib,
                                 nowdb_task_t   master,
                                 int           istream,
                                 int           ostream,
                                 int           estream);

// run a session everything is done internally
nowdb_err_t nowdb_session_run(nowdb_session_t *ses);

nowdb_err_t nowdb_session_stop(nowdb_session_t *ses);
nowdb_err_t nowdb_session_shutdown(nowdb_session_t *ses);

// destroy session
void nowdb_session_destroy(nowdb_session_t *ses);

// session entry point
void *nowdb_session_entry(void *session);

#endif
