/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Server interface (internal)
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
#include <nowdb/ifc/proc.h>

#include <tsalgo/tree.h>
#include <tsalgo/list.h>

/* ------------------------------------------------------------------------
 * session options
 * ------------------------------------------------------------------------
 */
typedef struct {
	char stype; /* session type (always SQL) */
	char rtype; /* return type (txt, le, be) */ 
        char ctype; /* ack option or not (never) */
        int  opts;  /* detailed options          */
} nowdb_ses_option_t;

#define NOWDB_SES_SQL 0
#define NOWDB_SES_LE  0
#define NOWDB_SES_TXT 1
#define NOWDB_SES_BE  2
#define NOWDB_SES_ACK 1
#define NOWDB_SES_NOACK 0

#define NOWDB_SES_TIMING 1

/* ------------------------------------------------------------------------
 * session cursor
 * ------------------------------------------------------------------------
 */
typedef struct {
	uint32_t curid;       /* unique cursor id      */
	uint64_t count;       /* total count of rows   */
	nowdb_cursor_t *cur;  /* internal cursor       */
} nowdb_ses_cursor_t;

/* ------------------------------------------------------------------------
 * session
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_lock_t        *lock; /* protect the session                 */
	void                 *lib; /* from where we get scopes            */
	nowdb_scope_t      *scope; /* on what we process                  */
	nowdb_task_t         task; /* this runs the session               */
	nowdb_task_t       master; /* the main thread                     */
	nowdbsql_parser_t *parser; /* the parser                          */
	nowdb_err_t           err; /* error                               */
	ts_algo_list_node_t *node; /* where to find us                    */
	char                 *buf; /* result buffer                       */
	ts_algo_tree_t   *cursors; /* open cursors                        */
	nowdb_proc_t        *proc; /* stored procedure interface          */
	uint64_t            curid; /* next free cursorid                  */
	uint32_t            bufsz; /* result buffer size                  */
	int               istream; /* incoming stream                     */
	int               ostream; /* outgoing stream (may be == istream) */
	int               estream; /* error stream (may be == ostream)    */
	char              running; /* running or waiting                  */
	char                 stop; /* terminate thread                    */
	char                alive; /* session alive                       */
        nowdb_ses_option_t    opt; /* session options                     */
} nowdb_session_t;

/* ------------------------------------------------------------------------
 * server library
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_rwlock_t     *lock; /* protect the library    */
	char               *base; /* base path              */
	ts_algo_tree_t   *scopes; /* tree of scope          */
	ts_algo_list_t *fthreads; /* list of free sessions  */
	ts_algo_list_t *uthreads; /* list of used sessions  */
	int             nthreads; /* max number of sessions */
	int               loglvl; /* max number of sessions */
} nowdb_t;

/* ------------------------------------------------------------------------
 * init library (called only once per process)
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_library_init(nowdb_t **lib, char *base,
                                int loglvl, int nthreads);

/* ------------------------------------------------------------------------
 * close library (called only once per process)
 * ------------------------------------------------------------------------
 */
void nowdb_library_close(nowdb_t *lib);

/* ------------------------------------------------------------------------
 * stop busy sessions
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_library_shutdown(nowdb_t *lib);

/* ------------------------------------------------------------------------
 * get scope from lib (used in nowdb_stmt_handle)
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_getScope(nowdb_t *lib, char *name,
                           nowdb_scope_t    **scope);

/* ------------------------------------------------------------------------
 * add scope to lib (used in nowdb_stmt_handle)
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_addScope(nowdb_t *lib, char *name,
                           nowdb_scope_t *scope); 

/* ------------------------------------------------------------------------
 * remove scope from lib (used in nowdb_stmt_handle)
 * ------------------------------------------------------------------------
 */
void nowdb_dropScope(nowdb_t *lib, char *name);

/* ------------------------------------------------------------------------
 * get a free session
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_getSession(nowdb_t *lib,
                             nowdb_session_t **ses,
                             nowdb_task_t master,
                             int istream,
                             int ostream,
                             int estream);

/* ------------------------------------------------------------------------
 * create a new session
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_session_create(nowdb_session_t **ses,
                                 nowdb_t          *lib,
                                 nowdb_task_t   master,
                                 int           istream,
                                 int           ostream,
                                 int           estream);

/* ------------------------------------------------------------------------
 * run session
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_session_run(nowdb_session_t *ses);

/* ------------------------------------------------------------------------
 * stop session
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_session_stop(nowdb_session_t *ses);

/* ------------------------------------------------------------------------
 * shutdown session (terminate thread)
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_session_shutdown(nowdb_session_t *ses);

/* ------------------------------------------------------------------------
 * destroy session object
 * ------------------------------------------------------------------------
 */
void nowdb_session_destroy(nowdb_session_t *ses);

/* ------------------------------------------------------------------------
 * session entry point
 * ------------------------------------------------------------------------
 */
void *nowdb_session_entry(void *session);

#endif
