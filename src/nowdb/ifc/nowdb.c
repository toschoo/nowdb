/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Collection of scopes + threadpool
 * ========================================================================
 */
#include <nowdb/sql/ast.h>
#include <nowdb/query/stmt.h>
#include <nowdb/query/cursor.h>
#include <nowdb/ifc/nowdb.h>

#include <signal.h>
#include <pthread.h>

static char *OBJECT = "lib";

#define BUFSIZE 0x10000
#define MAXROW  0x1000
#define HDRSIZE 16

#define INVALID(s) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, s);

#define NOMEM(s) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, s);

#define MAKEPARSEERR() \
	err = nowdb_err_get(nowdb_err_parser, FALSE, OBJECT, \
	         (char*)nowdbsql_parser_errmsg(ses->parser));

#define LIB(x) \
	((nowdb_t*)x)

#define SETERR() \
	err->cause = ses->err; \
	ses->err = err;

#define INTERNAL(s) \
	if (err != NOWDB_OK) { \
		SETERR(); \
	} \
	err = nowdb_err_get(nowdb_err_server, FALSE, OBJECT, s);

#define LOGMSG(m) \
	if (LIB(ses->lib)->loglvl > 0) { \
		fprintf(stderr, "[%lu] %s\n", pthread_self(), m); \
	}

#define LOGERRMSG(err) \
	if (LIB(ses->lib)->loglvl > 0) { \
		nowdb_err_print(err); \
		nowdb_err_release(err); \
	}

/* -----------------------------------------------------------------------
 * Scope Descriptor
 * -----------------------------------------------------------------------
 */
typedef struct {
	char           *name;
	char           inuse;
	nowdb_scope_t *scope;
} scope_t;

#define SCOPE(x) \
	((scope_t*)x)

/* -----------------------------------------------------------------------
 * compare scope descriptors
 * -----------------------------------------------------------------------
 */
static ts_algo_cmp_t scopecompare(void *rsc, void *one, void *two) {
	char x;
	x = strcmp(SCOPE(one)->name, SCOPE(two)->name);
	if (x < 0) return ts_algo_cmp_less;
	if (x > 0) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* -----------------------------------------------------------------------
 * destroy scope descriptors
 * -----------------------------------------------------------------------
 */
static void scopedestroy(void *rsc, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	if (SCOPE(*n)->name != NULL) {
		free(SCOPE(*n)->name);
	}
	if (SCOPE(*n)->scope != NULL) {
 		/* close them explicitly ! */
		NOWDB_IGNORE(nowdb_scope_close(SCOPE(*n)->scope));
		nowdb_scope_destroy(SCOPE(*n)->scope);
		free(SCOPE(*n)->scope);
	}
	free(*n); *n=NULL;
}

/* -----------------------------------------------------------------------
 * generic no update
 * -----------------------------------------------------------------------
 */
static ts_algo_rc_t noupdate(void *rsc, void *o, void *n) {
	return TS_ALGO_OK;
}

#define CUR(x) \
	((nowdb_ses_cursor_t*)x)

/* -----------------------------------------------------------------------
 * compare cursors
 * -----------------------------------------------------------------------
 */
static ts_algo_cmp_t cursorcompare(void *rsc, void *one, void *two) {
	if (CUR(one)->curid < CUR(two)->curid) return ts_algo_cmp_less;
	if (CUR(one)->curid > CUR(two)->curid) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* -----------------------------------------------------------------------
 * destroy cursor
 * -----------------------------------------------------------------------
 */
static void cursordestroy(void *rsc, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	if (CUR(*n)->cur != NULL) {
		nowdb_cursor_destroy(CUR(*n)->cur);
		free(CUR(*n)->cur); CUR(*n)->cur = NULL;
	}
	free(*n); *n = NULL;
}

/* ------------------------------------------------------------------------
 * generic read
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t readN(int sock, char *buf, int sz) {
	size_t t=0;
	size_t x;

	while(t<sz) {
		x = read(sock, buf+t, sz-t);
       		if (x <= 0) {
			return nowdb_err_get(nowdb_err_read, TRUE, OBJECT,
			                            "cannot read socket");
		}
		t+=x;
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * initialise library
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_library_init(nowdb_t **lib, char *base,
                                int loglvl, int nthreads,
                                uint64_t flags) {
	nowdb_err_t err;
	sigset_t s;
	int x;

	nowdb_init();

	sigemptyset(&s);
        sigaddset(&s, SIGUSR1);
        sigaddset(&s, SIGUSR2);
        sigaddset(&s, SIGINT);
        sigaddset(&s, SIGABRT);

	x = pthread_sigmask(SIG_SETMASK, &s, NULL);
	if (x != 0) {
		return nowdb_err_getRC(nowdb_err_sigset, x,
		                         OBJECT, "setmask");
	}

	if (lib == NULL) INVALID("library pointer is NULL");
	if (base == NULL) INVALID("base is NULL");
	if (nthreads < 0) INVALID("negative number of threads not supported");

	*lib = calloc(1, sizeof(nowdb_t));
	if (*lib == NULL) {
		NOMEM("allocating lib");
		return err;
	}

	(*lib)->nthreads = nthreads;
	(*lib)->loglvl   = loglvl;

	(*lib)->lock = calloc(1, sizeof(nowdb_rwlock_t));
	if ((*lib)->lock == NULL) {
		NOMEM("allocating lock");
		free(*lib); *lib = NULL;
		return err;
	}
	err = nowdb_rwlock_init((*lib)->lock);
	if (err != NOWDB_OK) {
		free((*lib)->lock); (*lib)->lock = NULL;
		nowdb_library_close(*lib); *lib = NULL;
		return err;
	}
	(*lib)->base = strdup(base);
	if ((*lib)->base == NULL) {
		NOMEM("allocating base");
		nowdb_library_close(*lib); *lib = NULL;
		return err;
	}
	(*lib)->fthreads = calloc(1,sizeof(ts_algo_list_t));
	if ((*lib)->fthreads == NULL) {
		NOMEM("allocating list");
		nowdb_library_close(*lib); *lib = NULL;
		return err;
	}
	ts_algo_list_init((*lib)->fthreads);
	(*lib)->uthreads = calloc(1,sizeof(ts_algo_list_t));
	if ((*lib)->uthreads == NULL) {
		NOMEM("allocating list");
		nowdb_library_close(*lib); *lib = NULL;
		return err;
	}
	ts_algo_list_init((*lib)->uthreads);

	(*lib)->scopes = ts_algo_tree_new(&scopecompare, NULL,
	                                  &noupdate,
	                                  &scopedestroy,
	                                  &scopedestroy);

	if ((*lib)->scopes == NULL) {
		nowdb_library_close(*lib); *lib = NULL;
		NOMEM("tree.new");
		return err;
	}

#ifdef _NOWDB_WITH_PYTHON
	if (flags & NOWDB_ENABLE_PYTHON) {
		(*lib)->pyEnabled = 1;

		Py_InitializeEx(0); // init python interpreter
		// Py_Initialize(); // init python interpreter
		PyEval_InitThreads(); // init threads
		(*lib)->mst = PyEval_SaveThread(); // save thread state
	}
#endif
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * signal all ongoing sessions
 * -----------------------------------------------------------------------
 */
static nowdb_err_t signalSessions(nowdb_t *lib, int what) {
	ts_algo_list_t *list;
	ts_algo_list_node_t *runner;
	nowdb_session_t *ses;
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;

	err = nowdb_lock_write(lib->lock);
	if (err != NOWDB_OK) return err;

	list=what==1?lib->uthreads:lib->fthreads;
	for(runner=list->head;runner!=NULL;runner=runner->nxt) {
		ses = runner->cont;
		switch(what) {
		case 1: err = nowdb_session_stop(ses); break;
		case 2: err = nowdb_session_shutdown(ses); break;
		} 
		if (err != NOWDB_OK) {
			LOGMSG("cannot stop session\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			break;
		}
		/*
		if (what == 2) {
			// LOGMSG("removing session\n");
			ts_algo_list_remove(list, runner);
		}
		*/
	}
	err2 = nowdb_unlock_write(lib->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* -----------------------------------------------------------------------
 * Terminate all living sessions
 * -----------------------------------------------------------------------
 */
static nowdb_err_t killSessions(nowdb_t *lib) {
	return signalSessions(lib, 2);
}

/* -----------------------------------------------------------------------
 * Just stop all ongoing sessions
 * -----------------------------------------------------------------------
 */
static nowdb_err_t stopSessions(nowdb_t *lib) {
	return signalSessions(lib, 1);
}

/* -----------------------------------------------------------------------
 * Wait until there are not more sessions ongoing
 * -----------------------------------------------------------------------
 */
#define DELAY 10000000l
static nowdb_err_t waitSessions(nowdb_t *lib, int what) {
	ts_algo_list_t *list;
	nowdb_err_t err=NOWDB_OK;
	int x, i=9;
	int t=0;

	list = what==1?lib->uthreads:lib->fthreads;
	for(;;) {
		err = nowdb_lock_read(lib->lock);
		if (err != NOWDB_OK) return err;

		x = list->len;
		t = list->head != NULL;

		err = nowdb_unlock_read(lib->lock);
		if (err != NOWDB_OK) return err;
		
		if (x <= 0) break;
		if (t == 0) break;

		fprintf(stderr, "waiting %d: %d (%d)\n", what, x, t);

		i--;

		if (i==0) {
			if (what == 1) {
				err = stopSessions(lib);
				if (err != NOWDB_OK) return err;
			} else {
				err = killSessions(lib);
				if (err != NOWDB_OK) return err;
			}
			i=9;
		}

		err = nowdb_task_sleep(DELAY);
		if (err != NOWDB_OK) return err;
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Destroy all sessions
 * -----------------------------------------------------------------------
 */
static void destroySessionList(ts_algo_list_t *list) {
	ts_algo_list_node_t *runner, *tmp;
	nowdb_session_t *ses;

	runner=list->head;
	while(runner!=NULL) {
		ses = runner->cont;
		nowdb_session_destroy(ses); free(ses);
		tmp = runner->nxt;
		ts_algo_list_remove(list, runner);
		free(runner); runner = tmp;
	}
}

/* -----------------------------------------------------------------------
 * Close library
 * -----------------------------------------------------------------------
 */
void nowdb_library_close(nowdb_t *lib) {
	if (lib == NULL) {
		nowdb_close(); return;
	}
	if (lib->uthreads != NULL) {
		destroySessionList(lib->uthreads);
		free(lib->uthreads); lib->uthreads = NULL;
	}
	if (lib->lock != NULL) {
		NOWDB_IGNORE(killSessions(lib));
		// NOWDB_IGNORE(waitSessions(lib,2));
		NOWDB_IGNORE(nowdb_lock_write(lib->lock));
	}
	if (lib->fthreads != NULL) {
		fprintf(stderr, "destroy free threads\n");
		destroySessionList(lib->fthreads);
		free(lib->fthreads); lib->fthreads = NULL;
	}
	if (lib->lock != NULL) {
		NOWDB_IGNORE(nowdb_unlock_write(lib->lock));
	}
	if (lib->scopes != NULL) {
		ts_algo_tree_destroy(lib->scopes);
		free(lib->scopes); lib->scopes = NULL;
	}
	if (lib->base != NULL) {
		free(lib->base); lib->base = NULL;
	}
	if (lib->lock != NULL) {
		nowdb_rwlock_destroy(lib->lock);
		free(lib->lock); lib->lock = NULL;
	}
#ifdef _NOWDB_WITH_PYTHON
	if (lib->pyEnabled && lib->mst != NULL) {
		fprintf(stderr, "finalizing python\n");

		PyEval_RestoreThread(lib->mst);
	
		lib->mst = NULL;
		
		Py_Finalize();

		/*
		if (Py_FinalizeEx() != 0) {
			fprintf(stderr, "Python finalizer failed:\n");
			PyErr_Print();
		}
		*/
	}
#endif
	free(lib);
	nowdb_close();
}

/* -----------------------------------------------------------------------
 * shutdown library (i.e. stop all ongoing sessions)
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_library_shutdown(nowdb_t *lib) {
	nowdb_err_t err;

	if (lib == NULL) INVALID("lib is NULL");
	if (lib->lock == NULL) INVALID("lib is not open");

	/* for soft shutdown:
	 * wait until all sessions have terminated.
	 * for hard shudown, we need a mechanism to
	 * stop all sessions 
	err = stopSessions(lib);
	if (err != NOWDB_OK) return err;
	 */

	err = waitSessions(lib, 1);
	if (err != NOWDB_OK) return err;

	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * find scope
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_getScope(nowdb_t *lib, char *name,
                           nowdb_scope_t    **scope) {
	scope_t pattern, *result;

	if (lib == NULL) INVALID("lib is NULL");
	if (name == NULL) INVALID("name is NULL");
	if (scope == NULL) INVALID("scope pointer is NULL");

	*scope = NULL;
	pattern.name = name;
	result = ts_algo_tree_find(lib->scopes, &pattern);
	if (result != NULL) *scope = result->scope;

	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * add scope
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_addScope(nowdb_t *lib, char *name,
                           nowdb_scope_t *scope) {
	nowdb_err_t err;
	scope_t *desc;

	desc = calloc(1,sizeof(scope_t));
	if (desc == NULL) {
		NOMEM("allocating scope descriptor");
		return err;
	}
	desc->name = strdup(name);
	if (desc->name == NULL) {
		NOMEM("allocating name");
		free(desc); return err;
	}
	desc->scope = scope;

	if (ts_algo_tree_insert(lib->scopes, desc) != TS_ALGO_OK) {
		NOMEM("tree.insert");
		free(desc->name); free(desc);
		return err;
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * drop scope
 * -----------------------------------------------------------------------
 */
void nowdb_dropScope(nowdb_t *lib, char *name) {
	scope_t pattern;
	pattern.name = name;
	ts_algo_tree_delete(lib->scopes, &pattern);
}

/* -----------------------------------------------------------------------
 * prepare session object for next round
 * -----------------------------------------------------------------------
 */
static nowdb_err_t initSession(nowdb_session_t  *ses,
                               nowdb_task_t   master,
                               int           istream,
                               int           ostream,
                               int           estream) 
{
	nowdb_err_t err;

	ses->err     = NOWDB_OK;
	ses->scope   = NULL;
	ses->istream = istream;
	ses->ostream = ostream;
	ses->estream = estream;
	ses->running = 0;
	ses->stop    = 0;
	ses->alive   = 1;
	ses->master  = master;
	ses->curid   = 0x100;

	if (ses->cursors != NULL) {
		ts_algo_tree_destroy(ses->cursors);
		free(ses->cursors); ses->cursors = NULL;
	}
	ses->cursors = ts_algo_tree_new(&cursorcompare, NULL,
                                        &noupdate,
	                                &cursordestroy,
	                                &cursordestroy);
	if (ses->cursors == NULL) {
		NOMEM("tree.alloc");
		return err;
	}
	if (ses->parser != NULL) {
		nowdbsql_parser_destroy(ses->parser); free(ses->parser);
	}
	ses->parser = calloc(1,sizeof(nowdbsql_parser_t));
	if (ses->parser == NULL) {
		NOMEM("allocating parser");
		return err;
	}
	if (nowdbsql_parser_initStreaming(ses->parser, ses->istream) != 0) {
		err = nowdb_err_get(nowdb_err_parser, FALSE, OBJECT,
		                              "cannot init parser");
		free(ses->parser); ses->parser = NULL;
		return err;
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * create a new session
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_session_create(nowdb_session_t **ses,
                                 nowdb_t          *lib,
                                 nowdb_task_t   master,
                                 int           istream,
                                 int           ostream,
                                 int           estream) {
	nowdb_err_t err;

	*ses = calloc(1,sizeof(nowdb_session_t));
	if (*ses == NULL) {
		NOMEM("allocating session");
		return err;
	}
	(*ses)->lib = lib;
	(*ses)->node = NULL;

	(*ses)->lock = calloc(1, sizeof(nowdb_lock_t));
	if ((*ses)->lock == NULL) {
		NOMEM("allocating lock");
		free(*ses); *ses = NULL;
		return err;
	}
	err = nowdb_lock_init((*ses)->lock);
	if (err != NOWDB_OK) {
		free((*ses)->lock); (*ses)->lock = NULL;
		free(*ses); *ses = NULL;
		return err;
	}

	(*ses)->buf = malloc(BUFSIZE);
	if ((*ses)->buf == NULL) {
		NOMEM("allocating buffer");
		nowdb_session_destroy(*ses);
		free(*ses); *ses = NULL;
		return err;
	}
	(*ses)->bufsz = BUFSIZE;

	err = nowdb_proc_create(&(*ses)->proc, lib, NULL);
	if (err != NOWDB_OK) {
		nowdb_session_destroy(*ses);
		free(*ses); *ses = NULL;
		return err;
	}

	err = initSession(*ses, master, istream, ostream, estream);
	if (err != NOWDB_OK) {
		nowdb_session_destroy(*ses);
		free(*ses); *ses = NULL;
		return err;
	}

	err = nowdb_task_create(&(*ses)->task, &nowdb_session_entry, *ses);
	if (err != NOWDB_OK) {
		nowdb_session_destroy(*ses);
		free(*ses); *ses = NULL;
		return err;
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * add a list node to a list (should be in tsalgo)
 * -----------------------------------------------------------------------
 */
static void addNode(ts_algo_list_t      *list,
                    ts_algo_list_node_t *node) {

	node->nxt = list->head;
	if (node->nxt != NULL) node->nxt->prv = node;
	list->head = node;
	list->len++;
}

/* -----------------------------------------------------------------------
 * get a session (either from freelist or newly created)
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_getSession(nowdb_t *lib,
                    nowdb_session_t **ses,
                      nowdb_task_t master,
                              int istream,
                              int ostream,
                              int estream) {
	ts_algo_list_node_t *n;
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;

	err = nowdb_lock_write(lib->lock);
	if (err != NULL) return err;

	// some housekeeping
	for(;;) {
		n = lib->fthreads->head;
		if (n == NULL) break;

		*ses = n->cont;

		if ((*ses)->alive == 0) {
			ts_algo_list_remove(lib->fthreads, n);
			nowdb_session_destroy(*ses);
			free(*ses); *ses = NULL;
			free(n); continue;
		}
		break;
	}

	// shall we allow ad-hoc threads?
	// shall we enforce a limit on living threads?
	if (lib->fthreads->len == 0) {
		if (lib->nthreads > 0 &&
		    lib->nthreads < lib->uthreads->len) {
			err = nowdb_err_get(nowdb_err_no_rsc,
			  FALSE, OBJECT, "empty thread pool");
			goto unlock;
		}
		err = nowdb_session_create(ses, lib, master,
		                  istream, ostream, estream);
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot create session\n");
			goto unlock;
		}

		n = malloc(sizeof(ts_algo_list_node_t));
		if (n == NULL) {
			NOMEM("allocating list node");
			nowdb_session_destroy(*ses);
			free(*ses); *ses = NULL;
			goto unlock;
		}
		n->cont = *ses;
		(*ses)->node = n;
		addNode(lib->uthreads, n);
		goto unlock;
	}

	// init session
	err = initSession(*ses, master, istream, ostream, estream);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot init session\n");
		(*ses)->alive = 0;
		*ses = NULL;
		goto unlock;
	}

	ts_algo_list_remove(lib->fthreads, n);
	addNode(lib->uthreads, n);

unlock:
	err2 = nowdb_unlock_write(lib->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

/* -----------------------------------------------------------------------
 * signal the master of all threads (to inform him that one session
 * is not running anymore)
 * -----------------------------------------------------------------------
 */
static nowdb_err_t signalMaster(nowdb_session_t *ses) {
	int x;

	if (ses->master == 0) return NOWDB_OK;
	x = pthread_kill(ses->master, SIGUSR1);
	if (x != 0) {
		return nowdb_err_getRC(nowdb_err_signal, x, OBJECT, NULL);
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * signal a session (to stop or start it)
 * -----------------------------------------------------------------------
 */
static nowdb_err_t signalSession(nowdb_session_t *ses) {
	int x;

	x = pthread_kill(ses->task, SIGUSR1);
	if (x != 0) {
		return nowdb_err_getRC(nowdb_err_signal, x, OBJECT, NULL);
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * get stop state
 * -----------------------------------------------------------------------
 */
static int getStop(nowdb_session_t *ses) {
	nowdb_err_t err;
	int stop;

	err = nowdb_lock(ses->lock);
	if (err != NOWDB_OK) {
		SETERR();
		return -1;
	}
	stop = ses->stop;

	err = nowdb_unlock(ses->lock);
	if (err != NOWDB_OK) {
		SETERR();
		return -1;
	}
	return stop;
}

/* -----------------------------------------------------------------------
 * set stop state:
 * 1: stop
 * 2: shutdown
 * -----------------------------------------------------------------------
 */
static int setStop(nowdb_session_t *ses, int stop) {
	nowdb_err_t err;

	err = nowdb_lock(ses->lock);
	if (err != NOWDB_OK) {
		SETERR();
		return -1;
	}

	if (ses->stop != 2) ses->stop = stop;

	err = nowdb_unlock(ses->lock);
	if (err != NOWDB_OK) {
		SETERR();
		return -1;
	}
	return 0;
}

/* -----------------------------------------------------------------------
 * set session 'waiting'
 * -----------------------------------------------------------------------
 */
static int setWaiting(nowdb_session_t *ses) {
	nowdb_err_t err;

	err = nowdb_lock(ses->lock);
	if (err != NOWDB_OK) {
		SETERR();
		return -1;
	}

	ses->stop = 0;
	ses->running = 0;

	err = nowdb_unlock(ses->lock);
	if (err != NOWDB_OK) {
		SETERR();
		return -1;
	}
	return 0;
}

/* -----------------------------------------------------------------------
 * start session: send signal
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_session_run(nowdb_session_t *ses) {
	if (ses == NULL) INVALID("session is NULL");
	if (ses->alive == 0) INVALID("session is dead");
	return signalSession(ses);
}

/* -----------------------------------------------------------------------
 * stop session: 
 * - set stop state (1)
 * - send signal
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_session_stop(nowdb_session_t *ses) {
	nowdb_err_t err;

	if (ses == NULL) INVALID("session is NULL");
	if (ses->alive == 0) return NOWDB_OK;

	if (setStop(ses, 1) != 0) {
		ses->alive = 0;
		return ses->err;
	}
	err = signalSession(ses);
	if (err != NOWDB_OK) {
		ses->alive = 0;
		return err;
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * shutdown session: 
 * - set stop state (2)
 * - send signal
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_session_shutdown(nowdb_session_t *ses) {
	nowdb_err_t err;

	if (ses == NULL) INVALID("session is NULL");

	if (ses->alive) {
		if (setStop(ses, 2) != 0) {
			ses->alive = 0;
			return ses->err;
		}
		err = signalSession(ses);
		if (err != NOWDB_OK) { 
			ses->alive = 0;
			return err;
		}
	}
	err = nowdb_task_join(ses->task);
	if (err != NOWDB_OK) { // what to do?
		LOGERRMSG(err);
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * destroy session
 * -----------------------------------------------------------------------
 */
void nowdb_session_destroy(nowdb_session_t *ses) {
	if (ses == NULL) return;
	if (ses->buf != NULL) {
		free(ses->buf); ses->buf = NULL;
	}
	if (ses->parser != NULL) {
		nowdbsql_parser_destroy(ses->parser);
		free(ses->parser); ses->parser = NULL;
	}
	if (ses->cursors != NULL) {
		ts_algo_tree_destroy(ses->cursors);
		free(ses->cursors); ses->cursors = NULL;
	}
	if (ses->proc != NULL) {
		nowdb_proc_destroy(ses->proc);
		free(ses->proc); ses->proc = NULL;
	}
	if (ses->lock != NULL) {
		nowdb_lock_destroy(ses->lock);
		free(ses->lock); ses->lock = NULL;
	}
}

/* -----------------------------------------------------------------------
 * send status: OK
 * -----------------------------------------------------------------------
 */
static int sendOK(nowdb_session_t *ses) {
	char status[2];
	nowdb_err_t err;

	status[0] = NOWDB_STATUS;
	status[1] = NOWDB_ACK;

	if (write(ses->ostream, status, 2) != 2) {
		err = nowdb_err_get(nowdb_err_write,
		        TRUE, OBJECT, "writing ACK");
		SETERR();
		return -1;
	}
	LOGMSG("OK");
	return 0;
}

/* -----------------------------------------------------------------------
 * send status: EOF
 * -----------------------------------------------------------------------
 */
static int sendEOF(nowdb_session_t *ses) {
	short errcode = (short)nowdb_err_eof;
	nowdb_err_t err;
	char status[4];

	status[0] = NOWDB_STATUS;
	status[1] = NOWDB_NOK;
	memcpy(status+2, &errcode, 2);

	// send NOK
	if (write(ses->ostream, status, 4) != 4) {
		err = nowdb_err_get(nowdb_err_write,
		     TRUE, OBJECT, "writing status");
		SETERR();
		return -1;
	}
	LOGMSG("EOF");
	return 0;
}

/* -----------------------------------------------------------------------
 * send status: NOK
 * -----------------------------------------------------------------------
 */
#define CLEANUP() \
	if (cause != NOWDB_OK) nowdb_err_release(cause); \
	if (freetmp) free(tmp);

static int sendErr(nowdb_session_t *ses,
                   nowdb_err_t    cause,
                   nowdb_ast_t     *ast) {
	char *tmp="unknown";
	int sz=0;
	char* status = ses->buf;
	char freetmp=0;
	short errcode;
	nowdb_err_t err;

	// send NOK
	status[0] = NOWDB_STATUS;
	status[1] = NOWDB_NOK;

	errcode = cause == NOWDB_OK?nowdb_err_unknown:cause->errcode;
	memcpy(status+2, &errcode, 2);

	// build error report
	if (cause != NOWDB_OK) {
		tmp = nowdb_err_describe(cause, ';');
		if (tmp == NULL) tmp = "out-of-mem";
		else freetmp = 1;
	}

	// length of report
	sz = strlen(tmp);
	memcpy(status+4, &sz, 4);

	// error report
	memcpy(status+8, tmp, sz);

	// send error report
	if (write(ses->ostream, status, sz+8) != sz+8) {
		err = nowdb_err_get(nowdb_err_write,
		      TRUE, OBJECT, "writing error");
		SETERR();
		CLEANUP();
		return -1;
	}
	CLEANUP();
	LOGMSG("ERR");
	return 0;
}

/* -----------------------------------------------------------------------
 * send report
 * -----------------------------------------------------------------------
 */
static int sendReport(nowdb_session_t *ses, nowdb_qry_result_t *res) {
	nowdb_err_t err;
	nowdb_qry_report_t *rep;
	char *status = ses->buf;

	if (res->result == NULL) {
		err = nowdb_err_get(nowdb_err_write, FALSE, OBJECT,
		                             "no report available");
		SETERR();
		return -1;
	}

	rep = res->result;

	status[0] = NOWDB_REPORT;
	status[1] = NOWDB_ACK;

	memcpy(status+2, &rep->affected, 8);
	memcpy(status+10, &rep->errors, 8);
	memcpy(status+18, &rep->runtime, 8);

	if (write(ses->ostream, status, 26) != 26) {
		err = nowdb_err_get(nowdb_err_write,
		     TRUE, OBJECT, "writing Report");
		SETERR();
		free(res->result); res->result = NULL;
		return -1;
	}
	free(res->result); res->result = NULL;
	LOGMSG("REPORT");
	return 0;
}

/* -----------------------------------------------------------------------
 * send cursor and rows
 * -----------------------------------------------------------------------
 */
static int sendRow(nowdb_session_t    *ses,
                   nowdb_qry_result_t *res) {
	nowdb_err_t err;
	char *status=ses->buf;
	nowdb_qry_row_t *row;

	status[0] = NOWDB_ROW;
	status[1] = NOWDB_ACK;

	row = res->result;

	memcpy(ses->buf+2, &row->sz, 4);
	memcpy(ses->buf+6, row->row, row->sz);

	if (write(ses->ostream, ses->buf, row->sz+6) != row->sz+6) {
		err = nowdb_err_get(nowdb_err_write, TRUE, OBJECT,
			                         "writing cursor");
		SETERR();
		return -1;
	}
	free(row->row);
	free(res->result); res->result = NULL;
	LOGMSG("ROW");
	return 0;
}

/* -----------------------------------------------------------------------
 * send cursor and rows
 * -----------------------------------------------------------------------
 */
static int sendCursor(nowdb_session_t   *ses,
                      uint64_t         curid,
                      char *buf, uint32_t sz) {
	nowdb_err_t err;
	char *status=buf+2;

	status[0] = NOWDB_CURSOR;
	status[1] = NOWDB_ACK;

	memcpy(status+2, &curid, 8);
	memcpy(status+10, &sz, 4);

	if (write(ses->ostream, buf+2, sz+14) != sz+14) {
		err = nowdb_err_get(nowdb_err_write, TRUE, OBJECT,
			                         "writing cursor");
		SETERR();
		return -1;
	}
	LOGMSG("CURSOR");
	return 0;
}

/* -----------------------------------------------------------------------
 * open cursor
 * -----------------------------------------------------------------------
 */
static int openCursor(nowdb_session_t *ses, nowdb_cursor_t *cur) {
	uint32_t osz;
	uint32_t cnt;
	nowdb_err_t err=NOWDB_OK;
	char *buf = ses->buf+HDRSIZE;
	uint32_t sz = ses->bufsz-HDRSIZE;
	nowdb_ses_cursor_t *scur;

	// open nowdb cursor
	err = nowdb_cursor_open(cur);
	if (err != NOWDB_OK) {
		if (err->errcode == nowdb_err_eof) {
			nowdb_err_release(err);
			if (sendEOF(ses) != 0) goto cleanup;
			return 0; // OK
		}
		// internal error
		INTERNAL("open cursor");
		if (sendErr(ses, err, NULL) != 0) {
			goto cleanup;
		}
		goto cleanup;
	}

	// the reason for this loop is a bug in row.project
	// for vertices. we should fix that instead of leaving
	// the otherwise meaningless loop here!
	do { 
		err = nowdb_cursor_fetch(cur, buf, sz, &osz, &cnt);
		if (err != NOWDB_OK) {
			if (err->errcode == nowdb_err_eof) {
				nowdb_err_release(err);
				if (osz == 0) {
					if (sendEOF(ses) != 0) goto cleanup;
				}
			} else {
				// internal error
				INTERNAL("fetching first row");
				sendErr(ses, err, NULL);
				goto cleanup;
			}
		}
	} while(osz==0);

	// insert cursor into tree
	ses->curid++;
	scur = calloc(1,sizeof(nowdb_ses_cursor_t));
	if (scur == NULL) {
		NOMEM("allocating cursor");
		INTERNAL("creating cursor");
		sendErr(ses, err, NULL);
		goto cleanup;
	}
	scur->curid = ses->curid;
	scur->count = cnt;
	scur->cur = cur;
	if (ts_algo_tree_insert(ses->cursors, scur) != TS_ALGO_OK) {
		NOMEM("tree.insert");
		INTERNAL("creating cursor");
		sendErr(ses, err, NULL);
		free(scur); goto cleanup;
	}

	// send to client
	if (sendCursor(ses, scur->curid, ses->buf, osz) != 0) {
		ts_algo_tree_delete(ses->cursors, scur);
		INTERNAL("sending results from cursor");
		sendErr(ses, err, NULL);
		return -1;
	}
	return 0;

cleanup:
	nowdb_cursor_destroy(cur); free(cur);
	if (ses->err != NOWDB_OK) return -1;
	return 0;
}

/* -----------------------------------------------------------------------
 * fetch cursor
 * -----------------------------------------------------------------------
 */
static int fetch(nowdb_session_t    *ses,
                 nowdb_ses_cursor_t *scur) 
{
	nowdb_err_t err;
	uint32_t osz;
	uint32_t cnt=0;
	char *buf = ses->buf+HDRSIZE;
	uint32_t sz = ses->bufsz-HDRSIZE;

	// already at eof
	if (nowdb_cursor_eof(scur->cur)) return sendEOF(ses);

	// fetch
	err = nowdb_cursor_fetch(scur->cur, buf, sz, &osz, &cnt);
	if (err != NOWDB_OK) {
		if (err->errcode == nowdb_err_eof) {
			nowdb_err_release(err);
			if (osz == 0) return sendEOF(ses);
		} else {
			INTERNAL("fetching cursor");
			sendErr(ses, err, NULL);
			return -1;
		}
	}
	scur->count += cnt;

	// debugging...
	if (osz < 9) {
		fprintf(stderr, "fetched: %u / %lu\n",
		                    osz, scur->count);
	}

	// send to client
	if (sendCursor(ses, scur->curid, ses->buf, osz) != 0) {
		INTERNAL("sending results from cursor");
		sendErr(ses, err, NULL);
		return -1;
	}
	return 0;
}

/* -----------------------------------------------------------------------
 * Special cases fetch and close cursor
 * -----------------------------------------------------------------------
 */
static int handleOp(nowdb_session_t *ses, nowdb_ast_t *ast) {
	char *tmp;
	nowdb_err_t err;
	nowdb_ses_cursor_t pattern;
	nowdb_ses_cursor_t *cur;

	// get curid
	pattern.curid = strtoul(ast->value, &tmp, 10);
	if (*tmp != 0) {
		err = nowdb_err_get(nowdb_err_invalid,
		  FALSE, OBJECT, "not a valid number");
		if (sendErr(ses, err, NULL) != 0) return -1;
		return 0;
	}

	switch(ast->ntype) {
	case NOWDB_AST_FETCH:
		LOGMSG("FETCHING CURSOR");
		cur = ts_algo_tree_find(ses->cursors, &pattern);
		if (cur == NULL) {
			err = nowdb_err_get(nowdb_err_invalid,
			  FALSE, OBJECT, "not an open cursor");
			if (sendErr(ses, err, NULL) != 0) return -1;
			return 0;
		}
		return fetch(ses, cur);
	
	case NOWDB_AST_CLOSE:
		LOGMSG("CLOSING CURSOR");
		ts_algo_tree_delete(ses->cursors, &pattern); 
		return sendOK(ses);

	default:
		err = nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                                 "unknown operation");
		return sendErr(ses, err, NULL);
	}
}

/* -----------------------------------------------------------------------
 * handle ast
 * -----------------------------------------------------------------------
 */
static int handleAst(nowdb_session_t *ses, nowdb_ast_t *ast) {
	nowdb_err_t err=NOWDB_OK;
	nowdb_qry_result_t res;
	char *path = LIB(ses->lib)->base;

	// handle the ast
	// it would be nice if we could distinguish between
	// internal errors and errors specific to this query
	// unfortunately, we cannot; so we may leak internals
	err = nowdb_stmt_handle(ast, ses->scope, ses->proc, path, &res);
	if (err != NOWDB_OK) return sendErr(ses, err, ast);

	switch(res.resType) {
	case NOWDB_QRY_RESULT_NOTHING: return sendOK(ses);

	case NOWDB_QRY_RESULT_REPORT: return sendReport(ses, &res);

	case NOWDB_QRY_RESULT_SCOPE:
		ses->scope = res.result;
		err = nowdb_proc_setScope(ses->proc, ses->scope);
		if (err != NOWDB_OK) return sendErr(ses, err, ast);
		return sendOK(ses);

	case NOWDB_QRY_RESULT_ROW:
		return sendRow(ses, &res);

	case NOWDB_QRY_RESULT_CURSOR:
		return openCursor(ses, res.result);

	case NOWDB_QRY_RESULT_OP:
		return handleOp(ses, res.result);

	default:
		err = nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                                    "unknown result");
		INTERNAL("unexpected result");
		sendErr(ses, err, NULL);
		return -1;
	}
	return 0;
}

/* -----------------------------------------------------------------------
 * negotiate session properties
 * -----------------------------------------------------------------------
 */
static nowdb_err_t negotiate(nowdb_session_t *ses) {
	nowdb_err_t err;
	char buf[8];

        err = readN(ses->istream, buf, 8);
	if (err != NOWDB_OK) return err;
	
	if (buf[0] != 'S') goto sql_error;
	if (buf[1] != 'Q') goto sql_error;
	if (buf[2] != 'L') goto sql_error;

	ses->opt.stype = NOWDB_SES_SQL;
	ses->opt.opts = 0;

	if (buf[3] == 'L' && buf[4] == 'E') {
		ses->opt.rtype = NOWDB_SES_LE;

	} else if (buf[3] == 'B' && buf[4] == 'E') {
		ses->opt.rtype = NOWDB_SES_BE;

	} else if (buf[3] == 'T' && buf[4] == 'X') {
		ses->opt.rtype = NOWDB_SES_TXT;

	} else {
		goto out_error;
	}
	if (buf[5] == '1') {
		ses->opt.ctype = NOWDB_SES_ACK;
	} else if (buf[5] == '0') {
		ses->opt.ctype = NOWDB_SES_NOACK;
	} else {
		goto chan_error;
	}
	if (buf[6] != ' ' || buf[7] != ' ') {
		goto term_error;
	}
	if (ses->opt.ctype == NOWDB_SES_ACK) {
		if (write(ses->ostream, buf, 8) != 8) {
			return nowdb_err_get(nowdb_err_write, TRUE,
			        OBJECT, "writing session options");
		}
		// await ack
		err = readN(ses->istream, buf, 2);
		if (err != NOWDB_OK) return err;
		if (buf[1] != NOWDB_ACK) goto ack_error;
	}
	return NOWDB_OK;

sql_error:
	return nowdb_err_get(nowdb_err_protocol, FALSE, OBJECT,
		                        "unknown session type");
out_error:
	return nowdb_err_get(nowdb_err_protocol, FALSE, OBJECT,
		                       "unknown response type");
chan_error:
	return nowdb_err_get(nowdb_err_protocol, FALSE, OBJECT,
		                       "unknown channel type");
term_error:
	return nowdb_err_get(nowdb_err_protocol, FALSE, OBJECT,
		                            "missing terminal");
ack_error:
	return nowdb_err_get(nowdb_err_protocol, FALSE, OBJECT,
		                   "session options not ack'd");
}

/* -----------------------------------------------------------------------
 * run session
 * -----------------------------------------------------------------------
 */
static void runSession(nowdb_session_t *ses) {
	nowdb_err_t err=NOWDB_OK;
	int rc = 0;
	nowdb_ast_t       *ast;
	struct timespec t1, t2;

	if (ses == NULL) return;
	if (ses->err != NOWDB_OK) return;
	if (ses->alive == 0) return;
	
	LOGMSG("SESSION STARTED");

	// negotiate seesion properties
	err = negotiate(ses);
	if (err != NOWDB_OK) {
		// SETERR();
		nowdb_err_print(err);
		nowdb_err_release(err);
		return;
	}

	// run a session
	for(;;) {
		// we need a parser
		if (ses->parser == NULL) {
			err = nowdb_err_get(nowdb_err_invalid, FALSE,
			           OBJECT, "session not initialised");
			SETERR();
			break;
		}

		// run parser on stream
		rc = nowdbsql_parser_runStream(ses->parser, &ast);

		// broken pipe: close session
		if (rc == NOWDB_SQL_ERR_CLOSED) {
			LOGMSG("SOCKET CLOSED\n");
			rc = 0; break;
		}

		// EOF: continue, there may come more
		if (rc == NOWDB_SQL_ERR_EOF) {
			LOGMSG("EOF\n");
			rc = 0; continue;
		}

		// parser error
		if (rc != 0) {
			MAKEPARSEERR();
			rc = sendErr(ses, err, ast);
			if (rc != 0) break;
			continue;
		}

		// this should not happen
		if (ast == NULL) {
			LOGMSG("no error, no ast :-(\n");
			err = nowdb_err_get(nowdb_err_parser, FALSE, OBJECT,
			            "unknown error occurred during parsing");
			rc = sendErr(ses, err, ast);
			if (rc != 0) break;
			continue;
		}

		// timing
		nowdb_timestamp(&t1);

		// handle ast
		rc = handleAst(ses, ast);
		if (rc != 0) {
			nowdb_ast_destroy(ast); free(ast);
			LOGMSG("cannot handle ast\n");
			/* only severe errors are passed through.
			 * other errors are handled internally.
			 * therefore: */
			break;
		}
		nowdb_ast_destroy(ast); free(ast); ast = NULL;

		// print timing information
		nowdb_timestamp(&t2);
		if (ses->opt.opts & NOWDB_SES_TIMING) {
			fprintf(stderr, "overall: %luus\n",
			   nowdb_time_minus(&t2, &t1)/1000);
		}
	}
	LOGMSG("SESSION ENDING\n");
}

/* -----------------------------------------------------------------------
 * leave session
 * -----------------------------------------------------------------------
 */
static void leaveSession(nowdb_session_t *ses, int stop) {
	nowdb_err_t err;

	// free resources
	if (ses->parser != NULL) {
		nowdbsql_parser_destroy(ses->parser);
		free(ses->parser); ses->parser = NULL;
	}
	if (ses->cursors != NULL) {
		ts_algo_tree_destroy(ses->cursors);
		free(ses->cursors); ses->cursors = NULL;
	}

	err = nowdb_proc_reinit(ses->proc);
	if (err != NOWDB_OK) {
		SETERR();
	}

	if (ses->istream >= 0) {
		close(ses->istream);
		ses->istream = -1;
	}

	if (stop == 2) return;

	// move session from used to free list
	// LOGMSG("locking ses->lib\n");
	err = nowdb_lock_write(LIB(ses->lib)->lock);
	if (err != NOWDB_OK) {
		SETERR();
		return;
	}

	if (LIB(ses->lib)->uthreads->head != NULL) {
		ts_algo_list_remove(LIB(ses->lib)->uthreads, ses->node);
		addNode(LIB(ses->lib)->fthreads, ses->node);
		fprintf(stderr, "used: %d, free: %d\n",
	                LIB(ses->lib)->uthreads->len,
	                LIB(ses->lib)->fthreads->len);
	}

	err = nowdb_unlock_write(LIB(ses->lib)->lock);
	if (err != NOWDB_OK) {
		SETERR();
		return;
	}
	// fprintf(stderr, "left session\n");
}

#define SIGNALEOS() \
	err = signalMaster(ses); \
	if (err != NOWDB_OK) { \
		LOGERRMSG(err); \
		break; \
	}

/* -----------------------------------------------------------------------
 * Session entry point and life cycle
 * -----------------------------------------------------------------------
 */
void *nowdb_session_entry(void *session) {
	nowdb_err_t err;
	nowdb_session_t *ses = session;
	sigset_t s;
	int sig, x;
	int stop;

	sigemptyset(&s);
        sigaddset(&s, SIGUSR1);

	sigset_t t;
	sigemptyset(&t);
	sigaddset(&s, SIGUSR1);
	sigaddset(&s, SIGINT);
	sigaddset(&s, SIGABRT);
	sigaddset(&s, SIGTERM);

	x = pthread_sigmask(SIG_BLOCK, &t, NULL);
	if (x != 0) {
		ses->err = nowdb_err_getRC(nowdb_err_sigwait,
		                             x, OBJECT, NULL);
	}
	for(;;) {
		// waiting for something to happen
		x = sigwait(&s, &sig);
		if (x != 0) {
			ses->err = nowdb_err_getRC(nowdb_err_sigwait,
			                             x, OBJECT, NULL);
			break;
		}

		// what shall we do?
		stop = getStop(ses);

		// fprintf(stderr, "STOP: %d\n", stop);

		if (stop < 0) break;
		if (stop == 2) break;
		if (stop == 1) continue;

		// we have an error 
		if (ses->err != NOWDB_OK) {
			LOGERRMSG(ses->err);
			ses->err = NOWDB_OK;
			break;
		}

		// run session / log error
		runSession(ses);
		if (ses->err != NOWDB_OK) {
			LOGERRMSG(ses->err);
			ses->err = NOWDB_OK;
		}

		// was stop set while we were busy?
		stop = getStop(ses);

		// fprintf(stderr, "STOP (2): %d\n", stop);

		if (stop < 0) break;
		if (stop == 2) break;

		// leave session
		leaveSession(ses,stop);

		// set state waiting
		if (setWaiting(ses) < 0) {
			LOGERRMSG(ses->err);
			ses->err = NOWDB_OK;
			SIGNALEOS();
			break;
		}

		// tell master that we are free
		SIGNALEOS();
	}
	ses->alive = 0;
	// fprintf(stderr, "terminating\n");
	return NULL;
}
