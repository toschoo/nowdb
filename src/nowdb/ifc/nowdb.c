/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Collection of scopes + threadpool
 * ========================================================================
 */
#include <nowdb/ifc/nowdb.h>

#include <signal.h>
#include <pthread.h>

static char *OBJECT = "lib";

#define INVALID(s) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, s);

#define NOMEM(s) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, s);

typedef struct {
	char           *name;
	char           inuse;
	nowdb_scope_t *scope;
} scope_t;

#define SCOPE(x) \
	((scope_t*)x)

static ts_algo_cmp_t scopecompare(void *rsc, void *one,
                                      void *two) {
	char x;
	x = strcmp(SCOPE(one)->name, SCOPE(two)->name);
	if (x < 0) return ts_algo_cmp_less;
	if (x > 0) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

static void scopedestroy(void *rsc, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	if (SCOPE(*n)->name != NULL) {
		free(SCOPE(*n)->name);
	}
	if (SCOPE(*n)->scope != NULL) {
 		/* close beeter close them explicitly ! */
		NOWDB_IGNORE(nowdb_scope_close(SCOPE(*n)->scope));
		nowdb_scope_destroy(SCOPE(*n)->scope);
	}
	free(*n); *n=NULL;
}

static ts_algo_rc_t noupdate(void *rsc, void *o, void *n) {
	return TS_ALGO_OK;
}

nowdb_err_t nowdb_library_init(nowdb_t **lib, int nthreads) {
	nowdb_err_t err;

	nowdb_init();

	if (lib == NULL) INVALID("library pointer is NULL");
	if (nthreads < 0) INVALID("negative number of threads not supported");

	*lib = calloc(1, sizeof(nowdb_t));
	if (*lib == NULL) {
		NOMEM("allocating lib");
		return err;
	}

	(*lib)->nthreads = nthreads;

	(*lib)->lock = calloc(1, sizeof(nowdb_rwlock_t));
	if ((*lib)->lock == NULL) {
		NOMEM("allocating lock");
		free(*lib); *lib = NULL;
		return err;
	}
	err = nowdb_rwlock_init((*lib)->lock);
	if (err != NOWDB_OK) {
		free((*lib)->lock); (*lib)->lock = NULL;
		nowdb_close(*lib); free(*lib); *lib = NULL;
		return err;
	}
	(*lib)->fthreads = calloc(1,sizeof(ts_algo_list_t));
	if ((*lib)->fthreads == NULL) {
		NOMEM("allocating list");
		nowdb_close(*lib); free(*lib); *lib = NULL;
		return err;
	}
	ts_algo_list_init((*lib)->fthreads);
	(*lib)->uthreads = calloc(1,sizeof(ts_algo_list_t));
	if ((*lib)->uthreads == NULL) {
		NOMEM("allocating list");
		nowdb_close(*lib); free(*lib); *lib = NULL;
		return err;
	}
	ts_algo_list_init((*lib)->uthreads);

	(*lib)->scopes = ts_algo_tree_new(&scopecompare, NULL,
	                                  &noupdate,
	                                  &scopedestroy,
	                                  &scopedestroy);

	if ((*lib)->scopes == NULL) {
		nowdb_close(*lib); free(*lib); *lib = NULL;
		NOMEM("tree.new");
		return err;
	}
	return NOWDB_OK;
}

void destroySessionList(ts_algo_list_t *list) {
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

void nowdb_library_close(nowdb_t *lib) {
	if (lib == NULL) return;

	if (lib->uthreads != NULL) {
		destroySessionList(lib->uthreads);
		free(lib->uthreads); lib->uthreads = NULL;
	}
	if (lib->fthreads != NULL) {
		destroySessionList(lib->fthreads);
		free(lib->fthreads); lib->fthreads = NULL;
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
	free(lib);
	nowdb_close();
}

nowdb_err_t nowdb_getScope(nowdb_t *lib, char *name,
                           nowdb_scope_t    **scope) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;
	scope_t pattern;

	if (lib == NULL) INVALID("lib is NULL");
	if (name == NULL) INVALID("name is NULL");
	if (scope == NULL) INVALID("scope pointer is NULL");

	err = nowdb_lock_read(lib->lock);
	if (err != NOWDB_OK) return err;

	pattern.name = name;
	*scope = ts_algo_tree_find(lib->scopes, &pattern);
	if (*scope == NULL) err = nowdb_err_get(nowdb_err_key_not_found,
	                                            FALSE, OBJECT, name);

	err2 = nowdb_unlock_read(lib->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

void nowdb_session_init(nowdb_session_t *ses,
                        int           istream,
                        int           ostream,
                        int           estream) 
{
	ses->err     = NOWDB_OK;
	ses->scope   = NULL;
	ses->istream = istream;
	ses->ostream = ostream;
	ses->estream = estream;
	ses->running = 0;
	ses->stop    = 0;
}

// run a session everything is done internally
nowdb_err_t nowdb_session_create(nowdb_session_t **ses,
                                 nowdb_t          *lib,
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

	nowdb_session_init(*ses, istream, ostream, estream);

	/* CAUTION:
	 * file must be set on each turn !!! */
	(*ses)->ifile = fdopen(istream, "r");
	if ((*ses)->ifile == NULL) {
		err = nowdb_err_get(nowdb_err_open, TRUE, OBJECT,
		                             "fdopen on istream");
		nowdb_session_destroy(*ses);
		free(*ses); *ses = NULL;
		return err;
	}

	(*ses)->parser = calloc(1,sizeof(nowdbsql_parser_t));
	if ((*ses)->parser == NULL) {
		nowdb_session_destroy(*ses);
		free(*ses); *ses = NULL;
		NOMEM("allocating parser");
		return err;
	}
	if (nowdbsql_parser_init((*ses)->parser, (*ses)->ifile) != 0) {
		err = nowdb_err_get(nowdb_err_parser, FALSE, OBJECT,
		                              "cannot init parser");
		free((*ses)->parser); (*ses)->parser = NULL;
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

static void addNode(ts_algo_list_t      *list,
                    ts_algo_list_node_t *node) {

	node->nxt = list->head;
	if (node->nxt != NULL) node->nxt->prv = node;
	list->head = node;
	list->len++;
}

nowdb_err_t nowdb_getSession(nowdb_t *lib,
                    nowdb_session_t **ses,
                              int istream,
                              int ostream,
                              int estream) {
	ts_algo_list_node_t *n;
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;

	err = nowdb_lock_write(lib->lock);
	if (err != NULL) return err;

	// shall we allow ad-hoc threads?
	if (lib->fthreads->len == 0) {
		if (lib->uthreads->len > lib->nthreads) {
			err = nowdb_err_get(nowdb_err_no_rsc,
			  FALSE, OBJECT, "empty thread pool");
			goto unlock;
		}
		err = nowdb_session_create(ses, lib,
		          istream, ostream, estream);
		if (err != NOWDB_OK) goto unlock;

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
	n = lib->fthreads->head;
	ts_algo_list_remove(lib->fthreads, n);
	
	*ses = n->cont;
	nowdb_session_init(*ses, istream, ostream, estream);

	addNode(lib->uthreads, n);

unlock:
	err2 = nowdb_unlock_write(lib->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

// run a session everything is done internally
nowdb_err_t nowdb_session_run(nowdb_session_t *ses) {
	int x;

	x = pthread_kill(ses->task, SIGUSR1);
	if (x != 0) {
		return nowdb_err_getRC(nowdb_err_signal, x, OBJECT, NULL);
	}
	return NOWDB_OK;
}

// destroy session
void nowdb_session_destroy(nowdb_session_t *ses) {
	nowdb_err_t err;

	if (ses == NULL) return;

	// tell thread to stop
	ses->stop = 1;
	/*
	err = nowdb_session_run(ses);
	if (err != NOWDB_OK) { // what to do?
		fprintf(stderr, "cannot stop session!\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return;
	}

	NOWDB_IGNORE(nowdb_task_join(ses->task));
	*/

	if (ses->parser != NULL) {
		nowdbsql_parser_destroy(ses->parser);
		free(ses->parser); ses->parser = NULL;
	}
	if (ses->ifile != NULL) {
		fclose(ses->ifile); ses->ifile = NULL;
	}
}

static void runSession(nowdb_session_t *ses) {
	if (ses->err != NOWDB_OK) return;
	fprintf(stderr, "running session\n");
}

#define LIB(x) \
	((nowdb_t*)x)

static void leaveSession(nowdb_session_t *ses) {
	nowdb_err_t err;
	nowdb_err_t err2;

	err2 = ses->err;

	err = nowdb_lock_write(ses->lib);
	if (err != NOWDB_OK) {
		err->cause = err2;
		ses->err = err;
		return;
	}

	ts_algo_list_remove(LIB(ses->lib)->uthreads, ses->node);
	addNode(LIB(ses->lib)->fthreads, ses->node);

	err = nowdb_unlock_write(ses->lib);
	if (err != NOWDB_OK) {
		err->cause = err2;
		ses->err = err;
		return;
	}
}

// session entry point
void *nowdb_session_entry(void *session) {
	nowdb_session_t *ses = session;
	sigset_t s;
	int sig, x;

	sigemptyset(&s);
        sigaddset(&s, SIGUSR1);

	for(;;) {
		ses->running = 0;

		x = sigwait(&s, &sig);
		if (x != 0) {
			ses->err = nowdb_err_getRC(nowdb_err_sigwait,
			                             x, OBJECT, NULL);
			break;
		}

		if (ses->stop) break;
	
		ses->running = 1;

		runSession(ses);
		leaveSession(ses);

		if (ses->err != NOWDB_OK) break;
	}
	return NULL;
}
