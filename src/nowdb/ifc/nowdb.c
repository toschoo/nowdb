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
 		/* close them explicitly ! */
		NOWDB_IGNORE(nowdb_scope_close(SCOPE(*n)->scope));
		nowdb_scope_destroy(SCOPE(*n)->scope);
	}
	free(*n); *n=NULL;
}

static ts_algo_rc_t noupdate(void *rsc, void *o, void *n) {
	return TS_ALGO_OK;
}

nowdb_err_t nowdb_library_init(nowdb_t **lib, char *base, int nthreads) {
	nowdb_err_t err;
	sigset_t s;
	int x;

	nowdb_init();

	sigemptyset(&s);
        sigaddset(&s, SIGUSR1);

	x = pthread_sigmask(SIG_SETMASK, &s, NULL);
	if (x != 0) {
		fprintf(stderr, "sigmask failed\n");
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
	return NOWDB_OK;
}

static nowdb_err_t signalSessions(nowdb_t *lib, int what) {
	ts_algo_list_t *list;
	ts_algo_list_node_t *runner;
	nowdb_session_t *ses;
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;

	err = nowdb_lock_write(lib->lock);
	if (err != NOWDB_OK) return err;

	int m = what==1?1:2;
	for(int i=0; i<m; i++) {
		list=i==0?lib->uthreads:lib->fthreads;
		for(runner=list->head;runner!=NULL;runner=runner->nxt) {
			ses = runner->cont;
			if (ses->alive == 0) continue;
			switch(what) {
			case 1: err = nowdb_session_stop(ses); break;
			case 2: err = nowdb_session_shutdown(ses); break;
			} 
			if (err != NOWDB_OK) {
				fprintf(stderr, "cannot stop session\n");
				nowdb_err_print(err);
				nowdb_err_release(err);
				break;
			}
		}
	}
	err2 = nowdb_unlock_write(lib->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

static nowdb_err_t killSessions(nowdb_t *lib) {
	return signalSessions(lib, 2);
}

static nowdb_err_t stopSessions(nowdb_t *lib) {
	return signalSessions(lib, 1);
}

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

void nowdb_library_close(nowdb_t *lib) {
	if (lib == NULL) {
		nowdb_close(); return;
	}
	if (lib->lock != NULL) {
		NOWDB_IGNORE(killSessions(lib));
	}
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

nowdb_err_t nowdb_library_shutdown(nowdb_t *lib) {

	if (lib == NULL) INVALID("lib is NULL");
	if (lib->lock == NULL) INVALID("lib is not open");

	return stopSessions(lib);
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

static nowdb_err_t initSession(nowdb_session_t *ses,
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

	if (ses->ifile != NULL) {
		fclose(ses->ifile);
	}
	ses->ifile = fdopen(istream, "r");
	if (ses->ifile == NULL) {
		fprintf(stderr, "istream: %d\n", istream);
		return nowdb_err_get(nowdb_err_open,
			   TRUE, OBJECT, "istream");
	}
	if (ses->parser != NULL) {
		nowdbsql_parser_destroy(ses->parser);
	} else {
		ses->parser = calloc(1,sizeof(nowdbsql_parser_t));
		if (ses->parser == NULL) {
			NOMEM("allocating parser");
			return err;
		}
	}
	if (nowdbsql_parser_init(ses->parser, ses->ifile) != 0) {
		err = nowdb_err_get(nowdb_err_parser, FALSE, OBJECT,
		                              "cannot init parser");
		free(ses->parser); ses->parser = NULL;
		return err;
	}
	return NOWDB_OK;
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

	err = initSession(*ses, istream, ostream, estream);
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

	for(;;) {
		n = lib->fthreads->head;
		*ses = n->cont;

		if ((*ses)->alive == 0) {
			ts_algo_list_remove(lib->fthreads, n);
			nowdb_session_destroy(*ses);
			free(*ses); *ses = NULL;
			free(n); continue;
		}
		break;
	}

	err = initSession(*ses, istream, ostream, estream);
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

#define LIB(x) \
	((nowdb_t*)x)

static nowdb_err_t signalSession(nowdb_session_t *ses) {
	int x;

	x = pthread_kill(ses->task, SIGUSR1);
	if (x != 0) {
		return nowdb_err_getRC(nowdb_err_signal, x, OBJECT, NULL);
	}
	return NOWDB_OK;
}

static int getStop(nowdb_session_t *ses) {
	nowdb_err_t err;
	int stop;

	err = nowdb_lock(ses->lock);
	if (err != NOWDB_OK) {
		err->cause = ses->err;
		ses->err = err;
		return -1;
	}
	stop = ses->stop;

	err = nowdb_unlock(ses->lock);
	if (err != NOWDB_OK) {
		err->cause = ses->err;
		ses->err = err;
		return -1;
	}
	return stop;
}

static int setStop(nowdb_session_t *ses, int stop) {
	nowdb_err_t err;

	err = nowdb_lock(ses->lock);
	if (err != NOWDB_OK) {
		err->cause = ses->err;
		ses->err = err;
		return -1;
	}

	ses->stop = stop;

	err = nowdb_unlock(ses->lock);
	if (err != NOWDB_OK) {
		err->cause = ses->err;
		ses->err = err;
		return -1;
	}
	return 0;
}

static int setWaiting(nowdb_session_t *ses) {
	nowdb_err_t err;

	err = nowdb_lock(ses->lock);
	if (err != NOWDB_OK) {
		err->cause = ses->err;
		ses->err = err;
		return -1;
	}

	ses->stop = 0;
	ses->running = 0;

	err = nowdb_unlock(ses->lock);
	if (err != NOWDB_OK) {
		err->cause = ses->err;
		ses->err = err;
		return -1;
	}
	return 0;
}

// run a session everything is done internally
nowdb_err_t nowdb_session_run(nowdb_session_t *ses) {
	if (ses == NULL) INVALID("session is NULL");
	if (ses->alive == 0) INVALID("session is dead");
	fprintf(stderr, "starting session\n");
	return signalSession(ses);
}

nowdb_err_t nowdb_session_stop(nowdb_session_t *ses) {
	nowdb_err_t err;

	if (ses == NULL) INVALID("session is NULL");
	if (ses->alive == 0) INVALID("session is dead");
	fprintf(stderr, "stopping session\n");

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

nowdb_err_t nowdb_session_shutdown(nowdb_session_t *ses) {
	nowdb_err_t err;

	if (ses == NULL) INVALID("session is NULL");
	if (ses->alive == 0) INVALID("session is dead");
	fprintf(stderr, "shutting down session %p\n", ses);

	if (setStop(ses, 2) != 0) {
		ses->alive = 0;
		return ses->err;
	}
	err = signalSession(ses);
	if (err != NOWDB_OK) { 
		ses->alive = 0;
		return err;
	}
	err = nowdb_task_join(ses->task);
	if (err != NOWDB_OK) { // what to do?
		fprintf(stderr, "cannot join!\n");
		return err;
	}
	return NOWDB_OK;
}

// destroy session
void nowdb_session_destroy(nowdb_session_t *ses) {
	if (ses == NULL) return;
	if (ses->parser != NULL) {
		nowdbsql_parser_destroy(ses->parser);
		free(ses->parser); ses->parser = NULL;
	}
	if (ses->ifile != NULL) {
		fclose(ses->ifile); ses->ifile = NULL;
	}
	if (ses->lock != NULL) {
		nowdb_lock_destroy(ses->lock);
		free(ses->lock); ses->lock = NULL;
	}
}

static int handleAst(char *path, nowdb_ast_t *ast) {
	nowdb_ast_show(ast);
	return 0;
}

static void runSession(nowdb_session_t *ses) {
	int rc = 0;
	nowdb_ast_t       *ast;
	struct timespec t1, t2;

	if (ses == NULL) return;
	if (ses->err != NOWDB_OK) return;
	if (ses->alive == 0) return;

	fprintf(stderr, "running session\n");

	for(;;) {

		// this must be interruptibel
		rc = nowdbsql_parser_run(ses->parser, &ast);
		if (rc == NOWDB_SQL_ERR_EOF) {
			rc = 0; break;
		}
		if (rc != 0) {
			fprintf(stderr, "parsing error\n");
			break;
		}
		if (ast == NULL) {
			fprintf(stderr, "no error, no ast :-(\n");
			rc = NOWDB_SQL_ERR_UNKNOWN; break;
		}
		nowdb_timestamp(&t1);

		rc = handleAst(LIB(ses->lib)->base, ast);
		if (rc != 0) {
			nowdb_ast_destroy(ast); free(ast);
			fprintf(stderr, "cannot handle ast\n"); break;
		}
		nowdb_ast_destroy(ast); free(ast); ast = NULL;

		nowdb_timestamp(&t2);
		// if (global_timing) {
			fprintf(stderr, "overall: %luus\n",
			   nowdb_time_minus(&t2, &t1)/1000);
		// }
	}
	if (rc != 0) {
		fprintf(stderr, "ERROR: %s\n",
		nowdbsql_parser_errmsg(ses->parser));
	}
}

static void leaveSession(nowdb_session_t *ses) {
	nowdb_err_t err;
	nowdb_err_t err2;

	fprintf(stderr, "leaving session\n");

	err2 = ses->err;

	err = nowdb_lock_write(LIB(ses->lib)->lock);
	if (err != NOWDB_OK) {
		err->cause = err2;
		ses->err = err;
		return;
	}

	ts_algo_list_remove(LIB(ses->lib)->uthreads, ses->node);
	addNode(LIB(ses->lib)->fthreads, ses->node);

	err = nowdb_unlock_write(LIB(ses->lib)->lock);
	if (err != NOWDB_OK) {
		err->cause = err2;
		ses->err = err;
		return;
	}
	if (ses->parser != NULL) {
		nowdbsql_parser_destroy(ses->parser);
		free(ses->parser); ses->parser = NULL;
	}
	if (ses->ifile != NULL) {
		fclose(ses->ifile); ses->ifile = NULL;
	}
}

// session entry point
void *nowdb_session_entry(void *session) {
	nowdb_session_t *ses = session;
	sigset_t s;
	int sig, x;
	int stop;

	fprintf(stderr, "session entry point\n");
	sigemptyset(&s);
        sigaddset(&s, SIGUSR1);

	// x = pthread_sigmask(SIG_SETMASK, &s, NULL);

	for(;;) {
		if (setWaiting(ses) < 0) break;

		fprintf(stderr, "waiting for signal\n");
		x = sigwait(&s, &sig);
		if (x != 0) {
			ses->err = nowdb_err_getRC(nowdb_err_sigwait,
			                             x, OBJECT, NULL);
			break;
		}
		fprintf(stderr, "got signal\n");

		stop = getStop(ses);
		if (stop < 0) break;
		if (stop == 2) break;

		runSession(ses);

		if (ses->err != NOWDB_OK) break;

		stop = getStop(ses);
		if (stop < 0) break;
		if (stop == 2) break;

		leaveSession(ses);
	}
	ses->alive = 0;
	fprintf(stderr, "terminating\n");
	return NULL;
}
