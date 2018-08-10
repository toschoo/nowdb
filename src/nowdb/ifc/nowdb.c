/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Collection of scopes + threadpool
 * ========================================================================
 */
#include <nowdb/ifc/nowdb.h>

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

static nowdb_err_t createSession(nowdb_t *lib, nowdb_session_t **ses) {
	nowdb_err_t err;

	err = nowdb_session_create(ses, lib, -1, -1, -1);
	if (err != NOWDB_OK) return err;

	return NOWDB_OK;
}

nowdb_err_t nowdb_library_init(nowdb_t **lib, int nthreads) {
	nowdb_err_t err;

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
	if (lib != NULL) return;

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
	(*ses)->istream = istream;
	(*ses)->ostream = ostream;
	(*ses)->estream = estream;
	(*ses)->running = 0;

	(*ses)->ifile = fdopen(istream, "r");
	if ((*ses)->ifile == NULL) {
		err = nowdb_err_get(nowdb_err_open, TRUE, OBJECT,
		                             "fdopen on istream");
		free(*ses); *ses = NULL;
		return err;
	}

	(*ses)->parser = calloc(1,sizeof(nowdbsql_parser_t));
	if ((*ses)->parser == NULL) {
		free(*ses); *ses = NULL;
		NOMEM("allocating parser");
		return err;
	}
	if (nowdbsql_parser_init((*ses)->parser, (*ses)->ifile) != 0) {
		err = nowdb_err_get(nowdb_err_parser, FALSE, OBJECT,
		                              "cannot init parser");
		free(*ses); *ses = NULL;
		return err;
	}

	err = nowdb_task_create(&(*ses)->task, &nowdb_session_entry, *ses);
	if (err != NOWDB_OK) {
		free(*ses); *ses = NULL;
		return err;
	}
	// set signal handlers!
	return NOWDB_OK;
}

void nowdb_session_init(nowdb_session_t *ses,
                        int           istream,
                        int           ostream,
                        int           estream) 
{
	ses->istream = istream;
	ses->ostream = ostream;
	ses->estream = estream;
}

nowdb_err_t nowdb_getSession(nowdb_t *lib, nowdb_session_t **ses) {
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
		err = createSession(lib, ses);
		if (err != NOWDB_OK) goto unlock;

		if (ts_algo_list_append(lib->uthreads, *ses) != TS_ALGO_OK) {
			nowdb_session_destroy(*ses);
			NOMEM("list.append");
			goto unlock;
		}
		goto unlock;
	}
	n = lib->fthreads->head;
	ts_algo_list_remove(lib->fthreads, n);

	n->nxt = lib->uthreads->head;
	if (n->nxt != NULL) n->nxt->prv = n->nxt;
	lib->uthreads->head = n;
	lib->uthreads->len++;
	
	*ses = n->cont;

unlock:
	err2 = nowdb_unlock_write(lib->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

// run a session everything is done internally
nowdb_err_t nowdb_session_run(nowdb_session_t *ses);

// destroy session
void nowdb_session_destroy(nowdb_session_t *ses) {
	// tell thread to stop
	NOWDB_IGNORE(nowdb_task_join(ses->task));
	// close ifile
	// destroy parser
	
}
