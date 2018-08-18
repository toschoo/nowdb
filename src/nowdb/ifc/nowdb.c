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

#define INVALID(s) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, s);

#define NOMEM(s) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, s);

#define SETERR() \
	err->cause = ses->err; \
	ses->err = err;

#define LOGERR(err) \
	nowdb_err_send(err, ses->estream); \
	nowdb_err_release(err);

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

static ts_algo_cmp_t scopecompare(void *rsc, void *one, void *two) {
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
		free(SCOPE(*n)->scope);
	}
	free(*n); *n=NULL;
}

static ts_algo_rc_t noupdate(void *rsc, void *o, void *n) {
	return TS_ALGO_OK;
}

#define CUR(x) \
	((nowdb_ses_cursor_t*)x)

static ts_algo_cmp_t cursorcompare(void *rsc, void *one, void *two) {
	if (CUR(one)->curid < CUR(two)->curid) return ts_algo_cmp_less;
	if (CUR(one)->curid > CUR(two)->curid) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

static void cursordestroy(void *rsc, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	if (CUR(*n)->cur != NULL) {
		nowdb_cursor_destroy(CUR(*n)->cur);
		free(CUR(*n)->cur); CUR(*n)->cur = NULL;
	}
	free(*n); *n = NULL;
}

static ts_algo_rc_t cursorupdate(void *rsc, void *o, void *n) {
	CUR(o)->off = CUR(n)->off;
	return TS_ALGO_OK;
}

nowdb_err_t nowdb_library_init(nowdb_t **lib, char *base, int nthreads) {
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
	scope_t pattern, *result;

	if (lib == NULL) INVALID("lib is NULL");
	if (name == NULL) INVALID("name is NULL");
	if (scope == NULL) INVALID("scope pointer is NULL");

	pattern.name = name;
	result = ts_algo_tree_find(lib->scopes, &pattern);
	if (result != NULL) *scope = result->scope;

	return NOWDB_OK;
}

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
	ses->master  = master; // pthread_self();
	ses->curid   = 0x100;

	if (ses->ifile != NULL) {
		fclose(ses->ifile);
	}
	if (ses->ofile != NULL) {
		fclose(ses->ofile);
	}
	if (ses->efile != NULL) {
		fclose(ses->efile);
	}
	ses->ifile = fdopen(istream, "r");
	if (ses->ifile == NULL) {
		fprintf(stderr, "istream: %d\n", istream);
		return nowdb_err_get(nowdb_err_open,
			   TRUE, OBJECT, "istream");
	}
	setbuf(ses->ifile, NULL);
	ses->ofile = fdopen(dup(ostream), "w");
	if (ses->ofile == NULL) {
		return nowdb_err_get(nowdb_err_open,
			   TRUE, OBJECT, "ostream");
	}
	setbuf(ses->ofile, NULL);
	ses->efile = fdopen(dup(estream), "w");
	if (ses->efile == NULL) {
		return nowdb_err_get(nowdb_err_open,
			   TRUE, OBJECT, "estream");
	}
	setbuf(ses->efile, NULL);

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

// run a session everything is done internally
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

	err = initSession(*ses, master, istream, ostream, estream);
	if (err != NOWDB_OK) {
		nowdb_session_destroy(*ses);
		free(*ses); *ses = NULL;
		return err;
	}

	(*ses)->cursors = ts_algo_tree_new(&cursorcompare, NULL,
                                           &cursorupdate,
	                                   &cursordestroy,
	                                   &cursordestroy);
	if ((*ses)->cursors == NULL) {
		NOMEM("tree.alloc");
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
                      nowdb_task_t master,
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

#define LIB(x) \
	((nowdb_t*)x)

static nowdb_err_t signalMaster(nowdb_session_t *ses) {
	int x;

	if (ses->master == 0) return NOWDB_OK;
	x = pthread_kill(ses->master, SIGUSR1);
	if (x != 0) {
		return nowdb_err_getRC(nowdb_err_signal, x, OBJECT, NULL);
	}
	return NOWDB_OK;
}

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

static int setStop(nowdb_session_t *ses, int stop) {
	nowdb_err_t err;

	err = nowdb_lock(ses->lock);
	if (err != NOWDB_OK) {
		SETERR();
		return -1;
	}

	ses->stop = stop;

	err = nowdb_unlock(ses->lock);
	if (err != NOWDB_OK) {
		SETERR();
		return -1;
	}
	return 0;
}

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

// run a session everything is done internally
nowdb_err_t nowdb_session_run(nowdb_session_t *ses) {
	if (ses == NULL) INVALID("session is NULL");
	if (ses->alive == 0) INVALID("session is dead");
	return signalSession(ses);
}

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
		fprintf(stderr, "cannot join!\n");
		return err;
	}
	return NOWDB_OK;
}

// destroy session
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
	if (ses->ifile != NULL) {
		fclose(ses->ifile); ses->ifile = NULL;
	}
	if (ses->ofile != NULL) {
		fclose(ses->ofile); ses->ofile = NULL;
	}
	if (ses->efile != NULL) {
		fclose(ses->efile); ses->efile = NULL;
	}
	if (ses->lock != NULL) {
		nowdb_lock_destroy(ses->lock);
		free(ses->lock); ses->lock = NULL;
	}
}

static int sendReport(nowdb_session_t *ses, nowdb_qry_result_t *res) {
	nowdb_qry_report_t *rep;

	if (res->result == NULL) {
		fprintf(stderr, "NO REPORT!\n");
		return -1;
	}
	rep = res->result;
	fprintf(stderr, "%lu rows loaded with %lu errors in %ldus\n",
	                rep->affected,
	                rep->errors,
	                rep->runtime);
	free(res->result); res->result = NULL;
	return 0;
}

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
	fprintf(stderr, "OK\n");
	return 0;
}

static int sendEOF(nowdb_session_t *ses) {
	short errcode = (short)nowdb_err_eof;
	nowdb_err_t err;
	char status[4];

	fprintf(stderr, "EOF\n");

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
	return 0;
}

#define CLEANUP() \
	if (cause != NOWDB_OK) nowdb_err_release(cause); \
	if (freetmp) free(tmp);

static int sendErr(nowdb_session_t *ses,
                   nowdb_err_t    cause,
                   nowdb_ast_t     *ast) {
	char *tmp="unknown";
	int sz=0;
	char status[4];
	char freetmp=0;
	short errcode;
	nowdb_err_t err;

	status[0] = NOWDB_STATUS;
	status[1] = NOWDB_NOK;
	errcode = cause == NOWDB_OK?nowdb_err_unknown:cause->errcode;
	memcpy(status+2, &errcode, 2);

	// send NOK
	if (write(ses->ostream, status, 4) != 4) {
		err = nowdb_err_get(nowdb_err_write,
		     TRUE, OBJECT, "writing status");
		SETERR();
		CLEANUP();
		return -1;
	}
	// build error report
	if (cause != NOWDB_OK) {
		tmp = nowdb_err_describe(cause, ';');
		if (tmp == NULL) tmp = "out-of-mem";
		else freetmp = 1;
	}

	sz = strlen(tmp);

	// send error report
	if (write(ses->ostream, &sz, 4) != 4) {
		err = nowdb_err_get(nowdb_err_write,
		      TRUE, OBJECT, "writing error");
		SETERR();
		CLEANUP();
		return -1;
	}
	if (write(ses->ostream, tmp, sz) != sz) {
		err = nowdb_err_get(nowdb_err_write,
		      TRUE, OBJECT, "writing error");
		SETERR();
		CLEANUP();
		return -1;
	}
	CLEANUP();
	return 0;
}

static int sendCursor(nowdb_session_t   *ses,
                      uint64_t         curid,
                      char *buf, uint32_t sz) {
	nowdb_err_t err;
	char status[16];

	status[0] = NOWDB_CURSOR;
	status[1] = NOWDB_ACK;

	fprintf(stderr, "sendinng curid %lu\n", curid);
	memcpy(status+2, &curid, 8);

	fprintf(stderr, "sendinng size %u\n", sz);
	memcpy(status+10, &sz, 4);

	if (write(ses->ostream, status, 14) != 14) {
		err = nowdb_err_get(nowdb_err_write, TRUE, OBJECT,
		                                 "writing cursor");
		SETERR();
		return -1;
	}
	if (write(ses->ostream, buf, sz) != sz) {
		err = nowdb_err_get(nowdb_err_write, TRUE, OBJECT,
		                                 "writing cursor");
		SETERR();
		return -1;
	}
	return 0;
}

static int openCursor(nowdb_session_t *ses, nowdb_cursor_t *cur) {
	uint32_t osz;
	uint32_t cnt;
	nowdb_err_t err=NOWDB_OK;
	char *buf = ses->buf;
	uint32_t sz = ses->bufsz;
	nowdb_ses_cursor_t *scur;

	err = nowdb_cursor_open(cur);
	if (err != NOWDB_OK) {
		if (err->errcode == nowdb_err_eof) {
			fprintf(stderr, "Read: 0\n");
			nowdb_err_release(err);
			if (sendEOF(ses) != 0) {
				fprintf(stderr, "cannot send eof\n");
				goto cleanup;
			}
		}
		goto cleanup;
	}
	err = nowdb_cursor_fetch(cur, buf, sz, &osz, &cnt);
	if (err != NOWDB_OK) {
		if (err->errcode == nowdb_err_eof) {
			fprintf(stderr, "EOF after fetch\n");
			nowdb_err_release(err);
			if (osz == 0) {
				if (sendEOF(ses) != 0) goto cleanup;
			}
		} else {
			if (sendErr(ses, err, NULL) != 0) {
				// in this case we must end the session
				fprintf(stderr, "cannot send error\n");
				SETERR();
				goto cleanup;
			}
			goto cleanup;
		}
	}

	// insert cursor into tree
	ses->curid++;
	scur = calloc(1,sizeof(nowdb_ses_cursor_t));
	if (scur == NULL) {
		NOMEM("allocating cursor");
		SETERR();
		goto cleanup;
	}
	scur->curid = ses->curid;
	scur->cur = cur;
	if (ts_algo_tree_insert(ses->cursors, scur) != TS_ALGO_OK) {
		NOMEM("tree.insert");
		SETERR();
		free(scur); goto cleanup;
	}
	if (sendCursor(ses, scur->curid, buf, osz) != 0) {
		perror("cannot send cursor");
		ts_algo_tree_delete(ses->cursors, scur);
		return -1;
	}
	return 0;

cleanup:
	nowdb_cursor_destroy(cur); free(cur);
	if (ses->err != NOWDB_OK) return -1;
	return 0;
}

static int fetch(nowdb_session_t    *ses,
                 nowdb_ses_cursor_t *scur) 
{
	nowdb_err_t err;
	uint32_t osz;
	uint32_t cnt;
	uint32_t sz = ses->bufsz;
	char *buf = ses->buf;

	if (nowdb_cursor_eof(scur->cur)) {
		if (sendEOF(ses) != 0) return -1;
		return 0;
	}
	err = nowdb_cursor_fetch(scur->cur, buf, sz, &osz, &cnt);
	if (err != NOWDB_OK) {
		if (err->errcode == nowdb_err_eof) {
			nowdb_err_release(err);
			if (osz == 0) {
				if (sendEOF(ses) != 0) {
					return -1;
				}
			}
			return 0;
		} else {
			if (sendErr(ses, err, NULL) != 0) {
				// in this case we must end the session
				fprintf(stderr, "cannot send error\n");
				SETERR();
				return -1;
			}
			return 0;
		}
	}
	if (sendCursor(ses, scur->curid, buf, osz) != 0) {
		return -1;
	}
	return 0;
}

static int handleOp(nowdb_session_t *ses, nowdb_ast_t *ast) {
	char *tmp;
	nowdb_err_t err;
	nowdb_ses_cursor_t pattern;
	nowdb_ses_cursor_t *cur;

	fprintf(stderr, "handle op: %d\n", ast->ntype);
	pattern.curid = strtoul(ast->value, &tmp, 10);
	if (*tmp != 0) {
		err = nowdb_err_get(nowdb_err_invalid,
		  FALSE, OBJECT, "not a valid number");
		if (sendErr(ses, err, NULL) != 0) return -1;
		return 0;
	}
	switch(ast->ntype) {
	case NOWDB_AST_FETCH:
		fprintf(stderr, "FETCH\n");
		cur = ts_algo_tree_find(ses->cursors, &pattern);
		if (cur == NULL) {
			err = nowdb_err_get(nowdb_err_invalid,
			  FALSE, OBJECT, "not an open cursor");
			if (sendErr(ses, err, NULL) != 0) return -1;
			return 0;
		}
		if (fetch(ses, cur) != 0) return -1;
		return 0;
	
	case NOWDB_AST_CLOSE:
		fprintf(stderr, "CLOSE\n");
		ts_algo_tree_delete(ses->cursors, &pattern); 
		if (sendOK(ses) != 0) return -1;
		return 0;

	default:
		fprintf(stderr, "AST: %d\n", ast->ntype);
		err = nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                                 "unknown operation");
		if (sendErr(ses, err, NULL) != 0) return -1;
		return 0;
	}
	return 0;
}

static int handleAst(nowdb_session_t *ses, nowdb_ast_t *ast) {
	nowdb_err_t err=NOWDB_OK;
	nowdb_qry_result_t res;
	char *path = LIB(ses->lib)->base;

	err = nowdb_stmt_handle(ast, ses->scope, ses->lib, path, &res);
	if (err != NOWDB_OK) return sendErr(ses, err, ast);
	switch(res.resType) {
	case NOWDB_QRY_RESULT_NOTHING: return sendOK(ses);
	case NOWDB_QRY_RESULT_REPORT: return sendReport(ses, &res);
	case NOWDB_QRY_RESULT_SCOPE:
		ses->scope = res.result; return sendOK(ses);
	case NOWDB_QRY_RESULT_CURSOR:
		return openCursor(ses, res.result);
	case NOWDB_QRY_RESULT_OP:
		return handleOp(ses, res.result);
	default:
		fprintf(stderr, "unknown result :-(\n");
		return -1;
	}
	return 0;
}

static nowdb_err_t negotiate(nowdb_session_t *ses) {
	char buf[8];

        if (read(ses->istream, buf, 8) != 8) {
		return nowdb_err_get(nowdb_err_read, TRUE, OBJECT,
		                        "reading session options");
	}
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
			return nowdb_err_get(nowdb_err_write, TRUE, OBJECT,
		                                 "writing session options");
		}
		// await ack
        	if (read(ses->istream, buf, 4) != 4) {
			return nowdb_err_get(nowdb_err_read, TRUE, OBJECT,
		                                            "reading ACK");
		}
		if (buf[0] != 'A' || buf[1] != 'C' || buf[2] != 'K')
			goto ack_error;
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

#define MAKEPARSEERR() \
	err = nowdb_err_get(nowdb_err_parser, FALSE, OBJECT, \
	         (char*)nowdbsql_parser_errmsg(ses->parser));

static void runSession(nowdb_session_t *ses) {
	nowdb_err_t err=NOWDB_OK;
	int rc = 0;
	nowdb_ast_t       *ast;
	struct timespec t1, t2;

	if (ses == NULL) return;
	if (ses->err != NOWDB_OK) return;
	if (ses->alive == 0) return;
	
	/*
	fprintf(stderr, "running session on behalf of %ld\n",
	                                         ses->master);
	*/

	err = negotiate(ses);
	if (err != NOWDB_OK) {
		SETERR();
		nowdb_err_print(err);
		return;
	}
	for(;;) {
		if (ses->parser == NULL) {
			err = nowdb_err_get(nowdb_err_invalid, FALSE,
			           OBJECT, "session not initialised");
			SETERR();
			break;
		}
		rc = nowdbsql_parser_runStream(ses->parser, &ast);
		if (rc == NOWDB_SQL_ERR_CLOSED) {
			fprintf(stderr, "SOCKET CLOSED\n");
			rc = 0; break;
		}
		if (rc == NOWDB_SQL_ERR_EOF) {
			fprintf(stderr, "EOF\n");
			rc = 0; continue;
		}
		if (rc != 0) {
			MAKEPARSEERR();
			rc = sendErr(ses, err, ast);
			if (rc != 0) break;
			continue;
		}
		if (ast == NULL) {
			fprintf(stderr, "no error, no ast :-(\n");
			rc = NOWDB_SQL_ERR_UNKNOWN; break;
		}
		nowdb_timestamp(&t1);

		rc = handleAst(ses, ast);
		if (rc != 0) {
			nowdb_ast_destroy(ast); free(ast);
			fprintf(stderr, "cannot handle ast\n"); break;
		}
		nowdb_ast_destroy(ast); free(ast); ast = NULL;

		nowdb_timestamp(&t2);
		if (ses->opt.opts & NOWDB_SES_TIMING) {
			fprintf(stderr, "overall: %luus\n",
			   nowdb_time_minus(&t2, &t1)/1000);
		}
	}
	if (rc != 0) {
		fprintf(stderr, "ERROR: %s\n",
		nowdbsql_parser_errmsg(ses->parser));
	}
	fprintf(stderr, "session ending\n");
}

static void leaveSession(nowdb_session_t *ses) {
	nowdb_err_t err;

	// fprintf(stderr, "leaving session\n");

	err = nowdb_lock_write(LIB(ses->lib)->lock);
	if (err != NOWDB_OK) {
		SETERR();
		return;
	}

	ts_algo_list_remove(LIB(ses->lib)->uthreads, ses->node);
	addNode(LIB(ses->lib)->fthreads, ses->node);

	err = nowdb_unlock_write(LIB(ses->lib)->lock);
	if (err != NOWDB_OK) {
		SETERR();
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

#define SIGNALEOS() \
	err = signalMaster(ses); \
	if (err != NOWDB_OK) { \
		SETERR(); \
		break; \
	}

// session entry point
void *nowdb_session_entry(void *session) {
	nowdb_err_t err;
	nowdb_session_t *ses = session;
	sigset_t s;
	int sig, x;
	int stop;

	sigemptyset(&s);
        sigaddset(&s, SIGUSR1);

	// x = pthread_sigmask(SIG_SETMASK, &s, NULL);

	for(;;) {
		x = sigwait(&s, &sig);
		if (x != 0) {
			ses->err = nowdb_err_getRC(nowdb_err_sigwait,
			                             x, OBJECT, NULL);
			break;
		}

		stop = getStop(ses);
		if (stop < 0) break;
		if (stop == 2) break;
		if (stop == 1) continue;

		if (ses->err != NOWDB_OK) {
			LOGERR(ses->err);
			ses->err = NOWDB_OK;
			break;
		}

		runSession(ses);
		if (ses->err != NOWDB_OK) {
			LOGERR(ses->err);
			ses->err = NOWDB_OK;
		}

		stop = getStop(ses);
		if (stop < 0) break;
		if (stop == 2) break;

		leaveSession(ses);

		if (setWaiting(ses) < 0) {
			LOGERR(ses->err);
			ses->err = NOWDB_OK;
			SIGNALEOS();
			break;
		}

		SIGNALEOS();
	}
	ses->alive = 0;
	// fprintf(stderr, "terminating\n");
	return NULL;
}
