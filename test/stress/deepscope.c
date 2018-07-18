/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Run queries on scope in parallel
 * ========================================================================
 */
#include <nowdb/scope/scope.h>
#include <nowdb/query/ast.h>
#include <nowdb/query/stmt.h>
#include <nowdb/query/cursor.h>
#include <nowdb/sql/parser.h>
#include <nowdb/io/dir.h>
#include <common/progress.h>
#include <common/cmd.h>
#include <common/bench.h>

#include <stdlib.h>
#include <stdio.h>
#include <time.h>

#include <pthread.h>

/* -----------------------------------------------------------------------
 * get a little help for my friends
 * -----------------------------------------------------------------------
 */
void helptxt(char *progname) {
	fprintf(stderr, "%s <path-to-base> [options]\n", progname);
	fprintf(stderr, "all options are in the format -opt value\n");
}

/* -----------------------------------------------------------------------
 * options
 * -------
 * global_threads: how many threads run concurrently
 * global_iter: how many queries each thread executes
 * -----------------------------------------------------------------------
 */
uint32_t global_threads = 0;
uint32_t global_iter    = 0;

/* -----------------------------------------------------------------------
 * expected result from the query
 * -----------------------------------------------------------------------
 */
uint64_t global_count = 0;

/* -----------------------------------------------------------------------
 * get options
 * -----------------------------------------------------------------------
 */
int parsecmd(int argc, char **argv) {
	int err = 0;

	global_threads = ts_algo_args_findUint(
	            argc, argv, 2, "threads", 10, &err);
	if (err != 0) {
		fprintf(stderr, "command line error: %d\n", err);
		return -1;
	}
	global_iter = ts_algo_args_findUint(
	            argc, argv, 2, "iter", 100, &err);
	if (err != 0) {
		fprintf(stderr, "command line error: %d\n", err);
		return -1;
	}
	return 0;
}

/* -----------------------------------------------------------------------
 * Have a nap
 * -----------------------------------------------------------------------
 */
void nap() {
	struct timespec tp = {0,1000000};
	nanosleep(&tp,NULL);
}

/* -----------------------------------------------------------------------
 * Thread control Parameters
 * -----------------------------------------------------------------------
 */
typedef struct {
	nowdb_lock_t    lock;
	pthread_mutex_t   mu;
	nowdb_scope_t *scope;
	nowdb_ast_t     *ast;
	int          running;
	int              err;
} params_t;

params_t params;

/* -----------------------------------------------------------------------
 * Init parameters
 * -----------------------------------------------------------------------
 */
int params_init(params_t *params, nowdb_scope_t *scope, nowdb_ast_t *ast) {
	nowdb_err_t err;
	int x;

	params->scope = scope;
	params->ast = ast;
	params->running = 0;
	params->err = 0;

	err = nowdb_lock_init(&params->lock);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot init lock");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	x = pthread_mutex_init(&params->mu, NULL);
	if (x != 0) {
		fprintf(stderr, "cannot init mutex: %d\n", x);
		return -1;
	}
	/*
	x = pthread_cond_init(&params->cond, NULL);
	if (x != 0) {
		fprintf(stderr, "cannot init conditional: %d\n", x);
		return -1;
	}
	*/
	return 0;
}

/* -----------------------------------------------------------------------
 * Destroy parameters
 * -----------------------------------------------------------------------
 */
void params_destroy(params_t *params) {
	pthread_mutex_destroy(&params->mu);
	nowdb_lock_destroy(&params->lock);
}

/* -----------------------------------------------------------------------
 * Process Cursor
 * -----------------------------------------------------------------------
 */
int processCursor(nowdb_cursor_t *cur) {
	int rc = 0;
	uint32_t osz, cnt;
	uint64_t total=0;
	nowdb_err_t err=NOWDB_OK;
	char *buf=NULL;

	buf = malloc(8192);
	if (buf == NULL) {
		fprintf(stderr, "out-of-memory\n");
		return -1;
	}

	err = nowdb_cursor_open(cur);
	if (err != NOWDB_OK) {
		if (err->errcode == nowdb_err_eof) {
			nowdb_err_release(err);
			err = NOWDB_OK;
		}
		goto cleanup;
	}
	for(;;) {
		err = nowdb_cursor_fetch(cur, buf, 8192, &osz, &cnt);
		if (err != NOWDB_OK) {
			if (err->errcode == nowdb_err_eof) {
				nowdb_err_release(err);
				err = NOWDB_OK;
			}
			break;
		}
		total += cnt;
	}
	if (global_count == 0) {
		fprintf(stderr, "setting expected to %lu\n", total);
		global_count = total;
	} else if (total != global_count) {
		fprintf(stderr, "wrong result: %lu\n", total);
		rc = -1; goto cleanup;
	}
cleanup:
	nowdb_cursor_destroy(cur); free(cur);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
	}
	if (buf != NULL) free(buf);
	return rc;
}

/* -----------------------------------------------------------------------
 * Handle ast
 * -----------------------------------------------------------------------
 */
int handleAst(nowdb_scope_t *scope, nowdb_ast_t *ast) {
	nowdb_err_t err;
	nowdb_qry_result_t res;

	err = nowdb_stmt_handle(ast, scope, NULL, NULL, &res);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot handle ast: \n");
		nowdb_ast_show(ast);
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	switch(res.resType) {
	case NOWDB_QRY_RESULT_CURSOR: 
		return processCursor(res.result);
		
	case NOWDB_QRY_RESULT_NOTHING:
	case NOWDB_QRY_RESULT_REPORT:
	case NOWDB_QRY_RESULT_SCOPE:
	case NOWDB_QRY_RESULT_PLAN:
	default:
		fprintf(stderr, "unknown result :-(\n");
		return -1;
	}
	return 0;
}

void *task(void *p) {
	struct timespec t1, t2;
	nowdb_err_t err;
	int x;
	int rc = 0;

	/* protect params */
	err = nowdb_lock(&params.lock);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot lock\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	/* count up */
	params.running++;
	// fprintf(stderr, "running: %d\n", params.running);

	/* protect params */
	err = nowdb_unlock(&params.lock);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot unlock\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}

	/* wait for the others */
	x = pthread_mutex_lock(&params.mu);
	if (x != 0) {
		fprintf(stderr, "cannot lock mutex: %d\n", x);
		return NULL;
	}

	/* advance */
	x = pthread_mutex_unlock(&params.mu);
	if (x != 0) {
		fprintf(stderr, "cannot unlock mutex: %d\n", x);
		return NULL;
	}

	/* do the job */
	timestamp(&t1);
	for(int i=0; i<global_iter; i++) {
		if (handleAst(params.scope, params.ast) != 0) {
			fprintf(stderr, "handle ast failed: %d\n", x);
			rc = -1; break;
		}
	}
	timestamp(&t2);
	fprintf(stderr, "[%lu] avg: %ldus\n",
	        pthread_self(), minus(&t2, &t1)/(global_iter*1000));

	/* protect params */
	err = nowdb_lock(&params.lock);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot unlock\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}

	/* count down */
	params.running--;
	if (rc != 0) params.err++;

	/* protect params */
	err = nowdb_unlock(&params.lock);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot unlock\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	return NULL;
}

/* -----------------------------------------------------------------------
 * Process queries
 * -----------------------------------------------------------------------
 */
int prepareQuery(FILE *stream, nowdb_ast_t **ast) {
	int rc = 0;
	nowdbsql_parser_t parser;

	if (nowdbsql_parser_init(&parser, stream) != 0) {
		fprintf(stderr, "cannot init parser\n");
		return -1;
	}

	rc = nowdbsql_parser_run(&parser, ast);
	if (rc == NOWDB_SQL_ERR_EOF) {
		rc = 0; goto cleanup;
	}
	if (rc != 0) {
		fprintf(stderr, "parsing error\n");
		goto cleanup;
	}
	if (*ast == NULL) {
		fprintf(stderr, "no error, no ast :-(\n");
		rc = -1; goto cleanup;
	}
cleanup:
	nowdbsql_parser_destroy(&parser);
	return rc;
}

/* -----------------------------------------------------------------------
 * Wait for something (either all threads running or all threads stopped)
 * -----------------------------------------------------------------------
 */
int64_t waitForEvent(int event) {
	struct timespec t1, t2;
	nowdb_err_t err;
	int count;

	timestamp(&t1);
	for(;;) {
		err = nowdb_lock(&params.lock);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			return -1;
		}
		count = params.running;
		err = nowdb_unlock(&params.lock);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			return -1;
		}
		if (count == event) break;
		// fprintf(stderr, "waiting: %d\n", count);
		nap();
	}
	timestamp(&t2);
	return minus(&t2,&t1);
}

/* -----------------------------------------------------------------------
 * Start threads
 * -----------------------------------------------------------------------
 */
int initThreads(pthread_t **tids, int threads) {
	int x;

	*tids = calloc(threads,sizeof(pthread_t));
	if (*tids == NULL) {
		fprintf(stderr, "out-of-memory\n");
		return -1;
	}
	for(int i=0;i<threads;i++) {
		x = pthread_create((*tids)+i, NULL, &task, NULL);
		if (x != 0) {
			fprintf(stderr, "cannot create pthread: %d\n", x);
			return -1;
		}
	}
	return 0;
}

/* -----------------------------------------------------------------------
 * Stop and detach threads
 * -----------------------------------------------------------------------
 */
void destroyThreads(pthread_t *tids, int threads) {
	int x;
	for(int i=0; i<threads; i++) {
		if (tids[i] == 0) break;
		x = pthread_join(tids[i], NULL);
		if (x != 0) {
			fprintf(stderr, "cannot create pthread: %d\n", x);
		}
	}
	free(tids);
}

/* -----------------------------------------------------------------------
 * Run threads
 * -----------------------------------------------------------------------
 */
int runThreads(nowdb_scope_t *scope, nowdb_ast_t *ast) {
	int x;
	int64_t d;
	pthread_t *tids;

	if (params_init(&params, scope, ast) != 0) {
		fprintf(stderr, "cannot init params\n");
		return -1;
	}
	x = pthread_mutex_lock(&params.mu);
	if (x != 0) {
		fprintf(stderr, "cannot lock mu: %d\n", x);
		return -1;
	}
	if (initThreads(&tids, global_threads) != 0) {
		fprintf(stderr, "cannot start threads\n");
		return -1;
	}
	d = waitForEvent(global_threads);
	if (d < 0) {
		fprintf(stderr, "cannot wait for start\n");
		return -1;
	}
	fprintf(stderr, "startup time: %ldus\n", d/1000);
	x = pthread_mutex_unlock(&params.mu);
	if (x != 0) {
		fprintf(stderr, "cannot unlock mu: %d\n", x);
		return -1;
	}
	d = waitForEvent(0);
	if (d < 0) {
		fprintf(stderr, "cannot wait end\n");
		return -1;
	}
	fprintf(stderr, "running time: %ldus\n", d/1000);
	destroyThreads(tids, global_threads);
	params_destroy(&params);
	return 0;
}

/* -----------------------------------------------------------------------
 * main
 * -----------------------------------------------------------------------
 */
int main(int argc, char **argv) {
	int rc = EXIT_SUCCESS;
	nowdb_err_t      err;
	nowdb_path_t path;
	nowdb_ast_t  *ast=NULL;
	nowdb_scope_t *scope=NULL;

	if (argc < 2) {
		helptxt(argv[0]);
		return EXIT_FAILURE;
	}
	path = argv[1];
	if (strnlen(path, 4096) == 4096) {
		fprintf(stderr, "path too long (max: 4096)\n");
		return EXIT_FAILURE;
	}
	if (path[0] == '-') {
		fprintf(stderr, "invalid path\n");
		helptxt(argv[0]);
		return EXIT_FAILURE;
	}
	if (parsecmd(argc, argv) != 0) {
		helptxt(argv[0]);
		return EXIT_FAILURE;
	}
	fprintf(stderr, "starting with %u threads and %u iterations each\n",
	        global_threads, global_iter);
	if (!nowdb_init()) {
		fprintf(stderr, "cannot init library\n");
		return EXIT_FAILURE;
	}
	err = nowdb_scope_new(&scope, path, 1);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot make scope\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		rc = EXIT_FAILURE; goto cleanup;
	}
	err = nowdb_scope_open(scope);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot open scope\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (prepareQuery(stdin, &ast) != 0) {
		fprintf(stderr, "cannot run query\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (handleAst(scope, ast) != 0) {
		fprintf(stderr, "cannot handle ast\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (runThreads(scope, ast) != 0) {
		fprintf(stderr, "cannot run threads\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

cleanup:
	if (scope != NULL) {
		err = nowdb_scope_close(scope);
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot close scope\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
		}
		nowdb_scope_destroy(scope); free(scope);
	}
	if (ast != NULL) {
		nowdb_ast_destroy(ast); free(ast);
	}
	nowdb_close();
	return rc;
}
