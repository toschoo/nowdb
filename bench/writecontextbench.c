/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Benchmarking insert into context 
 * ========================================================================
 */
#include <nowdb/scope/scope.h>
#include <nowdb/io/dir.h>
#include <common/progress.h>
#include <common/cmd.h>
#include <common/bench.h>

/* -----------------------------------------------------------------------
 * get a little help for my friends
 * -----------------------------------------------------------------------
 */
void helptxt(char *progname) {
	fprintf(stderr, "%s <path-to-file> [options]\n", progname);
	fprintf(stderr, "all options are in the format -opt value\n");
	fprintf(stderr, "-count n: number of edges to insert\n");
	fprintf(stderr, "-nocomp b: don't use compression\n");
	fprintf(stderr, "-nosort b: don't use sorting\n");
	fprintf(stderr, "           (b: 1/0, t/f, true/false)\n");
	fprintf(stderr, "-report n: report every n inserts\n");
	fprintf(stderr, "-context <name>: name of the context to use\n");
	fprintf(stderr, "                 if the context does not exists\n");
	fprintf(stderr, "                 it will be created.\n");
}

/* -----------------------------------------------------------------------
 * For large values of 'count', this will insert around 100K 'keys'
 * where key means, distinct values for origin, destin and edge
 * -----------------------------------------------------------------------
 */
nowdb_bool_t insertEdges(nowdb_context_t *ctx, uint64_t count) {
	nowdb_err_t err;
	nowdb_edge_t e;

	memset(&e,0,64);
	e.wtype[0] = NOWDB_TYP_UINT;

	for(uint32_t i=0; i<count; i++) {

		do e.origin = rand()%100; while(e.origin == 0);
		do e.destin = rand()%100; while(e.destin == 0);
		do e.edge   = rand()%10; while(e.edge == 0);
		e.weight = (uint64_t)i;
		// do e.label  = rand()%10; while(e.label == 0);
		if (i%10 == 0) {
			err = nowdb_time_now(&e.timestamp);
			if (err != NOWDB_OK) {
				fprintf(stderr, "insert error\n");
				nowdb_err_print(err);
				nowdb_err_release(err);
				return FALSE;
			}
		}

		err = nowdb_context_insert(ctx, &e);
		if (err != NOWDB_OK) {
			fprintf(stderr, "insert error\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
	}
	return TRUE;
}

/* -----------------------------------------------------------------------
 * options
 * -------
 * global_count: how many edges we will insert
 * global_report: report back every n edges
 * global_context: the context in which to insert
 * global_nocomp: don't compress
 * global_nosort: don't sort
 * -----------------------------------------------------------------------
 */
uint64_t global_count = 1000;
uint32_t global_report = 1;
char *global_context = NULL;
int global_nocomp = 0;
int global_nosort = 0;

/* -----------------------------------------------------------------------
 * get options
 * -----------------------------------------------------------------------
 */
int parsecmd(int argc, char **argv) {
	int err = 0;

	global_count = ts_algo_args_findUint(
	            argc, argv, 2, "count", 1000, &err);
	if (err != 0) {
		fprintf(stderr, "command line error: %d\n", err);
		return -1;
	}
	global_nocomp = ts_algo_args_findBool(
	            argc, argv, 2, "nocomp", 0, &err);
	if (err != 0) {
		fprintf(stderr, "command line error: %d\n", err);
		return -1;
	}
	global_nocomp = ts_algo_args_findBool(
	            argc, argv, 2, "nosort", 0, &err);
	if (err != 0) {
		fprintf(stderr, "command line error: %d\n", err);
		return -1;
	}
	global_report = (uint32_t)ts_algo_args_findUint(
	            argc, argv, 2, "report", global_count, &err);
	if (err != 0) {
		fprintf(stderr, "command line error: %d\n", err);
		return -1;
	}
	global_context = ts_algo_args_findString(
	            argc, argv, 2, "context", "bench", &err);
	if (err != 0) {
		fprintf(stderr, "command line error: %d\n", err);
		return -1;
	}
	return 0;
}

/* -----------------------------------------------------------------------
 * try to open scope, create it if not there
 * -----------------------------------------------------------------------
 */
nowdb_scope_t *getScope(nowdb_path_t path) {
	nowdb_err_t err;
	nowdb_scope_t *scope;

	err = nowdb_scope_new(&scope, path, 1);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	if (nowdb_path_exists(path, NOWDB_DIR_TYPE_DIR)) {
		err = nowdb_scope_open(scope);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			nowdb_scope_destroy(scope); free(scope);
			return NULL;
		}
		return scope;
	}
	err = nowdb_scope_create(scope);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_scope_destroy(scope); free(scope);
		return NULL;
	}
	err = nowdb_scope_open(scope);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_scope_destroy(scope); free(scope);
		return NULL;
	}
	return scope;
}

/* -----------------------------------------------------------------------
 * get context, create it if not there
 * -----------------------------------------------------------------------
 */
nowdb_context_t *getContext(nowdb_scope_t *scope, char *name) {
	nowdb_context_t *ctx;
	nowdb_ctx_config_t cfg;
	nowdb_err_t err;
	nowdb_bitmap64_t options;

	/* if the context does already exist, we use it */
	err = nowdb_scope_getContext(scope, name, &ctx);
	if (err == NOWDB_OK) return ctx;

	if (err->errcode != nowdb_err_key_not_found) {
		fprintf(stderr, "cannot get context\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	nowdb_err_release(err);

	/* otherwise we create it 
	 * with the following configuration */
	if (global_count < 100000) {
		options = NOWDB_CONFIG_SIZE_TINY;

	} else if (global_count < 1000000) {
		options = NOWDB_CONFIG_SIZE_SMALL;

	} else if (global_count < 10000000) {
		options = NOWDB_CONFIG_SIZE_SMALL |
		          NOWDB_CONFIG_INSERT_CONSTANT;

	} else if (global_count < 100000000) {
		options = NOWDB_CONFIG_SIZE_MEDIUM |
		          NOWDB_CONFIG_INSERT_STRESS;
		fprintf(stderr, "%lu\n", options);

	} else if (global_count < 10000000000lu) {
		options = NOWDB_CONFIG_SIZE_BIG |
		          NOWDB_CONFIG_INSERT_STRESS;

	} else if (global_count < 100000000000lu) {
		options = NOWDB_CONFIG_SIZE_LARGE |
		          NOWDB_CONFIG_INSERT_STRESS;
	} else {
		options = NOWDB_CONFIG_SIZE_HUGE |
		          NOWDB_CONFIG_INSERT_INSANE;
	}

	if (global_nocomp) options |= NOWDB_CONFIG_NOCOMP;
	if (global_nosort) options |= NOWDB_CONFIG_NOSORT;

	nowdb_ctx_config(&cfg, options);

	err = nowdb_scope_createContext(scope, name, &cfg);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create context\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	err = nowdb_scope_getContext(scope, name, &ctx);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot get context\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	return ctx;
}

/* -----------------------------------------------------------------------
 * get context, create it if not there
 * -----------------------------------------------------------------------
 */
int main(int argc, char **argv) {
	int rc = EXIT_SUCCESS;
	nowdb_err_t      err;
	nowdb_scope_t   *scope = NULL;
	nowdb_context_t *ctx = NULL;
	nowdb_path_t path;
	struct timespec t1, t2;
	uint32_t runs;
	uint64_t d=0;

	if (argc < 2) {
		helptxt(argv[0]);
		return EXIT_FAILURE;
	}
	path = argv[1];
	if (strnlen(path, 4097) == 4097) {
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
	fprintf(stderr, "count: %lu\n", global_count);

	if (!nowdb_err_init()) {
		fprintf(stderr, "cannot init library\n");
		return EXIT_FAILURE;
	}

	scope = getScope(path);
	if (scope == NULL) {
		fprintf(stderr, "no scope\n");
		return EXIT_FAILURE;
	}

	ctx = getContext(scope, global_context);
	if (ctx == NULL) {
		fprintf(stderr, "no context (%s)\n", global_context);
		rc = EXIT_FAILURE; goto cleanup;
	}

	fprintf(stderr, "got context %s\n", ctx->name);

	runs = global_count/global_report;
	for(int i=0; i<runs; i++) {
		timestamp(&t1);
		if (!insertEdges(ctx, global_report)) {
			rc = EXIT_FAILURE; goto cleanup;
		}
		timestamp(&t2);
		d += minus(&t2, &t1)/1000;
		if (global_report != global_count) {
			fprintf(stdout, "%u: %luus\n", global_report,
			                       minus(&t2, &t1)/1000);
		}
	}
	fprintf(stdout, "Running time: %luus\n", d);
	nowdb_task_sleep(1000000000);
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
	nowdb_err_destroy();
	return rc;
}
