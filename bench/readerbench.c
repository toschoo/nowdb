/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Benchmarking plain file read
 * ========================================================================
 */
#include <nowdb/io/file.h>
#include <common/progress.h>
#include <common/cmd.h>
#include <common/bench.h>
#include <nowdb/scope/scope.h>
#include <nowdb/reader/reader.h>

#include <stdlib.h>

uint32_t global_count = 1000000;
uint32_t global_iter = 1;
char *global_qry_type = NULL;
char *global_context  = NULL;

int parsecmd(int argc, char **argv) {
	int err = 0;

	global_count = (uint32_t)ts_algo_args_findUint(
	            argc, argv, 2, "count", 1000, &err);
	if (err != 0) {
		fprintf(stderr, "command line error: %d\n", err);
		return -1;
	}
	global_qry_type = ts_algo_args_findString(
	            argc, argv, 2, "query", "fullscan", &err);
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
	global_iter = (uint32_t)ts_algo_args_findUint(
	            argc, argv, 2, "iter", 1, &err);
	if (err != 0) {
		fprintf(stderr, "command line error: %d\n", err);
		return -1;
	}
	return 0;
}

void helptxt(char *progname) {
	fprintf(stderr, "%s <path-to-scope> [options]\n", progname);
	fprintf(stderr, "the scope must exist!\n");
	fprintf(stderr, "all options are in the format -opt value\n");
	fprintf(stderr, "-count n: number of edges to read\n");
	fprintf(stderr, "-conext c: existing contet within that scope\n");
	fprintf(stderr, "-query t: query type; possible values:\n");
	fprintf(stderr, "          'fullscan' : performs a simple fullscan\n");
	fprintf(stderr, "          'fullscan+': performs a fullscan +\n");
	fprintf(stderr, "                       a scan over pending files\n");
	fprintf(stderr, "          'search'   : performs an index search\n");
	fprintf(stderr, "          'search+'  : performs an index search +\n");
	fprintf(stderr, "                       a scan over pending files\n");
	fprintf(stderr, "          'range'    : performs a range scan\n");
	fprintf(stderr, "          'range+'   : performs a range scan +\n");
	fprintf(stderr, "                       a scan over pending files\n");
	fprintf(stderr, "-iter   n: number of iterations (default: 1)\n");
}

typedef struct {
	uint64_t     count;
	nowdb_time_t overall;
	nowdb_time_t overhead;
} result_t;

nowdb_bool_t performRead(nowdb_context_t *ctx, result_t *res) {
	struct timespec t1, t2, t3;

	nowdb_err_t err;
	nowdb_reader_t *reader;
	ts_algo_list_t files;
	uint64_t s = 0;

	timestamp(&t1);

	ts_algo_list_init(&files);
	err = nowdb_store_getReaders(&ctx->store, &files,
	                                 NOWDB_TIME_DAWN,
	                                 NOWDB_TIME_DUSK);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot get readers :-(\n");
		nowdb_err_print(err); nowdb_err_release(err);
		return FALSE;
	}
	err = nowdb_reader_fullscan(&reader, &files, NULL);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create reader :-(\n");
		nowdb_err_print(err); nowdb_err_release(err);
		nowdb_store_destroyFiles(&ctx->store, &files);
		return FALSE;
	}
	nowdb_store_destroyFiles(&ctx->store, &files);

	timestamp(&t2);

	for(;;) {
		err = nowdb_reader_move(reader);
		if (err != NOWDB_OK) {
			if (err->errcode != nowdb_err_eof) {
				fprintf(stderr, "move failed\n");
				nowdb_err_print(err);
				nowdb_err_release(err);
				nowdb_reader_destroy(reader); free(reader);
				return FALSE;
			}
			nowdb_err_release(err); break;
		}
		for(int i=0; i<NOWDB_IDX_PAGE; i+= reader->recsize) {
			s++;
		}
		if (s >= global_count) break;
	}
	nowdb_reader_destroy(reader); free(reader);
	timestamp(&t3);

	res->count    = s;
	res->overall  = minus(&t3, &t1)/1000;
	res->overhead = minus(&t2, &t1)/1000;
	
	return TRUE;
}

int main(int argc, char **argv) {
	nowdb_err_t err;
	int rc = EXIT_SUCCESS;
	nowdb_scope_t *scope = NULL;
	nowdb_context_t *ctx = NULL;
	nowdb_path_t path;
	result_t res;
	uint64_t *overall=NULL;
	uint64_t *overhead=NULL;
	uint64_t as = 0, hs = 0;
	uint64_t amx = 0, amn = 0xffffffffffffffff;
	uint64_t hmx = 0, hmn = 0xffffffffffffffff;

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
	fprintf(stderr, "count: %u\n", global_count);

	if (!nowdb_err_init()) {
		fprintf(stderr, "cannot init library\n");
		return EXIT_FAILURE;
	}
	err = nowdb_scope_new(&scope, path, 1);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		rc = EXIT_FAILURE; goto cleanup;
	}
	err = nowdb_scope_open(scope);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		rc = EXIT_FAILURE; goto cleanup;
	}
	err = nowdb_scope_getContext(scope, global_context, &ctx);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		rc = EXIT_FAILURE; goto cleanup;
	}

	overall = calloc(global_iter, sizeof(uint64_t));
	if (overall == NULL) {
		fprintf(stderr, "not enough memory\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

	overhead = calloc(global_iter, sizeof(uint64_t));
	if (overhead == NULL) {
		fprintf(stderr, "not enough memory\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	for(int i=0; i<global_iter; i++) {
		if (!performRead(ctx, &res)) {
			rc = EXIT_FAILURE; goto cleanup;
		}
		overall[i]  = res.overall;
		overhead[i] = res.overhead;
		as += res.overall;
		hs += res.overhead;
		if (amx < res.overall) amx = res.overall;
		if (amn > res.overall) amn = res.overall;
		if (hmx < res.overhead) hmx = res.overhead;
		if (hmn > res.overhead) hmn = res.overhead;
	}
	sort(overall, global_iter);

	fprintf(stdout, "effectively read: %lu\n", res.count);

	fprintf(stdout, "average : %lu / %lu\n",
	                       as/global_iter,
	                       hs/global_iter);
	fprintf(stdout, "median  : %lu / %lu\n",
	          median(overall, global_iter),
	          median(overhead, global_iter));
	fprintf(stdout, "max     : %lu / %lu\n", amx, hmx);
	fprintf(stdout, "min     : %lu / %lu\n", amn, hmn);
	fprintf(stdout, "75%%     : %lu / %lu\n", 
	                percentile(overall, global_iter, 75),
	                percentile(overhead, global_iter, 75));
	fprintf(stdout, "90%%     : %lu / %lu\n",
	                percentile(overall, global_iter, 90),
	                percentile(overhead, global_iter, 90));
	fprintf(stdout, "95%%     : %lu / %lu\n",
	                percentile(overall, global_iter, 95),
	                percentile(overhead, global_iter, 95));
	fprintf(stdout, "99%%     : %lu / %lu\n",
	                percentile(overall, global_iter, 99),
	                percentile(overhead, global_iter, 99));
	fprintf(stdout, "run time: %luus / %luus\n", as, hs);

cleanup:
	if (overall != NULL) free(overall);
	if (overhead!= NULL) free(overhead);
	if (scope != NULL) {
		NOWDB_IGNORE(nowdb_scope_close(scope));
		nowdb_scope_destroy(scope); free(scope);
	}
	nowdb_err_destroy();
	return rc;
}

