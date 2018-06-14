/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for index manager
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/index/index.h>
#include <nowdb/index/catalog.h>
#include <nowdb/index/man.h>

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

#define CATPATH "rsc/iman10"

static ts_algo_cmp_t ctxcompare(void *ignore, void *left, void *right) {
	int cmp = strcmp(((nowdb_context_t*)left)->name,
	                 ((nowdb_context_t*)right)->name);
	if (cmp < 0) return ts_algo_cmp_less;
	if (cmp > 0) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * Tree callbacks for contexts: update (do nothing!)
 * ------------------------------------------------------------------------
 */
static ts_algo_rc_t noupdate(void *ignore, void *o, void *n) {
	return TS_ALGO_OK;
}

/* ------------------------------------------------------------------------
 * Tree callbacks for contexts: delete and destroy
 * ------------------------------------------------------------------------
 */
static void ctxdestroy(void *ignore, void **n) {
	if (*n != NULL) {
		nowdb_context_destroy((nowdb_context_t*)(*n));
		free(*n); *n = NULL;
	}
}

nowdb_index_cat_t *makeICat(char *path,
                   ts_algo_tree_t *ctx) {
	nowdb_err_t err;
	nowdb_index_cat_t *icat;

	err = nowdb_index_cat_open(path, ctx, &icat);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	return icat;
}

ts_algo_tree_t *fakeCtx() {
	ts_algo_tree_t *ctx;
	ctx = ts_algo_tree_new(&ctxcompare, NULL, &noupdate,
	                       &ctxdestroy, &ctxdestroy);
	/* insert some contexts ... */
	return ctx;
}

int initMan(nowdb_index_man_t *iman,
            ts_algo_tree_t    *ctx) {
	nowdb_index_cat_t *cat;
	nowdb_err_t err;

	cat = makeICat(CATPATH, ctx);
	if (cat == NULL) {
		fprintf(stderr, "cannot create ICat\n");
		return -1;
	}
	err = nowdb_index_man_init(iman, cat);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot init iman\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_index_cat_close(cat); free(cat);
		return -1;
	}
	return 0;
}

int testOpenClose(nowdb_index_man_t *iman,
                  ts_algo_tree_t    *ctx) {
	if (initMan(iman, ctx) != 0) return -1;
	nowdb_index_man_destroy(iman);
	return 0;
}

int main() {
	int rc = EXIT_SUCCESS;
	nowdb_index_man_t man;
	ts_algo_tree_t *ctx=NULL;
	int haveMan = 0;

	if (!nowdb_err_init()) {
		fprintf(stderr, "cannot init error\n");
		return EXIT_FAILURE;
	}
	ctx = fakeCtx();
	if (ctx == NULL) {
		fprintf(stderr, "cannot fake context\n");
		return EXIT_FAILURE;
	}
	if (testOpenClose(&man, ctx) != 0) {
		fprintf(stderr, "testOpenClose failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (initMan(&man, ctx) != 0) {
		fprintf(stderr, "initMan failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	haveMan = 1;

cleanup:
	if (haveMan) nowdb_index_man_destroy(&man);
	if (ctx != NULL) {
		ts_algo_tree_destroy(ctx); free(ctx);
	}
	nowdb_err_destroy();
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}
