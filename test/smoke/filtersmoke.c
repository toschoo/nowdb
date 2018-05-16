/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for filters
 * ========================================================================
 */
#include <nowdb/reader/filter.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

int testTrue() {
	nowdb_bool_t x;
	nowdb_err_t err;
	nowdb_filter_t *f;

	err = nowdb_filter_newBool(&f, NOWDB_FILTER_TRUE);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot make new bool\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return 0;
	}
	x = nowdb_filter_eval(f,NULL);
	nowdb_filter_destroy(f); free(f);
	return x;
}

int testFalse() {
	nowdb_bool_t x;
	nowdb_err_t err;
	nowdb_filter_t *f;

	err = nowdb_filter_newBool(&f, NOWDB_FILTER_FALSE);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot make new bool\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return 0;
	}
	x = nowdb_filter_eval(f,NULL);
	nowdb_filter_destroy(f); free(f);
	return x;
}

nowdb_filter_t *mkCompare(int op, int off, int size, int typ, void *val) {
	nowdb_err_t   err;
	nowdb_filter_t *f;

	err = nowdb_filter_newCompare(&f, op, off, size, typ, val);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot make new compare\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	return f;
}

int testEQ() {
	nowdb_edge_t e;
	int64_t v;
	nowdb_bool_t    x;
	nowdb_filter_t *f;

	v = 5;
	e.weight = 5;
	f = mkCompare(NOWDB_FILTER_EQ, 40, 8,
	              NOWDB_TYP_INT, &v);
	if (f == NULL)  return 0;

	x = nowdb_filter_eval(f,&e);
	nowdb_filter_destroy(f); free(f);

	if (!x) return 0;

	e.weight = 6;
	f = mkCompare(NOWDB_FILTER_EQ, 40, 8,
	              NOWDB_TYP_INT, &v);
	if (f == NULL)  return 0;

	x = nowdb_filter_eval(f,&e);
	nowdb_filter_destroy(f); free(f);

	if (x) return 0;

	return 1;
}

int testNE() {
	nowdb_edge_t e;
	int64_t v;
	nowdb_bool_t    x;
	nowdb_filter_t *f;

	v = 5;
	e.weight = 6;
	f = mkCompare(NOWDB_FILTER_NE, 40, 8,
	              NOWDB_TYP_INT, &v);
	if (f == NULL)  return 0;

	x = nowdb_filter_eval(f,&e);
	nowdb_filter_destroy(f); free(f);
	return x;
}

int main() {
	int rc = EXIT_SUCCESS;

	if (!testTrue()) {
		fprintf(stderr, "testTrue() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testFalse()) {
		fprintf(stderr, "testFalse() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testEQ()) {
		fprintf(stderr, "testEQ() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testNE()) {
		fprintf(stderr, "testNE() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

	

cleanup:
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}

