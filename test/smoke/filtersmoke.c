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

nowdb_filter_t *mkBool(int op) {
	nowdb_err_t   err;
	nowdb_filter_t *f;

	err = nowdb_filter_newBool(&f, op);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot make new bool\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	return f;
}

int testTrue() {
	nowdb_bool_t x;
	nowdb_filter_t *f;

	f = mkBool(NOWDB_FILTER_TRUE);
	if (f == NULL) return 0;

	x = nowdb_filter_eval(f,NULL);
	nowdb_filter_destroy(f); free(f);

	return x;
}

int testFalse() {
	nowdb_bool_t x;
	nowdb_filter_t *f;

	f = mkBool(NOWDB_FILTER_FALSE);
	if (f == NULL) return 0;

	x = nowdb_filter_eval(f,NULL);
	nowdb_filter_destroy(f); free(f);

	return x;
}

int testNot() {
	nowdb_bool_t x;
	nowdb_filter_t *f;

	f = mkBool(NOWDB_FILTER_NOT);
	if (f == NULL) return 0;

	f->left = mkBool(NOWDB_FILTER_FALSE);
	if (f->left == NULL) return 0;

	x = nowdb_filter_eval(f,NULL);
	nowdb_filter_destroy(f); free(f);

	if (!x) return 0;

	f = mkBool(NOWDB_FILTER_NOT);
	if (f == NULL) return 0;

	f->left = mkBool(NOWDB_FILTER_TRUE);
	if (f->left == NULL) return 0;

	x = nowdb_filter_eval(f,NULL);
	nowdb_filter_destroy(f); free(f);

	if (x) return 0;

	return 1;
}

int testJust() {
	nowdb_bool_t x;
	nowdb_filter_t *f;

	f = mkBool(NOWDB_FILTER_JUST);
	if (f == NULL) return 0;

	f->left = mkBool(NOWDB_FILTER_FALSE);
	if (f->left == NULL) return 0;

	x = nowdb_filter_eval(f,NULL);
	nowdb_filter_destroy(f); free(f);

	if (x) return 0;

	f = mkBool(NOWDB_FILTER_JUST);
	if (f == NULL) return 0;

	f->left = mkBool(NOWDB_FILTER_TRUE);
	if (f->left == NULL) return 0;

	x = nowdb_filter_eval(f,NULL);
	nowdb_filter_destroy(f); free(f);

	if (!x) return 0;

	return 1;
}

int testOr() {
	nowdb_bool_t x;
	nowdb_filter_t *f;

	for(int i=0;i<4;i++) {
		f = mkBool(NOWDB_FILTER_OR);
		if (f == NULL) return 0;

		if (i==0 || i == 1) {
			f->left = mkBool(NOWDB_FILTER_FALSE);
		} else {
			f->left = mkBool(NOWDB_FILTER_TRUE);
		}
		if (f->left == NULL) return 0;

		if (i == 0 || i == 2) {
			f->right = mkBool(NOWDB_FILTER_FALSE);
		} else {
			f->right = mkBool(NOWDB_FILTER_TRUE);
		}
		if (f->right == NULL) return 0;

		x = nowdb_filter_eval(f,NULL);
		nowdb_filter_destroy(f); free(f);

		fprintf(stderr, "[OR] case %d: %d\n", i, x);

		switch(i) {
		case 0: if (x) return 0; break;
		default: if (!x) return 0;
		}
	}
	return 1;
}

int testAnd() {
	nowdb_bool_t x;
	nowdb_filter_t *f;

	for(int i=0;i<4;i++) {
		f = mkBool(NOWDB_FILTER_AND);
		if (f == NULL) return 0;

		if (i==0 || i == 1) {
			f->left = mkBool(NOWDB_FILTER_FALSE);
		} else {
			f->left = mkBool(NOWDB_FILTER_TRUE);
		}
		if (f->left == NULL) return 0;

		if (i == 0 || i == 2) {
			f->right = mkBool(NOWDB_FILTER_FALSE);
		} else {
			f->right = mkBool(NOWDB_FILTER_TRUE);
		}
		if (f->right == NULL) return 0;

		x = nowdb_filter_eval(f,NULL);
		nowdb_filter_destroy(f); free(f);

		fprintf(stderr, "[AND] case %d: %d\n", i, x);

		switch(i) {
		case 3: if (!x) return 0; break;
		default: if (x) return 0;
		}
	}
	return 1;
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

int testEQInt() {
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

int testLEInt() {
	nowdb_edge_t e;
	int64_t v;
	nowdb_bool_t    x;
	nowdb_filter_t *f;

	v = 5;
	e.weight = 5;
	f = mkCompare(NOWDB_FILTER_LE, 40, 8,
	              NOWDB_TYP_INT, &v);
	if (f == NULL)  return 0;

	x = nowdb_filter_eval(f,&e);
	nowdb_filter_destroy(f); free(f);

	if (!x) return 0;

	e.weight = 6;
	f = mkCompare(NOWDB_FILTER_LE, 40, 8,
	              NOWDB_TYP_INT, &v);
	if (f == NULL)  return 0;

	x = nowdb_filter_eval(f,&e);
	nowdb_filter_destroy(f); free(f);

	if (x) return 0;

	e.weight = 4;
	f = mkCompare(NOWDB_FILTER_LE, 40, 8,
	              NOWDB_TYP_INT, &v);
	if (f == NULL)  return 0;

	x = nowdb_filter_eval(f,&e);
	nowdb_filter_destroy(f); free(f);

	if (!x) return 0;

	return 1;
}

int testEQUInt() {
	nowdb_edge_t e;
	uint64_t v;
	nowdb_bool_t    x;
	nowdb_filter_t *f;

	v = 5;
	e.weight = 5;
	f = mkCompare(NOWDB_FILTER_EQ, 40, 8,
	              NOWDB_TYP_UINT, &v);
	if (f == NULL)  return 0;

	x = nowdb_filter_eval(f,&e);
	nowdb_filter_destroy(f); free(f);

	if (!x) return 0;

	e.weight = 6;
	f = mkCompare(NOWDB_FILTER_EQ, 40, 8,
	              NOWDB_TYP_UINT, &v);
	if (f == NULL)  return 0;

	x = nowdb_filter_eval(f,&e);
	nowdb_filter_destroy(f); free(f);

	if (x) return 0;

	return 1;
}

int testEQFloat() {
	nowdb_edge_t e;
	double v;
	nowdb_bool_t    x;
	nowdb_filter_t *f;

	v = 5;
	memcpy(&e.weight, &v, 8);
	f = mkCompare(NOWDB_FILTER_EQ, 40, 8,
	              NOWDB_TYP_FLOAT, &v);
	if (f == NULL)  return 0;

	x = nowdb_filter_eval(f,&e);
	nowdb_filter_destroy(f); free(f);

	if (!x) return 0;

	v = 5.1;
	f = mkCompare(NOWDB_FILTER_EQ, 40, 8,
	              NOWDB_TYP_FLOAT, &v);
	if (f == NULL)  return 0;

	x = nowdb_filter_eval(f,&e);
	nowdb_filter_destroy(f); free(f);

	if (x) return 0;

	memcpy(&e.weight, &v, 8);
	f = mkCompare(NOWDB_FILTER_EQ, 40, 8,
	              NOWDB_TYP_FLOAT, &v);
	if (f == NULL)  return 0;

	x = nowdb_filter_eval(f,&e);
	nowdb_filter_destroy(f); free(f);

	if (!x) return 0;

	v = 5;
	f = mkCompare(NOWDB_FILTER_EQ, 40, 8,
	              NOWDB_TYP_FLOAT, &v);
	if (f == NULL)  return 0;

	x = nowdb_filter_eval(f,&e);
	nowdb_filter_destroy(f); free(f);

	if (x) return 0;

	return 1;
}

int testLEFloat() {
	nowdb_edge_t e;
	double v;
	nowdb_bool_t    x;
	nowdb_filter_t *f;

	v = 5;
	memcpy(&e.weight, &v, 8);
	f = mkCompare(NOWDB_FILTER_LE, 40, 8,
	              NOWDB_TYP_FLOAT, &v);
	if (f == NULL)  return 0;

	x = nowdb_filter_eval(f,&e);
	nowdb_filter_destroy(f); free(f);

	if (!x) return 0;

	v = 5.1;
	f = mkCompare(NOWDB_FILTER_LE, 40, 8,
	              NOWDB_TYP_FLOAT, &v);
	if (f == NULL)  return 0;

	x = nowdb_filter_eval(f,&e);
	nowdb_filter_destroy(f); free(f);

	if (!x) return 0;

	memcpy(&e.weight, &v, 8);
	f = mkCompare(NOWDB_FILTER_LE, 40, 8,
	              NOWDB_TYP_FLOAT, &v);
	if (f == NULL)  return 0;

	x = nowdb_filter_eval(f,&e);
	nowdb_filter_destroy(f); free(f);

	if (!x) return 0;

	v = 5;
	f = mkCompare(NOWDB_FILTER_LE, 40, 8,
	              NOWDB_TYP_FLOAT, &v);
	if (f == NULL)  return 0;

	x = nowdb_filter_eval(f,&e);
	nowdb_filter_destroy(f); free(f);

	if (x) return 0;

	return 1;
}


int testNEInt() {
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

int testNEFloat() {
	nowdb_edge_t e;
	double v;
	nowdb_bool_t    x;
	nowdb_filter_t *f;

	v = 5;
	memcpy(&e.weight, &v, 8);
	f = mkCompare(NOWDB_FILTER_NE, 40, 8,
	              NOWDB_TYP_FLOAT, &v);
	if (f == NULL)  return 0;

	x = nowdb_filter_eval(f,&e);
	nowdb_filter_destroy(f); free(f);

	if (x) return 0;

	v = 5.1;
	f = mkCompare(NOWDB_FILTER_NE, 40, 8,
	              NOWDB_TYP_FLOAT, &v);
	if (f == NULL)  return 0;

	x = nowdb_filter_eval(f,&e);
	nowdb_filter_destroy(f); free(f);

	if (!x) return 0;

	memcpy(&e.weight, &v, 8);
	f = mkCompare(NOWDB_FILTER_NE, 40, 8,
	              NOWDB_TYP_FLOAT, &v);
	if (f == NULL)  return 0;

	x = nowdb_filter_eval(f,&e);
	nowdb_filter_destroy(f); free(f);

	if (x) return 0;

	v = 5;
	f = mkCompare(NOWDB_FILTER_NE, 40, 8,
	              NOWDB_TYP_FLOAT, &v);
	if (f == NULL)  return 0;

	x = nowdb_filter_eval(f,&e);
	nowdb_filter_destroy(f); free(f);

	if (!x) return 0;

	return 1;
}


int main() {
	int rc = EXIT_SUCCESS;

	if (!nowdb_err_init()) {
		fprintf(stderr, "cannot initialise error handler\n");
		return EXIT_FAILURE;
	}
	if (!testTrue()) {
		fprintf(stderr, "testTrue() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (testFalse()) {
		fprintf(stderr, "testFalse() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testNot()) {
		fprintf(stderr, "testNot() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testJust()) {
		fprintf(stderr, "testJust() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testOr()) {
		fprintf(stderr, "testOr() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testAnd()) {
		fprintf(stderr, "testAnd() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testEQInt()) {
		fprintf(stderr, "testEQInt() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testLEInt()) {
		fprintf(stderr, "testLEInt() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testNEInt()) {
		fprintf(stderr, "testNEInt() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testEQUInt()) {
		fprintf(stderr, "testEQUint() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testEQFloat()) {
		fprintf(stderr, "testEQFloat() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testLEFloat()) {
		fprintf(stderr, "testLEFloat() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testNEFloat()) {
		fprintf(stderr, "testNEFloat() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

cleanup:
	nowdb_err_destroy();
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}
