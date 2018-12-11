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
#include <stdarg.h>
#include <math.h>

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

	nowdb_filter_show(f,stderr); fprintf(stderr, "\n");
	x = nowdb_filter_eval(f,NULL);
	nowdb_filter_destroy(f); free(f);

	if (x) return 0;

	f = mkBool(NOWDB_FILTER_JUST);
	if (f == NULL) return 0;

	f->left = mkBool(NOWDB_FILTER_TRUE);
	if (f->left == NULL) return 0;

	nowdb_filter_show(f,stderr); fprintf(stderr, "\n");
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

		nowdb_filter_show(f,stderr); fprintf(stderr, "\n");
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

		nowdb_filter_show(f,stderr); fprintf(stderr, "\n");
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

	err = nowdb_filter_newCompare(&f, op, off, size, typ, val, NULL);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot make new compare\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	return f;
}

nowdb_filter_t *mkIn(int typ, int off, int size, int sz, ...) {
	va_list      args;
	nowdb_err_t   err;
	nowdb_filter_t *f;
	ts_algo_list_t vals;
	void *v, *tmp;
	char x=0;

	ts_algo_list_init(&vals);

	va_start(args, sz);
	for(int i=0; i<sz; i++) {
		tmp = va_arg(args, void*);
		if (tmp == NULL) continue;
		v = malloc(size);
		if (v == NULL) {
			fprintf(stderr, "out-of-mem\n");
			return NULL;
		}
		memcpy(v, tmp, size);
		if (ts_algo_list_append(&vals, v) != TS_ALGO_OK) {
			fprintf(stderr, "out-of-mem\n");
			x = 1; break;
		}
	}
	va_end(args);

	if (x) {
		ts_algo_list_node_t *run;
		for(run=vals.head; run!=NULL; run=run->nxt) {
			free(run->cont);
		}
		ts_algo_list_destroy(&vals);
		return NULL;
	}
	
	err = nowdb_filter_newCompare(&f, NOWDB_FILTER_IN,
	                      off, size, typ, NULL, &vals);
	ts_algo_list_destroy(&vals);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot make new compare\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	return f;
}

int testEQUInt() {
	nowdb_edge_t e;
	int64_t v;
	nowdb_bool_t    x;
	nowdb_filter_t *f;

	for(int i=0;i<4;i++) {
		switch(i) {
		case 0: v = 5; e.weight = 5; break;
		case 1: e.weight++; break;
		case 2: v++; break;
		case 3: v++; break;
		}

		f = mkCompare(NOWDB_FILTER_EQ, 40, 8,
		              NOWDB_TYP_UINT, &v);
		if (f == NULL)  return 0;

		x = nowdb_filter_eval(f,&e);
		nowdb_filter_destroy(f); free(f);

		fprintf(stderr, "[EQUInt] case %d: %d\n", i, x);

		switch(i) {
		case 0: if (!x) return 0; break;
		case 1: if (x) return 0; break;
		case 2: if (!x) return 0; break;
		case 3: if (x) return 0; break;
		}
	}
	return 1;
}

int testNEUInt() {
	nowdb_edge_t e;
	int64_t v;
	nowdb_bool_t    x;
	nowdb_filter_t *f;

	for(int i=0;i<4;i++) {
		switch(i) {
		case 0: v = 5; e.weight = 5; break;
		case 1: e.weight++; break;
		case 2: v++; break;
		case 3: v++; break;
		}

		f = mkCompare(NOWDB_FILTER_NE, 40, 8,
		              NOWDB_TYP_UINT, &v);
		if (f == NULL)  return 0;

		x = nowdb_filter_eval(f,&e);
		nowdb_filter_destroy(f); free(f);

		fprintf(stderr, "[NEUInt] case %d: %d\n", i, x);

		switch(i) {
		case 0: if (x) return 0; break;
		case 1: if (!x) return 0; break;
		case 2: if (x) return 0; break;
		case 3: if (!x) return 0; break;
		}
	}
	return 1;
}

int testLEUInt() {
	nowdb_edge_t e;
	int64_t v;
	nowdb_bool_t    x;
	nowdb_filter_t *f;

	for(int i=0;i<4;i++) {
		switch(i) {
		case 0: v = 5; e.weight = 5; break;
		case 1: e.weight++; break;
		case 2: v++; break;
		case 3: v++; break;
		}

		f = mkCompare(NOWDB_FILTER_LE, 40, 8,
		              NOWDB_TYP_UINT, &v);
		if (f == NULL)  return 0;

		x = nowdb_filter_eval(f,&e);
		nowdb_filter_destroy(f); free(f);

		fprintf(stderr, "[LEUInt] case %d: %d\n", i, x);

		switch(i) {
		case 0: if (!x) return 0; break;
		case 1: if (x) return 0; break;
		case 2: if (!x) return 0; break;
		case 3: if (!x) return 0; break;
		}
	}
	return 1;
}

int testGEUInt() {
	nowdb_edge_t e;
	int64_t v;
	nowdb_bool_t    x;
	nowdb_filter_t *f;

	for(int i=0;i<4;i++) {
		switch(i) {
		case 0: v = 5; e.weight = 5; break;
		case 1: e.weight++; break;
		case 2: v++; break;
		case 3: v++; break;
		}

		f = mkCompare(NOWDB_FILTER_GE, 40, 8,
		              NOWDB_TYP_UINT, &v);
		if (f == NULL)  return 0;

		x = nowdb_filter_eval(f,&e);
		nowdb_filter_destroy(f); free(f);

		fprintf(stderr, "[GEUInt] case %d: %d\n", i, x);

		switch(i) {
		case 0: if (!x) return 0; break;
		case 1: if (!x) return 0; break;
		case 2: if (!x) return 0; break;
		case 3: if (x) return 0; break;
		}
	}
	return 1;
}

int testLTUInt() {
	nowdb_edge_t e;
	int64_t v;
	nowdb_bool_t    x;
	nowdb_filter_t *f;

	for(int i=0;i<4;i++) {
		switch(i) {
		case 0: v = 5; e.weight = 5; break;
		case 1: e.weight++; break;
		case 2: v++; break;
		case 3: v++; break;
		}

		f = mkCompare(NOWDB_FILTER_LT, 40, 8,
		              NOWDB_TYP_UINT, &v);
		if (f == NULL)  return 0;

		x = nowdb_filter_eval(f,&e);
		nowdb_filter_destroy(f); free(f);

		fprintf(stderr, "[LTUInt] case %d: %d\n", i, x);

		switch(i) {
		case 0: if (x) return 0; break;
		case 1: if (x) return 0; break;
		case 2: if (x) return 0; break;
		case 3: if (!x) return 0; break;
		}
	}
	return 1;
}

int testGTUInt() {
	nowdb_edge_t e;
	int64_t v;
	nowdb_bool_t    x;
	nowdb_filter_t *f;

	for(int i=0;i<4;i++) {
		switch(i) {
		case 0: v = 5; e.weight = 5; break;
		case 1: e.weight++; break;
		case 2: v++; break;
		case 3: v++; break;
		}

		f = mkCompare(NOWDB_FILTER_GT, 40, 8,
		              NOWDB_TYP_UINT, &v);
		if (f == NULL)  return 0;

		x = nowdb_filter_eval(f,&e);
		nowdb_filter_destroy(f); free(f);

		fprintf(stderr, "[GTUInt] case %d: %d\n", i, x);

		switch(i) {
		case 0: if (x) return 0; break;
		case 1: if (!x) return 0; break;
		case 2: if (x) return 0; break;
		case 3: if (x) return 0; break;
		}
	}
	return 1;
}

int testEQInt() {
	nowdb_edge_t e;
	int64_t v;
	nowdb_bool_t    x;
	nowdb_filter_t *f;

	for(int i=0;i<4;i++) {
		switch(i) {
		case 0: v = 5; e.weight = 5; break;
		case 1: e.weight++; break;
		case 2: e.weight *= -1; break;
		case 3: v *= -1; v--; break;
		}

		f = mkCompare(NOWDB_FILTER_EQ, 40, 8,
		              NOWDB_TYP_INT, &v);
		if (f == NULL)  return 0;

		x = nowdb_filter_eval(f,&e);
		nowdb_filter_destroy(f); free(f);

		fprintf(stderr, "[EQInt] case %d: %d\n", i, x);

		switch(i) {
		case 0: if (!x) return 0; break;
		case 1: if (x) return 0; break;
		case 2: if (x) return 0; break;
		case 3: if (!x) return 0; break;
		}
	}
	return 1;
}

int testLEInt() {
	nowdb_edge_t e;
	int64_t v;
	nowdb_bool_t    x;
	nowdb_filter_t *f;

	for(int i=0;i<4;i++) {
		switch(i) {
		case 0: v = 5; e.weight = 5; break;
		case 1: e.weight++; break;
		case 2: e.weight *= -1; break;
		case 3: v *= -1; break;
		}

		f = mkCompare(NOWDB_FILTER_LE, 40, 8,
		              NOWDB_TYP_INT, &v);
		if (f == NULL)  return 0;

		x = nowdb_filter_eval(f,&e);
		nowdb_filter_destroy(f); free(f);

		fprintf(stderr, "[LEInt] case %d: %d\n", i, x);

		switch(i) {
		case 0: if (!x) return 0; break;
		case 1: if (x) return 0; break;
		case 2: if (!x) return 0; break;
		case 3: if (!x) return 0; break;
		}
	}
	return 1;
}

int testLTInt() {
	nowdb_edge_t e;
	int64_t v;
	nowdb_bool_t    x;
	nowdb_filter_t *f;

	for(int i=0;i<4;i++) {
		switch(i) {
		case 0: v = 5; e.weight = 5; break;
		case 1: e.weight++; break;
		case 2: e.weight *= -1; break;
		case 3: v *= -1; break;
		}

		f = mkCompare(NOWDB_FILTER_LT, 40, 8,
		              NOWDB_TYP_INT, &v);
		if (f == NULL)  return 0;

		x = nowdb_filter_eval(f,&e);
		nowdb_filter_destroy(f); free(f);

		fprintf(stderr, "[LTInt] case %d: %d\n", i, x);

		switch(i) {
		case 0: if (x) return 0; break;
		case 1: if (x) return 0; break;
		case 2: if (!x) return 0; break;
		case 3: if (!x) return 0; break;
		}
	}
	return 1;
}

int testGEInt() {
	nowdb_edge_t e;
	int64_t v;
	nowdb_bool_t    x;
	nowdb_filter_t *f;

	for(int i=0;i<4;i++) {
		switch(i) {
		case 0: v = 5; e.weight = 5; break;
		case 1: e.weight++; break;
		case 2: e.weight *= -1; break;
		case 3: v *= -1; v--; break;
		}

		f = mkCompare(NOWDB_FILTER_GE, 40, 8,
		              NOWDB_TYP_INT, &v);
		if (f == NULL)  return 0;

		x = nowdb_filter_eval(f,&e);
		nowdb_filter_destroy(f); free(f);

		fprintf(stderr, "[GEInt] case %d: %d\n", i, x);

		switch(i) {
		case 0: if (!x) return 0; break;
		case 1: if (!x) return 0; break;
		case 2: if (x) return 0; break;
		case 3: if (!x) return 0; break;
		}
	}
	return 1;
}

int testGTInt() {
	nowdb_edge_t e;
	int64_t v;
	nowdb_bool_t    x;
	nowdb_filter_t *f;

	for(int i=0;i<4;i++) {
		switch(i) {
		case 0: v = 5; e.weight = 5; break;
		case 1: e.weight++; break;
		case 2: e.weight *= -1; break;
		case 3: v *= -1; v--; break;
		}

		f = mkCompare(NOWDB_FILTER_GT, 40, 8,
		              NOWDB_TYP_INT, &v);
		if (f == NULL)  return 0;

		x = nowdb_filter_eval(f,&e);
		nowdb_filter_destroy(f); free(f);

		fprintf(stderr, "[GTInt] case %d: %d\n", i, x);

		switch(i) {
		case 0: if (x) return 0; break;
		case 1: if (!x) return 0; break;
		case 2: if (x) return 0; break;
		case 3: if (x) return 0; break;
		}
	}
	return 1;
}

int testNEInt() {
	nowdb_edge_t e;
	int64_t v;
	nowdb_bool_t    x;
	nowdb_filter_t *f;

	for(int i=0;i<4;i++) {
		switch(i) {
		case 0: v = 5; e.weight = 5; break;
		case 1: e.weight++; break;
		case 2: e.weight *= -1; break;
		case 3: v *= -1; v--; break;
		}

		f = mkCompare(NOWDB_FILTER_NE, 40, 8,
		              NOWDB_TYP_INT, &v);
		if (f == NULL)  return 0;

		x = nowdb_filter_eval(f,&e);
		nowdb_filter_destroy(f); free(f);

		fprintf(stderr, "[NEInt] case %d: %d\n", i, x);

		switch(i) {
		case 0: if (x) return 0; break;
		case 1: if (!x) return 0; break;
		case 2: if (!x) return 0; break;
		case 3: if (x) return 0; break;
		}
	}
	return 1;
}

int testEQFloat() {
	nowdb_edge_t e;
	double  v;
	nowdb_bool_t    x;
	nowdb_filter_t *f;

	for(int i=0;i<5;i++) {
		switch(i) {
		case 0: v = 5; memcpy(&e.weight, &v, 8); break;
		case 1: v /= 3; break;
		case 2: 
		case 4: memcpy(&e.weight, &v, 8); break;
		case 3: v *= 0.11111; break;
		}

		f = mkCompare(NOWDB_FILTER_EQ, 40, 8,
		              NOWDB_TYP_FLOAT, &v);
		if (f == NULL)  return 0;

		x = nowdb_filter_eval(f,&e);
		nowdb_filter_destroy(f); free(f);

		fprintf(stderr, "[EQFloat] case %d: %d\n", i, x);

		switch(i) {
		case 0: if (!x) return 0; break;
		case 1: if (x) return 0; break;
		case 2: if (!x) return 0; break;
		case 3: if (x) return 0; break;
		case 4: if (!x) return 0; break;
		}
	}
	return 1;
}

int testNEFloat() {
	nowdb_edge_t e;
	double  v;
	nowdb_bool_t    x;
	nowdb_filter_t *f;

	for(int i=0;i<5;i++) {
		switch(i) {
		case 0: v = 5; memcpy(&e.weight, &v, 8); break;
		case 1: v /= 3; break;
		case 2: 
		case 4: memcpy(&e.weight, &v, 8); break;
		case 3: v *= 0.11111; break;
		}

		f = mkCompare(NOWDB_FILTER_NE, 40, 8,
		              NOWDB_TYP_FLOAT, &v);
		if (f == NULL)  return 0;

		x = nowdb_filter_eval(f,&e);
		nowdb_filter_destroy(f); free(f);

		fprintf(stderr, "[NEFloat] case %d: %d\n", i, x);

		switch(i) {
		case 0: if (x) return 0; break;
		case 1: if (!x) return 0; break;
		case 2: if (x) return 0; break;
		case 3: if (!x) return 0; break;
		case 4: if (x) return 0; break;
		}
	}
	return 1;
}

int testLEFloat() {
	nowdb_edge_t e;
	double  v;
	nowdb_bool_t    x;
	nowdb_filter_t *f;

	for(int i=0;i<5;i++) {
		switch(i) {
		case 0: v = 5; memcpy(&e.weight, &v, 8); break;
		case 1: v /= 3; break;
		case 2: 
		case 4: memcpy(&e.weight, &v, 8); break;
		case 3: v *= 1.11111; break;
		}

		f = mkCompare(NOWDB_FILTER_LE, 40, 8,
		              NOWDB_TYP_FLOAT, &v);
		if (f == NULL)  return 0;

		x = nowdb_filter_eval(f,&e);
		nowdb_filter_destroy(f); free(f);

		fprintf(stderr, "[LEFloat] case %d: %d\n", i, x);

		switch(i) {
		case 0: if (!x) return 0; break;
		case 1: if (x) return 0; break;
		case 2: if (!x) return 0; break;
		case 3: if (!x) return 0; break;
		case 4: if (!x) return 0; break;
		}
	}
	return 1;
}

int testGEFloat() {
	nowdb_edge_t e;
	double v;
	// double t;
	nowdb_bool_t    x;
	nowdb_filter_t *f;

	for(int i=0;i<5;i++) {
		switch(i) {
		case 0: v = 5; memcpy(&e.weight, &v, 8); break;
		case 1: v /= 3; break;
		case 2: 
		case 4: memcpy(&e.weight, &v, 8); break;
		case 3: v *= 1.11111; break;
		}

		f = mkCompare(NOWDB_FILTER_GE, 40, 8,
		              NOWDB_TYP_FLOAT, &v);
		if (f == NULL)  return 0;

		x = nowdb_filter_eval(f,&e);
		nowdb_filter_destroy(f); free(f);

		fprintf(stderr, "[GEFloat] case %d: %d\n", i, x);
		/*
		memcpy(&t, &e.weight, 8);
		fprintf(stderr, "%.6f >= %.6f\n", t, v);
		*/

		switch(i) {
		case 0: if (!x) return 0; break;
		case 1: if (!x) return 0; break;
		case 2: if (!x) return 0; break;
		case 3: if (x) return 0; break;
		case 4: if (!x) return 0; break;
		}
	}
	return 1;
}

int testLTFloat() {
	nowdb_edge_t e;
	double  v;
	nowdb_bool_t    x;
	nowdb_filter_t *f;

	for(int i=0;i<5;i++) {
		switch(i) {
		case 0: v = 5; memcpy(&e.weight, &v, 8); break;
		case 1: v /= 3; break;
		case 2: 
		case 4: memcpy(&e.weight, &v, 8); break;
		case 3: v *= 1.11111; break;
		}

		f = mkCompare(NOWDB_FILTER_LT, 40, 8,
		              NOWDB_TYP_FLOAT, &v);
		if (f == NULL)  return 0;

		x = nowdb_filter_eval(f,&e);
		nowdb_filter_destroy(f); free(f);

		fprintf(stderr, "[LTFloat] case %d: %d\n", i, x);

		switch(i) {
		case 0: if (x) return 0; break;
		case 1: if (x) return 0; break;
		case 2: if (x) return 0; break;
		case 3: if (!x) return 0; break;
		case 4: if (x) return 0; break;
		}
	}
	return 1;
}

int testGTFloat() {
	nowdb_edge_t e;
	double  v;
	nowdb_bool_t    x;
	nowdb_filter_t *f;

	for(int i=0;i<5;i++) {
		switch(i) {
		case 0: v = 5; memcpy(&e.weight, &v, 8); break;
		case 1: v /= 3; break;
		case 2: 
		case 4: memcpy(&e.weight, &v, 8); break;
		case 3: v *= 1.11111; break;
		}

		f = mkCompare(NOWDB_FILTER_GT, 40, 8,
		              NOWDB_TYP_FLOAT, &v);
		if (f == NULL)  return 0;

		x = nowdb_filter_eval(f,&e);
		nowdb_filter_destroy(f); free(f);

		fprintf(stderr, "[GTFloat] case %d: %d\n", i, x);

		switch(i) {
		case 0: if (x) return 0; break;
		case 1: if (!x) return 0; break;
		case 2: if (x) return 0; break;
		case 3: if (x) return 0; break;
		case 4: if (x) return 0; break;
		}
	}
	return 1;
}

int testInUInt() {
	nowdb_edge_t e;
	uint64_t v=5;
	uint64_t w,x,y,z;
	nowdb_bool_t    r;
	nowdb_filter_t *f;

	w=1; x=2; y=w+x; z=x+y;

	fprintf(stderr, "[InUInt] search in (%lu, %lu, %lu, %lu)\n",
	                                                w, x, y, z);

	for(int i=0;i<5;i++) {
		switch(i) {
		case 0: v = 3; break;
		case 1: v = 4; break;
		case 2: v = 5; break; 
		case 3: v = 6; break;
		case 4: v = 9; break;
		}

		f = mkIn(NOWDB_TYP_INT, 40, 8, 4,
		         &w, &x, &y, &z);
		if (f == NULL)  return 0;

		memcpy(&e.weight, &v, 8); 

		r = nowdb_filter_eval(f,&e);
		nowdb_filter_destroy(f); free(f);

		fprintf(stderr, "[InUInt] case %d: %d (%lu)\n", i, r, v);

		switch(i) {
		case 0: if (!r) return 0; break;
		case 1: if (r) return 0; break;
		case 2: if (!r) return 0; break;
		case 3: if (r) return 0; break;
		case 4: if (r) return 0; break;
		}
	}
	return 1;
}

int testInInt() {
	nowdb_edge_t e;
	int64_t v=5;
	int64_t w,x,y,z;
	nowdb_bool_t    r;
	nowdb_filter_t *f;

	w=3; x=-1*w; y=5; z=7;

	fprintf(stderr, "[InInt] search in (%ld, %ld, %ld, %ld)\n",
	                                                w, x, y, z);

	for(int i=0;i<5;i++) {
		switch(i) {
		case 0: v = 5; break;
		case 1: v *= -1; break;
		case 2: v=3; break; 
		case 3: v *= -1; break;
		case 4: v = 9; break;
		}

		f = mkIn(NOWDB_TYP_INT, 40, 8, 4,
		         &w, &x, &y, &z);
		if (f == NULL)  return 0;

		memcpy(&e.weight, &v, 8); 

		r = nowdb_filter_eval(f,&e);
		nowdb_filter_destroy(f); free(f);

		fprintf(stderr, "[InInt] case %d: %d (%ld)\n", i, r, v);

		switch(i) {
		case 0: if (!r) return 0; break;
		case 1: if (r) return 0; break;
		case 2: if (!r) return 0; break;
		case 3: if (!r) return 0; break;
		case 4: if (r) return 0; break;
		}
	}
	return 1;
}

int testInFloat() {
	nowdb_edge_t e;
	double v=5;
	double w,x,y,z;
	nowdb_bool_t    r;
	nowdb_filter_t *f;

	w=5; x=w/2; y=3.14159; z=sqrt(2);

	fprintf(stderr, "[InFloat] search in (%f, %f, %f, %f)\n",
	                                              w, x, y, z);

	for(int i=0;i<5;i++) {
		switch(i) {
		case 0: v = 5; break;
		case 1: v /= 3; break;
		case 2: v=3.14159; break; 
		case 3: v *= 1.11111; break;
		case 4: v = sqrt(2); break;
		}

		f = mkIn(NOWDB_TYP_FLOAT, 40, 8, 4,
		         &w, &x, &y, &z);
		if (f == NULL)  return 0;

		memcpy(&e.weight, &v, 8); 

		r = nowdb_filter_eval(f,&e);
		nowdb_filter_destroy(f); free(f);

		fprintf(stderr, "[InFloat] case %d: %d (%f)\n", i, r, v);

		switch(i) {
		case 0: if (!r) return 0; break;
		case 1: if (r) return 0; break;
		case 2: if (!r) return 0; break;
		case 3: if (r) return 0; break;
		case 4: if (!r) return 0; break;
		}
	}
	return 1;
}

#define BOOL(f,x) \
	f = mkBool(x); \
	if (f == NULL) { \
		fprintf(stderr, "out-of-mem\n"); \
		return 0; \
	}

#define COMPARE(f,o,x,t,v) \
	f = mkCompare(o, x, 8, t, &v); \
	if (f == NULL) { \
		fprintf(stderr, "out-of-mem\n"); \
		return 0; \
	}

#define TESTRANGE(fl,o,s,e,a,b,x) \
	r = nowdb_filter_range(fl, 2, o, \
	            (char*)s, (char*)e); \
	nowdb_filter_destroy(fl); free(fl); \
	if ((x && !r) || (!x && r)) { \
		fprintf(stderr, "result not expected\n"); \
		return 0; \
	} \
	if (x) {\
		if (s[0] != a || s[1] != a || \
		    e[0] != b || e[1] != b) { \
			fprintf(stderr, \
			"start/end not v: %lu.%lu & %lu.%lu\n", \
			                s[0], s[1], e[0], e[1]); \
			return 0; \
		} \
	}
	

int testRange() {
	uint16_t off[2];
	uint64_t start[2], end[2];
	nowdb_filter_t *f, *f2;
	uint64_t v1=1, v2=2;
	uint64_t va, vb;
	char x, r;
	
	off[0] = NOWDB_OFF_EDGE;
	off[1] = NOWDB_OFF_ORIGIN;
	
	for(int i=0; i<4; i++) {
		memset(start, 0, 16);
		memset(end, 0, 16);
		switch(i) {
		case 0:
			fprintf(stderr, "RANGE with EQ (success)\n");
			x = 1;
			va = v1;
			vb = v1;

			BOOL(f, NOWDB_FILTER_AND);
			COMPARE(f->left, NOWDB_FILTER_EQ,
			                 NOWDB_OFF_EDGE,
			                 NOWDB_TYP_UINT, v1);
			COMPARE(f->right, NOWDB_FILTER_EQ,
			                  NOWDB_OFF_ORIGIN,
			                  NOWDB_TYP_UINT, v1);
			break;
		case 1:
			fprintf(stderr, "RANGE with EQ (fails)\n");
			x = 0;
			va = v1;
			vb = v1;

			BOOL(f, NOWDB_FILTER_AND);
			COMPARE(f->left, NOWDB_FILTER_NE,
			                 NOWDB_OFF_EDGE,
			                 NOWDB_TYP_UINT, v1);
			COMPARE(f->right, NOWDB_FILTER_EQ,
			                  NOWDB_OFF_ORIGIN,
			                  NOWDB_TYP_UINT, v1);
			break;
		case 2:
			fprintf(stderr, "RANGE with GE/LE (success)\n");
			x = 1;
			va = v1;
			vb = v2;

			BOOL(f, NOWDB_FILTER_AND);
			BOOL(f2, NOWDB_FILTER_AND);
			COMPARE(f2->left, NOWDB_FILTER_LE,
			                  NOWDB_OFF_EDGE,
			                  NOWDB_TYP_UINT, v2);
			COMPARE(f2->right, NOWDB_FILTER_LE,
			                   NOWDB_OFF_ORIGIN,
			                   NOWDB_TYP_UINT, v2);
			f->left = f2;

			BOOL(f2, NOWDB_FILTER_AND);
			COMPARE(f2->left, NOWDB_FILTER_GE,
			                  NOWDB_OFF_EDGE,
			                  NOWDB_TYP_UINT, v1);
			COMPARE(f2->right, NOWDB_FILTER_GE,
			                   NOWDB_OFF_ORIGIN,
			                   NOWDB_TYP_UINT, v1);
			f->right = f2;
			break;
		case 3:
			fprintf(stderr, "RANGE with GE/LE (fails)\n");
			x = 0;
			va = v1;
			vb = v2;

			BOOL(f, NOWDB_FILTER_AND);
			BOOL(f2, NOWDB_FILTER_AND);
			COMPARE(f2->left, NOWDB_FILTER_LE,
			                  NOWDB_OFF_EDGE,
			                  NOWDB_TYP_UINT, v2);
			COMPARE(f2->right, NOWDB_FILTER_LE,
			                   NOWDB_OFF_ORIGIN,
			                   NOWDB_TYP_UINT, v2);
			f->left = f2;

			BOOL(f2, NOWDB_FILTER_AND);
			COMPARE(f2->left, NOWDB_FILTER_LE,
			                  NOWDB_OFF_EDGE,
			                  NOWDB_TYP_UINT, v1);
			COMPARE(f2->right, NOWDB_FILTER_LE,
			                   NOWDB_OFF_ORIGIN,
			                   NOWDB_TYP_UINT, v1);
			f->right = f2;
			break;
		}
		TESTRANGE(f, off, start, end, va, vb, x);
	}
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
	if (!testEQUInt()) {
		fprintf(stderr, "testEQUint() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testNEUInt()) {
		fprintf(stderr, "testNEUint() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testLEUInt()) {
		fprintf(stderr, "testLEUint() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testGEUInt()) {
		fprintf(stderr, "testGEUint() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testLTUInt()) {
		fprintf(stderr, "testLTUint() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testGTUInt()) {
		fprintf(stderr, "testGTUint() failed\n");
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
	if (!testLTInt()) {
		fprintf(stderr, "testLTInt() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testGEInt()) {
		fprintf(stderr, "testGEInt() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testGTInt()) {
		fprintf(stderr, "testGTInt() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testNEInt()) {
		fprintf(stderr, "testNEInt() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testEQFloat()) {
		fprintf(stderr, "testEQFloat() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testNEFloat()) {
		fprintf(stderr, "testNEFloat() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testLEFloat()) {
		fprintf(stderr, "testLEFloat() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testGEFloat()) {
		fprintf(stderr, "testGEFloat() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testLTFloat()) {
		fprintf(stderr, "testLTFloat() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testGTFloat()) {
		fprintf(stderr, "testGTFloat() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testInUInt()) {
		fprintf(stderr, "testInUint() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testInInt()) {
		fprintf(stderr, "testInInt() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testInFloat()) {
		fprintf(stderr, "testInFloat() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testRange()) {
		fprintf(stderr, "testRange() failed\n");
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
