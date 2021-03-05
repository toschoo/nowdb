/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for filters
 * ========================================================================
 */
#include <nowdb/fun/expr.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <math.h>

#define OFF_USR_FIELD 25

int mkConst(nowdb_expr_t *c, void *v, nowdb_type_t t) {
	nowdb_err_t err;
	err = nowdb_expr_newConstant(c, v, t);
	if (err != NOWDB_OK) {
		fprintf(stderr, "ERROR: cannot create new constant\n");
		nowdb_err_print(err); nowdb_err_release(err);
		return -1;
	}
	return 0;
}

nowdb_expr_t mkBool(uint32_t b) {
	nowdb_err_t   err;
	nowdb_expr_t    f;

	err = nowdb_expr_newConstant(&f, &b, NOWDB_TYP_BOOL);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot make new bool\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	return f;
}

int binaryOp(int op, nowdb_expr_t o1, nowdb_expr_t o2, nowdb_type_t *t, void *r) {
	nowdb_err_t err;
	nowdb_expr_t  o;
	void *x=NULL;

	err = nowdb_expr_newOp(&o, op, o1, o2);
	if (err != NOWDB_OK) {
		fprintf(stderr, "ERROR: cannot create new operator\n");
		nowdb_expr_destroy(o1); free(o1);
		nowdb_expr_destroy(o2); free(o2);
		nowdb_err_print(err); nowdb_err_release(err);
		return -1;
	}
	// nowdb_expr_show(o, stderr); fprintf(stderr, "\n");
	err = nowdb_expr_eval(o, NULL, NULL, t, (void**)&x);
	if (err != NOWDB_OK) {
		fprintf(stderr, "ERROR: cannot evaluate\n");
		nowdb_expr_destroy(o); free(o);
		nowdb_err_print(err); nowdb_err_release(err);
		return -1;
	}
	memcpy(r, x, 8);
	nowdb_expr_destroy(o); free(o);
	return 0;
}

int unaryOp(int op, nowdb_expr_t o1, nowdb_type_t *t, void *r) {
	nowdb_err_t err;
	nowdb_expr_t  o;
	void *x=NULL;

	err = nowdb_expr_newOp(&o, op, o1);
	if (err != NOWDB_OK) {
		fprintf(stderr, "ERROR: cannot create new operator\n");
		nowdb_expr_destroy(o1); free(o1);
		nowdb_err_print(err); nowdb_err_release(err);
		return -1;
	}
	err = nowdb_expr_eval(o, NULL, NULL, t, (void**)&x);
	if (err != NOWDB_OK) {
		fprintf(stderr, "ERROR: cannot evaluate\n");
		nowdb_expr_destroy(o); free(o);
		nowdb_err_print(err); nowdb_err_release(err);
		return -1;
	}
	memcpy(r, x, 8);
	nowdb_expr_destroy(o); free(o);
	return 0;
}

int testBool0(int op) {
	int x = 0;
	nowdb_err_t err;
	nowdb_value_t *r;
	nowdb_expr_t  f;
	nowdb_type_t  t;

	f = mkBool(op);
	if (f == NULL) return 0;

	err = nowdb_expr_eval(f,NULL,NULL,&t,(void**)&r);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot evaluate true\n");
		nowdb_expr_destroy(f); free(f);
		nowdb_err_print(err); 
		nowdb_err_release(err); 
		return 0;
	}
	if (t != NOWDB_TYP_BOOL) {
		fprintf(stderr, "type is %u\n", t);
		nowdb_expr_destroy(f); free(f);
		return 0;
	}
	x = (*r != 0);
	nowdb_expr_destroy(f); free(f);
	return x;
}

int testTrue() {
	return testBool0(NOWDB_EXPR_OP_TRUE);
}

int testFalse() {
	return !testBool0(NOWDB_EXPR_OP_FALSE);
}

int testNot() {
	nowdb_expr_t   c;
	nowdb_type_t   t;
	nowdb_value_t  x;
	nowdb_value_t  r;

	for(int i=0;i<2;i++) {
		x = i==0?TRUE:FALSE;
		if (mkConst(&c, &x, NOWDB_TYP_BOOL) != 0) return 0;
		if (unaryOp(NOWDB_EXPR_OP_NOT, c, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "ERROR: wrong type: %u\n", t);
			return 0;
		}
		if ((x&&r) || !(x||r)) {
			fprintf(stderr, "ERROR: wrong result: %lu\n", r);
			return 0;
		}
	}
	return 1;
}

int testJust() {
	nowdb_expr_t   c;
	nowdb_type_t   t;
	nowdb_value_t  x;
	nowdb_value_t  r;

	for(int i=0;i<2;i++) {
		x = i==0?TRUE:FALSE;
		if (mkConst(&c, &x, NOWDB_TYP_BOOL) != 0) return 0;
		if (unaryOp(NOWDB_EXPR_OP_JUST, c, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "ERROR: wrong type: %u\n", t);
			return 0;
		}
		if (!(x&&r) && (x||r)) {
			fprintf(stderr, "ERROR: wrong result: %lu/%lu\n", x, r);
			return 0;
		}
	}
	return 1;
}

int testOr() {
	nowdb_expr_t c1,c2;
	nowdb_type_t     t;
	nowdb_value_t  a,b;
	nowdb_value_t    r;

	for(int i=0;i<4;i++) {
		a = i==0||i==1?TRUE:FALSE;
		if (mkConst(&c1, &a, NOWDB_TYP_BOOL) != 0) return 0;

		b = i==0||i==2?TRUE:FALSE;
		if (mkConst(&c2, &b, NOWDB_TYP_BOOL) != 0) {
			nowdb_expr_destroy(c1); free(c1);
			return 0;
		}

		if (binaryOp(NOWDB_EXPR_OP_OR, c1, c2, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "ERROR: wrong type: %u\n", t);
			return 0;
		}
		
		fprintf(stderr, "[OR] case %d: %lu, %lu = %lu\n", i, a, b, r);

		switch(i) {
		case 3: if (r) return 0; break;
		default: if (!r) return 0;
		}
	}
	return 1;
}

int testAnd() {
	nowdb_expr_t c1,c2;
	nowdb_type_t     t;
	nowdb_value_t  a,b;
	nowdb_value_t    r;

	for(int i=0;i<4;i++) {
		a = i==0||i==1?TRUE:FALSE;
		if (mkConst(&c1, &a, NOWDB_TYP_BOOL) != 0) return 0;

		b = i==0||i==2?TRUE:FALSE;
		if (mkConst(&c2, &b, NOWDB_TYP_BOOL) != 0) {
			nowdb_expr_destroy(c1); free(c1);
			return 0;
		}

		if (binaryOp(NOWDB_EXPR_OP_AND, c1, c2, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "ERROR: wrong type: %u\n", t);
			return 0;
		}
		
		fprintf(stderr, "[AND] case %d: %lu, %lu = %lu\n", i, a, b, r);

		switch(i) {
		case 0: if (!r) return 0; break;
		default: if (r) return 0;
		}
	}
	return 1;
}

nowdb_expr_t mkIn(int type, int size, int sz, ...) {
	va_list args;
	nowdb_err_t   err;
	nowdb_expr_t    c;
	ts_algo_tree_t *t;
	void     *v, *tmp;
	char x=0;

	err = nowdb_expr_newTree(&t, type);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create in-tree\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}

	va_start(args, sz);
	for(int i=0; i<sz;i++) {
		tmp = va_arg(args, void*);
		if (tmp == NULL) continue;
		v = malloc(size);
		if (v == NULL) {
			fprintf(stderr, "out-of-mem\n");
			return NULL;
		}
		memcpy(v, tmp, size);
		if (ts_algo_tree_insert(t, v) != TS_ALGO_OK) {
			x = 1; break;
		}
	}
	va_end(args);

	if (x) {
		fprintf(stderr, "cannot fill in-tree\n");
		ts_algo_tree_destroy(t); free(t);
		return NULL;
	}
	err = nowdb_expr_constFromTree(&c, t, type);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create in-constant\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		ts_algo_tree_destroy(t); free(t);
		return NULL;
	}
	return c;
}

int testEQUInt() {
	nowdb_expr_t c1,c2;
	nowdb_type_t     t;
	uint64_t        a,b;
	nowdb_value_t    r;

	a=5;
	for(int i=0;i<4;i++) {
		if (mkConst(&c1, &a, NOWDB_TYP_UINT) != 0) return 0;

		b = i==0||i==2?a:i==1?a+a:a-a;
		if (mkConst(&c2, &b, NOWDB_TYP_UINT) != 0) {
			nowdb_expr_destroy(c1); free(c1);
			return 0;
		}

		if (binaryOp(NOWDB_EXPR_OP_EQ, c1, c2, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "ERROR: wrong type: %u\n", t);
			return 0;
		}
		
		fprintf(stderr, "[EQ(UINT)] case %d: %lu, %lu = %lu\n", i, a, b, r);

		switch(i) {
		case 0: if (!r) return 0; break;
		case 2: if (!r) return 0; break;
		default: if (r) return 0;
		}
	}
	return 1;
}

int testNEUInt() {
	nowdb_expr_t c1,c2;
	nowdb_type_t     t;
	uint64_t        a,b;
	nowdb_value_t    r;

	a=5;
	for(int i=0;i<4;i++) {
		if (mkConst(&c1, &a, NOWDB_TYP_UINT) != 0) return 0;

		b = i==0||i==2?a:i==1?a+a:a-a;
		if (mkConst(&c2, &b, NOWDB_TYP_UINT) != 0) {
			nowdb_expr_destroy(c1); free(c1);
			return 0;
		}

		if (binaryOp(NOWDB_EXPR_OP_NE, c1, c2, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "ERROR: wrong type: %u\n", t);
			return 0;
		}
		
		fprintf(stderr, "[NE(UINT)] case %d: %lu, %lu = %lu\n", i, a, b, r);

		switch(i) {
		case 0: if (r) return 0; break;
		case 2: if (r) return 0; break;
		default: if (!r) return 0;
		}
	}
	return 1;
}

int testLEUInt() {
	nowdb_expr_t c1,c2;
	nowdb_type_t     t;
	uint64_t        a,b;
	nowdb_value_t    r;

	a=5;
	for(int i=0;i<4;i++) {
		if (mkConst(&c1, &a, NOWDB_TYP_UINT) != 0) return 0;

		b = i==0||i==2?a:i==1?a+a:a-a;
		if (mkConst(&c2, &b, NOWDB_TYP_UINT) != 0) {
			nowdb_expr_destroy(c1); free(c1);
			return 0;
		}

		if (binaryOp(NOWDB_EXPR_OP_LE, c1, c2, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "ERROR: wrong type: %u\n", t);
			return 0;
		}
		
		fprintf(stderr, "[LE(UINT)] case %d: %lu, %lu = %lu\n", i, a, b, r);

		switch(i) {
		case 0: if (!r) return 0; break;
		case 1: if (!r) return 0; break;
		case 2: if (!r) return 0; break;
		default: if (r) return 0;
		}
	}
	return 1;
}

int testGEUInt() {
	nowdb_expr_t c1,c2;
	nowdb_type_t     t;
	uint64_t        a,b;
	nowdb_value_t    r;

	a=5;
	for(int i=0;i<4;i++) {
		if (mkConst(&c1, &a, NOWDB_TYP_UINT) != 0) return 0;

		b = i==0||i==2?a:i==1?a+a:a-a;
		if (mkConst(&c2, &b, NOWDB_TYP_UINT) != 0) {
			nowdb_expr_destroy(c1); free(c1);
			return 0;
		}

		if (binaryOp(NOWDB_EXPR_OP_GE, c1, c2, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "ERROR: wrong type: %u\n", t);
			return 0;
		}
		
		fprintf(stderr, "[GE(UINT)] case %d: %lu, %lu = %lu\n", i, a, b, r);

		switch(i) {
		case 0: if (!r) return 0; break;
		case 2: if (!r) return 0; break;
		case 3: if (!r) return 0; break;
		default: if (r) return 0;
		}
	}
	return 1;
}

int testLTUInt() {
	nowdb_expr_t c1,c2;
	nowdb_type_t     t;
	uint64_t        a,b;
	nowdb_value_t    r;

	a=5;
	for(int i=0;i<4;i++) {
		if (mkConst(&c1, &a, NOWDB_TYP_UINT) != 0) return 0;

		b = i==0||i==2?a:i==1?a+a:a-a;
		if (mkConst(&c2, &b, NOWDB_TYP_UINT) != 0) {
			nowdb_expr_destroy(c1); free(c1);
			return 0;
		}

		if (binaryOp(NOWDB_EXPR_OP_LT, c1, c2, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "ERROR: wrong type: %u\n", t);
			return 0;
		}
		
		fprintf(stderr, "[LT(UINT)] case %d: %lu, %lu = %lu\n", i, a, b, r);

		switch(i) {
		case 1: if (!r) return 0; break;
		default: if (r) return 0;
		}
	}
	return 1;
}

int testGTUInt() {
	nowdb_expr_t c1,c2;
	nowdb_type_t     t;
	uint64_t        a,b;
	nowdb_value_t    r;

	a=5;
	for(int i=0;i<4;i++) {
		if (mkConst(&c1, &a, NOWDB_TYP_UINT) != 0) return 0;

		b = i==0||i==2?a:i==1?a+a:a-a;
		if (mkConst(&c2, &b, NOWDB_TYP_UINT) != 0) {
			nowdb_expr_destroy(c1); free(c1);
			return 0;
		}

		if (binaryOp(NOWDB_EXPR_OP_GT, c1, c2, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "ERROR: wrong type: %u\n", t);
			return 0;
		}
		
		fprintf(stderr, "[GT(UINT)] case %d: %lu, %lu = %lu\n", i, a, b, r);

		switch(i) {
		case 3: if (!r) return 0; break;
		default: if (r) return 0;
		}
	}
	return 1;
}

int testEQInt() {
	nowdb_expr_t c1,c2;
	nowdb_type_t     t;
	int64_t        a,b;
	nowdb_value_t    r;

	a=5;
	for(int i=0;i<4;i++) {
		if (mkConst(&c1, &a, NOWDB_TYP_INT) != 0) return 0;

		switch(i) {
		case 0: b=a; break;
		case 1: b=a+a; break;
		case 2: b=a-a; break;
		case 3: b=a-2*a; break;
		}
		if (mkConst(&c2, &b, NOWDB_TYP_INT) != 0) {
			nowdb_expr_destroy(c1); free(c1);
			return 0;
		}
		if (binaryOp(NOWDB_EXPR_OP_EQ, c1, c2, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "ERROR: wrong type: %u\n", t);
			return 0;
		}
		
		fprintf(stderr, "[EQ(INT)] case %d: %ld, %ld = %ld\n", i, a, b, r);

		switch(i) {
		case 0: if (!r) return 0; break;
		default: if (r) return 0;
		}
	}
	return 1;
}

int testLEInt() {
	nowdb_expr_t c1,c2;
	nowdb_type_t     t;
	int64_t        a,b;
	nowdb_value_t    r;

	a=5;
	for(int i=0;i<4;i++) {
		if (mkConst(&c1, &a, NOWDB_TYP_INT) != 0) return 0;

		switch(i) {
		case 0: b=a; break;
		case 1: b=a+a; break;
		case 2: b=a-a; break;
		case 3: b=a-2*a; break;
		}
		if (mkConst(&c2, &b, NOWDB_TYP_INT) != 0) {
			nowdb_expr_destroy(c1); free(c1);
			return 0;
		}
		if (binaryOp(NOWDB_EXPR_OP_LE, c1, c2, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "ERROR: wrong type: %u\n", t);
			return 0;
		}
		
		fprintf(stderr, "[LE(INT)] case %d: %ld, %ld = %ld\n", i, a, b, r);

		switch(i) {
		case 0: if (!r) return 0; break;
		case 1: if (!r) return 0; break;
		default: if (r) return 0;
		}
	}
	return 1;
}

int testLTInt() {
	nowdb_expr_t c1,c2;
	nowdb_type_t     t;
	int64_t        a,b;
	nowdb_value_t    r;

	a=5;
	for(int i=0;i<4;i++) {
		if (mkConst(&c1, &a, NOWDB_TYP_INT) != 0) return 0;

		switch(i) {
		case 0: b=a; break;
		case 1: b=a+a; break;
		case 2: b=a-a; break;
		case 3: b=a-2*a; break;
		}
		if (mkConst(&c2, &b, NOWDB_TYP_INT) != 0) {
			nowdb_expr_destroy(c1); free(c1);
			return 0;
		}
		if (binaryOp(NOWDB_EXPR_OP_LT, c1, c2, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "ERROR: wrong type: %u\n", t);
			return 0;
		}
		
		fprintf(stderr, "[LT(INT)] case %d: %ld, %ld = %ld\n", i, a, b, r);

		switch(i) {
		case 1: if (!r) return 0; break;
		default: if (r) return 0;
		}
	}
	return 1;
}

int testGEInt() {
	nowdb_expr_t c1,c2;
	nowdb_type_t     t;
	int64_t        a,b;
	nowdb_value_t    r;

	a=5;
	for(int i=0;i<4;i++) {
		if (mkConst(&c1, &a, NOWDB_TYP_INT) != 0) return 0;

		switch(i) {
		case 0: b=a; break;
		case 1: b=a+a; break;
		case 2: b=a-a; break;
		case 3: b=a-2*a; break;
		}
		if (mkConst(&c2, &b, NOWDB_TYP_INT) != 0) {
			nowdb_expr_destroy(c1); free(c1);
			return 0;
		}
		if (binaryOp(NOWDB_EXPR_OP_GE, c1, c2, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "ERROR: wrong type: %u\n", t);
			return 0;
		}
		
		fprintf(stderr, "[GE(INT)] case %d: %ld, %ld = %ld\n", i, a, b, r);

		switch(i) {
		case 0: if (!r) return 0; break;
		case 2: if (!r) return 0; break;
		case 3: if (!r) return 0; break;
		default: if (r) return 0;
		}
	}
	return 1;
}

int testGTInt() {
	nowdb_expr_t c1,c2;
	nowdb_type_t     t;
	int64_t        a,b;
	nowdb_value_t    r;

	a=5;
	for(int i=0;i<4;i++) {
		if (mkConst(&c1, &a, NOWDB_TYP_INT) != 0) return 0;

		switch(i) {
		case 0: b=a; break;
		case 1: b=a+a; break;
		case 2: b=a-a; break;
		case 3: b=a-2*a; break;
		}
		if (mkConst(&c2, &b, NOWDB_TYP_INT) != 0) {
			nowdb_expr_destroy(c1); free(c1);
			return 0;
		}
		if (binaryOp(NOWDB_EXPR_OP_GT, c1, c2, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "ERROR: wrong type: %u\n", t);
			return 0;
		}
		
		fprintf(stderr, "[GT(INT)] case %d: %ld, %ld = %ld\n", i, a, b, r);

		switch(i) {
		case 2: if (!r) return 0; break;
		case 3: if (!r) return 0; break;
		default: if (r) return 0;
		}
	}
	return 1;
}

int testNEInt() {
	nowdb_expr_t c1,c2;
	nowdb_type_t     t;
	int64_t        a,b;
	nowdb_value_t    r;

	a=5;
	for(int i=0;i<4;i++) {
		if (mkConst(&c1, &a, NOWDB_TYP_INT) != 0) return 0;

		switch(i) {
		case 0: b=a; break;
		case 1: b=a+a; break;
		case 2: b=a-a; break;
		case 3: b=a-2*a; break;
		}
		if (mkConst(&c2, &b, NOWDB_TYP_INT) != 0) {
			nowdb_expr_destroy(c1); free(c1);
			return 0;
		}
		if (binaryOp(NOWDB_EXPR_OP_NE, c1, c2, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "ERROR: wrong type: %u\n", t);
			return 0;
		}
		
		fprintf(stderr, "[NE(INT)] case %d: %ld, %ld = %ld\n", i, a, b, r);

		switch(i) {
		case 0: if (r) return 0; break;
		default: if (!r) return 0;
		}
	}
	return 1;
}

int testEQFloat() {
	nowdb_expr_t c1,c2;
	nowdb_type_t     t;
	double         a,b;
	nowdb_value_t    r;

	a=5.0;
	for(int i=0;i<5;i++) {
		if (mkConst(&c1, &a, NOWDB_TYP_FLOAT) != 0) return 0;

		switch(i) {
		case 0: b=a; break;
		case 1: b=a+a; break;
		case 2: b=a-a; break;
		case 3: b=a-2*a; break;
		case 4: b=0.5*a; break;
		}
		if (mkConst(&c2, &b, NOWDB_TYP_FLOAT) != 0) {
			nowdb_expr_destroy(c1); free(c1);
			return 0;
		}
		if (binaryOp(NOWDB_EXPR_OP_EQ, c1, c2, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "ERROR: wrong type: %u\n", t);
			return 0;
		}
		
		fprintf(stderr, "[EQ(FLOAT)] case %d: %f, %f = %ld\n", i, a, b, r);

		switch(i) {
		case 0: if (!r) return 0; break;
		default: if (r) return 0;
		}
	}
	return 1;
}

int testNEFloat() {
	nowdb_expr_t c1,c2;
	nowdb_type_t     t;
	double         a,b;
	nowdb_value_t    r;

	a=5.0;
	for(int i=0;i<5;i++) {
		if (mkConst(&c1, &a, NOWDB_TYP_FLOAT) != 0) return 0;

		switch(i) {
		case 0: b=a; break;
		case 1: b=a+a; break;
		case 2: b=a-a; break;
		case 3: b=a-2*a; break;
		case 4: b=0.5*a; break;
		}
		if (mkConst(&c2, &b, NOWDB_TYP_FLOAT) != 0) {
			nowdb_expr_destroy(c1); free(c1);
			return 0;
		}
		if (binaryOp(NOWDB_EXPR_OP_NE, c1, c2, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "ERROR: wrong type: %u\n", t);
			return 0;
		}
		
		fprintf(stderr, "[NE(FLOAT)] case %d: %f, %f = %ld\n", i, a, b, r);

		switch(i) {
		case 0: if (r) return 0; break;
		default: if (!r) return 0;
		}
	}
	return 1;
}

int testLEFloat() {
	nowdb_expr_t c1,c2;
	nowdb_type_t     t;
	double         a,b;
	nowdb_value_t    r;

	a=5.0;
	for(int i=0;i<5;i++) {
		if (mkConst(&c1, &a, NOWDB_TYP_FLOAT) != 0) return 0;

		switch(i) {
		case 0: b=a; break;
		case 1: b=a+a; break;
		case 2: b=a-a; break;
		case 3: b=a-2*a; break;
		case 4: b=0.5*a; break;
		}
		if (mkConst(&c2, &b, NOWDB_TYP_FLOAT) != 0) {
			nowdb_expr_destroy(c1); free(c1);
			return 0;
		}
		if (binaryOp(NOWDB_EXPR_OP_LE, c1, c2, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "ERROR: wrong type: %u\n", t);
			return 0;
		}
		
		fprintf(stderr, "[LE(FLOAT)] case %d: %f, %f = %ld\n", i, a, b, r);

		switch(i) {
		case 0:
		case 1: if (!r) return 0; break;
		default: if (r) return 0;
		}
	}
	return 1;
}

int testGEFloat() {
	nowdb_expr_t c1,c2;
	nowdb_type_t     t;
	double         a,b;
	nowdb_value_t    r;

	a=5.0;
	for(int i=0;i<5;i++) {
		if (mkConst(&c1, &a, NOWDB_TYP_FLOAT) != 0) return 0;

		switch(i) {
		case 0: b=a; break;
		case 1: b=a+a; break;
		case 2: b=a-a; break;
		case 3: b=a-2*a; break;
		case 4: b=0.5*a; break;
		}
		if (mkConst(&c2, &b, NOWDB_TYP_FLOAT) != 0) {
			nowdb_expr_destroy(c1); free(c1);
			return 0;
		}
		if (binaryOp(NOWDB_EXPR_OP_GE, c1, c2, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "ERROR: wrong type: %u\n", t);
			return 0;
		}
		
		fprintf(stderr, "[GE(FLOAT)] case %d: %f, %f = %ld\n", i, a, b, r);

		switch(i) {
		case 1: if (r) return 0; break;
		default: if (!r) return 0;
		}
	}
	return 1;
}

int testLTFloat() {
	nowdb_expr_t c1,c2;
	nowdb_type_t     t;
	double         a,b;
	nowdb_value_t    r;

	a=5.0;
	for(int i=0;i<5;i++) {
		if (mkConst(&c1, &a, NOWDB_TYP_FLOAT) != 0) return 0;

		switch(i) {
		case 0: b=a; break;
		case 1: b=a+a; break;
		case 2: b=a-a; break;
		case 3: b=a-2*a; break;
		case 4: b=0.5*a; break;
		}
		if (mkConst(&c2, &b, NOWDB_TYP_FLOAT) != 0) {
			nowdb_expr_destroy(c1); free(c1);
			return 0;
		}
		if (binaryOp(NOWDB_EXPR_OP_LT, c1, c2, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "ERROR: wrong type: %u\n", t);
			return 0;
		}
		
		fprintf(stderr, "[LT(FLOAT)] case %d: %f, %f = %ld\n", i, a, b, r);

		switch(i) {
		case 1: if (!r) return 0; break;
		default: if (r) return 0;
		}
	}
	return 1;
}

int testGTFloat() {
	nowdb_expr_t c1,c2;
	nowdb_type_t     t;
	double         a,b;
	nowdb_value_t    r;

	a=5.0;
	for(int i=0;i<5;i++) {
		if (mkConst(&c1, &a, NOWDB_TYP_FLOAT) != 0) return 0;

		switch(i) {
		case 0: b=a; break;
		case 1: b=a+a; break;
		case 2: b=a-a; break;
		case 3: b=a-2*a; break;
		case 4: b=0.5*a; break;
		}
		if (mkConst(&c2, &b, NOWDB_TYP_FLOAT) != 0) {
			nowdb_expr_destroy(c1); free(c1);
			return 0;
		}
		if (binaryOp(NOWDB_EXPR_OP_GT, c1, c2, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "ERROR: wrong type: %u\n", t);
			return 0;
		}
		
		fprintf(stderr, "[GT(FLOAT)] case %d: %f, %f = %ld\n", i, a, b, r);

		switch(i) {
		case 0:
		case 1: if (r) return 0; break;
		default: if (!r) return 0;
		}
	}
	return 1;
}

int testEQText() {
	nowdb_expr_t c1,c2;
	nowdb_type_t     t;
	char         *a,*b;
	nowdb_value_t    r;
	char *str1 = "foo";
	char *str2 = "bar";

	a=str1;
	for(int i=0;i<2;i++) {
		if (mkConst(&c1, a, NOWDB_TYP_TEXT) != 0) return 0;

		switch(i) {
		case 0: b=a; break;
		case 1: b=str2; break;
		}
		if (mkConst(&c2, b, NOWDB_TYP_TEXT) != 0) {
			nowdb_expr_destroy(c1); free(c1);
			return 0;
		}
		if (binaryOp(NOWDB_EXPR_OP_EQ, c1, c2, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "ERROR: wrong type: %u\n", t);
			return 0;
		}
		
		fprintf(stderr, "[EQ(TEXT)] case %d: %s, %s = %ld\n", i, a, b, r);

		switch(i) {
		case 0: if (!r) return 0; break;
		default: if (r) return 0;
		}
	}
	return 1;
}

int testNEText() {
	nowdb_expr_t c1,c2;
	nowdb_type_t     t;
	char         *a,*b;
	nowdb_value_t    r;
	char *str1 = "foo";
	char *str2 = "bar";

	a=str1;
	for(int i=0;i<2;i++) {
		if (mkConst(&c1, a, NOWDB_TYP_TEXT) != 0) return 0;

		switch(i) {
		case 0: b=a; break;
		case 1: b=str2; break;
		}
		if (mkConst(&c2, b, NOWDB_TYP_TEXT) != 0) {
			nowdb_expr_destroy(c1); free(c1);
			return 0;
		}
		if (binaryOp(NOWDB_EXPR_OP_NE, c1, c2, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "ERROR: wrong type: %u\n", t);
			return 0;
		}
		
		fprintf(stderr, "[NE(TEXT)] case %d: %s, %s = %ld\n", i, a, b, r);

		switch(i) {
		case 0: if (r) return 0; break;
		default: if (!r) return 0;
		}
	}
	return 1;
}

int testInUInt() {
	nowdb_expr_t c1, c2;
	uint64_t v=5;
	uint64_t w,x,y,z;
	nowdb_type_t  t;
	nowdb_value_t r;

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

		if (mkConst(&c1, &v, NOWDB_TYP_UINT) != 0) return 0;
		c2 = mkIn(NOWDB_TYP_UINT, 8, 4, &w, &x, &y, &z);
		if (c2 == NULL)  return 0;

		if (binaryOp(NOWDB_EXPR_OP_IN, c1, c2, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "wrong type: %u\n", t);
			return 0;
		}

		fprintf(stderr, "[InUInt] case %d: %lu (%lu)\n", i, r, v);

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
	nowdb_expr_t c1, c2;
	int64_t v=5;
	int64_t w,x,y,z;
	nowdb_type_t  t;
	nowdb_value_t r;

	w=1; x=2; y=w+x; z=x+y;

	fprintf(stderr, "[InInt] search in (%ld, %ld, %ld, %ld)\n",
	                                               w, x, y, z);
	for(int i=0;i<5;i++) {
		switch(i) {
		case 0: v = 3; break;
		case 1: v = 4; break;
		case 2: v = 5; break; 
		case 3: v = 6; break;
		case 4: v = 9; break;
		}

		if (mkConst(&c1, &v, NOWDB_TYP_INT) != 0) return 0;
		c2 = mkIn(NOWDB_TYP_INT, 8, 4, &w, &x, &y, &z);
		if (c2 == NULL)  return 0;

		if (binaryOp(NOWDB_EXPR_OP_IN, c1, c2, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "wrong type: %u\n", t);
			return 0;
		}

		fprintf(stderr, "[InInt] case %d: %lu (%ld)\n", i, r, v);

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

int testInFloat() {
	nowdb_expr_t c1, c2;
	double  v=5;
	double  w,x,y,z;
	nowdb_type_t  t;
	nowdb_value_t r;

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

		if (mkConst(&c1, &v, NOWDB_TYP_FLOAT) != 0) return 0;
		c2 = mkIn(NOWDB_TYP_FLOAT, 8, 4, &w, &x, &y, &z);
		if (c2 == NULL)  return 0;

		if (binaryOp(NOWDB_EXPR_OP_IN, c1, c2, &t, &r) != 0) return 0;
		if (t != NOWDB_TYP_BOOL) {
			fprintf(stderr, "wrong type: %u\n", t);
			return 0;
		}

		fprintf(stderr, "[InFloat] case %d: %lu (%f)\n", i, r, v);

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

#define BOOL(f,o,o1,o2) \
	err = nowdb_expr_newOp(f, o, o1, o2); \
	if (err != NOWDB_OK) { \
		fprintf(stderr, "ERROR: cannot create new operator\n"); \
		nowdb_expr_destroy(o1); free(o1); \
		nowdb_expr_destroy(o2); free(o2); \
		nowdb_err_print(err); nowdb_err_release(err); \
		return -1; \
	}

#define COMPARE(f,o,o1,o2) \
	err = nowdb_expr_newOp(f, o, o1, o2); \
	if (err != NOWDB_OK) { \
		fprintf(stderr, "ERROR: cannot create new operator\n"); \
		nowdb_expr_destroy(o1); free(o1); \
		nowdb_expr_destroy(o2); free(o2); \
		nowdb_err_print(err); nowdb_err_release(err); \
		return -1; \
	}

#define FIELD(f,x) \
	err = nowdb_expr_newEdgeField(f,"any",x,NOWDB_TYP_UINT,2); \
	if (err != NOWDB_OK) { \
		fprintf(stderr, "ERROR: cannot create edge field\n"); \
		nowdb_err_print(err); nowdb_err_release(err); \
		return -1; \
	}

#define CONST(f,x,t) \
	if (mkConst(f,x,t) != 0) return -1;

#define TESTRANGE(fl,o,s,e,a,b,x) \
	r = nowdb_expr_range(fl, 2, o, \
	          (char*)s, (char*)e); \
	nowdb_expr_destroy(fl); free(fl); \
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
			fprintf(stderr, \
			"expected: %lu.%lu & %lu.%lu\n", a, a, b, b); \
			return 0; \
		} \
	}

int testRange() {
	nowdb_err_t err;
	uint16_t off[2];
	nowdb_expr_t f1, f2, f3, f4;
	nowdb_expr_t c1, c2, c3, c4;
	nowdb_expr_t e1, e2, e3, e4;
	nowdb_expr_t b1, b2, b3;
	uint64_t start[2], end[2];
	uint64_t v1=1, v2=2;
	uint64_t va, vb;
	char x, r;
	
	off[0] = OFF_USR_FIELD;
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

			FIELD(&f1, OFF_USR_FIELD);
			CONST(&c1, &v1, NOWDB_TYP_UINT);
			COMPARE(&e1, NOWDB_EXPR_OP_EQ, f1, c1);

			FIELD(&f2, NOWDB_OFF_ORIGIN);
			CONST(&c2, &v1, NOWDB_TYP_UINT);
			COMPARE(&e2, NOWDB_EXPR_OP_EQ, f2, c2);

			BOOL(&b1, NOWDB_EXPR_OP_AND, e1, e2);

			break;
		case 1:
			fprintf(stderr, "RANGE with EQ (fails)\n");
			x = 0;
			va = v1;
			vb = v1;

			FIELD(&f1, OFF_USR_FIELD);
			CONST(&c1, &v1, NOWDB_TYP_UINT);
			COMPARE(&e1, NOWDB_EXPR_OP_NE, f1, c1);

			FIELD(&f2, NOWDB_OFF_ORIGIN);
			CONST(&c2, &v1, NOWDB_TYP_UINT);
			COMPARE(&e2, NOWDB_EXPR_OP_EQ, f2, c2);

			BOOL(&b1, NOWDB_EXPR_OP_AND, e1, e2);

			break;
		case 2:
			fprintf(stderr, "RANGE with GE/LE (success)\n");

			x = 1;

			va = v1;
			vb = v2;

			FIELD(&f1, OFF_USR_FIELD);
			CONST(&c1, &v1, NOWDB_TYP_UINT);
			COMPARE(&e1, NOWDB_EXPR_OP_GE, f1, c1);

			FIELD(&f2, NOWDB_OFF_ORIGIN);
			CONST(&c2, &v1, NOWDB_TYP_UINT);
			COMPARE(&e2, NOWDB_EXPR_OP_GE, f2, c2);

			BOOL(&b2, NOWDB_EXPR_OP_AND, e1, e2);

			FIELD(&f3, OFF_USR_FIELD);
			CONST(&c3, &v2, NOWDB_TYP_UINT);
			COMPARE(&e3, NOWDB_EXPR_OP_LE, f3, c3);

			FIELD(&f4, NOWDB_OFF_ORIGIN);
			CONST(&c4, &v2, NOWDB_TYP_UINT);
			COMPARE(&e4, NOWDB_EXPR_OP_LE, f4, c4);

			BOOL(&b3, NOWDB_EXPR_OP_AND, e3, e4);

			BOOL(&b1, NOWDB_EXPR_OP_AND, b2, b3);

			nowdb_expr_show(b1, stderr); fprintf(stderr, "\n");

			break;
		case 3:
			fprintf(stderr, "RANGE with GE/LE (fails)\n");

			x = 0;

			va = v1;
			vb = v2;

			FIELD(&f1, OFF_USR_FIELD);
			CONST(&c1, &v1, NOWDB_TYP_UINT);
			COMPARE(&e1, NOWDB_EXPR_OP_LE, f1, c1);

			FIELD(&f2, NOWDB_OFF_ORIGIN);
			CONST(&c2, &v1, NOWDB_TYP_UINT);
			COMPARE(&e2, NOWDB_EXPR_OP_LE, f2, c2);

			BOOL(&b2, NOWDB_EXPR_OP_AND, e1, e2);

			FIELD(&f3, OFF_USR_FIELD);
			CONST(&c3, &v2, NOWDB_TYP_UINT);
			COMPARE(&e3, NOWDB_EXPR_OP_LE, f3, c3);

			FIELD(&f4, NOWDB_OFF_ORIGIN);
			CONST(&c4, &v2, NOWDB_TYP_UINT);
			COMPARE(&e4, NOWDB_EXPR_OP_LE, f4, c4);

			BOOL(&b3, NOWDB_EXPR_OP_AND, e3, e4);

			BOOL(&b1, NOWDB_EXPR_OP_AND, b2, b3);

			nowdb_expr_show(b1, stderr); fprintf(stderr, "\n");

			break;
		}
		TESTRANGE(b1, off, start, end, va, vb, x);
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
	if (!testEQText()) {
		fprintf(stderr, "testEQText() failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!testNEText()) {
		fprintf(stderr, "testNEText() failed\n");
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
