/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for manually creating rows
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/time.h>
#include <nowdb/fun/expr.h>

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>
#include <math.h>
#include <limits.h>
#include <float.h>

#define IT 1000
#define MAXSTR 20

char *randstr(uint32_t *sz) {
	char *str;
	uint32_t s;

	do {s = rand()%MAXSTR;} while(s==0);

	str = malloc(s+1);

	for(int i=0; i<s; i++) {
		str[i] = rand()%25;
		str[i]+=65;
	}
	str[s] = 0; *sz = s;
	// fprintf(stderr, "'%s'\n", str);
	return str;
}

int mkConst(nowdb_expr_t *c, void *v, nowdb_type_t t) {
	nowdb_err_t err;
	void *x = calloc(1,sizeof(uint64_t));
	if (x == NULL) {
		fprintf(stderr, "ERROR: cannot allocate value\n");
		return -1;
	}
	memcpy(x,v,8);
	err = nowdb_expr_newConstant(c, x, t); \
	if (err != NOWDB_OK) {
		fprintf(stderr, "ERROR: cannot create new constant\n"); \
		free(x);
		nowdb_err_print(err); nowdb_err_release(err);
		return -1;
	}
	return 0;
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

int testUINTAdd() {
	nowdb_expr_t c1, c2;
	uint64_t a, b, c, r;
	nowdb_type_t t = NOWDB_TYP_UINT;

	a = rand()%1000000;
	b = rand()%1000000;

	c = a + b;

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (mkConst(&c2, &b, t) != 0) {
		nowdb_expr_destroy(c1); free(c1);
		return -1;
	}
	if (binaryOp(NOWDB_EXPR_OP_ADD, c1, c2, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_UINT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING %lu + %lu = %lu\n", a, b, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %lu / %lu\n", c, r);
		return -1;
	}
	return 0;
}

int testINTAdd() {
	nowdb_expr_t c1, c2;
	int64_t a, b, c, r;
	nowdb_type_t t = NOWDB_TYP_INT;

	a = rand()%1000000;
	b = rand()%1000000;

	if (rand()%2 == 0) a*=-1;

	c = a + b;

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (mkConst(&c2, &b, t) != 0) {
		nowdb_expr_destroy(c1); free(c1);
		return -1;
	}
	if (binaryOp(NOWDB_EXPR_OP_ADD, c1, c2, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_INT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING %ld + %ld = %ld\n", a, b, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %ld / %ld\n", c, r);
		return -1;
	}
	return 0;
}

int testFLOATAdd() {
	nowdb_expr_t c1, c2;
	double a, b, c, d, r;
	nowdb_type_t t = NOWDB_TYP_FLOAT;

	a = rand()%1000000;
	b = rand()%1000000;
	d = rand()%1000000; d++;

	if (rand()%2 == 0) a*=-1;
	if (rand()%2 == 0) b*=-1;

	if (rand()%2 == 0) a/=d;
	if (rand()%2 == 0) b/=d;

	c = a + b;

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (mkConst(&c2, &b, t) != 0) {
		nowdb_expr_destroy(c1); free(c1);
		return -1;
	}
	if (binaryOp(NOWDB_EXPR_OP_ADD, c1, c2, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_FLOAT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING %f + %f = %f\n", a, b, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %f / %f\n", c, r);
		return -1;
	}
	return 0;
}

int testUINTSub() {
	nowdb_expr_t c1, c2;
	uint64_t a, b, c, r;
	nowdb_type_t t = NOWDB_TYP_UINT;

	a = rand()%1000000;
	b = rand()%1000000;

	// swap
	if (a < b) {
		a^=b;b^=a;a^=b;
	}

	c = a - b;

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (mkConst(&c2, &b, t) != 0) {
		nowdb_expr_destroy(c1); free(c1);
		return -1;
	}
	if (binaryOp(NOWDB_EXPR_OP_SUB, c1, c2, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_UINT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING %lu - %lu = %lu\n", a, b, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %lu / %lu\n", c, r);
		return -1;
	}
	return 0;
}

int testINTSub() {
	nowdb_expr_t c1, c2;
	int64_t a, b, c, r;
	nowdb_type_t t = NOWDB_TYP_INT;

	a = rand()%1000000;
	b = rand()%1000000;

	if (rand()%2 == 0) a *= -1;

	c = a - b;

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (mkConst(&c2, &b, t) != 0) {
		nowdb_expr_destroy(c1); free(c1);
		return -1;
	}
	if (binaryOp(NOWDB_EXPR_OP_SUB, c1, c2, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_INT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING %ld - %ld = %ld\n", a, b, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %ld / %ld\n", c, r);
		return -1;
	}
	return 0;
}

int testFLOATSub() {
	nowdb_expr_t c1, c2;
	double a, b, c, d, r;
	nowdb_type_t t = NOWDB_TYP_FLOAT;

	a = rand()%1000000;
	b = rand()%1000000;
	d = rand()%1000000; d++;

	if (rand()%2 == 0) a *= -1;
	if (rand()%2 == 0) b *= -1;
	if (rand()%2 == 0) a /= d;
	if (rand()%2 == 0) b /= d;

	c = a - b;

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (mkConst(&c2, &b, t) != 0) {
		nowdb_expr_destroy(c1); free(c1);
		return -1;
	}
	if (binaryOp(NOWDB_EXPR_OP_SUB, c1, c2, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_FLOAT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING %f - %f = %f\n", a, b, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %f / %f\n", c, r);
		return -1;
	}
	return 0;
}

int testUINTMul() {
	nowdb_expr_t c1, c2;
	uint64_t a, b, c, r;
	nowdb_type_t t = NOWDB_TYP_UINT;

	a = rand()%1000;
	b = rand()%1000;

	c = a * b;

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (mkConst(&c2, &b, t) != 0) {
		nowdb_expr_destroy(c1); free(c1);
		return -1;
	}
	if (binaryOp(NOWDB_EXPR_OP_MUL, c1, c2, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_UINT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING %lu * %lu = %lu\n", a, b, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %lu / %lu\n", c, r);
		return -1;
	}
	return 0;
}

int testINTMul() {
	nowdb_expr_t c1, c2;
	int64_t a, b, c, r;
	nowdb_type_t t = NOWDB_TYP_INT;

	a = rand()%1000;
	b = rand()%1000;

	if (rand()%2 == 0) a*=-1;

	c = a * b;

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (mkConst(&c2, &b, t) != 0) {
		nowdb_expr_destroy(c1); free(c1);
		return -1;
	}
	if (binaryOp(NOWDB_EXPR_OP_MUL, c1, c2, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_INT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING %ld * %ld = %ld\n", a, b, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %ld / %ld\n", c, r);
		return -1;
	}
	return 0;
}

int testFLOATMul() {
	nowdb_expr_t c1, c2;
	double a, b, c, d, r;
	nowdb_type_t t = NOWDB_TYP_FLOAT;

	a = rand()%1000;
	b = rand()%1000;
	d = rand()%1000; d++;

	if (rand()%2 == 0) a*=-1;
	if (rand()%2 == 0) b*=-1;
	if (rand()%2 == 0) a/=d;
	if (rand()%2 == 0) b/=d;

	c = a * b;

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (mkConst(&c2, &b, t) != 0) {
		nowdb_expr_destroy(c1); free(c1);
		return -1;
	}
	if (binaryOp(NOWDB_EXPR_OP_MUL, c1, c2, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_FLOAT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING %f * %f = %f\n", a, b, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %f / %f\n", c, r);
		return -1;
	}
	return 0;
}

int testUINTDiv() {
	nowdb_expr_t c1, c2;
	uint64_t a, b, c, r;
	nowdb_type_t t = NOWDB_TYP_UINT;

	a = rand()%1000;
	b = rand()%1000;

	c = b == 0 ? 0 : a/b;

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (mkConst(&c2, &b, t) != 0) {
		nowdb_expr_destroy(c1); free(c1);
		return -1;
	}
	if (binaryOp(NOWDB_EXPR_OP_DIV, c1, c2, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_UINT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING %lu / %lu = %lu\n", a, b, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %lu / %lu\n", c, r);
		return -1;
	}
	return 0;
}

int testINTDiv() {
	nowdb_expr_t c1, c2;
	int64_t a, b, c, r;
	nowdb_type_t t = NOWDB_TYP_INT;

	a = rand()%1000;
	b = rand()%1000;

	if (rand()%2 == 0) a*=-1;

	c = b == 0 ? 0 : a/b;

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (mkConst(&c2, &b, t) != 0) {
		nowdb_expr_destroy(c1); free(c1);
		return -1;
	}
	if (binaryOp(NOWDB_EXPR_OP_DIV, c1, c2, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_INT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING %ld / %ld = %ld\n", a, b, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %ld / %ld\n", c, r);
		return -1;
	}
	return 0;
}

int testFLOATDiv() {
	nowdb_expr_t c1, c2;
	double a, b, c, d, r;
	nowdb_type_t t = NOWDB_TYP_FLOAT;

	a = rand()%1000;
	b = rand()%1000;
	d = rand()%1000; d++;

	if (rand()%2 == 0) a*=-1;
	if (rand()%2 == 0) b*=-1;
	if (rand()%2 == 0) a/=d;
	if (rand()%2 == 0) b/=d;

	c = b == 0 ? 0 : a/b;

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (mkConst(&c2, &b, t) != 0) {
		nowdb_expr_destroy(c1); free(c1);
		return -1;
	}
	if (binaryOp(NOWDB_EXPR_OP_DIV, c1, c2, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_FLOAT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING %f / %f = %f\n", a, b, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %f / %f\n", c, r);
		return -1;
	}
	return 0;
}

int testUINTRem() {
	nowdb_expr_t c1, c2;
	uint64_t a, b, c, r;
	nowdb_type_t t = NOWDB_TYP_UINT;

	a = rand()%1000;
	b = rand()%1000;

	c = b == 0 ? 0 : a%b;

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (mkConst(&c2, &b, t) != 0) {
		nowdb_expr_destroy(c1); free(c1);
		return -1;
	}
	if (binaryOp(NOWDB_EXPR_OP_REM, c1, c2, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_UINT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING %ld %% %ld = %ld\n", a, b, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %ld %% %ld\n", c, r);
		return -1;
	}
	return 0;
}

int testINTRem() {
	nowdb_expr_t c1, c2;
	int64_t a, b, c, r;
	nowdb_type_t t = NOWDB_TYP_INT;

	a = rand()%1000;
	b = rand()%1000;

	if (rand()%2 == 0) a*=-1;

	c = b == 0 ? 0 : a%b;

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (mkConst(&c2, &b, t) != 0) {
		nowdb_expr_destroy(c1); free(c1);
		return -1;
	}
	if (binaryOp(NOWDB_EXPR_OP_REM, c1, c2, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_INT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING %ld %% %ld = %ld\n", a, b, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %ld %% %ld\n", c, r);
		return -1;
	}
	return 0;
}

int testUINTPow() {
	nowdb_expr_t c1, c2;
	uint64_t a, b;
	double   c, r;
	nowdb_type_t t = NOWDB_TYP_UINT;

	a = rand()%1000;
	b = rand()%10;

	c = pow(a,b);

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (mkConst(&c2, &b, t) != 0) {
		nowdb_expr_destroy(c1); free(c1);
		return -1;
	}
	if (binaryOp(NOWDB_EXPR_OP_POW, c1, c2, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_FLOAT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING %lu ^ %lu = %f\n", a, b, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %f != %f\n", c, r);
		return -1;
	}
	return 0;
}

int testINTPow() {
	nowdb_expr_t c1, c2;
	int64_t a, b;
	double  c, r;
	nowdb_type_t t = NOWDB_TYP_INT;

	a = rand()%1000;
	b = rand()%10;

	if (rand()%2 == 0) a*=-1;
	if (rand()%2 == 0) b*=-1;

	c = pow(a,b);

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (mkConst(&c2, &b, t) != 0) {
		nowdb_expr_destroy(c1); free(c1);
		return -1;
	}
	if (binaryOp(NOWDB_EXPR_OP_POW, c1, c2, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_FLOAT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING %ld ^ %ld = %f\n", a, b, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %f /= %f\n", c, r);
		return -1;
	}
	return 0;
}

int testFLOATPow() {
	nowdb_expr_t c1, c2;
	double a, b, c, d, r;
	nowdb_type_t t = NOWDB_TYP_FLOAT;

	a = rand()%1000;
	b = rand()%10;
	d = rand()%1000; d++;

	if (rand()%2 == 0) a*=-1;
	if (rand()%2 == 0) b*=-1;
	if (rand()%2 == 0) a/=d;

	c = pow(a,b);

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (mkConst(&c2, &b, t) != 0) {
		nowdb_expr_destroy(c1); free(c1);
		return -1;
	}
	if (binaryOp(NOWDB_EXPR_OP_POW, c1, c2, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_FLOAT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING %f ^ %f = %f\n", a, b, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %f ^ %f\n", c, r);
		return -1;
	}
	return 0;
}

int testFLOATLog() {
	nowdb_expr_t c1;
	double a, c, d, r;
	nowdb_type_t t = NOWDB_TYP_FLOAT;

	a = rand()%1000;
	d = rand()%1000; d++;

	if (rand()%2 == 0) a/=d;

	c = log(a);

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (unaryOp(NOWDB_EXPR_OP_LOG, c1, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_FLOAT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING log(%f) = %f\n", a, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %f ^ %f\n", c, r);
		return -1;
	}
	return 0;
}

int testFLOATRound() {
	nowdb_expr_t c1;
	double a, c, d, r;
	nowdb_type_t t = NOWDB_TYP_FLOAT;

	a = rand()%1000;
	d = rand()%1000; d++;

	if (rand()%2 == 0) a*=-1;
	if (rand()%2 == 0) a/=d;

	c = round(a);

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (unaryOp(NOWDB_EXPR_OP_ROUND, c1, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_FLOAT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING ROUND(%f) = %f\n", a, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %f ^ %f\n", c, r);
		return -1;
	}
	return 0;
}

int testFLOATCeil() {
	nowdb_expr_t c1;
	double a, c, d, r;
	nowdb_type_t t = NOWDB_TYP_FLOAT;

	a = rand()%1000;
	d = rand()%1000; d++;

	if (rand()%2 == 0) a*=-1;
	if (rand()%2 == 0) a/=d;

	c = ceil(a);

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (unaryOp(NOWDB_EXPR_OP_CEIL, c1, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_FLOAT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING CEIL(%f) = %f\n", a, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %f ^ %f\n", c, r);
		return -1;
	}
	return 0;
}

int testFLOATFloor() {
	nowdb_expr_t c1;
	double a, c, d, r;
	nowdb_type_t t = NOWDB_TYP_FLOAT;

	a = rand()%1000;
	d = rand()%1000; d++;

	if (rand()%2 == 0) a*=-1;
	if (rand()%2 == 0) a/=d;

	c = floor(a);

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (unaryOp(NOWDB_EXPR_OP_FLOOR, c1, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_FLOAT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING FLOOR(%f) = %f\n", a, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %f ^ %f\n", c, r);
		return -1;
	}
	return 0;
}

int testINTAbs() {
	nowdb_expr_t c1;
	int64_t a, c, r;
	nowdb_type_t t = NOWDB_TYP_INT;

	a = rand()%1000;
	if (rand()%2 == 0) a*=-1;

	c = abs(a);

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (unaryOp(NOWDB_EXPR_OP_ABS, c1, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_INT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING ABS(%ld) = %ld\n", a, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %ld ^ %ld\n", c, r);
		return -1;
	}
	return 0;
}

int testFLOATAbs() {
	nowdb_expr_t c1;
	double a, c, d, r;
	nowdb_type_t t = NOWDB_TYP_FLOAT;

	a = rand()%1000;
	d = rand()%1000; d++;

	if (rand()%2 == 0) a*=-1;
	if (rand()%2 == 0) a/=d;

	c = fabs(a);

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (unaryOp(NOWDB_EXPR_OP_ABS, c1, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_FLOAT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING ABS(%f) = %f\n", a, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %f ^ %f\n", c, r);
		return -1;
	}
	return 0;
}

int testFLOAT2FLOAT() {
	nowdb_expr_t c1;
	double a, c, d, r;
	nowdb_type_t t = NOWDB_TYP_FLOAT;

	a = rand()%1000;
	d = rand()%1000; d++;

	if (rand()%2 == 0) a*=-1;
	if (rand()%2 == 0) a/=d;

	c = (double)a;

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (unaryOp(NOWDB_EXPR_OP_FLOAT, c1, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_FLOAT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING float(%f) = %f\n", a, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %f ^ %f\n", c, r);
		return -1;
	}
	return 0;
}

int testINT2FLOAT() {
	nowdb_expr_t c1;
	int64_t a;
	double c, r;
	nowdb_type_t t = NOWDB_TYP_INT;

	a = rand()%1000;
	if (rand()%2 == 0) a*=-1;

	c = (double)a;

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (unaryOp(NOWDB_EXPR_OP_FLOAT, c1, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_FLOAT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING float(%ld) = %f\n", a, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %f ^ %f\n", c, r);
		return -1;
	}
	return 0;
}

int testUINT2FLOAT() {
	nowdb_expr_t c1;
	uint64_t a;
	double c, r;
	nowdb_type_t t = NOWDB_TYP_UINT;

	a = rand()%1000;
	c = (double)a;

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (unaryOp(NOWDB_EXPR_OP_FLOAT, c1, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_FLOAT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING float(%lu) = %f\n", a, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %f ^ %f\n", c, r);
		return -1;
	}
	return 0;
}

int testUINT2INT() {
	nowdb_expr_t c1;
	uint64_t a;
	int64_t c, r;
	nowdb_type_t t = NOWDB_TYP_UINT;

	a = rand()%1000;
	c = (int64_t)a;

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (unaryOp(NOWDB_EXPR_OP_INT, c1, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_INT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING int(%lu) = %ld\n", a, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %ld / %ld\n", c, r);
		return -1;
	}
	return 0;
}

int testFLOAT2INT() {
	nowdb_expr_t c1;
	double a;
	int64_t c, r;
	nowdb_type_t t = NOWDB_TYP_FLOAT;

	a = rand()%1000;
	if (rand()%2 == 0) a*=-1;
	if (rand()%2 == 0) a/=(rand()%1000);
	
	c = (int64_t)a;

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (unaryOp(NOWDB_EXPR_OP_INT, c1, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_INT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING int(%f) = %ld\n", a, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %ld / %ld\n", c, r);
		return -1;
	}
	return 0;
}

int testFLOAT2TIME() {
	nowdb_expr_t c1;
	double a;
	int64_t c, r;
	nowdb_type_t t = NOWDB_TYP_FLOAT;

	a = rand()%1000000000;
	if (rand()%2 == 0) a*=-1;
	if (rand()%2 == 0) a/=(rand()%1000);
	
	c = (int64_t)a;

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (unaryOp(NOWDB_EXPR_OP_TIME, c1, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_TIME) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING time(%f) = %ld\n", a, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %ld / %ld\n", c, r);
		return -1;
	}
	return 0;
}

int testFLOAT2UINT() {
	nowdb_expr_t c1;
	double a;
	uint64_t c, r;
	nowdb_type_t t = NOWDB_TYP_FLOAT;

	a = rand()%1000000000;
	if (rand()%2 == 0) a*=-1;
	if (rand()%2 == 0) a/=(rand()%1000);
	
	c = (uint64_t)a;

	if (mkConst(&c1, &a, t) != 0) return -1;
	if (unaryOp(NOWDB_EXPR_OP_UINT, c1, &t, &r) != 0) return -1;
	if (t != NOWDB_TYP_UINT) {
		fprintf(stderr, "ERROR: wrong type returned: %u\n", t);
		return -1;
	}
	fprintf(stderr, "COMPUTING uint(%f) = %lu\n", a, r);
	if (r != c) {
		fprintf(stderr, "ERROR: results differ: %lu / %lu\n", c, r);
		return -1;
	}
	return 0;
}

int main() {
	int rc = EXIT_SUCCESS;

	srand(time(NULL));

	if (!nowdb_init()) {
		fprintf(stderr, "cannot init library\n");
		return EXIT_FAILURE;
	}
	for(int i=0;i<IT;i++) {
		// UINT
		if (testUINTAdd() != 0) {
			fprintf(stderr, "testUIntAdd failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testUINTSub() != 0) {
			fprintf(stderr, "testUIntSub failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testUINTMul() != 0) {
			fprintf(stderr, "testUIntSub failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testUINTDiv() != 0) {
			fprintf(stderr, "testUIntDiv failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testUINTRem() != 0) {
			fprintf(stderr, "testUIntRem failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testUINTPow() != 0) {
			fprintf(stderr, "testUIntPow failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		// INT
		if (testINTAdd() != 0) {
			fprintf(stderr, "testIntAdd failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testINTSub() != 0) {
			fprintf(stderr, "testIntSub failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testINTMul() != 0) {
			fprintf(stderr, "testIntSub failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testINTDiv() != 0) {
			fprintf(stderr, "testIntDiv failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testINTRem() != 0) {
			fprintf(stderr, "testIntRem failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testINTPow() != 0) {
			fprintf(stderr, "testIntPow failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testINTAbs() != 0) {
			fprintf(stderr, "testIntAbsfailed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		// FLOAT
		if (testFLOATAdd() != 0) {
			fprintf(stderr, "testFloatAdd failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testFLOATSub() != 0) {
			fprintf(stderr, "testFloatSub failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testFLOATMul() != 0) {
			fprintf(stderr, "testFloatMul failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testFLOATDiv() != 0) {
			fprintf(stderr, "testFloatDiv failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testFLOATPow() != 0) {
			fprintf(stderr, "testFloatPow failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testFLOATLog() != 0) {
			fprintf(stderr, "testFloatLog failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testFLOATRound() != 0) {
			fprintf(stderr, "testFLOATRound failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testFLOATCeil() != 0) {
			fprintf(stderr, "testFLOATCeil failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testFLOATFloor() != 0) {
			fprintf(stderr, "testFLOATFloor failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testFLOATAbs() != 0) {
			fprintf(stderr, "testFLOATAbsfailed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}

		// Conversions
		if (testFLOAT2FLOAT() != 0) {
			fprintf(stderr, "testFLOAT2FLOAT\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testINT2FLOAT() != 0) {
			fprintf(stderr, "testINT2FLOAT\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testUINT2FLOAT() != 0) {
			fprintf(stderr, "testUINT2FLOAT\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testUINT2INT() != 0) {
			fprintf(stderr, "testUINT2INT\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testFLOAT2INT() != 0) {
			fprintf(stderr, "testFLOAT2INT\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testFLOAT2TIME() != 0) {
			fprintf(stderr, "testFLOAT2TIME\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
		if (testFLOAT2UINT() != 0) {
			fprintf(stderr, "testFLOAT2UINT\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}

cleanup:
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	nowdb_close();
	return rc;
}

