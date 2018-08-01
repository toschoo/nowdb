/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for SQL functions 
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/time.h>
#include <nowdb/fun/fun.h>

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>
#include <math.h>
#include <limits.h>
#include <float.h>

#define ITER 100

nowdb_edge_t *edges;

uint64_t aunity = NOWDB_UNITY_ADD;
uint64_t munity = NOWDB_UNITY_MUL;
double  afunity = NOWDB_UNITY_FADD;
double  mfunity = NOWDB_UNITY_FMUL;

uint64_t uinfty = NOWDB_UMAX;
int64_t iinfty = NOWDB_IMAX;
double finfty = NOWDB_FMAX;

uint64_t umin = NOWDB_UMIN;
int64_t imin = NOWDB_IMIN;
double ffmin = NOWDB_FMIN;

void getValue(void *v, nowdb_type_t type) {
	double f;
	uint64_t u;
	int64_t i;

	u = (uint64_t)rand()%1000;
	switch(type) {
		case NOWDB_TYP_FLOAT:
			f = sqrt((double)u);
			memcpy(v, &f, 8);
			break;

		case NOWDB_TYP_INT:
			if (rand()%3) i = ((int64_t)u);
			else i = -(int64_t)u;
			memcpy(v, &i, 8);
			break;

		default: 
			memcpy(v, &u, 8);
	}
}

int prepare(nowdb_type_t type, char calm) {
	int x;
	do x = calm?rand()%10:rand()%1000; while (x==0);
	edges = calloc(x, sizeof(nowdb_edge_t));
	if (edges == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return -1;
	}
	for(int i=0; i<x; i++) {
		getValue(&edges[i].weight, type);
		getValue(&edges[i].weight2, type);
	}
	return x;
}

#define OP(t, x, v) \
	switch(t) { \
	case NOWDB_FUN_SUM: x+=v; break; \
	case NOWDB_FUN_PROD:x*=v; break; \
	case NOWDB_FUN_MIN: x=x>v?v:x; break; \
	case NOWDB_FUN_MAX: x=x<v?v:x; break; \
	default: \
		return; \
	}
	

void op(uint32_t ftype, void *x, void *v, nowdb_type_t type) {
	switch(type) {
	case NOWDB_TYP_FLOAT:
		// fprintf(stderr, "%.4f x %.4f\n", *(double*)x, *(double*)v);
		OP(ftype, *(double*)x, *(double*)v); break;
	case NOWDB_TYP_UINT:
		// fprintf(stderr, "%lu x %lu\n", *(uint64_t*)x, *(uint64_t*)v);
		OP(ftype, *(uint64_t*)x, *(uint64_t*)v); break;
	case NOWDB_TYP_INT:
		// fprintf(stderr, "%ld x %ld\n", *(int64_t*)x, *(int64_t*)v);
		OP(ftype, *(int64_t*)x, *(int64_t*)v); break;
	}
}

int eval(void *x, void *v, nowdb_type_t t) {
	int r=0;

	switch(t) {
	case NOWDB_TYP_FLOAT:
		fprintf(stderr, "%.4f / %.4f\n", *(double*)x, *(double*)v);
		r = (*(double*)x == *(double*)v); break;
	case NOWDB_TYP_UINT:
		fprintf(stderr, "%lu / %lu\n", *(uint64_t*)x, *(uint64_t*)v);
		r = (*(uint64_t*)x == *(uint64_t*)v); break;
	case NOWDB_TYP_INT:
		fprintf(stderr, "%ld / %ld\n", *(int64_t*)x, *(int64_t*)v);
		r = (*(int64_t*)x == *(int64_t*)v); break;
	}
	if (r) return 0; else return -1;
}

int checkResult(uint32_t ftype,
            nowdb_type_t dtype,
                  uint16_t off,
                     void *res,
           nowdb_value_t *init,
                       int  mx) {
	uint64_t u;

	memcpy(&u, init, 8);
	for(int i=0; i<mx; i++) {
		switch(ftype) {
		case NOWDB_FUN_COUNT:
			u++; break;

		case NOWDB_FUN_SUM:
			op(ftype, &u, &edges[i].weight, dtype); break;

		case NOWDB_FUN_PROD:
			op(ftype, &u, &edges[i].weight, dtype); break;

		case NOWDB_FUN_MAX:
		case NOWDB_FUN_MIN:
			op(ftype, &u, &edges[i].weight, dtype); break;

		default:
			fprintf(stderr, "unknown funtion\n");
			return -1;
		}
	}
	return eval(&u, res, dtype);
}

int testFun(uint32_t ftype,
        nowdb_type_t dtype,
              uint16_t off,
                void *init) 
{
	int rc = 0;
	nowdb_fun_t *fun;
	nowdb_err_t  err;
	int mx;

	fun = calloc(1, sizeof(nowdb_fun_t));
	if (fun == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return -1;
	}
	err = nowdb_fun_init(fun, ftype,
	                NOWDB_CONT_EDGE,
	            off, 8, dtype, init);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot init fun\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	mx = prepare(dtype, ftype == NOWDB_FUN_PROD);
	if (mx < 0) {
		fprintf(stderr, "cannot prepare\n");
		nowdb_fun_destroy(fun); free(fun);
		return -1;
	}
	fprintf(stderr, "testing on %d edges\n", mx);
	for(int i=0; i<mx; i++) {
		err = nowdb_fun_map(fun, edges+i);
		if (err != NOWDB_OK) break;
	}
	if (err != NOWDB_OK) {
		fprintf(stderr, "error in map phase\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		rc = -1; goto cleanup;
	}
	err = nowdb_fun_reduce(fun);
	if (err != NOWDB_OK) {
		fprintf(stderr, "error in reduce phase\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		rc = -1; goto cleanup;
	}

	/* check value */
	if (checkResult(ftype, dtype, off,
	    &fun->value, &fun->init, mx) != 0) {
		fprintf(stderr, "results differ\n");
		rc = -1; goto cleanup;
	}

cleanup:
	nowdb_fun_destroy(fun); free(fun);
	free(edges); return rc;
	
}

int main() {
	int rc = EXIT_SUCCESS;

	srand(time(NULL) ^ (uint64_t)&printf);

	fprintf(stderr, "TESTING COUNT, UINT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_COUNT,
		            NOWDB_TYP_UINT,
		            NOWDB_OFF_WEIGHT,&aunity) != 0) {
			fprintf(stderr, "testFun COUNT, UINT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING SUM, UINT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_SUM,
		            NOWDB_TYP_UINT,
		            NOWDB_OFF_WEIGHT,&aunity) != 0) {
			fprintf(stderr, "testFun SUM, UINT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING SUM, FLOAT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_SUM,
		            NOWDB_TYP_FLOAT,
		            NOWDB_OFF_WEIGHT,&afunity) != 0) {
			fprintf(stderr, "testFun SUM, FLOAT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING PROD, UINT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_PROD,
		            NOWDB_TYP_UINT,
		            NOWDB_OFF_WEIGHT,&munity) != 0) {
			fprintf(stderr, "testFun PROD, UINT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING PROD, FLOAT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_PROD,
		            NOWDB_TYP_FLOAT,
		            NOWDB_OFF_WEIGHT,&mfunity) != 0) {
			fprintf(stderr, "testFun PROD, FLOAT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING MIN, UINT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_MIN,
		            NOWDB_TYP_UINT,
		            NOWDB_OFF_WEIGHT,&uinfty) != 0) {
			fprintf(stderr, "testFun MIN, UINT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING MIN, FLOAT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_MIN,
		            NOWDB_TYP_FLOAT,
		            NOWDB_OFF_WEIGHT,&finfty) != 0) {
			fprintf(stderr, "testFun MIN, FLOAT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING MAX, UINT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_MAX,
		            NOWDB_TYP_UINT,
		            NOWDB_OFF_WEIGHT,&umin) != 0) {
			fprintf(stderr, "testFun MIN, UINT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING MAX, FLOAT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_MAX,
		            NOWDB_TYP_FLOAT,
		            NOWDB_OFF_WEIGHT,&ffmin) != 0) {
			fprintf(stderr, "testFun MIN, FLOAT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}

cleanup:
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}
