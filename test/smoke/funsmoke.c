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

#define ITER 10

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

void getTime(int64_t *tm) {
	int64_t d = rand()%100000000;
	char x = rand()%2;
	NOWDB_IGNORE(nowdb_time_now(tm));
	if (x) *tm += d; else *tm -= d;
}

int prepare(nowdb_type_t type, char calm) {
	int x;
	do x = rand()%10; while(x==0); // calm?rand()%10:rand()%1000; while (x==0);
	edges = calloc(x, sizeof(nowdb_edge_t));
	if (edges == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return -1;
	}
	for(int i=0; i<x; i++) {
		getValue(&edges[i].weight, type);
		fprintf(stderr, "%lu\n", edges[i].weight);
		getValue(&edges[i].weight2, type);
		getTime(&edges[i].timestamp);
	}
	return x;
}

#define OP(t, x, v) \
	switch(t) { \
	case NOWDB_FUN_SUM: x+=v; break; \
	case NOWDB_FUN_PROD:x*=v; break; \
	case NOWDB_FUN_MIN: x=x>v?v:x; break; \
	case NOWDB_FUN_MAX: x=x<v?v:x; break; \
	case           SUB: x-=v; break; \
	case           DIV: x/=v; break; \
	default: \
		return; \
	}

void doAvg(nowdb_type_t t, uint64_t *u, uint64_t *h, double *r) {
	double a, b;

	switch(t) {
	case NOWDB_TYP_FLOAT:
		a = (double)(*(double*)u); break;
	case NOWDB_TYP_UINT: 
	case NOWDB_TYP_TIME:
		a = (double)(*(uint64_t*)u); break;
	case NOWDB_TYP_INT:
		a = (double)(*(int64_t*)u); break;
	default:
		a=1;
	}
	b = (double)(*(uint64_t*)h);
	*r = a/b;
}

#define SUB -100
#define DIV -200

void op(int ftype, void *x, void *v, nowdb_type_t type) {
	switch(type) {
	case NOWDB_TYP_FLOAT:
		// fprintf(stderr, "%.4f x %.4f\n", *(double*)x, *(double*)v);
		OP(ftype, *(double*)x, *(double*)v); break;
	case NOWDB_TYP_UINT:
	case NOWDB_TYP_TIME:
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
	case NOWDB_TYP_TIME:
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
	nowdb_type_t mytype = dtype;
	double   r;
	uint64_t u;
	uint64_t h;

	memcpy(&u, init, 8);
	memcpy(&h, init, 8);
	for(int i=0; i<mx; i++) {
		switch(ftype) {
		case NOWDB_FUN_COUNT:
			u++; break;

		case NOWDB_FUN_SUM:
			op(ftype, &u, (char*)(edges+i)+off, dtype); break;

		case NOWDB_FUN_PROD:
			op(ftype, &u, (char*)(edges+i)+off, dtype); break;

		case NOWDB_FUN_MAX:
		case NOWDB_FUN_MIN:
			if (i == 0) {
				memcpy(&u, (char*)(edges+i)+off, 8); break;
			}
			op(ftype, &u, (char*)(edges+i)+off, dtype); break;

		case NOWDB_FUN_SPREAD:
			if (i == 0) {
				memcpy(&u, (char*)(edges+i)+off, 8);
				memcpy(&h, (char*)(edges+i)+off, 8); break;
			}
			op(NOWDB_FUN_MAX, &u, (char*)(edges+i)+off, dtype); 
			op(NOWDB_FUN_MIN, &h, (char*)(edges+i)+off, dtype); 
			break;

		case NOWDB_FUN_AVG:
			op(NOWDB_FUN_SUM, &u, (char*)(edges+i)+off, dtype); 
			h++; break;

		case NOWDB_FUN_MEDIAN: break;
		case NOWDB_FUN_STDDEV: break;

		default:
			fprintf(stderr, "unknown funtion\n");
			return -1;
		}
	}
	switch(ftype) {
	case NOWDB_FUN_SPREAD:
		op(SUB, &u, &h, dtype); break;

	case NOWDB_FUN_AVG:
		doAvg(dtype, &u, &h, &r);
		memcpy(&u, &r, 8);
		mytype = NOWDB_TYP_FLOAT;
		break;

	case NOWDB_FUN_MEDIAN: return 0;
	case NOWDB_FUN_STDDEV: return 0;

	default: break;
	}
	return eval(&u, res, mytype);
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
	    &fun->r1, &fun->init, mx) != 0) {
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
		            NOWDB_OFF_WEIGHT,&umin) != 0) {
			fprintf(stderr, "testFun MIN, UINT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING MIN, FLOAT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_MIN,
		            NOWDB_TYP_FLOAT,
		            NOWDB_OFF_WEIGHT,&umin) != 0) {
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
		            NOWDB_OFF_WEIGHT,&umin) != 0) {
			fprintf(stderr, "testFun MIN, FLOAT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING SPREAD, UINT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_SPREAD,
		            NOWDB_TYP_UINT,
		            NOWDB_OFF_WEIGHT,&umin) != 0) {
			fprintf(stderr, "testFun SPREAD, UINT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING SPREAD, TIME, TIMESTAMP\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_SPREAD,
		            NOWDB_TYP_TIME,
		            NOWDB_OFF_TMSTMP,&umin) != 0) {
			fprintf(stderr, "testFun SPREAD, TIME, TIMESTAMP, failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING SPREAD, FLOAT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_SPREAD,
		            NOWDB_TYP_FLOAT,
		            NOWDB_OFF_WEIGHT,&umin) != 0) {
			fprintf(stderr, "testFun SPREAD, FLOAT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING AVG, UINT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_AVG,
		            NOWDB_TYP_UINT,
		            NOWDB_OFF_WEIGHT,&aunity) != 0) {
			fprintf(stderr, "testFun AVG, UINT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING AVG, FLOAT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_AVG,
		            NOWDB_TYP_FLOAT,
		            NOWDB_OFF_WEIGHT,&afunity) != 0) {
			fprintf(stderr, "testFun AVG, FLOAT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING STDDEV, UINT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_STDDEV,
		            NOWDB_TYP_UINT,
		            NOWDB_OFF_WEIGHT,&aunity) != 0) {
			fprintf(stderr, "testFun STDDEV, UINT, WEIGHT failed\n");
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
