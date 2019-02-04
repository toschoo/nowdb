/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tests for SQL functions 
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/time.h>
#include <nowdb/fun/fun.h>
#include <nowdb/scope/scope.h>

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>
#include <math.h>
#include <limits.h>
#include <float.h>

#define ITER 1000

uint64_t fullmap = 0xffffffffffffffff;

nowdb_edge_t *edges;

uint64_t uzero = 0;
uint64_t uone = 1;
double  fzero = 0;
double  fone = 1;

// fake a model
uint64_t MYEDGE = 101;
nowdb_roleid_t MYORI = 111;
nowdb_roleid_t MYDST = 110;

nowdb_model_edge_t _edge;
nowdb_model_vertex_t _ori;
nowdb_model_vertex_t _dst;
nowdb_edge_helper_t  _ce;
nowdb_eval_t        _hlp;

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
	nowdb_time_now(tm);
	if (x) *tm += d; else *tm -= d;
}

int prepare(nowdb_type_t type, char calm) {
	int x;
	x = calm?rand()%10:rand()%1000;
	edges = calloc(x, sizeof(nowdb_edge_t));
	if (edges == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return -1;
	}
	for(int i=0; i<x; i++) {
		edges[i].edge = MYEDGE;
		edges[i].origin = MYORI;
		edges[i].destin = MYDST;
		getValue(&edges[i].weight, type);
		/*
		double d;
		memcpy(&d,&edges[i].weight, 8);
		fprintf(stderr, "%.4f\n", d);
		*/
		getValue(&edges[i].weight2, type);
		getTime(&edges[i].timestamp);
	}
	return x;
}

#define EDG(x) \
	((nowdb_edge_t*)x)

int wcompare(const void *one, const void *two) {
	if (EDG(one)->weight < EDG(two)->weight) return -1;
	if (EDG(one)->weight > EDG(two)->weight) return  1;
	return 0;
}

double medianf(int n) {
	int h;
	double w1, w2;

	qsort(edges, n, NOWDB_EDGE_SIZE, &wcompare);

	h = n/2;
	if (n%2 == 0) h--;

	memcpy(&w1, &edges[h].weight, 8);

	fprintf(stderr, "%d/2=%d: %.4f\n", n, h, w1);
	
	if (n%2 != 0) return w1;

	h++;
	memcpy(&w2, &edges[h].weight, 8);

	fprintf(stderr, "%d/2+1=%d: %.4f\n", n, h, w2);

	return ((w1 + w2)/2);
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

#define TOFLOAT(v,t) \
	switch(t) { \
	case NOWDB_TYP_DATE: \
	case NOWDB_TYP_TIME: \
	case NOWDB_TYP_UINT: \
		memcpy(&utmp, v, 8); \
		x = (double)utmp; \
		memcpy(v, &x, 8); break; \
	case NOWDB_TYP_INT: \
		memcpy(&itmp, v, 8); \
		x = (double)itmp; \
		memcpy(v, &x, 8); break; \
	default: break; \
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
		r = (round(*(double*)x * 1000) ==
		    (round(*(double*)v * 1000))); break;
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
	double   r, x, y, z;
	uint64_t u;
	uint64_t h;
	uint64_t utmp;
	int64_t  itmp;

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

		case NOWDB_FUN_STDDEV: 
			TOFLOAT((char*)(edges+i)+off, dtype);
			op(NOWDB_FUN_SUM, &u,(char*)(edges+i)+off,
			   NOWDB_TYP_FLOAT);
			break;

		default:
			fprintf(stderr, "unknown funtion\n");
			return -1;
		}
	}
	switch(ftype) {
	case NOWDB_FUN_SPREAD:
		if (mx < 2) {
			u = 0;  break;
		}
		op(SUB, &u, &h, dtype); break;

	case NOWDB_FUN_AVG:
		if (mx < 1) {
			u = 0;  break;
		}
		doAvg(dtype, &u, &h, &r);
		memcpy(&u, &r, 8);
		mytype = NOWDB_TYP_FLOAT;
		break;

	case NOWDB_FUN_MEDIAN: 
		if (mx < 1) {
			u = 0;  break;
		}
		r = medianf(mx);
		memcpy(&u, &r, 8);
		mytype = NOWDB_TYP_FLOAT;
		break;

	case NOWDB_FUN_STDDEV:
		if (mx < 2) {
			u = 0; break;
		}
		memcpy(&x,&u,8);

		x /= (double)mx;

		z = 0;
		for(int i=0; i<mx; i++) {
			memcpy(&y, (char*)(edges+i)+off, 8);
			y -= x;
			y *= y;
			z += y;
		}
		r = sqrt((z/((double)mx-1)));
		mytype = NOWDB_TYP_FLOAT;
		memcpy(&u, &r, 8);
		break;

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
	nowdb_expr_t expr;
	int mx;

	_edge.weight = dtype;

	err = nowdb_expr_newEdgeField(&expr, off);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create edge field\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}

	err = nowdb_fun_new(&fun, ftype,
	                NOWDB_CONT_EDGE,
	                    expr, init);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot init fun\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_expr_destroy(expr); free(expr);
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
		err = nowdb_fun_map(fun, &_hlp, fullmap, edges+i);
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

int initEval() {
	_edge.edgeid = MYEDGE;
	_edge.origin = MYORI;
	_edge.destin = MYDST;
	_edge.name = "MYEDGE";

	_ori.name = "MYORIGIN";
	_ori.roleid = MYORI;
	_ori.vid = NOWDB_MODEL_NUM;

	_dst.name = "MYDESTIN";
	_dst.roleid = MYDST;
	_dst.vid = NOWDB_MODEL_NUM;

	_ce.e = &_edge;
	_ce.o = &_ori;
	_ce.d = &_dst;

	_hlp.ce = &_ce;

	return 0;
}

int main() {
	int rc = EXIT_SUCCESS;

	srand(time(NULL) ^ (uint64_t)&printf);

	if (initEval() != 0) {
		fprintf(stderr, "cannot init eval helper\n");
		return EXIT_FAILURE;
	}

	fprintf(stderr, "TESTING COUNT, UINT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_COUNT,
		            NOWDB_TYP_UINT,
		            NOWDB_OFF_WEIGHT,&uzero) != 0) {
			fprintf(stderr, "testFun COUNT, UINT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING SUM, UINT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_SUM,
		            NOWDB_TYP_UINT,
		            NOWDB_OFF_WEIGHT,&uzero) != 0) {
			fprintf(stderr, "testFun SUM, UINT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING SUM, FLOAT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_SUM,
		            NOWDB_TYP_FLOAT,
		            NOWDB_OFF_WEIGHT,&fzero) != 0) {
			fprintf(stderr, "testFun SUM, FLOAT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING PROD, UINT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_PROD,
		            NOWDB_TYP_UINT,
		            NOWDB_OFF_WEIGHT,&uone) != 0) {
			fprintf(stderr, "testFun PROD, UINT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING PROD, FLOAT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_PROD,
		            NOWDB_TYP_FLOAT,
		            NOWDB_OFF_WEIGHT,&fone) != 0) {
			fprintf(stderr, "testFun PROD, FLOAT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING MIN, UINT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_MIN,
		            NOWDB_TYP_UINT,
		            NOWDB_OFF_WEIGHT,&uzero) != 0) {
			fprintf(stderr, "testFun MIN, UINT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING MIN, FLOAT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_MIN,
		            NOWDB_TYP_FLOAT,
		            NOWDB_OFF_WEIGHT,&fzero) != 0) {
			fprintf(stderr, "testFun MIN, FLOAT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING MAX, UINT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_MAX,
		            NOWDB_TYP_UINT,
		            NOWDB_OFF_WEIGHT,&uzero) != 0) {
			fprintf(stderr, "testFun MIN, UINT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING MAX, FLOAT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_MAX,
		            NOWDB_TYP_FLOAT,
		            NOWDB_OFF_WEIGHT,&fzero) != 0) {
			fprintf(stderr, "testFun MIN, FLOAT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING SPREAD, UINT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_SPREAD,
		            NOWDB_TYP_UINT,
		            NOWDB_OFF_WEIGHT,&uzero) != 0) {
			fprintf(stderr, "testFun SPREAD, UINT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING SPREAD, TIME, TIMESTAMP\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_SPREAD,
		            NOWDB_TYP_TIME,
		            NOWDB_OFF_TMSTMP,&uzero) != 0) {
			fprintf(stderr, "testFun SPREAD, TIME, TIMESTAMP, failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING SPREAD, FLOAT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_SPREAD,
		            NOWDB_TYP_FLOAT,
		            NOWDB_OFF_WEIGHT,&fzero) != 0) {
			fprintf(stderr, "testFun SPREAD, FLOAT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING AVG, UINT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_AVG,
		            NOWDB_TYP_UINT,
		            NOWDB_OFF_WEIGHT,&uzero) != 0) {
			fprintf(stderr, "testFun AVG, UINT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING AVG, FLOAT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_AVG,
		            NOWDB_TYP_FLOAT,
		            NOWDB_OFF_WEIGHT,&fzero) != 0) {
			fprintf(stderr, "testFun AVG, FLOAT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING STDDEV, UINT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_STDDEV,
		            NOWDB_TYP_UINT,
		            NOWDB_OFF_WEIGHT,&uzero) != 0) {
			fprintf(stderr, "testFun STDDEV, UINT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING STDDEV, FLOAT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_STDDEV,
		            NOWDB_TYP_FLOAT,
		            NOWDB_OFF_WEIGHT,&fzero) != 0) {
			fprintf(stderr, "testFun STDDEV, FLOAT, WEIGHT failed\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	fprintf(stderr, "TESTING MEDIAN, FLOAT, WEIGHT\n");
	for(int i=0; i<ITER; i++) {
		if (testFun(NOWDB_FUN_MEDIAN,
		            NOWDB_TYP_FLOAT,
		            NOWDB_OFF_WEIGHT,&fzero) != 0) {
			fprintf(stderr, "testFun MEDIAN, FLOAT, WEIGHT failed\n");
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
