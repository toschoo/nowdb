/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Generic SQL functions
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/fun/fun.h>

#include <math.h>

static char *OBJECT  = "fun";

#define BLOCKSIZE 4096

#define INVALID(s) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, s);

#define NOMEM(s) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, s);

#define ADD(v1,v2) \
	v1+=v2;

#define SUB(v1,v2) \
	v1-=v2;

#define MUL(v1,v2) \
	v1*=v2;

#define SQR(v1,v2) \
	v1=pow(v2,2);

#define MAX(v1,v2) \
	if (v1<v2) v1=v2;

#define MIN(v1,v2) \
	if (v1>v2) v1=v2;

#define QUOT(v1,v2) \
	v1 /= v2;

#define TOFLOAT(v1,v2) \
	v1 = (double)v2;

/* CAUTION: this one needs one more variable! */
#define DIV(v1,v2) \
	*r = (double)v1 / (double)v2;

#define PERFM(o) \
	switch(t) { \
	case NOWDB_TYP_DATE: \
	case NOWDB_TYP_TIME: \
	case NOWDB_TYP_UINT: \
		o(*(uint64_t*)v1, *(uint64_t*)v2); \
		return NOWDB_OK; \
	case NOWDB_TYP_INT: \
		o(*(int64_t*)v1, *(int64_t*)v2); \
		return NOWDB_OK; \
	case NOWDB_TYP_FLOAT: \
		o(*(double*)v1, *(double*)v2); \
		return NOWDB_OK; \
	default: return nowdb_err_get(nowdb_err_not_supp, \
	                             FALSE, OBJECT, NULL); \
	}
	
/* -----------------------------------------------------------------------
 * All my neat functions
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t nowadd(void *v1, void *v2, nowdb_type_t t) {
	PERFM(ADD);
}
static inline nowdb_err_t nowsub(void *v1, void *v2, nowdb_type_t t) {
	PERFM(SUB);
}
static inline nowdb_err_t nowmul(void *v1, void *v2, nowdb_type_t t) {
	PERFM(MUL);
}
static inline nowdb_err_t nowdiv(double *r, void *v1, void *v2,
                                                nowdb_type_t t) {
	PERFM(DIV);
}
static inline nowdb_err_t nowquot(void *v1, void *v2, nowdb_type_t t) {
	PERFM(QUOT);
}
static inline nowdb_err_t nowsquare(void *v1, void *v2, nowdb_type_t t) {
	PERFM(SQR);
}
static inline nowdb_err_t nowmax(void *v1, void *v2, nowdb_type_t t) {
	PERFM(MAX);
}
static inline nowdb_err_t nowmin(void *v1, void *v2, nowdb_type_t t) {
	PERFM(MIN);
}
static inline nowdb_err_t now2float(void *v1, void *v2, nowdb_type_t t) {
	PERFM(TOFLOAT);
}

/* -----------------------------------------------------------------------
 * Helper: fun to type
 * -----------------------------------------------------------------------
 */
static inline int getType(uint32_t fun) {
	switch(fun) {
	case NOWDB_FUN_COUNT:
		return NOWDB_FUN_ZERO;

	case NOWDB_FUN_SUM:
	case NOWDB_FUN_PROD:
	case NOWDB_FUN_MAX:
	case NOWDB_FUN_MIN:
	case NOWDB_FUN_SPREAD:
	case NOWDB_FUN_AVG:
		return NOWDB_FUN_ONE;

	case NOWDB_FUN_MEDIAN:
	case NOWDB_FUN_STDDEV:
	case NOWDB_FUN_INTEGRAL:
		return NOWDB_FUN_MANY;

	case NOWDB_FUN_MODE:
		return NOWDB_FUN_TREE;

	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Helper: Init free list
 * -----------------------------------------------------------------------
 */
static nowdb_err_t initFlist(nowdb_fun_t *fun) {
	nowdb_err_t err;

	fun->flist = calloc(1, sizeof(nowdb_blist_t));
	if (fun->flist == NULL) {
		NOMEM("allocating free list");
		return err;
	}
	err = nowdb_blist_init(fun->flist, BLOCKSIZE);
	if (err != NOWDB_OK) {
		free(fun->flist); fun->flist = NULL;
		return err;
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Init already allocated function
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_fun_init(nowdb_fun_t          *fun,
                           uint32_t             type,
                           nowdb_content_t     ctype,
                           uint16_t            field,
                           uint16_t            fsize,
                           nowdb_type_t        dtype,
                           nowdb_value_t       *init) {
	nowdb_err_t err;

	if (fun == NULL) INVALID("fun pointer is NULL");

	ts_algo_list_init(&fun->many);

	fun->ftype = getType(type);
	fun->fun   = type;
	fun->ctype = ctype;
	fun->field = field;
	fun->fsize = fsize;
	fun->dtype = dtype;
	fun->flist = NULL;
	fun->value = 0;
	fun->hlp   = 0;
	fun->off   = 0;
	fun->first = 1;

	if (init != NULL) {
		memcpy(&fun->init, init, fun->fsize);
		memcpy(&fun->value, &fun->init, fun->fsize);
	}

	if (fun->ftype < 0) INVALID("unknown function type");

	fun->recsz = ctype == NOWDB_CONT_EDGE?
	                      NOWDB_EDGE_SIZE:
	                      NOWDB_VERTEX_SIZE;

	if (fun->ftype == NOWDB_FUN_MANY) {
		err = initFlist(fun);
		if (err != NOWDB_OK) return err;
	}

	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Helper: destroy list
 * -----------------------------------------------------------------------
 */
static void destroyList(ts_algo_list_t *list) {
	ts_algo_list_node_t *runner, *tmp;
	nowdb_block_t *b;
	runner = list->head;
	while(runner != NULL) {
		tmp = runner->nxt;
		if (runner->cont != NULL) {
			b = runner->cont;
			free(b->block);
			free(b); runner->cont = NULL;
		}
		ts_algo_list_remove(list, runner);
		free(runner); runner = tmp;
	}
}

/* -----------------------------------------------------------------------
 * Destroy
 * -----------------------------------------------------------------------
 */
void nowdb_fun_destroy(nowdb_fun_t *fun) {
	if (fun == NULL) return;
	if (fun->flist != NULL) {
		nowdb_blist_destroy(fun->flist);
		free(fun->flist); fun->flist = NULL;
	}
	destroyList(&fun->many);
}

/* -----------------------------------------------------------------------
 * Reset
 * -----------------------------------------------------------------------
 */
void nowdb_fun_reset(nowdb_fun_t *fun);

/* -----------------------------------------------------------------------
 * Helper: collect values
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t collect(nowdb_fun_t *fun, char *record) {
	nowdb_err_t err;
	nowdb_block_t *b;

	if (fun->many.len == 0 || fun->off >= BLOCKSIZE) {
		err = nowdb_blist_give(fun->flist, &fun->many);
		if (err != NOWDB_OK) return err;
		fun->off = 0;
	}
	b = fun->many.head->cont;
	memcpy(b->block+fun->off, record+fun->field, fun->fsize);
	fun->off += fun->fsize;
	b->sz = fun->off;
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Helper: apply 0-function
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t apply_(nowdb_fun_t *fun) {
	switch(fun->fun) {
	case NOWDB_FUN_COUNT: fun->value++; break;
	default: INVALID("function cannot be applied here");
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Helper: apply function
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t apply(nowdb_fun_t *fun,  int ftype,
                                uint16_t field, char *record) {
	nowdb_err_t err;

	switch(ftype) {
	case NOWDB_FUN_SUM:
		return nowadd(&fun->value, record+field, fun->dtype);
	case NOWDB_FUN_PROD:
		return nowmul(&fun->value, record+field, fun->dtype);
	case NOWDB_FUN_MIN:
		if (fun->first) {
			memcpy(&fun->value, record+field, fun->fsize);
			return NOWDB_OK;
		}
		return nowmin(&fun->value, record+field, fun->dtype);
	case NOWDB_FUN_MAX:
		if (fun->first) {
			memcpy(&fun->value, record+field, fun->fsize);
			return NOWDB_OK;
		}
		return nowmax(&fun->value, record+field, fun->dtype);

	case NOWDB_FUN_SPREAD:
		if (fun->first) {
			memcpy(&fun->value, record+field, fun->fsize);
			memcpy(&fun->hlp, record+field, fun->fsize);
			return NOWDB_OK;
		}
		err = nowmax(&fun->value, record+field, fun->dtype);
		if (err != NOWDB_OK) return err;
		return nowmin(&fun->hlp, record+field, fun->dtype);

	case NOWDB_FUN_AVG:
		fun->hlp++;
		return nowadd(&fun->value, record+field, fun->dtype);

	case NOWDB_FUN_STDDEV:
		err = now2float(record+field, record+field, fun->dtype);
		if (err != NOWDB_OK) return err;
		err = nowsub(record+field, &fun->value, NOWDB_TYP_FLOAT);
		if (err != NOWDB_OK) return err;
		return nowsquare(record+field, record+field, NOWDB_TYP_FLOAT);
	
	default: INVALID("function cannot be applied here");
	}
}

/* -----------------------------------------------------------------------
 * Map
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_fun_map(nowdb_fun_t *fun, void *record) {
	nowdb_err_t err;
	switch(fun->ftype) {
	case NOWDB_FUN_ZERO: err = apply_(fun); break;
	case NOWDB_FUN_ONE:
		err = apply(fun, fun->fun, fun->field, record); break;

	case NOWDB_FUN_MANY: err = collect(fun, record); break;
	case NOWDB_FUN_TREE:
		err = nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT,
		                     "tree functions not implemented");
		break;
	default: INVALID("unknown function type");
	}
	fun->first = 0;
	return err;
}

/* -----------------------------------------------------------------------
 * Helper: iterate over collected values
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t map2(nowdb_fun_t *fun, uint32_t ftype) {
	ts_algo_list_node_t *runner;
	nowdb_block_t *b;
	int off=0;
	nowdb_err_t err;

	runner = fun->many.head;
	if (runner != NULL) b=runner->cont; else return NOWDB_OK;
	for(;;) {
		if (off >= b->sz) {
			runner = runner->nxt;
			if (runner == NULL) break;
			b = runner->cont;
			off = 0;
		}
		err = apply(fun, ftype, 0, b->block+off);
		if (err != NOWDB_OK) return err;
		off += fun->fsize;
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Reduce
 * -----------------------------------------------------------------------
 */
static nowdb_err_t reduce(nowdb_fun_t *fun, uint32_t ftype) {
	nowdb_err_t err;
	double x,z,n;

	switch(ftype) {
	case NOWDB_FUN_COUNT:
	case NOWDB_FUN_SUM:
	case NOWDB_FUN_PROD:
	case NOWDB_FUN_MIN:
	case NOWDB_FUN_MAX: return NOWDB_OK;

	case NOWDB_FUN_SPREAD:
		return nowsub(&fun->value, &fun->hlp, fun->dtype);

	case NOWDB_FUN_AVG:
		if (fun->hlp == 0) {
			fun->value = 0;
			return NOWDB_OK;
		}
		if (fun->dtype == NOWDB_TYP_FLOAT) {
			x = (double)(*(uint64_t*)&fun->hlp);
			memcpy(&fun->hlp, &x, 8);
		}
		err = nowdiv(&x, &fun->value, &fun->hlp, fun->dtype);
		if (err != NOWDB_OK) return err;
		memcpy(&fun->value, &x, fun->fsize);
		return NOWDB_OK;

	case NOWDB_FUN_MEDIAN:
		fprintf(stderr, "computing median\n");
		return NOWDB_OK;

	case NOWDB_FUN_STDDEV:
		fprintf(stderr, "computing stddev\n");
		err = map2(fun, NOWDB_FUN_AVG);
		if (err != NOWDB_OK) return err;

		x = (double)fun->hlp;
		fprintf(stderr, "COUNT: %.4f\n", x);

		if (x < 2) return NOWDB_OK;

		err = reduce(fun, NOWDB_FUN_AVG);
		if (err != NOWDB_OK) return err;

		memcpy(&fun->hlp, &x, 8);
		err = map2(fun, NOWDB_FUN_STDDEV);
		if (err != NOWDB_OK) return err;

		memcpy(&x, &fun->value, 8);
		fprintf(stderr, "%.4f\n", x);

		nowdb_type_t tmp = fun->dtype;
		fun->dtype = NOWDB_TYP_FLOAT;
		err = map2(fun, NOWDB_FUN_SUM);
		fun->dtype = tmp;
		if (err != NOWDB_OK) return err;

		memcpy(&n, &fun->hlp, 8);
		memcpy(&x, &fun->value, 8);
		z = sqrt(x/(n-1));
		fprintf(stderr, "sqrt(%.4f/(%.4f-1)) = %.4f\n", x, n, z);

		memcpy(&fun->value, &z, 8);
		return NOWDB_OK;
	
	default: return NOWDB_OK;
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Reduce
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_fun_reduce(nowdb_fun_t *fun) {
	return reduce(fun, fun->fun);
}
