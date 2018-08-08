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

#define MAX(v1,v2) \
	if (v1<v2) v1=v2;

#define MIN(v1,v2) \
	if (v1>v2) v1=v2;

#define QUOT(v1,v2) \
	v1 /= v2;

#define MOV(r1,r2) \
	memcpy(r1, r2, 8);

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
	default: return NOWDB_OK; \
	}

#define TOFLOAT() \
	switch(t) { \
	case NOWDB_TYP_DATE: \
	case NOWDB_TYP_TIME: \
	case NOWDB_TYP_UINT: \
		x = (double)(*(uint64_t*)v2); \
		MOV(v1, &x); break; \
	case NOWDB_TYP_INT: \
		x = (double)(*(int64_t*)v2); \
		MOV(v1, &x); break; \
	default: \
		MOV(v1, v2); break; \
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
static inline nowdb_err_t nowmax(void *v1, void *v2, nowdb_type_t t) {
	PERFM(MAX);
}
static inline nowdb_err_t nowmin(void *v1, void *v2, nowdb_type_t t) {
	PERFM(MIN);
}
static inline void now2float(void *v1, void *v2, nowdb_type_t t) {
	double x;
	TOFLOAT();
}

/* -----------------------------------------------------------------------
 * Helper: fun to type
 * -----------------------------------------------------------------------
 */
static inline int getFType(uint32_t fun) {
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
 * Helper: fun to output type
 * -----------------------------------------------------------------------
 */
static inline nowdb_type_t getOType(nowdb_fun_t *fun) {
	switch(fun->fun) {
	case NOWDB_FUN_COUNT:
		return NOWDB_TYP_UINT;

	case NOWDB_FUN_SUM:
	case NOWDB_FUN_PROD:
	case NOWDB_FUN_MAX:
	case NOWDB_FUN_MIN:
	case NOWDB_FUN_SPREAD:
	case NOWDB_FUN_MODE:
		return fun->dtype;

	case NOWDB_FUN_AVG:
	case NOWDB_FUN_MEDIAN:
	case NOWDB_FUN_STDDEV:
	case NOWDB_FUN_INTEGRAL:
		return NOWDB_TYP_FLOAT;

	default: return fun->dtype;
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
 * Helper: reset
 * -----------------------------------------------------------------------
 */
static inline void reset(nowdb_fun_t *fun) {

	fun->r1    = 0;
	fun->r2    = 0;
	fun->r3    = 0;
	fun->r4    = 0;
	fun->off   = 0;
	fun->first = 1;

	memcpy(&fun->r1, &fun->init, fun->fsize);
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

	fun->ftype = getFType(type);
	fun->fun   = type;
	fun->ctype = ctype;
	fun->field = field;
	fun->fsize = fsize;
	fun->dtype = dtype;
	fun->flist = NULL;

	if (dtype == NOWDB_TYP_NOTHING) {
		switch(field) {
		case NOWDB_OFF_TMSTMP:
			fun->dtype = NOWDB_TYP_TIME; break;
		case NOWDB_OFF_WEIGHT:
		case NOWDB_OFF_WEIGHT2:
		default:
			fun->dtype = NOWDB_TYP_NOTHING;
		}
	}
	fun->otype = getOType(fun);

	if (init != NULL) {
		memcpy(&fun->init, init, fun->fsize);
	} else {
		fun->init = 0;
	}

	reset(fun);

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
void nowdb_fun_reset(nowdb_fun_t *fun) {
	reset(fun);
}

/* -----------------------------------------------------------------------
 * Helper: type correction
 * -----------------------------------------------------------------------
 */
static inline void correcttype(nowdb_fun_t     *fun,
                               uint16_t       field,
                               char         *record,
                               nowdb_type_t *mytype) 
{
	if (fun->ctype == NOWDB_CONT_EDGE) {
		if (field == NOWDB_OFF_WEIGHT) {
			memcpy(mytype, record+NOWDB_OFF_WTYPE,
				         sizeof(nowdb_type_t));
		} else {
			memcpy(mytype, record+NOWDB_OFF_WTYPE2,
				          sizeof(nowdb_type_t));
		}
	} else {
		memcpy(mytype, record+NOWDB_OFF_VTYPE,
				sizeof(nowdb_type_t));
	}
	fun->otype = getOType(fun);
}

/* -----------------------------------------------------------------------
 * Helper: collect values
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t collect(nowdb_fun_t *fun, char *record) {
	nowdb_err_t err;
	nowdb_block_t *b;

	if (fun->dtype == NOWDB_TYP_NOTHING) {
		correcttype(fun, fun->field, record, &fun->dtype);
	}
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
	case NOWDB_FUN_COUNT: fun->r1++; break;
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

	if (fun->dtype == NOWDB_TYP_NOTHING) {
		correcttype(fun, field, record, &fun->dtype);
	}

	switch(ftype) {
	case NOWDB_FUN_SUM:
		return nowadd(&fun->r1, record+field, fun->dtype);
	case NOWDB_FUN_PROD:
		return nowmul(&fun->r1, record+field, fun->dtype);
	case NOWDB_FUN_MIN:
		if (fun->first) {
			memcpy(&fun->r1, record+field, fun->fsize);
			return NOWDB_OK;
		}
		return nowmin(&fun->r1, record+field, fun->dtype);
	case NOWDB_FUN_MAX:
		if (fun->first) {
			memcpy(&fun->r1, record+field, fun->fsize);
			return NOWDB_OK;
		}
		return nowmax(&fun->r1, record+field, fun->dtype);

	case NOWDB_FUN_SPREAD:
		if (fun->first) {
			memcpy(&fun->r1, record+field, fun->fsize);
			memcpy(&fun->r2, record+field, fun->fsize);
			return NOWDB_OK;
		}
		err = nowmax(&fun->r1, record+field, fun->dtype);
		if (err != NOWDB_OK) return err;
		return nowmin(&fun->r2, record+field, fun->dtype);

	case NOWDB_FUN_AVG:
		fun->r2++;
		return nowadd(&fun->r1, record+field, fun->dtype);

	case NOWDB_FUN_STDDEV:
		now2float(record+field, record+field, fun->dtype);

		err = nowsub(record+field, &fun->r2, NOWDB_TYP_FLOAT);
		if (err != NOWDB_OK) return err;

		err = nowmul(record+field, record+field, NOWDB_TYP_FLOAT);
		if (err != NOWDB_OK) return err;

		return nowadd(&fun->r1, record+field, NOWDB_TYP_FLOAT);
	
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

#define BLOCK(x) \
	((nowdb_block_t*)x)

/* -----------------------------------------------------------------------
 * Helper: find median
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t median(nowdb_fun_t *fun) {
	nowdb_err_t err;
	ts_algo_list_node_t *runner;
	uint64_t k=0, c=0, i=0, f;
	double x=2;
	char two=0;

	// count number of bytes
	for(runner=fun->many.head; runner!=NULL; runner=runner->nxt) {
		k+=BLOCK(runner->cont)->sz;
	}

	// edge cases: 0 or 1
	if (k < fun->fsize) {
		x = 0;
		MOV(&fun->r1, &x);
		return NOWDB_OK;

	} else if (k == fun->fsize) {
		now2float(&fun->r1, BLOCK(runner->cont)->block, fun->dtype);
		return NOWDB_OK;
	}

	// compute centre
	c = k/2;

	// even or odd?
	if ((k/fun->fsize)%2==0) {
		c -= fun->fsize; two=1;
	}

	// go to centre
	for(runner=fun->many.head; runner!=NULL; runner=runner->nxt) {
		if (i + BLOCK(runner->cont)->sz > c) break;
		i+=BLOCK(runner->cont)->sz;
	}
	while(i<c) i++;

	// round to fsize and convert to float
	f = i/fun->fsize; f *= fun->fsize;
	now2float(&fun->r1, BLOCK(runner->cont)->block+f, fun->fsize);

	// odd, we're done
	if (!two) return NOWDB_OK;

	// even: get n/2+1
	i+=fun->fsize;
	if (i>=BLOCK(runner->cont)->sz) {
		runner=runner->nxt; i=0;
	}

	// round to fsize and convert to float
	f = i/fun->fsize; f *= fun->fsize;
	now2float(&fun->r2, BLOCK(runner->cont)->block+f, fun->fsize);

	// add n/2 and n/2+1
	err = nowadd(&fun->r1, &fun->r2, NOWDB_TYP_FLOAT);
	if (err != NOWDB_OK) return err;

	// divide by 2
	err = nowdiv(&x, &fun->r1, &x, NOWDB_TYP_FLOAT);
	if (err != NOWDB_OK) return err;

	MOV(&fun->r1, &x);

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
		return nowsub(&fun->r1, &fun->r2, fun->dtype);

	case NOWDB_FUN_AVG:
		if (fun->r2 == 0) {
			fun->r1 = 0;
			return NOWDB_OK;
		}
		if (fun->dtype == NOWDB_TYP_FLOAT) {
			x = (double)(*(uint64_t*)&fun->r2);
			MOV(&fun->r2, &x);
		}
		err = nowdiv(&x, &fun->r1, &fun->r2, fun->dtype);
		if (err != NOWDB_OK) return err;
		memcpy(&fun->r1, &x, fun->fsize);
		return NOWDB_OK;

	case NOWDB_FUN_MEDIAN:
		fprintf(stderr, "computing median\n");

		err = nowdb_block_sort(&fun->many,
		                        fun->flist,
		                        fun->fsize,
		  nowdb_sort_getCompare(fun->dtype));
		if (err != NOWDB_OK) return err;

		return median(fun);

	case NOWDB_FUN_STDDEV:
		fprintf(stderr, "computing stddev\n");

		/* compute average */
		err = map2(fun, NOWDB_FUN_AVG);
		if (err != NOWDB_OK) return err;

		x = (double)fun->r2;
		if (x < 2) {
			x = 0;
			MOV(&fun->r1, &x);
			return NOWDB_OK;
		}

		/* store count in helper register */
		MOV(&fun->r3, &x);

		/* finalise average */
		err = reduce(fun, NOWDB_FUN_AVG);
		if (err != NOWDB_OK) return err;

		/* put avg in r2 and zero in r1 */
		MOV(&fun->r2, &fun->r1);
		x = 0;
		MOV(&fun->r1, &x);

		/* compute (x-a)^2 */
		err = map2(fun, NOWDB_FUN_STDDEV);
		if (err != NOWDB_OK) return err;

		/* finally compute the stddev */
		MOV(&n, &fun->r3);
		MOV(&x, &fun->r1);
		z = sqrt(x/(n-1));

		// fprintf(stderr, "sqrt(%.4f/(%.4f-1)) = %.4f\n", x, n, z);

		/* store the result */
		MOV(&fun->r1, &z);
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

/* -----------------------------------------------------------------------
 * Function type from name
 * -----------------------------------------------------------------------
 */
uint32_t nowdb_fun_fromName(const char *name) {
	if (strcasecmp(name, "COUNT") == 0) return NOWDB_FUN_COUNT;
	if (strcasecmp(name, "SUM") == 0) return NOWDB_FUN_SUM;
	if (strcasecmp(name, "PROD") == 0) return NOWDB_FUN_PROD;
	if (strcasecmp(name, "PRODUCT") == 0) return NOWDB_FUN_PROD;
	if (strcasecmp(name, "MAX") == 0) return NOWDB_FUN_MAX;
	if (strcasecmp(name, "MIN") == 0) return NOWDB_FUN_MIN;
	if (strcasecmp(name, "SPREAD") == 0) return NOWDB_FUN_SPREAD;
	if (strcasecmp(name, "AVG") == 0) return NOWDB_FUN_AVG;
	if (strcasecmp(name, "AVERAGE") == 0) return NOWDB_FUN_AVG;
	if (strcasecmp(name, "MEDIAN") == 0) return NOWDB_FUN_MEDIAN;
	if (strcasecmp(name, "MODE") == 0) return NOWDB_FUN_MODE;
	if (strcasecmp(name, "STDDEV") == 0) return NOWDB_FUN_STDDEV;
	if (strcasecmp(name, "INTEGRAL") == 0) return NOWDB_FUN_INTEGRAL;
	return -1;
}
