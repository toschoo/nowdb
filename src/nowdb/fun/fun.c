/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Generic SQL functions
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/fun/fun.h>

static char *OBJECT  = "fun";

#define BLOCKSIZE 4096

#define INVALID(s) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, s);

#define NOMEM(s) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, s);

#define ADD(v1,v2) \
	v1+=v2;

#define MUL(v1,v2) \
	v1*=v2;

#define MAX(v1,v2) \
	if (v1<v2) v1=v2;

#define MIN(v1,v2) \
	if (v1>v2) v1=v2;

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
	                            FALSE, OBJECT, "add"); \
	}
	
/* -----------------------------------------------------------------------
 * All my neat functions
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t add(void *v1, void *v2, nowdb_type_t t) {
	PERFM(ADD);
}
static inline nowdb_err_t mul(void *v1, void *v2, nowdb_type_t t) {
	PERFM(MUL);
}
static inline nowdb_err_t max(void *v1, void *v2, nowdb_type_t t) {
	PERFM(MAX);
}
static inline nowdb_err_t min(void *v1, void *v2, nowdb_type_t t) {
	PERFM(MIN);
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
                           nowdb_value_t        init) {
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

	memcpy(&fun->value, &fun->init, fun->fsize);

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
 * Destroy
 * -----------------------------------------------------------------------
 */
void nowdb_fun_destroy(nowdb_fun_t *fun) {
	if (fun == NULL) return;

	ts_algo_list_destroy(&fun->many);

	if (fun->flist != NULL) {
		nowdb_blist_destroy(fun->flist);
		free(fun->flist); fun->flist = NULL;
	}
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
	char *block;

	if (fun->many.len == 0 || fun->off >= BLOCKSIZE) {
		err = nowdb_blist_give(fun->flist, &fun->many);
		if (err != NOWDB_OK) return err;
		fun->off = 0;
	}
	block = fun->many.head->cont;
	memcpy(block+fun->off, record+fun->field, fun->fsize);
	fun->off += fun->fsize;
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Helper: apply 0-function
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t apply_(nowdb_fun_t *fun) {
	switch(fun->fun) {
	case NOWDB_FUN_COUNT: fun->value++; break;
	default: INVALID("function cannot be applied in context");
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Helper: apply function
 * -----------------------------------------------------------------------
 */
static inline nowdb_err_t apply(nowdb_fun_t *fun, char *record) {
	switch(fun->fun) {
	case NOWDB_FUN_SUM:
		return add(&fun->value, record+fun->field, fun->dtype);
	case NOWDB_FUN_PROD:
		return mul(&fun->value, record+fun->field, fun->dtype);
	
	default: INVALID("function cannot be applied in context");
	}
}

/* -----------------------------------------------------------------------
 * Collect
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_fun_map(nowdb_fun_t *fun, void *record) {
	switch(fun->ftype) {
	case NOWDB_FUN_ZERO: return apply_(fun);
	case NOWDB_FUN_ONE: return apply(fun, record);
	case NOWDB_FUN_MANY: return collect(fun, record);
	case NOWDB_FUN_TREE: return nowdb_err_get(nowdb_err_not_supp,
	             FALSE, OBJECT, "tree functions not implemented");
	default: INVALID("unknown function type");
	}
}

/* -----------------------------------------------------------------------
 * Reduce
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_fun_reduce(nowdb_fun_t *fun) {
	return NOWDB_OK;
}
