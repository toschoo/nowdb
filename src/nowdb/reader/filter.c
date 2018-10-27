/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Filter: generic data filter
 * ========================================================================
 */
#include <nowdb/reader/filter.h>

#include <stdlib.h>
#include <stdio.h>

static char *OBJECT="filter";

#define NOMEM(m) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, m);

#define TREETYPE(t) \
	(nowdb_type_t)(uint64_t)(((ts_algo_tree_t*)t)->rsc)

static ts_algo_cmp_t eqcompare(void *tree, void *one, void *two) {
	switch(TREETYPE(tree)) {
	case NOWDB_TYP_TEXT:
	case NOWDB_TYP_LONGTEXT:
	case NOWDB_TYP_UINT:
		/*
		fprintf(stderr, "comparing %lu and %lu\n", *(uint64_t*)one,
		                                           *(uint64_t*)two);
		*/
		if (*(uint64_t*)one <
		    *(uint64_t*)two) return ts_algo_cmp_less;
		if (*(uint64_t*)one >
		    *(uint64_t*)two) return ts_algo_cmp_greater;
		return ts_algo_cmp_equal;

	case NOWDB_TYP_DATE:
	case NOWDB_TYP_TIME:
	case NOWDB_TYP_INT:
		/*
		fprintf(stderr, "comparing %ld and %ld\n", *(int64_t*)one,
		                                           *(int64_t*)two);
		*/
		if (*(int64_t*)one <
		    *(int64_t*)two) return ts_algo_cmp_less;
		if (*(int64_t*)one >
		    *(int64_t*)two) return ts_algo_cmp_greater;
		return ts_algo_cmp_equal;

	case NOWDB_TYP_FLOAT:
		/*
		fprintf(stderr, "comparing %f and %f\n", *(double*)one,
		                                         *(double*)two);
		*/
		if (*(double*)one <
		    *(double*)two) return ts_algo_cmp_less;
		if (*(double*)one >
		    *(double*)two) return ts_algo_cmp_greater;
		return ts_algo_cmp_equal;

	default: return ts_algo_cmp_greater;

	// BOOL
	}
}

static ts_algo_rc_t noupdate(void *ignore, void *o, void *n) {
	return TS_ALGO_OK;
}

static void valdestroy(void *ignore, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	free(*n); *n=NULL;
}

static nowdb_err_t createInTree(nowdb_filter_t *filter,
                                ts_algo_list_t *in) {
	nowdb_err_t          err;
	ts_algo_list_node_t *run;

	filter->in = ts_algo_tree_new(&eqcompare, NULL,
	                              &noupdate,
	                              &valdestroy,
	                              &valdestroy);
	if (filter->in == NULL) {
		NOMEM("tree.new");
		return err;
	}
	filter->in->rsc = (void*)(uint64_t)filter->typ;
	for(run=in->head; run!=NULL; run=run->nxt) {
		if (ts_algo_tree_insert(filter->in,
		                         run->cont) != TS_ALGO_OK)
		{
			NOMEM("tree.insert");
			return err;
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Create a compare node
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_filter_newCompare(nowdb_filter_t **filter, int op,
                                        uint32_t off, uint32_t size,
                                        nowdb_type_t typ, void *val,
                                                 ts_algo_list_t *in) 
{
	nowdb_err_t err;

	*filter = calloc(1, sizeof(nowdb_filter_t));
	if (*filter == NULL) return nowdb_err_get(nowdb_err_no_mem, FALSE,
	                               "filter", "allocating filter node");

	(*filter)->ntype = NOWDB_FILTER_COMPARE;
	(*filter)->op = op;
	(*filter)->val = val;
	(*filter)->in = NULL;
	(*filter)->own = 0;
	(*filter)->left = NULL;
	(*filter)->right = NULL;

	(*filter)->off = off;
	(*filter)->size = size;
	(*filter)->typ = typ;

	if (in != NULL) {
		err = createInTree(*filter, in);
		if (err != NOWDB_OK) {
			free(*filter); *filter = NULL;
			return err;
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Filter owns the value
 * ------------------------------------------------------------------------
 */
void nowdb_filter_own(nowdb_filter_t *filter) {
	filter->own = 1;
}

/* ------------------------------------------------------------------------
 * Create a boolean node
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_filter_newBool(nowdb_filter_t **filter, int op) {

	*filter = calloc(1, sizeof(nowdb_filter_t));
	if (*filter == NULL) return nowdb_err_get(nowdb_err_no_mem, FALSE,
	                               "filter", "allocating filter node");

	(*filter)->ntype = NOWDB_FILTER_BOOL;
	(*filter)->op = op;
	(*filter)->val = NULL;
	(*filter)->own = 0;
	(*filter)->left = NULL;
	(*filter)->right = NULL;

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Destroy node
 * ------------------------------------------------------------------------
 */
void nowdb_filter_destroy(nowdb_filter_t *filter) {
	if (filter == NULL) return;
	if (filter->left != NULL) {
		nowdb_filter_destroy(filter->left);
		free(filter->left); filter->left = NULL;
	}
	if (filter->right != NULL) {
		nowdb_filter_destroy(filter->right);
		free(filter->right); filter->right= NULL;
	}
	if (filter->val != NULL) {
		if (filter->own) free(filter->val);
		filter->val = NULL;
	}
	if (filter->in != NULL) {
		ts_algo_tree_destroy(filter->in);
		free(filter->in);
		filter->in = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Apply boolean operator
 * ------------------------------------------------------------------------
 */
static nowdb_bool_t bval(nowdb_filter_t *filter, char *data) {
	switch(filter->op) {
	case NOWDB_FILTER_TRUE: return TRUE;
	case NOWDB_FILTER_FALSE: return FALSE;
	case NOWDB_FILTER_JUST:
		return nowdb_filter_eval(filter->left, data);

	case NOWDB_FILTER_NOT:
		return !nowdb_filter_eval(filter->left, data);

	case NOWDB_FILTER_AND:
		return (nowdb_filter_eval(filter->left, data) &&
		        nowdb_filter_eval(filter->right, data));

	case NOWDB_FILTER_OR:
		return (nowdb_filter_eval(filter->left, data) ||
		        nowdb_filter_eval(filter->right, data));

	default: return FALSE;
	}
}

/* ------------------------------------------------------------------------
 * A tricky macro:
 * ---------------
 * a: a pointer
 * b: a pointer
 * t: a type
 * c: A C compare operation (e.g. '==', '!=', etc.)
 * ------------------------------------------------------------------------
 */
#define DOCOMPARE(a, b, t, c) \
	return ((*(t*)a) c (*(t*)b));

/* ------------------------------------------------------------------------
 * Another tricky macro:
 * ---------------------
 * substitute EQ, LE, GE, ... for
 *            ==, <=, >=, ...
 * ------------------------------------------------------------------------
 */
#define COMPARE(a, b, t) \
	switch(filter->op) { \
	case NOWDB_FILTER_EQ: DOCOMPARE(a, b, t, ==) \
	case NOWDB_FILTER_LE: DOCOMPARE(a, b, t, <=) \
	case NOWDB_FILTER_GE: DOCOMPARE(a, b, t, >=) \
	case NOWDB_FILTER_LT: DOCOMPARE(a, b, t, <)  \
	case NOWDB_FILTER_GT: DOCOMPARE(a, b, t, >)  \
	case NOWDB_FILTER_NE: DOCOMPARE(a, b, t, !=) \
	}

/* ------------------------------------------------------------------------
 * Apply compare operator
 * ------------------------------------------------------------------------
 */
static inline nowdb_bool_t cval(nowdb_filter_t *filter, char *data) {
	switch(filter->typ) {
	case NOWDB_TYP_TEXT:
	case NOWDB_TYP_LONGTEXT:
	case NOWDB_TYP_UINT:
		if (filter->size == 4) {
			COMPARE((data+filter->off), filter->val, uint32_t);
		} else {
			COMPARE((data+filter->off), filter->val, uint64_t);
		}

	case NOWDB_TYP_DATE:
	case NOWDB_TYP_TIME:
	case NOWDB_TYP_INT:
		if (filter->size == 4) {
			COMPARE((data+filter->off), filter->val, int32_t);
		} else {
			COMPARE((data+filter->off), filter->val, int64_t);
		}

	case NOWDB_TYP_FLOAT:
		COMPARE((data+filter->off), filter->val, double);

	// BOOL

	default: return FALSE;
	}
}

/* ------------------------------------------------------------------------
 * Apply compare operator
 * ------------------------------------------------------------------------
 */
static inline nowdb_bool_t inval(nowdb_filter_t *filter, char *data) {
	return (ts_algo_tree_find(filter->in, (data+filter->off)) != NULL);
}

/* ------------------------------------------------------------------------
 * Evaluate filter
 * ------------------------------------------------------------------------
 */
nowdb_bool_t nowdb_filter_eval(nowdb_filter_t *filter, void *data) {
	if (filter->ntype == NOWDB_FILTER_BOOL) return bval(filter, data);
	if (filter->op == NOWDB_FILTER_IN) return inval(filter, data);
	return cval(filter, data);
}

/* ------------------------------------------------------------------------
 * Extract period from filter
 * TODO:
 * - currently we only check top-level 'and'
 * ------------------------------------------------------------------------
 */
void nowdb_filter_period(nowdb_filter_t *filter,
                         nowdb_time_t   *start,
                         nowdb_time_t   *end) {
	if (filter == NULL) return;
	if (filter->ntype == NOWDB_FILTER_BOOL) {
		switch(filter->op) {
		case NOWDB_FILTER_AND:
			nowdb_filter_period(filter->left, start, end);
			nowdb_filter_period(filter->right, start, end);
			return;

		case NOWDB_FILTER_JUST:
			nowdb_filter_period(filter->left, start, end);
			return;

		default: return;
		}
	} else if (filter->typ == NOWDB_TYP_TIME ||
	           filter->typ == NOWDB_TYP_DATE) 
	{
		switch(filter->op) {

		case NOWDB_FILTER_EQ:
			memcpy(start, filter->val, 8);
			memcpy(end, filter->val, 8); return;

		case NOWDB_FILTER_LE:
			memcpy(end, filter->val, 8); return;

		case NOWDB_FILTER_GE:
			memcpy(start, filter->val, 8); return;

		case NOWDB_FILTER_LT:
			memcpy(end, filter->val, 8);
			(*end)--; return;

		case NOWDB_FILTER_GT:
			memcpy(start, filter->val, 8);
			(*start)++; return;

		default: return;
		}
	}
}

/* ------------------------------------------------------------------------
 * Helper: find filter offset in key offsets
 * ------------------------------------------------------------------------
 */
static inline int getOff(nowdb_filter_t *filter,
                         uint16_t sz, uint16_t *off) {
	for(int i=0; i<sz; i++) {
		if (off[i] == filter->off) return i;
	}
	return -1;
}

/* ------------------------------------------------------------------------
 * Helper: recursively extract key range from filter
 * ------------------------------------------------------------------------
 */
static void findRange(nowdb_filter_t     *filter,
                      uint16_t sz, uint16_t *off,
                      char *rstart,   char *rend,
                      nowdb_bitmap32_t      *map) {
	int o;

	if (filter == NULL) return;
	if (filter->ntype == NOWDB_FILTER_BOOL) {
		switch(filter->op) {
		case NOWDB_FILTER_AND:
			findRange(filter->left, sz, off,
			              rstart, rend, map);
			findRange(filter->right, sz, off,
			              rstart, rend, map);

		case NOWDB_FILTER_JUST:
			findRange(filter->left, sz, off,
			              rstart, rend, map);
		default: return;
		}
	} else {
		o = getOff(filter, sz, off);
		if (o < 0) return;

		switch(filter->op) {
		case NOWDB_FILTER_EQ:
			memcpy(rstart+o*8, filter->val, filter->size);
			memcpy(rend+o*8, filter->val, filter->size);
			*map |= (1<<o);
			*map |= (65536<<o);
			return; 

		case NOWDB_FILTER_GE:
		case NOWDB_FILTER_GT:
			memcpy(rstart+o*8, filter->val, filter->size);
			*map |= (1<<o);
			return;

		case NOWDB_FILTER_LE:
		case NOWDB_FILTER_LT:
			memcpy(rend+o*8, filter->val, filter->size);
			*map |= (65536<<o);
			return;

		default: return;
		}
	}
}

/* ------------------------------------------------------------------------
 * Extract key range from filter
 * ------------------------------------------------------------------------
 */
char nowdb_filter_range(nowdb_filter_t *filter,
                        uint16_t sz, uint16_t *off,
                        char *rstart, char *rend) {
	nowdb_bitmap32_t map = 0;

	findRange(filter, sz, off, rstart, rend, &map);
	return (popcount32(map) == 2*sz);
}

/* ------------------------------------------------------------------------
 * Print filter to 'stream'
 * ------------------------------------------------------------------------
 */
void nowdb_filter_show(nowdb_filter_t *filter, FILE *stream) {
	if (filter == NULL) return;
	if (filter->ntype == NOWDB_FILTER_BOOL) {
		switch(filter->op) {
		case NOWDB_FILTER_TRUE: fprintf(stream, "TRUE"); break;
		case NOWDB_FILTER_FALSE: fprintf(stream, "FALSE"); break;

		case NOWDB_FILTER_JUST: 
			nowdb_filter_show(filter->left, stream); break;

		case NOWDB_FILTER_NOT: 
			fprintf(stream, "(~");
			nowdb_filter_show(filter->left, stream);
			fprintf(stream, ")");
			break;

		case NOWDB_FILTER_AND:
			fprintf(stream, "(");
			nowdb_filter_show(filter->left, stream);
			fprintf(stream, " & ");
			nowdb_filter_show(filter->right, stream);
			fprintf(stream, ")");
			break;

		case NOWDB_FILTER_OR: fprintf(stream, "|");
			fprintf(stream, "(");
			nowdb_filter_show(filter->left, stream);
			fprintf(stream, " v ");
			nowdb_filter_show(filter->right, stream);
			fprintf(stream, ")");
			break;
		}
	} else {
		fprintf(stream, "%d ", filter->off);

		switch(filter->op) {
		case NOWDB_FILTER_EQ:
			fprintf(stream, " = "); break;
		case NOWDB_FILTER_LE:
			fprintf(stream, " <= "); break;
		case NOWDB_FILTER_GE:
			fprintf(stream, " >= "); break;
		case NOWDB_FILTER_LT:
			fprintf(stream, " < "); break;
		case NOWDB_FILTER_GT:
			fprintf(stream, " > "); break;
		case NOWDB_FILTER_NE:
			fprintf(stream, " != "); break;
		case NOWDB_FILTER_IN:
			fprintf(stream, " IN "); break;
		}

		if (filter->op != NOWDB_FILTER_IN) {
			switch(filter->typ) {
			case NOWDB_TYP_TEXT:
			case NOWDB_TYP_UINT:
				fprintf(stream, "%lu",
				*(uint64_t*)filter->val);
				break;
	
			case NOWDB_TYP_TIME:
			case NOWDB_TYP_DATE:
			case NOWDB_TYP_INT:
				fprintf(stream, "%ld",
				 *(int64_t*)filter->val);
				break;
	
			case NOWDB_TYP_FLOAT:
				fprintf(stream, "%f",
				 *(double*)filter->val);
				break;
	
			default:
				fprintf(stream, "?%lu",
				 *(int64_t*)filter->val);
			}
		} else {
			fprintf(stream, "(...)");
		}
	}
}
