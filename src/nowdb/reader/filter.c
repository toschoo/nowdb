/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Filter: generic data filter
 * ========================================================================
 */
#include <nowdb/reader/filter.h>

#include <stdlib.h>
#include <stdio.h>

/* ------------------------------------------------------------------------
 * Create a compare node
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_filter_newCompare(nowdb_filter_t **filter, int op,
                                        uint32_t off, uint32_t size,
                                        nowdb_type_t typ, void *val) 
{
	*filter = calloc(1, sizeof(nowdb_filter_t));
	if (*filter == NULL) return nowdb_err_get(nowdb_err_no_mem, FALSE,
	                               "filter", "allocating filter node");

	(*filter)->ntype = NOWDB_FILTER_COMPARE;
	(*filter)->op = op;
	(*filter)->val = val;
	(*filter)->left = NULL;
	(*filter)->right = NULL;

	(*filter)->off = off;
	(*filter)->size = size;
	(*filter)->typ = typ;

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Create a boolean node
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_filter_newBool(nowdb_filter_t **filter, int op) {
	int x;

	*filter = calloc(1, sizeof(nowdb_filter_t));
	if (*filter == NULL) return nowdb_err_get(nowdb_err_no_mem, FALSE,
	                               "filter", "allocating filter node");

	(*filter)->ntype = NOWDB_FILTER_BOOL;
	(*filter)->op = op;
	(*filter)->val = NULL;
	(*filter)->left = NULL;
	(*filter)->right = NULL;

	switch(op) {
	case NOWDB_FILTER_TRUE:
	case NOWDB_FILTER_FALSE: x = 0; break;
	case NOWDB_FILTER_JUST:
	case NOWDB_FILTER_NOT: x = 1; break;
	case NOWDB_FILTER_AND:
	case NOWDB_FILTER_OR: x = 2; break;
	default: 
		free(*filter); *filter = NULL;
		return nowdb_err_get(nowdb_err_invalid, FALSE, "filter",
		                                   "invalid operation");
	}
	if (x >= 1) {
		(*filter)->left = calloc(1, sizeof(nowdb_filter_t));
		if ((*filter)->left == NULL) {
			free(*filter); *filter = NULL;
			return nowdb_err_get(nowdb_err_invalid, FALSE,
			          "filter", "allocating filter node");
		}
	}
	if (x == 2) {
		(*filter)->right = calloc(1, sizeof(nowdb_filter_t));
		if ((*filter)->right == NULL) {
			free(*filter); *filter = NULL;
			return nowdb_err_get(nowdb_err_invalid, FALSE,
			          "filter", "allocating filter node");
		}
	}
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
		COMPARE((data+filter->off), filter->val, uint64_t);

	case NOWDB_TYP_DATE:
	case NOWDB_TYP_TIME:
	case NOWDB_TYP_INT:
		/*
		fprintf(stderr, "comparing %ld and %ld\n",
		            *(int64_t*)(filter->val),
		            *(int64_t*)(data+filter->off));
		*/
		COMPARE((data+filter->off), filter->val, int64_t);

	case NOWDB_TYP_FLOAT:
		COMPARE((data+filter->off), filter->val, double);

	default: return FALSE;
	}
}

/* ------------------------------------------------------------------------
 * Evaluate filter
 * ------------------------------------------------------------------------
 */
nowdb_bool_t nowdb_filter_eval(nowdb_filter_t *filter, void *data) {
	if (filter->ntype == NOWDB_FILTER_BOOL) return bval(filter, data);
	return cval(filter, data);
}
