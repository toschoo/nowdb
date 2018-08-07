/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Private Text LRU Cache
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/task/lock.h>
#include <nowdb/mem/pplru.h>

static char *OBJECT = "PPLRU";

typedef struct {
	nowdb_pageid_t pge;
	char         *page;
} pplrupair_t;

#define LRUNULL() \
	if (lru == NULL) return nowdb_err_get(nowdb_err_invalid, \
	                           FALSE, OBJECT, "LRU is NULL");

#define NOMEM(x) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, x);

#define PAIR(x) \
	((pplrupair_t*)x)

/* ------------------------------------------------------------------------
 * LRU Callbacks: compare
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t compare(void *ignore, void *one, void *two) {
	if (PAIR(one)->pge < PAIR(two)->pge) return ts_algo_cmp_less;
	if (PAIR(one)->pge > PAIR(two)->pge) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * LRU Callbacks: delete
 * ------------------------------------------------------------------------
 */
static void destroy(void *ignore, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	if (PAIR(*n)->page != NULL &&
	    PAIR(*n)->page != NOWDB_BLACK_PAGE) {
		free(PAIR(*n)->page);
		PAIR(*n)->page = NULL;
	}
	free(*n); *n=NULL;
}

/* ------------------------------------------------------------------------
 * LRU Callbacks: update
 * ------------------------------------------------------------------------
 */
static ts_algo_rc_t noupdate(void *ignore, void *o, void *n) {
	return TS_ALGO_OK;
}

/* ------------------------------------------------------------------------
 * Init LRU
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_pplru_init(nowdb_pplru_t *lru,
                             uint32_t       max) {
	nowdb_err_t err;

	LRUNULL();

	if (ts_algo_lru_init(lru, max, &compare,
	                               &noupdate, 
	                               &destroy, 
	                               &destroy) != TS_ALGO_OK) {
		NOMEM("allocating lru");
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Destroy LRU
 * ------------------------------------------------------------------------
 */
void nowdb_pplru_destroy(nowdb_pplru_t *lru) {
	if (lru == NULL) return;
	ts_algo_lru_destroy(lru);
}

/* ------------------------------------------------------------------------
 * Get data from LRU
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_pplru_get(nowdb_pplru_t *lru,
                            nowdb_pageid_t pge,
                            char        **page) {
	nowdb_err_t err=NOWDB_OK;
	pplrupair_t *res, pair;
	
	LRUNULL();

	pair.pge = pge;

	res = ts_algo_lru_get(lru, &pair);
	*page = res!=NULL?res->page:NULL;

	return err;
}

/* ------------------------------------------------------------------------
 * Add data to LRU
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_pplru_add(nowdb_pplru_t *lru,
                            nowdb_pageid_t pge,
                            char         *page) {
	nowdb_err_t err=NOWDB_OK;
	pplrupair_t *pair;
	
	LRUNULL();

	pair = malloc(sizeof(pplrupair_t));
	if (pair == NULL) {
		NOMEM("allocating lru node");
		return err;
	}

	pair->pge = pge;
	pair->page = malloc(NOWDB_IDX_PAGE);
	if (pair->page == NULL) {
		free(pair);
		NOMEM("allocating page");
		return err;
	}
	if (page != NOWDB_BLACK_PAGE) {
		memcpy(pair->page, page, NOWDB_IDX_PAGE);
	}

	if (ts_algo_lru_add(lru, pair) != TS_ALGO_OK) {
		NOMEM("adding content to LRU");
		free(pair->page); free(pair);
	}

	return err;
}
