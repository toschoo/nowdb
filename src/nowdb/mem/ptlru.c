/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Private Text LRU Cache
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/task/lock.h>
#include <nowdb/mem/ptlru.h>

static char *OBJECT = "PTLRU";

typedef struct {
	nowdb_key_t key;
	char       *str;
	char       used;
} ptlrupair_t;

#define LRUNULL() \
	if (lru == NULL) return nowdb_err_get(nowdb_err_invalid, \
	                           FALSE, OBJECT, "LRU is NULL");

#define NOMEM(x) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, x);

#define PAIR(x) \
	((ptlrupair_t*)x)

/* ------------------------------------------------------------------------
 * LRU Callbacks: compare
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t compare(void *ignore, void *one, void *two) {
	if (PAIR(one)->key < PAIR(two)->key) return ts_algo_cmp_less;
	if (PAIR(one)->key > PAIR(two)->key) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * LRU Callbacks: delete
 * ------------------------------------------------------------------------
 */
static void destroy(void *ignore, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	if (PAIR(*n)->str != NULL) {
		free(PAIR(*n)->str);
		PAIR(*n)->str = NULL;
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
nowdb_err_t nowdb_ptlru_init(nowdb_ptlru_t *lru,
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
void nowdb_ptlru_destroy(nowdb_ptlru_t *lru) {
	if (lru == NULL) return;
	ts_algo_lru_destroy(lru);
}

/* ------------------------------------------------------------------------
 * Get data from LRU
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_ptlru_get(nowdb_ptlru_t *lru,
                            nowdb_key_t    key,
                            char         **str) {
	nowdb_err_t err=NOWDB_OK;
	ptlrupair_t *res, pair;
	
	LRUNULL();

	pair.key = key;

	res = ts_algo_lru_get(lru, &pair);
	*str = res!=NULL?res->str:NULL;

	return err;
}

/* ------------------------------------------------------------------------
 * Add data to LRU
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_ptlru_add(nowdb_ptlru_t *lru,
                            nowdb_key_t    key,
                            char          *str) {
	nowdb_err_t err=NOWDB_OK;
	ptlrupair_t *pair;
	
	LRUNULL();

	pair = malloc(sizeof(ptlrupair_t));
	if (pair == NULL) {
		NOMEM("allocating lru node");
		return err;
	}

	pair->key = key;
	pair->str = str;

	if (ts_algo_lru_add(lru, pair) != TS_ALGO_OK) {
		NOMEM("adding content to LRU");
		free(pair->str); free(pair);
	}

	return err;
}
