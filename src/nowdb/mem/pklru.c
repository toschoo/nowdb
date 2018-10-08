/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Private Text->Key LRU Cache
 * ========================================================================
 */

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/task/lock.h>
#include <nowdb/mem/pklru.h>

static char *OBJECT = "PKLRU";

typedef struct {
	nowdb_key_t key;
	char       *str;
} pklrupair_t;

#define LRUNULL() \
	if (lru == NULL) return nowdb_err_get(nowdb_err_invalid, \
	                           FALSE, OBJECT, "LRU is NULL");

#define NOMEM(x) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, x);

#define PAIR(x) \
	((pklrupair_t*)x)

/* ------------------------------------------------------------------------
 * LRU Callbacks: compare
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t compare(void *ignore, void *one, void *two) {
	int x = strcmp(PAIR(one)->str, PAIR(two)->str);
	if (x < 0) return ts_algo_cmp_less;
	if (x > 0) return ts_algo_cmp_greater;
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
nowdb_err_t nowdb_pklru_init(nowdb_pklru_t *lru,
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
void nowdb_pklru_destroy(nowdb_pklru_t *lru) {
	if (lru == NULL) return;
	ts_algo_lru_destroy(lru);
}

/* ------------------------------------------------------------------------
 * Get data from LRU
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_pklru_get(nowdb_pklru_t *lru,
                            char          *str,
                            nowdb_key_t   *key,
                            char        *found) {
	nowdb_err_t err=NOWDB_OK;
	pklrupair_t *res, pair;
	
	LRUNULL();

	*found = 0;

	pair.str = str;

	res = ts_algo_lru_get(lru, &pair);

	if (res == NULL) return NOWDB_OK;

	*key = res->key;
	*found = 1;

	return err;
}

/* ------------------------------------------------------------------------
 * Add data to LRU
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_pklru_add(nowdb_pklru_t *lru,
                            char          *str,
                            nowdb_key_t    key) {
	nowdb_err_t err=NOWDB_OK;
	pklrupair_t *pair;
	
	LRUNULL();

	pair = malloc(sizeof(pklrupair_t));
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
