/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Private 8 Byte LRU Cache with residents (main use case: vertex pk)
 * ========================================================================
 */
#include <nowdb/mem/plru8r.h>
#include <stdio.h>

static char *OBJECT = "LRU8r";

#define KEY(x) \
	(*(nowdb_key_t*)x)

#define LRUNULL() \
	if (lru == NULL) return nowdb_err_get(nowdb_err_invalid, \
	                           FALSE, OBJECT, "LRU is NULL");

#define NOMEM(x) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, x);

/* ------------------------------------------------------------------------
 * LRU Callbacks: compare
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t compare(void *ignore, void *one, void *two) {
	if (KEY(one) < KEY(two)) return ts_algo_cmp_less;
	if (KEY(one) > KEY(two)) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * LRU Callbacks: delete
 * ------------------------------------------------------------------------
 */
static void destroy(void *ignore, void **n) {
	if (n == NULL) return;
	if (*n == NULL) return;
	free(*n); *n=NULL;
}

/* ------------------------------------------------------------------------
 * LRU Callbacks: update
 * ------------------------------------------------------------------------
 */
static ts_algo_rc_t noupdate(void *ignore, void *o, void *n) {
	if (n != NULL) free(n);
	return TS_ALGO_OK;
}

/* ------------------------------------------------------------------------
 * Init LRU
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_plru8r_init(nowdb_plru8r_t *lru,
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
void nowdb_plru8r_destroy(nowdb_plru8r_t *lru) {
 	if (lru == NULL) return;
	ts_algo_lru_destroy(lru);
}


/* ------------------------------------------------------------------------
 * Get data from LRU
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_plru8r_get(nowdb_plru8r_t  *lru,
                             nowdb_key_t      key,
                             char          *found) {
	nowdb_key_t *x;

	LRUNULL();

	x = ts_algo_lru_get(lru, &key);

	*found = x!=NULL;

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Add data to LRU (either resident or not)
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t add2lru(nowdb_plru8r_t *lru,
                                  nowdb_key_t     key,
                                  char          rsdnt) {
	nowdb_err_t err=NOWDB_OK;
	nowdb_key_t *k;
	
	LRUNULL();

	k = malloc(sizeof(nowdb_key_t));
	if (k == NULL) {
		NOMEM("allocating lru node");
		return err;
	}

	*k = key;

	if (rsdnt) {
		if (ts_algo_lru_addResident(lru, k) != TS_ALGO_OK) {
			NOMEM("adding content to LRU");
			free(k);
		}
	} else {
		if (ts_algo_lru_add(lru, k) != TS_ALGO_OK) {
			NOMEM("adding content to LRU");
			free(k);
		}
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Add data to LRU as resident
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_plru8r_addResident(nowdb_plru8r_t *lru,
                                     nowdb_key_t     key) {
	return add2lru(lru,key,1);
}

/* ------------------------------------------------------------------------
 * Add data to LRU
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_plru8r_add(nowdb_plru8r_t *lru,
                            nowdb_key_t      key) {
	return add2lru(lru,key,0);
}

/* ------------------------------------------------------------------------
 * Revoke residence
 * ------------------------------------------------------------------------
 */
void nowdb_plru8r_revoke(nowdb_plru8r_t *lru,
                         nowdb_key_t     key) {
	ts_algo_lru_revokeResidence(lru, &key);
}
