/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * LRU Cache
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/task/lock.h>
#include <nowdb/mem/lru.h>

static char *OBJECT = "LRU";

#define LRUNULL() \
	if (lru == NULL) return nowdb_err_get(nowdb_err_invalid, \
	                           FALSE, OBJECT, "LRU is NULL");

#define NOMEM(x) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, x);

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
nowdb_err_t nowdb_lru_init(nowdb_lru_t      *lru,
                           uint32_t          max,
                           ts_algo_comprsc_t compare,
                           ts_algo_delete_t  destroy) {
	nowdb_err_t err;

	LRUNULL();

	lru->lock = NULL;
	lru->lru  = NULL;

	lru->lock = calloc(1,sizeof(nowdb_rwlock_t));
	if (lru->lock == NULL) {
		NOMEM("allocating lock");
		return err;
	}
	err = nowdb_rwlock_init(lru->lock);
	if (err != NOWDB_OK) {
		free(lru->lock); lru->lock = NULL;
		return err;
	}

	lru->lru = ts_algo_lru_new(max, compare, &noupdate, destroy, destroy);
	if (lru->lru == NULL) {
		nowdb_lru_destroy(lru);
		NOMEM("allocating lru");
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Destroy LRU
 * ------------------------------------------------------------------------
 */
void nowdb_lru_destroy(nowdb_lru_t *lru) {
	if (lru == NULL) return;
	if (lru->lock != NULL) {
		nowdb_rwlock_destroy(lru->lock);
		free(lru->lock); lru->lock = NULL;
	}
	if (lru->lru != NULL) {
		ts_algo_lru_destroy(lru->lru);
		free(lru->lru); lru->lru= NULL;
	}
}

/* ------------------------------------------------------------------------
 * Get data from LRU
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_lru_get(nowdb_lru_t *lru,
                          void       *what,
                          void      **cont) {
	nowdb_err_t err2, err=NOWDB_OK;
	
	LRUNULL();

	err = nowdb_lock_read(lru->lock);
	if (err != NOWDB_OK) return err;

	*cont = ts_algo_lru_get(lru->lru, what);

	err2 = nowdb_unlock_read(lru->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Add data to LRU
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_lru_add(nowdb_lru_t *lru,
                          void       *cont) {
	nowdb_err_t err2, err=NOWDB_OK;
	
	LRUNULL();

	err = nowdb_lock_write(lru->lock);
	if (err != NOWDB_OK) return err;

	if (ts_algo_lru_add(lru->lru, cont) != TS_ALGO_OK) {
		NOMEM("adding content to LRU");
	}

	err2 = nowdb_unlock_write(lru->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err;
		return err2;
	}
	return err;
}
