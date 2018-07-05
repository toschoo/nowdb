/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * LRU Cache
 * ========================================================================
 * do we need that at all?
 * ========================================================================
 */
#ifndef nowdb_lru_decl
#define nowdb_lru_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/task/lock.h>

#include <tsalgo/lru.h>

/* ------------------------------------------------------------------------
 * Generic LRU
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_rwlock_t *lock;
	ts_algo_lru_t  *lru;
} nowdb_lru_t;

/* ------------------------------------------------------------------------
 * Init LRU
 * - compare: the compare callback
 * - destroy: the destroy callback
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_lru_init(nowdb_lru_t      *lru,
                           uint32_t          max,
                           ts_algo_comprsc_t compare,
                           ts_algo_delete_t  destroy);

/* ------------------------------------------------------------------------
 * Destroy LRU
 * ------------------------------------------------------------------------
 */
void nowdb_lru_destroy(nowdb_lru_t *lru);

/* ------------------------------------------------------------------------
 * Get data from LRU
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_lru_get(nowdb_lru_t *lru,
                          void       *what,
                          void      **cont);

/* ------------------------------------------------------------------------
 * Add data to LRU
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_lru_add(nowdb_lru_t *lru,
                          void       *cont);

#endif

