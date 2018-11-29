/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Private 12 Byte LRU Cache (i.e. roleid/key)
 * ========================================================================
 */
#ifndef nowdb_plru12_decl
#define nowdb_plru12_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/task/lock.h>

#include <tsalgo/lru.h>

/* ------------------------------------------------------------------------
 * Private 12Byte LRU
 * ------------------------------------------------------------------------
 */
typedef ts_algo_lru_t nowdb_plru12_t;

/* ------------------------------------------------------------------------
 * Init LRU
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_plru12_init(nowdb_plru12_t *lru,
                              uint32_t       max);

/* ------------------------------------------------------------------------
 * Destroy LRU
 * ------------------------------------------------------------------------
 */
void nowdb_plru12_destroy(nowdb_plru12_t *lru);

/* ------------------------------------------------------------------------
 * Get data from LRU
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_plru12_get(nowdb_plru12_t  *lru,
                             nowdb_roleid_t  role,
                             nowdb_key_t      key,
                             char          *found);

/* ------------------------------------------------------------------------
 * Add data to LRU
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_plru12_add(nowdb_plru12_t *lru,
                             nowdb_roleid_t role,
                             nowdb_key_t     key);

/* ------------------------------------------------------------------------
 * Add data to LRU as resident
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_plru12_addResident(nowdb_plru12_t *lru,
                                     nowdb_roleid_t role,
                                     nowdb_key_t     key);

/* ------------------------------------------------------------------------
 * Revoke residence
 * ------------------------------------------------------------------------
 */
void nowdb_plru12_revoke(nowdb_plru12_t *lru,
                         nowdb_roleid_t  role,
                         nowdb_key_t     key);
#endif
