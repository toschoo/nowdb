/* ========================================================================
 * (c) Tobias Schoofs, 2018 -- 2019
 * ========================================================================
 * Text to text Map
 * ========================================================================
 */
#ifndef nowdb_t2tmap_decl
#define nowdb_t2tmap_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/task/lock.h>

#include <tsalgo/tree.h>

/* ------------------------------------------------------------------------
 * Text to Text Map
 * ------------------------------------------------------------------------
 */
typedef ts_algo_tree_t nowdb_t2tmap_t;

/* ------------------------------------------------------------------------
 * New Map
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_t2tmap_new(nowdb_t2tmap_t **map);

/* ------------------------------------------------------------------------
 * Init Map
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_t2tmap_init(nowdb_t2tmap_t *map);

/* ------------------------------------------------------------------------
 * Destroy Map
 * ------------------------------------------------------------------------
 */
void nowdb_t2tmap_destroy(nowdb_t2tmap_t *map);

/* ------------------------------------------------------------------------
 * Get data from Map
 * ------------------------------------------------------------------------
 */
char *nowdb_t2tmap_get(nowdb_t2tmap_t *map,
                       char           *key);

/* ------------------------------------------------------------------------
 * Add data to Map
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_t2tmap_add(nowdb_t2tmap_t *lru,
                             char           *key,
                             char           *val);
#endif
