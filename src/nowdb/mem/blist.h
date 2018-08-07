/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Block list
 * "Let me go on like I blister in the sun" (Violent Femmes)
 * ========================================================================
 */
#ifndef nowdb_blist_decl
#define nowdb_blist_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>

#include <stdint.h>

#include <tsalgo/list.h>

typedef struct {
	uint32_t sz; /* effective block size */
	char *block; /* bytes                */
} nowdb_block_t;

typedef struct {
	uint32_t          sz; /* block size */
	ts_algo_list_t flist; /* free list  */
} nowdb_blist_t;

/* -----------------------------------------------------------------------
 * Init already allocated block list
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_blist_init(nowdb_blist_t *blist, uint32_t sz);

/* -----------------------------------------------------------------------
 * Destroy
 * -----------------------------------------------------------------------
 */
void nowdb_blist_destroy(nowdb_blist_t *blist);

/* -----------------------------------------------------------------------
 * Get block
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_blist_get(nowdb_blist_t        *blist,
                            ts_algo_list_node_t **block);

/* -----------------------------------------------------------------------
 * Give me a block (i.e. add it to my list 'blocks')
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_blist_give(nowdb_blist_t  *blist,
                             ts_algo_list_t *blocks);

/* -----------------------------------------------------------------------
 * Take back a block from my list 'blocks' 
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_blist_take(nowdb_blist_t  *blist,
                             ts_algo_list_t *blocks);

/* -----------------------------------------------------------------------
 * Free block
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_blist_free(nowdb_blist_t *blist, 
                       ts_algo_list_node_t *block);

/* -----------------------------------------------------------------------
 * Free all blocks
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_blist_freeAll(nowdb_blist_t *blist,
                             ts_algo_list_t *blocks);

#endif
