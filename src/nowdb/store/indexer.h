/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Indexer
 * ========================================================================
 */
#ifndef nowdb_store_idx_decl
#define nowdb_store_idx_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/index/index.h>

#include <tsalgo/tree.h>
#include <beet/index.h>

/* ------------------------------------------------------------------------
 * Indexer
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_index_t  *idx;
	ts_algo_tree_t *tree;
} nowdb_indexer_t;

/* ------------------------------------------------------------------------
 * Initialise one indexer
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_indexer_init(nowdb_indexer_t *xer,
                               nowdb_index_t   *idx,
                               uint32_t         isz);

/* ------------------------------------------------------------------------
 * Destroy one indexer
 * ------------------------------------------------------------------------
 */
void nowdb_indexer_destroy(nowdb_indexer_t *xer);

/* ------------------------------------------------------------------------
 * Index a buffer using an array of indexers
 * --------------
 * Parameters:
 * - xers: the array of indexers
 * - n   : number of indexers
 * - pge : pageid of the buffer
 * - isz : size of one item
 * - bsz : size of one buffer
 * - buf : buffer
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_indexer_index(nowdb_indexer_t *xers,
                                uint32_t         n,
                                nowdb_pageid_t   pge,
                                uint32_t         isz,
                                uint32_t         bsz ,
                                char            *buf);
#endif
