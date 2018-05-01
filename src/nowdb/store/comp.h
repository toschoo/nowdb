/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Compression context for store
 * ========================================================================
 */
#ifndef nowdb_store_comp_decl
#define nowdb_store_comp_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/io/dir.h>
#include <nowdb/task/lock.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include <zstd.h>
#include <zstd/zdict.h>

/* ------------------------------------------------------------------------
 * Compression Context
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_lock_t      lock; /* exclusive lock                   */
	uint32_t         csize; /* number of compression contexts   */
	uint32_t         dsize; /* number of decompression contexts */
	ZSTD_CCtx       **cctx; /* ZSTD compression contexts        */
	nowdb_bitmap32_t  cmap; /* free/used compression contexts   */
	ZSTD_DCtx       **dctx; /* ZSTD decompression contexts      */
	nowdb_bitmap64_t dmap1; /* free/used decompression contexts */
	nowdb_bitmap64_t dmap2; /* free/used decompression contexts */
	ZSTD_CDict      *cdict; /* compression dictionary           */
	ZSTD_DDict      *ddict; /* decompression dictionary         */
} nowdb_compctx_t;

/* ------------------------------------------------------------------------
 * Allocate and initialise a new compression context
 * -----------------------
 * Parameters:
 * - ctx  : pointer to the new context
 * - csize: size of compression contexts (max: 32)
 * - dsize: size of decompression contexts (max: 128)
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_compctx_new(nowdb_compctx_t **ctx,
                              uint32_t        csize,
                              uint32_t        dsize);

/* ------------------------------------------------------------------------
 * Initialise an already allocated compression context
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_compctx_init(nowdb_compctx_t *ctx,
                               uint32_t       csize,
                               uint32_t       dsize);

/* ------------------------------------------------------------------------
 * Destroy compression context
 * ------------------------------------------------------------------------
 */
void nowdb_compctx_destroy(nowdb_compctx_t *ctx);

/* ------------------------------------------------------------------------
 * Load dictionary from disk
 * ---------------
 * NOTE: This function does not lock.
 *       The context must be locked explicitly,
 *                when this function is called!   
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_compctx_loadDict(nowdb_compctx_t *ctx,
                                   nowdb_path_t   path);

/* ------------------------------------------------------------------------
 * Train dictionary and store it to disk
 * ---------------
 * NOTE: This function does not lock.
 *       The context must be locked explicitly,
 *                when this function is called!   
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_compctx_trainDict(nowdb_compctx_t *ctx,
                                    nowdb_path_t    path,
                                    char            *buf,
                                    uint32_t       size);

/* ------------------------------------------------------------------------
 * Get a compression context
 * -------------------------
 * fails if there are no new compression contexts available
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_compctx_getCCtx(nowdb_compctx_t *ctx,
                                  ZSTD_CCtx    **cctx);

/* ------------------------------------------------------------------------
 * Get a decompression context
 * ---------------------------
 * If all preallocated decompression contexts are in use,
 * an new one is allocated.
 * Fails if no new decompression contexts can be allocated.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_compctx_getDCtx(nowdb_compctx_t *ctx,
                                  ZSTD_DCtx    **dctx);

/* ------------------------------------------------------------------------
 * Release compression context
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_compctx_releaseCCtx(nowdb_compctx_t *ctx,
                                      ZSTD_CCtx     *cctx);

/* ------------------------------------------------------------------------
 * Release decompression context
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_compctx_releaseDCtx(nowdb_compctx_t *ctx,
                                      ZSTD_DCtx     *dctx);

/* ------------------------------------------------------------------------
 * Get compression dictionary
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_compctx_getCDict(nowdb_compctx_t *ctx,
                                   ZSTD_CDict  **cdict);

/* ------------------------------------------------------------------------
 * Get decompression dictionary
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_compctx_getDDict(nowdb_compctx_t *ctx,
                                   ZSTD_DDict  **ddict);
#endif
