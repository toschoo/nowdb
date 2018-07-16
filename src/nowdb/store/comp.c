/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Compression for stores
 * ========================================================================
 */
#include <nowdb/store/comp.h>
#include <nowdb/io/file.h>

/* ------------------------------------------------------------------------
 * Allocate and initialise new Compression Context
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_compctx_new(nowdb_compctx_t **ctx,
                              uint32_t        csize,
                              uint32_t        dsize) {
	nowdb_err_t err;
	if (ctx == NULL) return nowdb_err_get(nowdb_err_invalid,
                                                 FALSE, "store",
	              "pointer to compression context is NULL");
	*ctx = malloc(sizeof(nowdb_compctx_t));
	if (*ctx == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                                         FALSE, "store",
	                      "allocating compression context");
	err = nowdb_compctx_init(*ctx, csize, dsize);
	if (err != NOWDB_OK) {
		free(*ctx); *ctx = NULL; return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: destroy all compression contexts up to size
 * ------------------------------------------------------------------------
 */
static inline void destroyCCtxes(ZSTD_CCtx **cctx, int size) {
	if (size == 0) return;
	for(int i=0;i<size;i++) {
		if (cctx[i] != NULL) {
			ZSTD_freeCCtx(cctx[i]); cctx[i] = NULL;
		}
	}
}

/* ------------------------------------------------------------------------
 * Helper: destroy all decompression contexts up to size
 * ------------------------------------------------------------------------
 */
static inline void destroyDCtxes(ZSTD_DCtx **dctx, int size) {
	if (size == 0) return;
	for(int i=0;i<size;i++) {
		if (dctx[i] != NULL) {
			ZSTD_freeDCtx(dctx[i]); dctx[i] = NULL;
		}
	}
}

/* ------------------------------------------------------------------------
 * Initialise an already allocated Compression Context
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_compctx_init(nowdb_compctx_t *ctx,
                               uint32_t       csize,
                               uint32_t       dsize) {
	nowdb_err_t err;

	if (ctx == NULL) return nowdb_err_get(nowdb_err_invalid,
                                                 FALSE, "store",
	                         "compression context is NULL");
	ctx->cctx  = NULL;
	ctx->dctx  = NULL;
	ctx->cdict = NULL;
	ctx->ddict = NULL;
	ctx->cmap  = 0;
	ctx->dmap1 = 0;
	ctx->dmap2 = 0;

	if (csize > 32) return nowdb_err_get(nowdb_err_invalid,
	                                        FALSE, "store",
	         "compression context size too big (max: 32)");
	if (dsize > 128) return nowdb_err_get(nowdb_err_invalid,
	                                         FALSE, "store",
	       "decompression context size too big (max: 128)");
	
	ctx->csize = csize;
	ctx->dsize = dsize;

	err = nowdb_lock_init(&ctx->lock);
	if (err != NOWDB_OK) return err;

	ctx->cctx = calloc(csize, sizeof(ZSTD_CCtx*));
	if (ctx->cctx == NULL) {
		nowdb_lock_destroy(&ctx->lock);
		return nowdb_err_get(nowdb_err_no_mem, FALSE, "store",
	                            "allocating compression context");
	}
	for(int i=0;i<csize;i++) {
		ctx->cctx[i] = ZSTD_createCCtx();
		if (ctx->cctx[i] == NULL) {
			nowdb_lock_destroy(&ctx->lock);
			destroyCCtxes(ctx->cctx, i);
			free(ctx->cctx); ctx->cctx = NULL;
			return nowdb_err_get(nowdb_err_no_mem, FALSE, "store",
	                               "allocating ZSTD compression context");
		}
	}
	ctx->dctx = calloc(dsize, sizeof(ZSTD_DCtx*));
	if (ctx->dctx == NULL) {
		nowdb_lock_destroy(&ctx->lock);
		destroyCCtxes(ctx->cctx, csize);
		free(ctx->cctx); ctx->cctx = NULL;
		return nowdb_err_get(nowdb_err_no_mem, FALSE, "store",
	                          "allocating decompression context");
	}
	for(int i=0;i<dsize;i++) {
		ctx->dctx[i] = ZSTD_createDCtx();
		if (ctx->dctx[i] == NULL) {
			nowdb_lock_destroy(&ctx->lock);
			destroyCCtxes(ctx->cctx, csize);
			free(ctx->cctx); ctx->cctx = NULL;
			destroyDCtxes(ctx->dctx, i);
			free(ctx->dctx); ctx->dctx = NULL;
			return nowdb_err_get(nowdb_err_no_mem, FALSE, "store",
	                             "allocating ZSTD decompression context");
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Destroy Compression Context
 * ------------------------------------------------------------------------
 */
void nowdb_compctx_destroy(nowdb_compctx_t *ctx) {
	if (ctx == NULL) return;
	nowdb_lock_destroy(&ctx->lock);
	if (ctx->cctx != NULL) {
		destroyCCtxes(ctx->cctx, ctx->csize);
		free(ctx->cctx); ctx->cctx = NULL;
	}
	if (ctx->dctx != NULL) {
		destroyDCtxes(ctx->dctx, ctx->dsize);
		free(ctx->dctx); ctx->dctx = NULL;
	}
	if (ctx->cdict != NULL) {
		ZSTD_freeCDict(ctx->cdict); ctx->cdict = NULL;
	}
	if (ctx->ddict != NULL) {
		ZSTD_freeDDict(ctx->ddict); ctx->ddict = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Load Dictionary from disk
 * ------------------------------------------------------------------------
 */
#define DICTNAME "zdict"
nowdb_err_t nowdb_compctx_loadDict(nowdb_compctx_t *ctx,
                                   nowdb_path_t   path) {
	char *buf;
	nowdb_err_t err = NOWDB_OK;
	nowdb_path_t p;
	struct stat st;
	ssize_t x;
	FILE *d;

	/* we already have a dictionary */
	if (ctx->cdict != NULL &&
	    ctx->ddict != NULL) NOWDB_OK;
	
	/* otherwise clean up the mess and start */
	if (ctx->cdict != NULL) {
		ZSTD_freeCDict(ctx->cdict); ctx->cdict = NULL;
	}
	if (ctx->ddict != NULL) {
		ZSTD_freeDDict(ctx->ddict); ctx->ddict = NULL;
	}

	/* path to dict */
	p = nowdb_path_append(path, DICTNAME);
	if (p == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                   FALSE, "store", "append to path");

	/* if we cannot stat return an error */
	if (stat(p, &st) != 0) {
		err = nowdb_err_get(nowdb_err_stat, TRUE, "store", p);
		free(p); return err;
	}

	/* load the dictionary */
	buf = malloc(st.st_size);
	if (buf == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, "store", p);
		free(p); return err;
	}
	d = fopen(p, "rb");
	if (d == NULL) {
		err = nowdb_err_get(nowdb_err_open, TRUE, "store", p);
		free(buf); free(p); return err;
	}
	x = fread(buf, 1, st.st_size, d);
	if (x != st.st_size) {
		err = nowdb_err_get(nowdb_err_read, TRUE, "store", p);
		fclose(d); free(buf); free(p); return err;
	}
	fclose(d); free(p);

	/* create compression dictionary */
	ctx->cdict = ZSTD_createCDict(buf, st.st_size, NOWDB_ZSTD_LEVEL);
	if (ctx->cdict == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, "store",
		                      "cannot load ZSTD dictionary");
		free(buf); return err;
	}

	/* create decompression dictionary */
	ctx->ddict = ZSTD_createDDict(buf, st.st_size);
	if (ctx->cdict == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, "store",
		                      "cannot load ZSTD dictionary");
		ZSTD_freeCDict(ctx->cdict); ctx->cdict = NULL;
		free(buf); return err;
	}
	free(buf);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Train dictionary and store it to disk
 * ------------------------------------------------------------------------
 */
#define DICTSIZE 102400
nowdb_err_t nowdb_compctx_trainDict(nowdb_compctx_t *ctx,
                                    nowdb_path_t    path,
                                    char            *buf,
                                    uint32_t        size) {
	nowdb_path_t  p;
	nowdb_err_t err = NOWDB_OK;
	size_t dsz;
	size_t *samplesz;
	size_t nm;
	char *dictbuf;
	FILE *d;
	ssize_t x;

	dictbuf = malloc(DICTSIZE);
	if (dictbuf == NULL) {
		return nowdb_err_get(nowdb_err_no_mem, FALSE, "store",
		                                 "allocating buffer");
	}

	/* NOTE: as long as NOWDB_IDX_PAGE divides size,
	         there is no issue with remainders here! */
	nm = size/NOWDB_IDX_PAGE;
	if (nm == 0) {
		err = nowdb_err_get(nowdb_err_invalid, FALSE, "store",
		                          "training buffer too small");
		free(dictbuf); return err;
	}

	/* 'nm' samples, so 'nm' times the size */
	samplesz = malloc(nm*sizeof(size_t));
	if (samplesz == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, "store",
		                                "allocating buffer"); 
		free(dictbuf); return err;
	}

	/* the samples all have the same size */
	for(int i=0;i<nm;i++) {
		samplesz[i] = NOWDB_IDX_PAGE;
	}

	/* train samples */
	dsz = ZDICT_trainFromBuffer(dictbuf, DICTSIZE, buf, samplesz, nm);
	if (ZDICT_isError(dsz)) {
		err = nowdb_err_get(nowdb_err_compdict, FALSE, "store",
		                       (char*)ZDICT_getErrorName(dsz));
		free(dictbuf); free(samplesz); return err;
	}

	/* now write the dictionary to disk */
	p = nowdb_path_append(path, DICTNAME);
	if (p == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, "store",
		                                "appending to path");
		free(dictbuf); free(samplesz); return err;
	}
	d = fopen(p, "wb");
	if (d == NULL) {
		err = nowdb_err_get(nowdb_err_open, TRUE, "store", p);
		free(p); free(dictbuf); free(samplesz); return err;
	}
	x = fwrite(dictbuf, 1, dsz, d);
	if (x != dsz) {
		err = nowdb_err_get(nowdb_err_write, TRUE, "store", p);
		fclose(d); free(p); free(dictbuf); free(samplesz);
		return err;
	}
	fclose(d); free(p);
	free(dictbuf); free(samplesz);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Get an unused Compression Context
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_compctx_getCCtx(nowdb_compctx_t *ctx,
                                  ZSTD_CCtx    **cctx) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;
	nowdb_bitmap32_t k = 1;

	if (ctx == NULL) return nowdb_err_get(nowdb_err_invalid,
	                                         FALSE, "store",
	                         "compression context is NULL");
	if (cctx == NULL) return nowdb_err_get(nowdb_err_invalid,
	                                          FALSE, "store",
	           "pointer to ZSTD compression context is NULL");

	err = nowdb_lock(&ctx->lock);
	if (err != NOWDB_OK) return err;

	*cctx = NULL;

	for(int i=0; i<ctx->csize; i++) {
		if ((ctx->cmap & k) == 0) {
			*cctx = ctx->cctx[i];
			ctx->cmap |= k;
			break;
		}
		k <<= 1;
	}
	// fprintf(stderr, "%u - %p\n", ctx->cmap, *cctx);
	if (*cctx == NULL) err = nowdb_err_get(nowdb_err_no_rsc,
                                                 FALSE, "store",
	                              "no compression context");
	err2 = nowdb_unlock(&ctx->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Get an unused Decompression Context
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_compctx_getDCtx(nowdb_compctx_t *ctx,
                                  ZSTD_DCtx    **dctx) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_err_t err2;
	nowdb_bitmap64_t k1 = 1;
	nowdb_bitmap64_t k2 = 1;

	if (ctx == NULL) return nowdb_err_get(nowdb_err_invalid,
	                                         FALSE, "store",
	                         "compression context is NULL");
	if (dctx == NULL) return nowdb_err_get(nowdb_err_invalid,
	                                          FALSE, "store",
	         "pointer to ZSTD decompression context is NULL");

	err = nowdb_lock(&ctx->lock);
	if (err != NOWDB_OK) return err;

	*dctx = NULL;

	for(int i=0; i<ctx->dsize; i++) {
		if (i < 64) {
			if ((ctx->dmap1 & k1) == 0) {
				*dctx = ctx->dctx[i];
				ctx->dmap1 |= k1;
				// fprintf(stderr, "using dctx %d\n", i);
				break;
			}
			k1 <<= 1;
		} else {
			if ((ctx->dmap2 & k2) == 0) {
				*dctx = ctx->dctx[i];
				ctx->dmap2 |= k2;
				// fprintf(stderr, "using dctx %d\n", i);
				break;
			}
			k2 <<= 1;
		}
	}
	if (*dctx == NULL) {
		*dctx = ZSTD_createDCtx();
		if (*dctx  == NULL) {
	   		err2 = nowdb_err_get(nowdb_err_no_rsc, FALSE, "store",
	                                          "no decompression context");
			err = nowdb_err_get(nowdb_err_no_mem, FALSE, "store",
			                       "cannot create ZSTD context");
			err->cause = err2;
		}
	}
	err2 = nowdb_unlock(&ctx->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Release Compression Context
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_compctx_releaseCCtx(nowdb_compctx_t *ctx,
                                      ZSTD_CCtx     *cctx) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_bitmap32_t k = 1;

	if (ctx == NULL) return nowdb_err_get(nowdb_err_invalid,
	                                         FALSE, "store",
	                         "compression context is NULL");

	err = nowdb_lock(&ctx->lock);
	if (err != NOWDB_OK) return err;

	for(int i=0; i<ctx->csize; i++) {
		if (cctx == ctx->cctx[i]) {
			ctx->cmap ^= k; break;
		}
		k <<= 1;
	}
	// fprintf(stderr, "%u + %p\n", ctx->cmap, cctx);
	return nowdb_unlock(&ctx->lock);
}

/* ------------------------------------------------------------------------
 * Release Decompression Context
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_compctx_releaseDCtx(nowdb_compctx_t *ctx,
                                      ZSTD_DCtx     *dctx) {
	nowdb_err_t err = NOWDB_OK;
	char found = 0;
	nowdb_bitmap64_t k1 = 1;
	nowdb_bitmap64_t k2 = 1;

	if (ctx == NULL) return nowdb_err_get(nowdb_err_invalid,
	                                         FALSE, "store",
	                         "compression context is NULL");

	err = nowdb_lock(&ctx->lock);
	if (err != NOWDB_OK) return err;

	for(int i=0; i<ctx->dsize; i++) {
		if (i < 64) {
			if (dctx == ctx->dctx[i]) {
				if (ctx->dmap1 & k1) ctx->dmap1 ^= k1;
				// fprintf(stderr, "releasing dctx %d\n", i);
				found = 1; break;
			}
			k1 <<= 1;
		} else {
			if (dctx == ctx->dctx[i]) {
				if (ctx->dmap2 & k2) ctx->dmap2 ^= k2;
				// fprintf(stderr, "releasing dctx %d\n", i);
				found = 1; break;
			}
			k2 <<= 1;
		}
	}
	if (!found) ZSTD_freeDCtx(dctx);

	return nowdb_unlock(&ctx->lock);
}

/* ------------------------------------------------------------------------
 * Get Compression Dictionary
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_compctx_getCDict(nowdb_compctx_t *ctx,
                                   ZSTD_CDict   **cdict) {
	nowdb_err_t err = NOWDB_OK;

	if (ctx == NULL) return nowdb_err_get(nowdb_err_invalid,
	                                         FALSE, "store",
	                         "compression context is NULL");
	if (cdict == NULL) return nowdb_err_get(nowdb_err_invalid,
	                                           FALSE, "store",
	        "pointer to ZSTD compression dictionary is NULL");

	err = nowdb_lock(&ctx->lock);
	if (err != NOWDB_OK) return err;

	*cdict = ctx->cdict;

	return nowdb_unlock(&ctx->lock);
}

/* ------------------------------------------------------------------------
 * Get Decompression Dictionary
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_compctx_getDDict(nowdb_compctx_t *ctx,
                                   ZSTD_DDict   **ddict) {
	nowdb_err_t err;

	if (ctx == NULL) return nowdb_err_get(nowdb_err_invalid,
	                                         FALSE, "store",
	                         "compression context is NULL");
	if (ddict == NULL) return nowdb_err_get(nowdb_err_invalid,
	                                           FALSE, "store",
	       "pointer to ZSTD decompression dictionary is NULL");

	err = nowdb_lock(&ctx->lock);
	if (err != NOWDB_OK) return err;

	*ddict = ctx->ddict;

	return nowdb_unlock(&ctx->lock);
}
