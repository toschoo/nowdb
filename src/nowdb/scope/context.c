/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Context is just a wrapper around store
 * ========================================================================
 */
#include <nowdb/scope/context.h>

/* ------------------------------------------------------------------------
 * Wrap error into 'context' error
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_context_err(nowdb_context_t *ctx,
                              nowdb_err_t      err) {
	if (err == NOWDB_OK) return NOWDB_OK;
	return (nowdb_err_cascade(
	        nowdb_err_get(nowdb_err_context,
	        FALSE, "ctx", ctx->name), err));
}

/* ------------------------------------------------------------------------
 * Destroy context
 * ------------------------------------------------------------------------
 */
void nowdb_context_destroy(nowdb_context_t *ctx) {
	if (ctx == NULL) return;
	if (ctx->name != NULL) {
		free(ctx->name); ctx->name = NULL;
	}
	nowdb_store_destroy(&ctx->store);
}

/* ------------------------------------------------------------------------
 * Insert one record
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_context_insert(nowdb_context_t *ctx,
                                 void           *data) {
	return nowdb_context_err(ctx, nowdb_store_insert(&ctx->store, data));
}

/* ------------------------------------------------------------------------
 * Insert n records
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_context_insertBulk(nowdb_context_t *ctx,
                                     void           *data,
                                     uint32_t      count);

/* -----------------------------------------------------------------------
 * Predefine configurations
 * -----------------------------------------------------------------------
 */
void nowdb_ctx_config(nowdb_ctx_config_t   *cfg,
                      nowdb_bitmap64_t options) {

	if (cfg == NULL) return;

	cfg->sort = 1;
	cfg->encp = NOWDB_ENCP_NONE;

	if (options & NOWDB_CONFIG_SIZE_TINY) {

		cfg->allocsize = NOWDB_MEGA;
		cfg->largesize = NOWDB_MEGA;
		cfg->sorters   = 1;
		cfg->comp      = NOWDB_COMP_FLAT;

	} else if (options & NOWDB_CONFIG_SIZE_SMALL) {

		cfg->allocsize = NOWDB_MEGA;
		cfg->largesize = 8 * NOWDB_MEGA;
		cfg->sorters   = 1;
		cfg->comp      = NOWDB_COMP_ZSTD;

	} else if (options & NOWDB_CONFIG_SIZE_MEDIUM) {

		cfg->allocsize = 8 * NOWDB_MEGA;
		cfg->largesize = 64 * NOWDB_MEGA;
		cfg->sorters   = 2;
		cfg->comp      = NOWDB_COMP_ZSTD;

	} else if (options & NOWDB_CONFIG_SIZE_BIG) {

		cfg->allocsize = 8 * NOWDB_MEGA;
		cfg->largesize = 128 * NOWDB_MEGA;
		cfg->sorters   = 4;
		cfg->comp      = NOWDB_COMP_ZSTD;

	} else if (options & NOWDB_CONFIG_SIZE_LARGE) {

		cfg->allocsize = 8 * NOWDB_MEGA;
		cfg->largesize = 256 * NOWDB_MEGA;
		cfg->sorters   = 6;
		cfg->comp      = NOWDB_COMP_ZSTD;

	} else if (options & NOWDB_CONFIG_SIZE_HUGE) {

		cfg->allocsize = 8 * NOWDB_MEGA;
		cfg->largesize = 1024 * NOWDB_MEGA;
		cfg->sorters   = 8;
		cfg->comp      = NOWDB_COMP_ZSTD;
	}
	if (options & NOWDB_CONFIG_INSERT_MODERATE) {

		if (cfg->sorters < 2) cfg->sorters = 2;

	} else if (options & NOWDB_CONFIG_INSERT_CONSTANT) {

		cfg->sorters += 2;

	} else if (options & NOWDB_CONFIG_INSERT_INSANE) {

		cfg->sorters *= 2;
		if (cfg->allocsize < 8) cfg->allocsize = 8;
		cfg->largesize = 1024;
	}
	if (options & NOWDB_CONFIG_DISK_HDD) {
		if (cfg->largesize < 128 * NOWDB_MEGA) {
			cfg->largesize = 128 * NOWDB_MEGA;
		}
	}
	if (options & NOWDB_CONFIG_NOCOMP) {
		cfg->comp = NOWDB_COMP_FLAT;
	}
	if (options & NOWDB_CONFIG_NOSORT) {
		cfg->sort = 0;
	}
}

