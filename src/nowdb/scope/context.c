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
	if (ctx->strgname != NULL) {
		free(ctx->strgname); ctx->strgname = NULL;
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
