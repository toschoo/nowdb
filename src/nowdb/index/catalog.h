/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Index catalogue
 * ========================================================================
 */
#ifndef nowdb_index_cat_decl
#define nowdb_index_cat_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/scope/context.h>
#include <nowdb/index/index.h>

#include <beet/index.h>

#include <stdlib.h>
#include <stdint.h>

typedef struct {
	uint32_t sz;
	char   *buf;
} nowdb_index_buf_t;

typedef struct {
	char          *path;
	FILE          *file;
	ts_algo_tree_t *ctx;
	ts_algo_tree_t *buf;
} nowdb_index_cat_t;

nowdb_err_t nowdb_index_cat_open(char *path,
                                 ts_algo_tree_t     *ctx,
                                 nowdb_index_cat_t **icat);

void nowdb_index_cat_close(nowdb_index_cat_t *icat);

nowdb_err_t nowdb_index_cat_add(nowdb_index_cat_t  *icat,
                                nowdb_index_desc_t *desc);

nowdb_err_t nowdb_index_cat_remove(nowdb_index_cat_t *icat,
                                   char             *name);
#endif

