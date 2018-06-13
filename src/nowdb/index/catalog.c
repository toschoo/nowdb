/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Index catalogue
 * ========================================================================
 */
#include <nowdb/index/catalog.h>

#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <stdint.h>

nowdb_err_t nowdb_index_cat_open(char *path,
                                 ts_algo_tree_t *contexts,
                                 nowdb_index_cat_t **cat);

void nowdb_index_cat_close(nowdb_index_cat_t *cat) {}

nowdb_err_t nowdb_index_cat_add(nowdb_index_cat_t  *cat,
                                nowdb_index_desc_t *desc);

nowdb_err_t nowdb_index_cat_remove(nowdb_index_cat_t *cat,
                                   char            *name);

