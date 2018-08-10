/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Collection of scopes + threadpool
 * ========================================================================
 */
#ifndef nowdb_main_decl
#define nowdb_main_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>

#include <tsalgo/tree.h>
#include <tsalgo/list.h>

typedef struct {
	nowdb_lock_t   *lock;
	char           *base;
	ts_algo_tree_t *scopes;
	ts_algo_list_t *fthreads;
	ts_algo_list_t *uthreads;
} nowdb_t;

nowdb_err_t nowdb_library_init(nowdb_t **lib);
void nowdb_library_close(nowdb_t *lib);

#endif
