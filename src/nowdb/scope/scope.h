/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Scope: a collection of contexts
 * ========================================================================
 *
 * ========================================================================
 */
#ifndef nowdb_scope_decl
#define nowdb_scope_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/task/lock.h>
#include <nowdb/io/dir.h>
#include <nowdb/store/store.h>

#include <tsalgo/tree.h>

typedef struct {
	char          *name;
	nowdb_store_t store;
} nowdb_context_t;

typedef struct {
	nowdb_rwlock_t lock;     /* read/write lock */
	char          *name;     /* base path       */
	nowdb_path_t   path;     /* base path       */
	nowdb_store_t vertices;  /* vertices        */
	ts_algo_tree_t contexts; /* contexts        */
	ts_algo_tree_t indices;  /* indices         */
	                         /* model           */
	                         /* strings         */
} nowdb_scope_t;

#endif

