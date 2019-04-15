/* ========================================================================
 * (c) Tobias Schoofs, 2018 -- 2019
 * ========================================================================
 * IPC: inter-process communication (i.e.: process=session)
 * Currently contains:
 * - read/write locks
 * - queues
 * ========================================================================
 */
#ifndef nowdb_ipc_decl
#define nowdb_ipc_decl

#include <nowdb/types/error.h>
#include <nowdb/types/types.h>
#include <nowdb/task/lock.h>

#include <tsalgo/tree.h>

#define NOWDB_IPC_FREE  0
#define NOWDB_IPC_RLOCK 1
#define NOWDB_IPC_WLOCK 2

/* ------------------------------------------------------------------------
 * Lock
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_lock_t lock; // protect this structure
 	uint64_t    owner; // session id
	char        *name; // name of this lock
	char        state; // state free -> rlock|wlock -> free
                           // do we need to guarantee order?
} nowdb_ipc_lock_t;

/* ------------------------------------------------------------------------
 * IPC Manager
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_lock_t    lock;  // protect this structure
	char           *path;  // path to ipc catalogue
	ts_algo_tree_t *locks; // tree of locks
} nowdb_ipc_t;

/* ------------------------------------------------------------------------
 * Initialise IPC Manager
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_ipc_init(nowdb_ipc_t *ipc, char *path);

/* ------------------------------------------------------------------------
 * Load IPC Catalogue
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_ipc_load(nowdb_ipc_t *ipc);

/* ------------------------------------------------------------------------
 * Destroy IPC Manager
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_ipc_destroy(nowdb_ipc_t *ipc);

/* ------------------------------------------------------------------------
 * Create lock
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_ipc_lock_create(nowdb_ipc_t *ipc, char *name);

/* ------------------------------------------------------------------------
 * Destroy Lock
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_ipc_lock_drop(nowdb_ipc_t *ipc, char *name);

/* ------------------------------------------------------------------------
 * Lock
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_ipc_lock(nowdb_ipc_t *ipc, char mode);

/* ------------------------------------------------------------------------
 * Unlock
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_ipc_unlock(nowdb_ipc_t *ipc);

#endif

