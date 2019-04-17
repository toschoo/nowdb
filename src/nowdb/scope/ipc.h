/* ========================================================================
 * (c) Tobias Schoofs, 2018 -- 2019
 * ========================================================================
 * IPC: inter-process communication (i.e.: process=session)
 * Currently contains:
 * - read/write locks
 * - events
 * - queues (queues should be part of the model!)
 * TODO:
 * - locks: should we iplement deadlock detection (easy, but some
 *          overhead on locking...)?
 * ========================================================================
 */
#ifndef nowdb_ipc_decl
#define nowdb_ipc_decl

#include <nowdb/types/error.h>
#include <nowdb/types/types.h>
#include <nowdb/task/lock.h>

#include <tsalgo/tree.h>
#include <tsalgo/list.h>

#include <pthread.h>

#define NOWDB_IPC_RLOCK 1
#define NOWDB_IPC_WLOCK 2

/* ------------------------------------------------------------------------
 * IPC Manager
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_rwlock_t  lock;   // protect this structure
	char           *base;   // path that contains ipc catalogue
	char           *path;   // path to ipc catalogue
	ts_algo_tree_t *things; // tree of ipc operators
	ts_algo_list_t *free;   // list nodes for free
} nowdb_ipc_t;

/* ------------------------------------------------------------------------
 * Allocate and initialise IPC Manager
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_ipc_new(nowdb_ipc_t **ipc, char *path);

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
 * Close IPC Manager
 * ------------------------------------------------------------------------
 */
void nowdb_ipc_close(nowdb_ipc_t *ipc);

/* ------------------------------------------------------------------------
 * Destroy IPC Manager
 * ------------------------------------------------------------------------
 */
void nowdb_ipc_destroy(nowdb_ipc_t *ipc);

/* ------------------------------------------------------------------------
 * Create lock
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_ipc_createLock(nowdb_ipc_t *ipc, char *name);

/* ------------------------------------------------------------------------
 * Destroy Lock
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_ipc_dropLock(nowdb_ipc_t *ipc, char *name);

/* ------------------------------------------------------------------------
 * Lock
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_ipc_lock(nowdb_ipc_t *ipc, char *name,
                           char mode, int tmo);

/* ------------------------------------------------------------------------
 * Unlock
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_ipc_unlock(nowdb_ipc_t *ipc, char *name);

#endif

