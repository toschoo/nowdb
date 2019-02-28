/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Storage: storage parameters and store workers
 * ========================================================================
 *
 * ========================================================================
 */
#ifndef nowdb_storage_decl
#define nowdb_storage_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/io/file.h>
#include <nowdb/task/lock.h>
#include <nowdb/task/worker.h>

typedef struct {
	nowdb_lock_t          lock; // protect storage resources
	char                 *name; // storage name
	uint32_t          filesize; // size of new files
	uint32_t         largesize; // size of new readers
	char                  sort; // sort files
	nowdb_comp_t          comp; // compression
	nowdb_encp_t          encp; // encryption
	uint32_t           tasknum; // number of sorter tasks
	nowdb_worker_t     syncwrk; // background sync
	nowdb_worker_t     sortwrk; // background sorter
	ts_algo_list_t      stores; // managed by this storage
	char               started; // storage was started
} nowdb_storage_t;

/* -----------------------------------------------------------------------
 * Storage configurator
 * -----------------------------------------------------------------------
 */
typedef struct {
	uint32_t  filesize;
	uint32_t largesize;
	uint32_t   sorters;
	nowdb_bool_t  sort;
	nowdb_comp_t  comp;
	nowdb_encp_t  encp;
} nowdb_storage_config_t;

nowdb_err_t nowdb_storage_new(nowdb_storage_t **strg,
                              char             *name,
                              nowdb_storage_config_t *cfg);

nowdb_err_t nowdb_storage_init(nowdb_storage_t *strg,
                               char            *name,
                               nowdb_storage_config_t *cfg);

void nowdb_storage_destroy(nowdb_storage_t *strg);

nowdb_err_t nowdb_storage_addStore(nowdb_storage_t *s,
                                   void        *store);

nowdb_err_t nowdb_storage_start(nowdb_storage_t *strg);
nowdb_err_t nowdb_storage_stop(nowdb_storage_t *strg);

/* -----------------------------------------------------------------------
 * Predefine configurations
 * ------------------------
 * as combinations of SIZE and INSERT values.
 * -----------------------------------------------------------------------
 */
void nowdb_storage_config(nowdb_storage_config_t *cfg,
                          nowdb_bitmap64_t    options);

/* -----------------------------------------------------------------------
 * SIZING
 * -----------------------------------------------------------------------
 */
#define NOWDB_CONFIG_SIZE_TINY    1
#define NOWDB_CONFIG_SIZE_SMALL   2
#define NOWDB_CONFIG_SIZE_MEDIUM  4
#define NOWDB_CONFIG_SIZE_BIG     8
#define NOWDB_CONFIG_SIZE_LARGE  16
#define NOWDB_CONFIG_SIZE_HUGE   32

/* -----------------------------------------------------------------------
 * Context INSERT pattern
 * -----------------------------------------------------------------------
 */
#define NOWDB_CONFIG_INSERT_MODERATE 128 
#define NOWDB_CONFIG_INSERT_CONSTANT 256
#define NOWDB_CONFIG_INSERT_INSANE   512  

/* -----------------------------------------------------------------------
 * Disk Type
 * -----------------------------------------------------------------------
 */
#define NOWDB_CONFIG_DISK_SSD  1024
#define NOWDB_CONFIG_DISK_HDD  2048
#define NOWDB_CONFIG_DISK_RAID 4096

/* -----------------------------------------------------------------------
 * Miscellaneous options
 * -----------------------------------------------------------------------
 */
#define NOWDB_CONFIG_NOCOMP 1048576
#define NOWDB_CONFIG_NOSORT 2097152

#endif
