/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Store: a collection of files
 * ========================================================================
 *
 * ========================================================================
 */
#ifndef nowdb_store_decl
#define nowdb_store_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/io/file.h>
#include <nowdb/io/dir.h>
#include <nowdb/task/lock.h>
#include <nowdb/task/worker.h>

#include <tsalgo/list.h>
#include <tsalgo/tree.h>

/* does not belong here */
typedef char (*nowdb_compare_t)(void*, void*, void*);

typedef struct {
	nowdb_rwlock_t     lock; /* read/write lock             */
	nowdb_version_t version; /* database version            */
	uint32_t        recsize; /* size of record stored       */
	uint32_t       filesize; /* size of new files           */
	nowdb_path_t       path; /* base path                   */
	nowdb_path_t    catalog; /* path to catalog             */
	nowdb_file_t    *writer; /* where we currently write to */
	ts_algo_list_t   spares; /* available spares            */
	ts_algo_list_t  waiting; /* unprepard readers           */
	ts_algo_tree_t  readers; /* collection of readers       */
	nowdb_fileid_t   nextid; /* next free fileid            */
	                         /* compression                 */
	nowdb_compare_t compare; /* comparison                  */
	nowdb_worker_t  syncwrk; /* background sync             */
	nowdb_worker_t  sortwrk; /* background sorter           */
	nowdb_bool_t   starting; /* set during startup          */
} nowdb_store_t;

/* ------------------------------------------------------------------------
 * Allocate and initialise new store object
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_new(nowdb_store_t **store,
                            nowdb_path_t     base,
                            nowdb_version_t   ver,
                            uint32_t      recsize,
                            uint32_t     filesize);

/* ------------------------------------------------------------------------
 * Initialise already allocated store object
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_init(nowdb_store_t  *store,
                             nowdb_path_t     base,
                             nowdb_version_t   ver,
                             uint32_t      recsize,
                             uint32_t     filesize);

/* ------------------------------------------------------------------------
 * Destroy store
 * ------------------------------------------------------------------------
 */
void nowdb_store_destroy(nowdb_store_t *store);

/* ------------------------------------------------------------------------
 * Open store
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_open(nowdb_store_t *store);

/* ------------------------------------------------------------------------
 * Close store
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_close(nowdb_store_t *store);

/* ------------------------------------------------------------------------
 * Create store physically on disk
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_create(nowdb_store_t *store);

/* ------------------------------------------------------------------------
 * Remove store physically from disk
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_drop(nowdb_store_t *store);

/* ------------------------------------------------------------------------
 * Insert one record
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_insert(nowdb_store_t *store,
                               void          *data);

/* ------------------------------------------------------------------------
 * Insert n records
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_insertBulk(nowdb_store_t *store,
                                   void           *data,
                                   uint32_t       count);

/* ------------------------------------------------------------------------
 * Get all readers for period start - end
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_getFiles(nowdb_store_t  *store,
                                 ts_algo_list_t *files,
                                 nowdb_time_t    start,
                                 nowdb_time_t     end);

/* ------------------------------------------------------------------------
 * Destroy files and list
 * ------------------------------------------------------------------------
 */
void nowdb_store_destroyFiles(ts_algo_list_t *files);

/* ------------------------------------------------------------------------
 * Find file in waiting
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_findWaiting(nowdb_store_t *store,
                                    nowdb_file_t  *file,
                                    nowdb_bool_t  *found);

/* ------------------------------------------------------------------------
 * Find file in spares
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_findSpare(nowdb_store_t *store,
                                  nowdb_file_t  *file,
                                  nowdb_bool_t  *found); 

/* ------------------------------------------------------------------------
 * Find file in readers
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_findReader(nowdb_store_t *store,
                                   nowdb_file_t  *file,
                                   nowdb_bool_t  *found);

/* ------------------------------------------------------------------------
 * Find reader with capacity to store more
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_getFreeReader(nowdb_store_t *store,
                                      nowdb_file_t **file);

/* ------------------------------------------------------------------------
 * Create new reader
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_createReader(nowdb_store_t *store,
                                     nowdb_file_t  **file);

/* ------------------------------------------------------------------------
 * Get waiting
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_getWaiting(nowdb_store_t *store,
                                   nowdb_file_t **file);

/* ------------------------------------------------------------------------
 * Donate empty file to spares
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_donate(nowdb_store_t *store, nowdb_file_t *file);

/* ------------------------------------------------------------------------
 * Promote
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_promote(nowdb_store_t  *store,
                                 nowdb_file_t *waiting,
                                 nowdb_file_t  *reader);

/* ------------------------------------------------------------------------
 * Add a file
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_addFile(nowdb_store_t *store,
                                nowdb_file_t  *file);

/* ------------------------------------------------------------------------
 * Remove a file
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_removeFile(nowdb_store_t *store,
                                   nowdb_file_t  *file);

/* ------------------------------------------------------------------------
 * Drop all files containing only data before that timestamp
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_dropFiles(nowdb_store_t *store,
                                  nowdb_time_t   stamp);

/* ------------------------------------------------------------------------
 * For debugging: pretty-print the catalog
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_store_showCatalog(nowdb_store_t *store);

#endif
