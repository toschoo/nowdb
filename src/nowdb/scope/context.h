/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Context is just a wrapper around store
 * ========================================================================
 *
 * ========================================================================
 */
#ifndef nowdb_context_decl
#define nowdb_context_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/task/lock.h>
#include <nowdb/io/dir.h>
#include <nowdb/store/store.h>

/* -----------------------------------------------------------------------
 * Context
 * -----------------------------------------------------------------------
 */
typedef struct {
	char          *name;
	nowdb_store_t store;
} nowdb_context_t;

/* ------------------------------------------------------------------------
 * Destroy context
 * ------------------------------------------------------------------------
 */
void nowdb_context_destroy(nowdb_context_t *ctx);

/* ------------------------------------------------------------------------
 * Wrap error into context error
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_context_err(nowdb_context_t *ctx, nowdb_err_t err);

/* ------------------------------------------------------------------------
 * Insert one record
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_context_insert(nowdb_context_t *ctx,
                                 void          *data);

/* ------------------------------------------------------------------------
 * Insert n records
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_context_insertBulk(nowdb_context_t *ctx,
                                     void           *data,
                                     uint32_t      count);

/* -----------------------------------------------------------------------
 * Context configurator
 * -----------------------------------------------------------------------
 */
typedef struct {
	uint32_t allocsize;
	uint32_t largesize;
	uint32_t   sorters;
	nowdb_bool_t  sort;
	nowdb_comp_t  comp;
	nowdb_encp_t  encp;
} nowdb_ctx_config_t;

/* -----------------------------------------------------------------------
 * Predefine configurations
 * ------------------------
 * as combinations of SIZE and INSERT values.
 * -----------------------------------------------------------------------
 */
void nowdb_ctx_config(nowdb_ctx_config_t   *cfg,
                      nowdb_bitmap64_t options);

/* -----------------------------------------------------------------------
 * Context SIZE 
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
