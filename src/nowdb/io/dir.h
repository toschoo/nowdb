/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Directories and paths
 * ========================================================================
 *
 * ========================================================================
 */
#ifndef nowdb_dir_decl
#define nowdb_dir_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/types/time.h>

#include <tsalgo/list.h>

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <sys/stat.h>

/* ------------------------------------------------------------------------
 * This is a path
 * ------------------------------------------------------------------------
 */
typedef char* nowdb_path_t;

/* ------------------------------------------------------------------------
 * File type selector
 * ------------------------------------------------------------------------
 */
typedef nowdb_bitmap8_t nowdb_dir_type_t;

#define NOWDB_DIR_TYPE_ANY NOWDB_BITMAP8_ALL
#define NOWDB_DIR_TYPE_FILE  1
#define NOWDB_DIR_TYPE_DIR   2
#define NOWDB_DIR_TYPE_SYM   4
#define NOWDB_DIR_TYPE_OTHER 8

/* ------------------------------------------------------------------------
 * Dirs: owner can read/write/execute
 * ------------------------------------------------------------------------
 */
#define NOWDB_DIR_MODE S_IRWXU

/* ------------------------------------------------------------------------
 * Files: owner can read/write
 * ------------------------------------------------------------------------
 */
#define NOWDB_FILE_MODE S_IRUSR | S_IWUSR

/* ------------------------------------------------------------------------
 * Append a path to another path
 * The result is 'base/toadd'.
 * ------------------------------------------------------------------------
 */
nowdb_path_t nowdb_path_append(nowdb_path_t  base,
                               nowdb_path_t toadd);

/* ------------------------------------------------------------------------
 * Extract filename from a path
 * ------------------------------------------------------------------------
 */
nowdb_path_t nowdb_path_filename(nowdb_path_t path);

/* ------------------------------------------------------------------------
 * File exists and has one of the indicated types
 * ------------------------------------------------------------------------
 */
nowdb_bool_t nowdb_path_exists(nowdb_path_t  path,
                               nowdb_dir_type_t t);

/* ------------------------------------------------------------------------
 * File size of path
 * ------------------------------------------------------------------------
 */
uint32_t nowdb_path_filesize(nowdb_path_t path);

/* ------------------------------------------------------------------------
 * Create directory
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_dir_create(nowdb_path_t path);

/* ------------------------------------------------------------------------
 * Remove file identified by path
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_path_remove(nowdb_path_t path);

/* ------------------------------------------------------------------------
 * Move file 'src' to 'trg'
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_path_move(nowdb_path_t src,
                            nowdb_path_t trg);

/* ------------------------------------------------------------------------
 * Represents an object in a directory
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_path_t  path;
	nowdb_dir_type_t t;
} nowdb_dir_ent_t;

/* ------------------------------------------------------------------------
 * Get the content of a dir
 * adds a nowdb_dir_ent_t object into the list
 * for every object in the dir of one of the indicated types
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_dir_content(nowdb_path_t path,
                          nowdb_dir_type_t    t,
                          ts_algo_list_t *list);

/* ------------------------------------------------------------------------
 * Destroy list filled by nowdb_dir_content
 * ------------------------------------------------------------------------
 */
void nowdb_dir_content_destroy(ts_algo_list_t *list);

/* ------------------------------------------------------------------------
 * Write file with backup
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_writeFileWithBkp(nowdb_path_t base,
                                   nowdb_path_t file,
                           char *buf, uint32_t size);

/* ------------------------------------------------------------------------
 * Read file 
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_readFile(nowdb_path_t file,
                   char *buf, uint32_t size);

#endif

