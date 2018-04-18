/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Directories
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

typedef char* nowdb_path_t;

typedef nowdb_bitmap8_t nowdb_dir_type_t;

#define NOWDB_DIR_TYPE_ANY NOWDB_BITMAP8_ALL
#define NOWDB_DIR_TYPE_FILE  1
#define NOWDB_DIR_TYPE_DIR   2
#define NOWDB_DIR_TYPE_SYM   4
#define NOWDB_DIR_TYPE_OTHER 8

#define NOWDB_FILE_MODE S_IRWXU

nowdb_path_t nowdb_path_append(nowdb_path_t  base,
                               nowdb_path_t toadd);

nowdb_path_t nowdb_path_filename(nowdb_path_t path);

nowdb_bool_t nowdb_path_exists(nowdb_path_t  path,
                               nowdb_dir_type_t t);

nowdb_err_t nowdb_dir_create(nowdb_path_t path);
nowdb_err_t nowdb_path_remove(nowdb_path_t path);
nowdb_err_t nowdb_path_move(nowdb_path_t src,
                            nowdb_path_t trg);

typedef struct {
	nowdb_path_t  path;
	nowdb_dir_type_t t;
} nowdb_dir_ent_t;

nowdb_err_t nowdb_dir_content(nowdb_path_t path,
                          nowdb_dir_type_t    t,
                          ts_algo_list_t *list);

void nowdb_dir_content_destroy(ts_algo_list_t *list);


#endif

