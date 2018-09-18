/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Stored Procedure/Function Manager
 * ========================================================================
 */
#ifndef nowdb_stored_decl
#define nowdb_stored_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/io/dir.h>
#include <nowdb/task/lock.h>

#include <tsalgo/tree.h>

#define NOWDB_STORED_PYTHON 1
#define NOWDB_STORED_C      2
#define NOWDB_STORED_LUA    3

#define NOWDB_STORED_PROC   1
#define NOWDB_STORED_FUN    2

/* ------------------------------------------------------------------------
 * Stored Procedure/Function Manager
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_rwlock_t  *lock;
	char            *base;
	char             *cfg;
	ts_algo_tree_t *procs;
} nowdb_procman_t;

/* ------------------------------------------------------------------------
 * Procedure Descriptor
 * ------------------------------------------------------------------------
 */
typedef struct {
	char   *name;
	uint16_t typ;
	uint16_t pos;
	void    *def;
} nowdb_proc_arg_t;

/* ------------------------------------------------------------------------
 * Helper to destroy parameters
 * ------------------------------------------------------------------------
 */
void nowdb_proc_args_destroy(uint16_t argn, nowdb_proc_arg_t *args);

/* ------------------------------------------------------------------------
 * Procedure Descriptor
 * ------------------------------------------------------------------------
 */
typedef struct {
	char             *name; /* procedure / function name    */
	char           *module; /* module from where to load it */
	nowdb_proc_arg_t *args; /* parameters                   */
	uint16_t          argn; /* number of parameters         */
	uint16_t         rtype; /* return type                  */
	char              type; /* procedure / function         */
	char              lang; /* language                     */
} nowdb_proc_desc_t;

void nowdb_proc_desc_destroy(nowdb_proc_desc_t *pd);

nowdb_err_t nowdb_procman_new(nowdb_procman_t **pm,
                              nowdb_path_t    base);

nowdb_err_t nowdb_procman_init(nowdb_procman_t *pm,
                               nowdb_path_t   base);

void nowdb_procman_destroy(nowdb_procman_t *pm);

nowdb_err_t nowdb_procman_load(nowdb_procman_t *pm);

nowdb_err_t nowdb_procman_add(nowdb_procman_t   *pm,
                              nowdb_proc_desc_t *pd);

nowdb_err_t nowdb_procman_remove(nowdb_procman_t *pm, char *name);

nowdb_err_t nowdb_procman_get(nowdb_procman_t   *pm,
                              char              *name,
                              nowdb_proc_desc_t **pd);

nowdb_err_t nowdb_procman_all(nowdb_procman_t *pm,
                              ts_algo_list_t  *plist);
#endif
