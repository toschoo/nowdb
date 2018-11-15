/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Processing aggregates in a group
 * ========================================================================
 */
#ifndef nowdb_group_decl
#define nowdb_group_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/index/index.h>
#include <nowdb/fun/fun.h>
#include <nowdb/fun/expr.h>

#include <stdint.h>

#include <tsalgo/list.h>

typedef struct {
	uint32_t       sz; /* size of the array       */
	uint32_t      lst; /* used in init            */
	nowdb_fun_t **fun; /* array of fun pointers   */
	nowdb_eval_t *hlp; /* evaluation helper       */
	char       mapped; /* we map before we reduce */
	char      reduced; /* we reduce only once     */
} nowdb_group_t;

/* -----------------------------------------------------------------------
 * Init already allocated group 
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_group_init(nowdb_group_t *group,
                             uint32_t          sz);

/* -----------------------------------------------------------------------
 * Allocate and initialise new group
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_group_new(nowdb_group_t **group,
                            uint32_t           sz);

/* -----------------------------------------------------------------------
 * Allocate and initialise from list of fields
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_group_fromList(nowdb_group_t  **group,
                                 ts_algo_list_t *fields);

/* -----------------------------------------------------------------------
 * Add fun to group
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_group_add(nowdb_group_t *group, nowdb_fun_t *fun);

/* -----------------------------------------------------------------------
 * Destroy
 * -----------------------------------------------------------------------
 */
void nowdb_group_destroy(nowdb_group_t *group);

/* -----------------------------------------------------------------------
 * Set evaluation helper
 * -----------------------------------------------------------------------
 */
void nowdb_group_setEval(nowdb_group_t *group, nowdb_eval_t *hlp);

/* -----------------------------------------------------------------------
 * Reset
 * -----------------------------------------------------------------------
 */
void nowdb_group_reset(nowdb_group_t *group);

/* -----------------------------------------------------------------------
 * Map
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_group_map(nowdb_group_t *group,
                            nowdb_content_t type,
                            char         *record);

/* -----------------------------------------------------------------------
 * Reduce
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_group_reduce(nowdb_group_t *group,
                               nowdb_content_t type);

/* -----------------------------------------------------------------------
 * Write value of nth fun into row
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_group_result(nowdb_group_t *group,
                               int n,    char *row);

/* -----------------------------------------------------------------------
 * Get type of nth fun
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_group_getType(nowdb_group_t   *group,
                                int n, nowdb_type_t *t);
#endif
