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

#include <stdint.h>

typedef struct {
	uint32_t       sz; /* size of the array     */
	uint32_t      lst; /* used in init          */
	nowdb_fun_t **fun; /* array of fun pointers */
} nowdb_group_t;

/* -----------------------------------------------------------------------
 * Init already allocated group 
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_group_init(nowdb_group_t *group,
                             uint32_t          sz);

/* -----------------------------------------------------------------------
 * Allocated and initialise new group
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_group_new(nowdb_group_t **group,
                            uint32_t           sz);

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
 * Reset
 * -----------------------------------------------------------------------
 */
void nowdb_group_reset(nowdb_group_t *group);

/* -----------------------------------------------------------------------
 * Apply
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_group_apply(nowdb_group_t *group,
                              nowdb_content_t type,
                              char         *record);

/* -----------------------------------------------------------------------
 * Write values into record
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_group_results(nowdb_group_t *group,
                                nowdb_content_t type,
                                char         *record);
#endif
