/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Processing aggregates in a group
 * ========================================================================
 */
#include <nowdb/fun/group.h>

#include <stdint.h>

static char *OBJECT = "group";

#define INVALID(s) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, s);

#define NOMEM(s) \
	return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, s);

/* -----------------------------------------------------------------------
 * Init already allocated group 
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_group_init(nowdb_group_t *group,
                             uint32_t          sz)
{
	nowdb_err_t err;

	if (group == NULL) INVALID("group is NULL");
	group->sz = sz;
	group->lst = 0;

	group->fun = calloc(sz, sizeof(nowdb_fun_t*));
	if (group->fun == NULL) {
		NOMEM("allocating functions");
		return err;
	}
	for(int i=0; i<sz; i++) {
		group->fun[i] = NULL;
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Allocated and initialise new group
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_group_new(nowdb_group_t **group,
                            uint32_t           sz) {
	nowdb_err_t err;

	if (group == NULL) INVALID("group pointer is NULL");
	
	*group = calloc(1, sizeof(nowdb_group_t));
	if (*group == NULL) {
		NOMEM("allocating group");
		return err;
	}
	return nowdb_group_init(*group, sz);
}

/* -----------------------------------------------------------------------
 * Add fun to group
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_group_add(nowdb_group_t *group, nowdb_fun_t *fun) 
{
	if (group == NULL) INVALID("group is NULL");
	if (fun == NULL) INVALID("fun is NULL");
	if (group->lst >= group->sz) INVALID("no more room in group");

	group->fun[group->lst] = fun;
	group->lst++;

	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Destroy
 * -----------------------------------------------------------------------
 */
void nowdb_group_destroy(nowdb_group_t *group) {
	if (group == NULL) return;
	if (group->fun == NULL) return;
	for(int i=0; i<group->lst; i++) {
		if (group->fun[i] == NULL) {
			nowdb_fun_destroy(group->fun[i]);
			free(group->fun[i]);
			group->fun[i] = NULL;
		}
	}
	free(group->fun); group->fun = NULL;
}

/* -----------------------------------------------------------------------
 * Reset
 * -----------------------------------------------------------------------
 */
void nowdb_group_reset(nowdb_group_t *group) {
	for(int i=0; i<group->lst; i++) {
		nowdb_fun_reset(group->fun[i]);
	}
}

/* -----------------------------------------------------------------------
 * Apply
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_group_apply(nowdb_group_t *group,
                              nowdb_content_t type,
                              char         *record) {
	nowdb_err_t err;

	for(int i=0; i<group->lst; i++) {
		if (group->fun[i]->ctype == type) {
			err = nowdb_fun_map(group->fun[i], record);
			if (err != NOWDB_OK) return err;
		}
	}
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Write values into record
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_group_results(nowdb_group_t *group,
                                nowdb_content_t type,
                                char         *record) {
	nowdb_err_t err;

	for(int i=0; i<group->lst; i++) {
		if (group->fun[i]->ctype == type) {
			err = nowdb_fun_reduce(group->fun[i]);
			if (err != NOWDB_OK) return err;

			memcpy(record+group->fun[i]->field,
			             &group->fun[i]->r1,
			              group->fun[i]->fsize);
		}
	}
	return NOWDB_OK;
}
