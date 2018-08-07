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
	group->mapped = 0;
	group->reduced = 0;

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
 * Allocate and initialise new group
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
 * Allocate and initialise group from list
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_group_fromList(nowdb_group_t  **group,
                                 ts_algo_list_t *fields) 
{
	nowdb_err_t err;
	ts_algo_list_node_t *runner;

	if (fields == NULL) INVALID("fields list is NULL");
	if (fields->len == 0) INVALID("no fields");

	err = nowdb_group_new(group, fields->len);
	if (err != NOWDB_OK) return err;

	for(runner=fields->head; runner!=NULL; runner=runner->nxt) {
		err = nowdb_group_add(*group, runner->cont);
		if (err != NOWDB_OK) {
			nowdb_group_destroy(*group);
			free(*group); return err;
		}
	}
	return NOWDB_OK;
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
		if (group->fun[i] != NULL) {
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
	group->mapped = 0;
	group->reduced= 0;
}

/* -----------------------------------------------------------------------
 * Map
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_group_map(nowdb_group_t *group,
                            nowdb_content_t type,
                            char         *record) {
	nowdb_err_t err;

	for(int i=0; i<group->lst; i++) {
		if (group->fun[i]->ctype == type) {
			err = nowdb_fun_map(group->fun[i], record);
			if (err != NOWDB_OK) return err;
		}
	}
	group->mapped = 1;
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Reduce
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_group_reduce(nowdb_group_t *group,
                               nowdb_content_t type) {
	nowdb_err_t err;

	if (group->reduced) return NOWDB_OK;
	for(int i=0; i<group->lst; i++) {
		if (group->fun[i]->ctype == type) {
			err = nowdb_fun_reduce(group->fun[i]);
			if (err != NOWDB_OK) return err;
		}
	}
	group->reduced = 1;
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Get result type
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_group_getType(nowdb_group_t   *group,
                                int n, nowdb_type_t *t) 
{
	if (n >= group->lst) INVALID("index out of range");
	*t = group->fun[n]->otype;
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Write values into record
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_group_result(nowdb_group_t *group,
                               int n,     char *row) 
{
	if (n >= group->lst) INVALID("index out of range");
	memcpy(row, &group->fun[n]->r1,
		     group->fun[n]->fsize);
	return NOWDB_OK;
}
