/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Index Manager
 * ========================================================================
 */

#include <nowdb/index/man.h>

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>

static char *OBJECT = "idxman";

#define DESC(x) \
	((nowdb_index_desc_t*)x)

/* ------------------------------------------------------------------------
 * Tree callbacks: compare by name
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t comparebyname(void *ignore, void *left, void *right) {
	int cmp = strcmp(DESC(left)->name,
	                 DESC(right)->name);
	if (cmp < 0) return ts_algo_cmp_less;
	if (cmp > 0) return ts_algo_cmp_greater;
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * Helper: compare offset by offset
 * ------------------------------------------------------------------------
 */
static inline ts_algo_cmp_t comparekeys(nowdb_index_keys_t *left,
                          nowdb_index_keys_t *right) 
{
	for(int i=0; i<left->sz; i++) {
		if (left->off[i] < right->off[i]) return ts_algo_cmp_less;
		if (left->off[i] > right->off[i]) return ts_algo_cmp_greater;
	}
	return ts_algo_cmp_equal;
}

/* ------------------------------------------------------------------------
 * Tree callbacks: compare by key
 * ------------------------------------------------------------------------
 */
static ts_algo_cmp_t comparebykey(void *ignore, void *left, void *right) {
	if (DESC(left)->ctx < DESC(right)->ctx) return ts_algo_cmp_less;
	if (DESC(left)->ctx > DESC(right)->ctx) return ts_algo_cmp_greater;
	if (DESC(left)->keys->sz < 
	    DESC(right)->keys->sz) return ts_algo_cmp_less;
	if (DESC(left)->keys->sz > 
	    DESC(right)->keys->sz) return ts_algo_cmp_greater;
	return comparekeys(DESC(left)->keys, DESC(right)->keys);
}

/* ------------------------------------------------------------------------
 * Tree callbacks for index desc: update (do nothing!)
 * ------------------------------------------------------------------------
 */
static ts_algo_rc_t noupdate(void *ignore, void *o, void *n) {
	return TS_ALGO_OK;
}

/* ------------------------------------------------------------------------
 * Tree callbacks for index desc: delete and destroy
 * ------------------------------------------------------------------------
 */
static void destroydesc(void *ignore, void **n) {
	if (*n != NULL) {
		nowdb_index_desc_destroy((nowdb_index_desc_t*)(*n));
		free(*n); *n = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Tree callbacks for index desc: don't destroy twice!
 * ------------------------------------------------------------------------
 */
static void nodestroy(void *ignore, void **n) {}

/* ------------------------------------------------------------------------
 * Init index manager
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_man_init(nowdb_index_man_t *iman,
                                 nowdb_index_cat_t *icat) {
	nowdb_err_t err;

	if (iman == NULL) return nowdb_err_get(nowdb_err_invalid,
	                    FALSE, OBJECT, "index manager NULL");
	if (icat == NULL) return nowdb_err_get(nowdb_err_invalid,
	                    FALSE, OBJECT, "index catalog NULL");
	iman->icat = NULL;
	iman->byname = NULL;
	iman->bykey  = NULL;

	err = nowdb_rwlock_init(&iman->lock);
	if (err != NOWDB_OK) return err;

	iman->byname = ts_algo_tree_new(&comparebyname,
                                        NULL, &noupdate,
	                                &destroydesc,
	                                &destroydesc);
	if (iman->byname == NULL) {
		nowdb_index_man_destroy(iman);
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                       "byname.new");
	}
	iman->bykey = ts_algo_tree_new(&comparebykey,
                                       NULL, &noupdate,
	                               &nodestroy,
	                               &nodestroy);
	if (iman->bykey == NULL) {
		nowdb_index_man_destroy(iman);
		return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                        "bykey.new");
	}
	iman->icat = icat;

	/* missing: set byname and bykey from icat */

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Destroy index manager
 * ------------------------------------------------------------------------
 */
void nowdb_index_man_destroy(nowdb_index_man_t *iman) {
	if (iman == NULL) return;
	if (iman->bykey != NULL) {
		ts_algo_tree_destroy(iman->bykey);
		free(iman->bykey); iman->bykey = NULL;
	}
	if (iman->byname != NULL) {
		ts_algo_tree_destroy(iman->byname);
		free(iman->byname); iman->byname = NULL;
	}
	if (iman->icat != NULL) {
		nowdb_index_cat_close(iman->icat);
		free(iman->icat); iman->icat= NULL;
	}
	nowdb_rwlock_destroy(&iman->lock);
}

/* ------------------------------------------------------------------------
 * Register an index
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_man_register(nowdb_index_man_t  *iman,
                                     char               *name,
                                     nowdb_context_t    *ctx,
                                     nowdb_index_keys_t *keys,
                                     nowdb_index_t      *idx) {
	nowdb_err_t err = NOWDB_OK, err2;
	nowdb_index_desc_t *desc, *tmp;
	size_t s;

	if (iman == NULL) return nowdb_err_get(nowdb_err_invalid,
	                    FALSE, OBJECT, "index manager NULL");
	if (name == NULL) return nowdb_err_get(nowdb_err_invalid,
	                             FALSE, OBJECT, "name NULL");
	if (keys == NULL) return nowdb_err_get(nowdb_err_invalid,
	                             FALSE, OBJECT, "keys NULL");

	s = strnlen(name, 4097);
	if (s > 4096) return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
	                                 "index name too long (max: 4096)");

	desc = calloc(1, sizeof(nowdb_index_desc_t));
	if (desc == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                      FALSE, OBJECT, "allocating desc");
	desc->name = strdup(name);
	if (desc->name == NULL) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
	                                   "allocating index name");
		free(desc); return err;
	}
	desc->ctx = ctx;
	desc->keys = keys;
	desc->idx = idx;

	/* lock ! */
	err = nowdb_lock_write(&iman->lock);
	if (err != NOWDB_OK) return err;

	tmp = ts_algo_tree_find(iman->byname, desc);
	if (tmp != NULL) {
		free(desc);
		err = nowdb_err_get(nowdb_err_dup_key, FALSE, OBJECT,
		                                       "index name");
		goto unlock;
	}
	tmp = ts_algo_tree_find(iman->bykey, desc);
	if (tmp != NULL) {
		free(desc);
		err = nowdb_err_get(nowdb_err_dup_key, FALSE, OBJECT,
		                                       "index keys");
		goto unlock;
	}
	if (ts_algo_tree_insert(iman->byname, desc) != TS_ALGO_OK) {
		free(desc);
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                    "byname.insert");
		goto unlock;
	}
	if (ts_algo_tree_insert(iman->bykey, desc) != TS_ALGO_OK) {
		err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
		                                     "bykey.insert");
		ts_algo_tree_delete(iman->byname, desc);
		goto unlock;
	}
	err = nowdb_index_cat_add(iman->icat, desc);
unlock:
	err2 = nowdb_unlock_write(&iman->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * unregister an index
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_man_unregister(nowdb_index_man_t *iman,
                                       char              *name) {
	nowdb_err_t err = NOWDB_OK, err2;
	nowdb_index_desc_t *desc, tmp;

	if (iman == NULL) return nowdb_err_get(nowdb_err_invalid,
	                    FALSE, OBJECT, "index manager NULL");
	if (name == NULL) return nowdb_err_get(nowdb_err_invalid,
	                             FALSE, OBJECT, "name NULL");

	err = nowdb_lock_write(&iman->lock);
	if (err != NOWDB_OK) return err;

	tmp.name = name;
	desc = ts_algo_tree_find(iman->byname, &tmp);
	if (desc != NULL) {
		err = nowdb_err_get(nowdb_err_key_not_found,
		                     FALSE, OBJECT, "name");
		goto unlock;
	}
	ts_algo_tree_delete(iman->bykey, desc);
	ts_algo_tree_delete(iman->byname, &tmp);

	err = nowdb_index_cat_remove(iman->icat, name);
unlock:
	err2 = nowdb_unlock_write(&iman->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * get index by name
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_man_getByName(nowdb_index_man_t *iman,
                                      char              *name,
                                      nowdb_index_t    **idx) {
	nowdb_err_t err = NOWDB_OK, err2;
	nowdb_index_desc_t *desc, tmp;

	if (iman == NULL) return nowdb_err_get(nowdb_err_invalid,
	                    FALSE, OBJECT, "index manager NULL");
	if (name == NULL) return nowdb_err_get(nowdb_err_invalid,
	                             FALSE, OBJECT, "name NULL");
	if (idx == NULL) return nowdb_err_get(nowdb_err_invalid,
	                 FALSE, OBJECT, "index pointer is NULL");

	err = nowdb_lock_read(&iman->lock);
	if (err != NOWDB_OK) return err;

	tmp.name = name;

	desc = ts_algo_tree_find(iman->byname, &tmp);
	if (desc == NULL) {
		err = nowdb_err_get(nowdb_err_key_not_found,
		                     FALSE, OBJECT, "name");
		goto unlock;
	}
	*idx = desc->idx;
unlock:
	err2 = nowdb_unlock_read(&iman->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * get index by key
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_index_man_getByKeys(nowdb_index_man_t  *iman,
                                      nowdb_context_t    *ctx,
                                      nowdb_index_keys_t *keys,
                                      nowdb_index_t     **idx) {
	nowdb_err_t err = NOWDB_OK, err2;
	nowdb_index_desc_t *desc, tmp;

	if (iman == NULL) return nowdb_err_get(nowdb_err_invalid,
	                    FALSE, OBJECT, "index manager NULL");
	if (keys == NULL) return nowdb_err_get(nowdb_err_invalid,
	                             FALSE, OBJECT, "keys NULL");
	if (idx == NULL) return nowdb_err_get(nowdb_err_invalid,
	                 FALSE, OBJECT, "index pointer is NULL");

	err = nowdb_lock_read(&iman->lock);
	if (err != NOWDB_OK) return err;

	tmp.ctx = ctx;
	tmp.keys = keys;

	desc = ts_algo_tree_find(iman->bykey, &tmp);
	if (desc == NULL) {
		err = nowdb_err_get(nowdb_err_key_not_found,
		                     FALSE, OBJECT, "keys");
		goto unlock;
	}
	*idx = desc->idx;
unlock:
	err2 = nowdb_unlock_read(&iman->lock);
	if (err2 != NOWDB_OK) {
		err2->cause = err; return err2;
	}
	return err;
}
