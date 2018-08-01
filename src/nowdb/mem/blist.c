/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Block list
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/mem/blist.h>

static char *OBJECT = "blist";

#define INVALID(s) \
	return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT, s);

#define NOMEM(s) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, s);

/* -----------------------------------------------------------------------
 * Init already allocated blist
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_blist_init(nowdb_blist_t *blist, uint32_t sz) {
	if (blist == NULL) INVALID("no blist object");
	blist->sz = 0;
	if (sz == 0) INVALID("block size is 0");
	ts_algo_list_init(&blist->flist);
	blist->sz = sz;
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Helper: destroy list
 * -----------------------------------------------------------------------
 */
static void destroyList(ts_algo_list_t *list) {
	ts_algo_list_node_t *runner, *tmp;
	runner = list->head;
	while(runner != NULL) {
		tmp = runner->nxt;
		if (runner->cont != NULL) {
			free(runner->cont); runner->cont = NULL;
		}
		ts_algo_list_remove(list, runner);
		free(runner); runner = tmp;
	}
}

/* -----------------------------------------------------------------------
 * Helper: add an already existing node to a list
 *         (should go to tsalgo)
 * -----------------------------------------------------------------------
 */
static void addNode(ts_algo_list_t *list, ts_algo_list_node_t *node) {
	if (list->head == NULL) {
		list->head = node;
		list->last = node;
		node->nxt = NULL;
		node->prv = NULL;
		list->len++;
		return;
	}
	node->nxt = list->head;
	node->prv = NULL;
	list->head->prv = node;
	list->len++;
}

/* -----------------------------------------------------------------------
 * Destroy blist
 * -----------------------------------------------------------------------
 */
void nowdb_blist_destroy(nowdb_blist_t *blist) {
	if (blist == NULL) return;
	if (blist->sz == 0) return;
	destroyList(&blist->flist);
	blist->sz = 0;
}

/* -----------------------------------------------------------------------
 * Get block from blist
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_blist_get(nowdb_blist_t  *blist,
                      ts_algo_list_node_t **block) {
	nowdb_err_t err;
	char *tmp;

	if (blist == NULL) INVALID("no blist object");
	if (block == NULL) INVALID("no block pointer");

	if (blist->flist.len == 0) {
		tmp = malloc(blist->sz);
		if (tmp == NULL) {
			NOMEM("allocating block");
			return err;
		}
		if (ts_algo_list_insert(&blist->flist, tmp) != TS_ALGO_OK) {
			NOMEM("list.insert");
			return err;
		}
	}
	*block = blist->flist.head;
	ts_algo_list_remove(&blist->flist, blist->flist.head);
	return NOWDB_OK;
	
}

/* -----------------------------------------------------------------------
 * Get block from blist and add it to my list
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_blist_give(nowdb_blist_t   *blist,
                             ts_algo_list_t *blocks) {
	nowdb_err_t err;
	ts_algo_list_node_t *tmp;

	err = nowdb_blist_get(blist, &tmp);
	if (err != NOWDB_OK) return err;

	addNode(blocks, tmp);

	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Free block
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_blist_free(nowdb_blist_t *blist,
                       ts_algo_list_node_t *block) {

	if (blist == NULL) INVALID("no blist object");
	if (block == NULL) INVALID("no block pointer");

	addNode(&blist->flist, block);

	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Get block from my blist and add it to free list
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_blist_take(nowdb_blist_t   *blist,
                             ts_algo_list_t *blocks) {
	ts_algo_list_node_t *tmp;

	if (blist == NULL) INVALID("no blist object");
	if (blocks == NULL) INVALID("no block list");
	if (blocks->len == 0) INVALID("block list is empty");

	tmp = blocks->head;
	ts_algo_list_remove(blocks, tmp);
	addNode(&blist->flist, tmp);

	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Free all blocks
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_blist_freeAll(nowdb_blist_t *blist,
                               ts_algo_list_t *blocks) {

	ts_algo_list_node_t *runner, *tmp;

	if (blist == NULL) INVALID("no blist object");
	if (blocks == NULL) INVALID("no block list");

	runner = blocks->head;
	while(runner != NULL) {
		tmp = runner->nxt;
		ts_algo_list_remove(blocks, runner);
		addNode(&blist->flist, runner);
		runner = tmp;
	}
	return NOWDB_OK;
}

