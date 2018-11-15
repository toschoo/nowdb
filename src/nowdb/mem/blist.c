/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Block list
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/mem/blist.h>

#include <stdio.h>

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
	nowdb_block_t *block;
	runner = list->head;
	while(runner != NULL) {
		tmp = runner->nxt;
		if (runner->cont != NULL) {
			block = runner->cont;
			free(block->block);
			free(block); runner->cont = NULL;
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
static void addNode(ts_algo_list_t      *list,
                    ts_algo_list_node_t *node,
                    char                where) {
	if (list->head == NULL) {
		list->head = node;
		list->last = node;
		node->nxt = NULL;
		node->prv = NULL;
		list->len++;
		return;
	}
	if (where == 0) {
		node->nxt = list->head;
		node->prv = NULL;
		list->head->prv = node;
		list->head = node;
	} else {
		node->nxt = NULL;
		list->last->nxt = node;
		node->prv = list->last;
		list->last = node;
	}
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

/* ------------------------------------------------------------------------
 * Destroy block list using blist free list
 * ------------------------------------------------------------------------
 */
void nowdb_blist_destroyBlockList(ts_algo_list_t *list,
                                  nowdb_blist_t *flist) {
	ts_algo_list_node_t *tmp;
	nowdb_err_t err;
	nowdb_block_t *b;

	if (list == NULL) return;
	while(list->len > 0) {
		// fprintf(stderr, "len: %d\n", list->len);
		err = nowdb_blist_take(flist, list);
		if (err != NOWDB_OK) {
			nowdb_err_release(err);
			tmp = list->head;
			ts_algo_list_remove(list, tmp);
			b = tmp->cont;
			if (b->block != NULL) {
				free(b->block);
			}
			free(b); free(tmp);
		}
	}
}

/* -----------------------------------------------------------------------
 * Get block from blist
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_blist_get(nowdb_blist_t  *blist,
                      ts_algo_list_node_t **block) {
	nowdb_err_t  err;
	nowdb_block_t *b;

	if (blist == NULL) INVALID("no blist object");
	if (block == NULL) INVALID("no block pointer");

	if (blist->flist.len == 0) {
		b = calloc(1, sizeof(nowdb_block_t));
		if (b == NULL) {
			NOMEM("allocating block");
			return err;
		}
		b->block = malloc(blist->sz);
		if (b->block == NULL) {
			free(b);
			NOMEM("allocating block");
			return err;
		}
		if (ts_algo_list_insert(&blist->flist, b) != TS_ALGO_OK) {
			free(b->block); free(b);
			NOMEM("list.insert");
			return err;
		}
	}
	*block = blist->flist.head;
	ts_algo_list_remove(&blist->flist, blist->flist.head);
	b = (*block)->cont; b->sz = 0;
	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Get block from blist and add it as head to my list
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_blist_give(nowdb_blist_t   *blist,
                             ts_algo_list_t *blocks) {
	nowdb_err_t err;
	ts_algo_list_node_t *tmp=NULL;

	err = nowdb_blist_get(blist, &tmp);
	if (err != NOWDB_OK) return err;

	addNode(blocks, tmp, 0);

	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * Get block from blist and add it as last to my list
 * -----------------------------------------------------------------------
 */
nowdb_err_t nowdb_blist_giva(nowdb_blist_t   *blist,
                             ts_algo_list_t *blocks) {
	nowdb_err_t err;
	ts_algo_list_node_t *tmp=NULL;

	err = nowdb_blist_get(blist, &tmp);
	if (err != NOWDB_OK) return err;

	addNode(blocks, tmp, 1);

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

	addNode(&blist->flist, block, 0);

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
	addNode(&blist->flist, tmp, 0);

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
		addNode(&blist->flist, runner, 0);
		runner = tmp;
	}
	return NOWDB_OK;
}
