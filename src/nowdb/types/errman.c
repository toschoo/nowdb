/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Error Memory Manager
 * ========================================================================
 */
#include <tsalgo/list.h>
#include <nowdb/types/error.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

/* ------------------------------------------------------------------------
 * Error Descriptors are allocated block-wise
 * -----------------
 * and held in a list; each block has a bitmap
 * that indicates which blocks are still available.
 * ------------------------------------------------------------------------
 */
typedef struct {
	uint64_t         bitmap;
	nowdb_errdesc_t buf[64];
} nowdb_errman_block_t;

/* ------------------------------------------------------------------------
 * Global static variables to manage blocks of error descriptors:
 * - lock: a simple mutex
 * - errors: a list of blocks
 * ------------------------------------------------------------------------
 */
static pthread_mutex_t lock;
static ts_algo_list_t errors;

/* ------------------------------------------------------------------------
 * initialise a new block
 * ------------------------------------------------------------------------
 */
static void initblock(nowdb_errman_block_t *block) {
	block->bitmap = 0;
	memset(block->buf, 0, 64);
}

/* ------------------------------------------------------------------------
 * allocate a new block
 * ------------------------------------------------------------------------
 */
static nowdb_bool_t newblock() {
	nowdb_errman_block_t *block;
	block = malloc(sizeof(nowdb_errman_block_t));
	if (block == NULL) return FALSE;
	initblock(block);
	if (ts_algo_list_append(&errors, block) != TS_ALGO_OK) {
		free(block); return FALSE;
	}
	return TRUE;
}

/* ------------------------------------------------------------------------
 * find a free slot
 * ------------------------------------------------------------------------
 */
static nowdb_err_t findslot(nowdb_errman_block_t *block) {
	uint64_t k=1;
	if (popcount64(block->bitmap) == 64) return NULL;
	for(int i=0; i<64; i++) {
		if (!(block->bitmap & k)) {
			block->bitmap |= k;
			return block->buf+i;
		}
		k <<= 1;
	}
	return NULL;
}

/* ------------------------------------------------------------------------
 * free a slot
 * ------------------------------------------------------------------------
 */
static void freeerr(nowdb_errman_block_t *block, nowdb_err_t err) {
	uint64_t k=1;
	for(int i=0; i<64; i++) {
		if (err == (nowdb_err_t)block->buf+i) {
			block->bitmap ^= k; break;
		}
		k <<= 1;
	}
}

/* ------------------------------------------------------------------------
 * initialise error memory manager:
 * - initialise the mutex
 * - initialise the list
 * - create a new block
 * ------------------------------------------------------------------------
 */
nowdb_bool_t nowdb_errman_init() {
	if (pthread_mutex_init(&lock, NULL) != 0) return FALSE;
	ts_algo_list_init(&errors);
	return newblock();
}

/* ------------------------------------------------------------------------
 * destroy error memory manager
 * ------------------------------------------------------------------------
 */
void nowdb_errman_destroy() {
	pthread_mutex_destroy(&lock);
	ts_algo_list_destroyAll(&errors);
}

/* ------------------------------------------------------------------------
 * get an error descriptor; fails only, if no memory is available
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_errman_get() {
	ts_algo_list_node_t *runner;
	nowdb_err_t err = NULL;

	if (pthread_mutex_lock(&lock) != 0) return NULL;
	for(runner=errors.head; runner!=NULL; runner=runner->nxt) {
		err = findslot(runner->cont);
		if (err != NULL) break;
	}
	if (err == NULL) {
		if (newblock()) {
			err = findslot(errors.last->cont);
		}
	}
	if (pthread_mutex_unlock(&lock) != 0) return NULL;
	return err;
}

/* ------------------------------------------------------------------------
 * release error descriptor
 * ------------------------------------------------------------------------
 */
void nowdb_errman_release(nowdb_err_t err) {
	ts_algo_list_node_t *runner;
	
	if (pthread_mutex_lock(&lock) != 0) return;
	for(runner=errors.head; runner!=NULL; runner=runner->nxt) {
		if ((char*)err >= (char*)runner->cont &&
		    (char*)err < (char*)(runner->cont)+64*32) {
			freeerr(runner->cont, err); break;
		}
	}
	pthread_mutex_unlock(&lock);
}
