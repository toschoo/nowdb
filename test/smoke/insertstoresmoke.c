/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple insert tests for store
 * ========================================================================
 */
#include <nowdb/io/file.h>
#include <nowdb/store/store.h>
#include <common/stores.h>
#include <common/bench.h>

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

/*
uint64_t origin;
uint64_t destin;
int64_t  timestamp;
char     byte; // control byte
uint64_t edge;
uint64_t label;
uint64_t weight;
*/

#define EDGE_OFF  25
#define LABEL_OFF 33
#define WEIGHT_OFF 41

void setRandomValue(char *e, uint32_t off) {
	uint64_t x;
	do x = rand()%100; while(x == 0);
	memcpy(e+off, &x, 8);
}

void setValue(char *e, uint32_t off, uint64_t v) {
	memcpy(e+off, &v, 8);
}

void makeEdgePattern(char *e) {
	setValue(e, NOWDB_OFF_ORIGIN, 0xa);
	setValue(e, NOWDB_OFF_DESTIN, 0xb);
	nowdb_time_now((nowdb_time_t*)(e+NOWDB_OFF_STAMP));
	setValue(e, NOWDB_OFF_USER, 3);
	setValue(e, EDGE_OFF, 0xc);
	setValue(e, LABEL_OFF, 0xd);
	setValue(e, WEIGHT_OFF, 0);
}

#define RECPAGE (NOWDB_IDX_PAGE/recsz)
#define FULL (128*RECPAGE)
#define HALF (FULL/2)
#define FULLPOS NOWDB_MEGA
#define HALFPOS (FULLPOS/2)

nowdb_bool_t insertEdges(nowdb_store_t *store, uint32_t count, uint64_t start) {
	nowdb_err_t err;
	char *e;
	uint64_t max = start + count;
	uint32_t recsz = nowdb_edge_recSize(1,3);

	e = calloc(1, recsz);
	if (e == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return FALSE;
	}

	makeEdgePattern(e);
	for(uint64_t i=start; i<max; i++) {
		setValue(e, WEIGHT_OFF, i);
		err = nowdb_store_insert(store, e);
		if (err != NOWDB_OK) {
			fprintf(stderr, "insert error\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			free(e); return FALSE;
		}
	}
	fprintf(stderr, "inserted %u from %lu to %lu (%lu)\n",
	       count, start, max, *(uint64_t*)(e+WEIGHT_OFF));
	free(e);
	return TRUE;
}

nowdb_bool_t checkInitial(nowdb_store_t *store) {
	if (store->writer == NULL) {
		fprintf(stderr, "store without writer\n");
		return FALSE;
	}
	if (store->writer->size != 0) {
		fprintf(stderr, "store not at initial position\n");
		return FALSE;
	}
	if (store->spares.len == 0) {
		fprintf(stderr, "store has no spares\n");
		return FALSE;
	}
	if (store->spares.len < 2) {
		fprintf(stderr, "store has too few spares\n");
		return FALSE;
	}
	if (store->waiting.len > 0) {
		fprintf(stderr, "store has pending files\n");
		return FALSE;
	}
	return TRUE;
}

nowdb_bool_t checkHalf(nowdb_store_t *store) {
	uint32_t recsz = nowdb_edge_recSize(1,3);

	if (store->writer == NULL) {
		fprintf(stderr, "store without writer\n");
		return FALSE;
	}
	if (store->writer->size != HALFPOS) { // *store->recsize) {
		fprintf(stderr, "store not at 'half' position: %u/%u\n",
		              HALF*store->recsize, store->writer->size);
		return FALSE;
	}
	if (store->spares.len == 0) {
		fprintf(stderr, "store has no spares\n");
		return FALSE;
	}
	if (store->spares.len < 2) {
		fprintf(stderr, "store has too few spares\n");
		return FALSE;
	}
	if (store->waiting.len > 0) {
		fprintf(stderr, "store has pending files\n");
		return FALSE;
	}
	return TRUE;
}

nowdb_bool_t checkFull(nowdb_store_t *store) {
	uint32_t recsz = nowdb_edge_recSize(1,3);
	if (store->writer == NULL) {
		fprintf(stderr, "store without writer\n");
		return FALSE;
	}
	if (store->writer->size != HALFPOS) {
		fprintf(stderr, "store not at 'full' position: %u/%u\n",
		              HALF*store->recsize, store->writer->size);
		return FALSE;
	}
	if (store->spares.len == 0) {
		fprintf(stderr, "store has no spares\n");
		return FALSE;
	}
	if (store->spares.len < 2) {
		fprintf(stderr, "store has too few spares\n");
		return FALSE;
	}
	if (store->waiting.len != 1) {
		fprintf(stderr, "store has no pending file\n");
		return FALSE;
	}
	return TRUE;
}

nowdb_bool_t find(nowdb_file_t *file, uint32_t count, uint64_t start) {
	nowdb_err_t err;
	char *e=NULL;
	uint64_t j = start;
	uint64_t c = 0;
	nowdb_bool_t closeit = FALSE;
	uint32_t realsz;
	uint32_t remsz;
	uint32_t recsz;

	recsz = nowdb_edge_recSize(1,3);
	realsz = (NOWDB_IDX_PAGE/recsz)*recsz;
	remsz = NOWDB_IDX_PAGE - realsz;

	if (file->state == nowdb_file_state_closed) {
		err = nowdb_file_open(file);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
		closeit = TRUE;
	}
	err = nowdb_file_rewind(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		if (closeit) NOWDB_IGNORE(nowdb_file_close(file));
		return FALSE;
	}
	fprintf(stderr, "file   id: %u\n", file->id);
	fprintf(stderr, "file size: %u\n", file->size);
	fprintf(stderr, "file  pos: %u\n", file->pos);
	fprintf(stderr, "file  buf: %u\n", file->bufsize);
	for(;;) {
		if (file->pos >= file->size) break;
		// fprintf(stderr, "next page %u (%u)\n",
		//            file->pos, file->pos%8192);
		err = nowdb_file_move(file);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			if (closeit) NOWDB_IGNORE(nowdb_file_close(file));
			return FALSE;
		}
		for(int i=0;i<file->bufsize;) {
			if ((i%NOWDB_IDX_PAGE) >= realsz) {
				i+=remsz; continue;
			}
			e = file->bptr+i;
			
			/*
			fprintf(stderr, "read: %lu (%d)\n",
				 *(uint64_t*)(e+WEIGHT_OFF), i);
			*/
			
			if (*(uint64_t*)(e+WEIGHT_OFF) != j) {
				fprintf(stderr, "wrong number: %lu (%lu)\n",
				          *(uint64_t*)(e+WEIGHT_OFF),
				          *(uint64_t*)(file->bptr+i+NOWDB_OFF_ORIGIN));
				if (closeit) NOWDB_IGNORE(nowdb_file_close(file));
				return FALSE;
			}
			j++; c++;
			i+=file->recordsize;
		}
	}
	if (e == NULL) {
		fprintf(stderr, "no edge\n");
		if (closeit) NOWDB_IGNORE(nowdb_file_close(file));
		return FALSE;
	}
	if (*(uint64_t*)(e+WEIGHT_OFF) != start + count) {
		fprintf(stderr, "last number is wrong: %lu (%lu)\n",
		           *(uint64_t*)(e+WEIGHT_OFF), start+count);
		if (closeit) NOWDB_IGNORE(nowdb_file_close(file));
		return FALSE;
	}
	if (closeit) {
		err = nowdb_file_close(file);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			NOWDB_IGNORE(nowdb_file_close(file));
			return FALSE;
		}
	}
	return TRUE;
}

nowdb_bool_t checkFiles(nowdb_store_t *store) {
	nowdb_bool_t   found;
	nowdb_err_t      err;
	ts_algo_list_t files;
	nowdb_file_t   *file;
	uint32_t recsz = nowdb_edge_recSize(1,3);
	ts_algo_list_node_t *runner;
	ts_algo_list_init(&files);

	err = nowdb_store_getFiles(store, &files,
	                           NOWDB_TIME_DAWN,
	                           NOWDB_TIME_DUSK);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		ts_algo_list_destroy(&files);
		nowdb_store_destroyFiles(store, &files);
		return FALSE;
	}
	if (files.len != 2) {
		fprintf(stderr, "expecting 2 files: %d\n", files.len);
		nowdb_store_destroyFiles(store, &files);
		return FALSE;
	}
	for(runner=files.head; runner!=NULL; runner=runner->nxt) {
		file = runner->cont;
		err = nowdb_store_findWaiting(store, file, &found);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			nowdb_store_destroyFiles(store, &files);
			return FALSE;
		}
		if (file->id != store->writer->id && !found) {
			nowdb_store_destroyFiles(store, &files);
			return FALSE;
		}
		/* if waiting find from 0...16383 */
		/* CAUTION: waiting may have been sorted already !!! */
		if (file->id != store->writer->id) {
			if (!find(file, FULL-1, 0)) {
				nowdb_store_destroyFiles(store, &files);
				return FALSE;
			}
		}
		/* if writer find from 16384 ...24575 */
		if (file->id == store->writer->id) {
			if (!find(file, HALF-1, FULL)) {
				nowdb_store_destroyFiles(store, &files);
				return FALSE;
			}
		}
	}
	nowdb_store_destroyFiles(store, &files);
	return TRUE;
}

int main() {
	struct timespec t1, t2;
	int rc = EXIT_SUCCESS;
	nowdb_store_t *store;
	uint32_t recsz;

	recsz = nowdb_edge_recSize(1,3);

	nowdb_err_init();
	store = bootstrap("rsc/store10", recsz);
	if (store == NULL) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!checkInitial(store)) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!insertEdges(store, HALF, 0)) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!checkHalf(store)) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!insertEdges(store, FULL, HALF)) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!checkFull(store)) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!checkFiles(store)) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!closeStore(store)) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!openStore(store)) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!checkFull(store)) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!checkFiles(store)) {
		rc = EXIT_FAILURE; goto cleanup;
	}
	
cleanup:
	if (store != NULL) {
		nowdb_bool_t x;
		timestamp(&t1);
		x = closeStore(store);
		timestamp(&t2);
		fprintf(stderr, "closing took %luus\n",
		                 minus(&t2, &t1)/1000);
		if (x) {
			destroyStore(store);
			free(store);
		}
	}
	nowdb_err_destroy();
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}

