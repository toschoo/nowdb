/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Benchmarking plain file read
 * ========================================================================
 */
#include <nowdb/io/file.h>
#include <nowdb/task/task.h>
#include <common/progress.h>
#include <common/cmd.h>
#include <common/bench.h>
#include <common/stores.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>

#define EDGE_OFF  24
#define LABEL_OFF 32
#define WEIGHT_OFF 40

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
	setValue(e, EDGE_OFF, 0xc);
	setValue(e, LABEL_OFF, 0xd);
	setValue(e, WEIGHT_OFF, 0);
	setValue(e, WEIGHT_OFF+8, 63);
}

#define RECPAGE (NOWDB_IDX_PAGE/recsz)
#define FULL (128*RECPAGE)
#define HALF (FULL/2)
#define FULLPOS NOWDB_MEGA
#define HALFPOS (FULLPOS/2)

#define ONEANDHALF (FULL+HALF)
#define DELAY   10000000

nowdb_bool_t insertEdges(nowdb_store_t *store, uint32_t count) {
	nowdb_err_t err;
	char *e;
	uint64_t max = count;
	uint32_t recsz = nowdb_recSize(6);

	e = calloc(1, recsz);
	if (e == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return FALSE;
	}

	makeEdgePattern(e);
	for(uint64_t i=0; i<max; i++) {
		setValue(e, WEIGHT_OFF, i);
		err = nowdb_store_insert(store, e);
		if (err != NOWDB_OK) {
			fprintf(stderr, "insert error\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			free(e); return FALSE;
		}
	}
	fprintf(stderr, "inserted %u from 0, to %lu\n",
	       count, *(uint64_t*)(e+WEIGHT_OFF));
	free(e);
	return TRUE;
}

nowdb_bool_t waitForSort(nowdb_store_t *store) {
	nowdb_err_t err;
	int len;

	for(;;) {
		err = nowdb_lock_read(&store->lock);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
		len = store->waiting.len;
		err = nowdb_unlock_read(&store->lock);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
		if (len == 0) break;
		err = nowdb_task_sleep(DELAY);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
	}
	return TRUE;
}

nowdb_bool_t checkSorted(nowdb_file_t *file) {
	nowdb_err_t err;
	char *e=NULL;
	char *last;
	char first = 1;
	uint32_t recsz = nowdb_recSize(6);
	uint32_t realsz = NOWDB_IDX_PAGE/recsz;
	uint32_t remsz = NOWDB_IDX_PAGE - realsz;

	last = calloc(1,recsz);
	if (last == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return FALSE;
	}

	err = nowdb_file_open(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		free(last); return FALSE;
	}
	err = nowdb_file_rewind(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		NOWDB_IGNORE(nowdb_file_close(file));
		free(last); return FALSE;
	}
	fprintf(stderr, "%u == %u\n",
	        file->bufsize, file->blocksize);
	first = TRUE;
	// for(;file->pos<file->size;) {
	for(;;) {
		err = nowdb_file_move(file);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			if (err->errcode != nowdb_err_eof) {
				NOWDB_IGNORE(nowdb_file_close(file));
				free(last); return FALSE;
			} else {
				break;
			}
		}

		/*
		fprintf(stderr, "block read to %u - %zu\n",
		        file->pos, lseek(file->fd, 0, SEEK_CUR));
		*/

		for(int i=0;i<file->bufsize;) {
			if (i >= realsz) {
				i+=remsz; continue;
			}
			e = (file->bptr+i);
			if (first) {
				memcpy(last,e,recsz); first = 0; continue;
			}
			if (*(uint64_t*)(e+NOWDB_OFF_ORIGIN) <
			    *(uint64_t*)(last+NOWDB_OFF_ORIGIN)) {
				fprintf(stderr, "not sorted (origin)\n");
				NOWDB_IGNORE(nowdb_file_close(file));
				free(last); return FALSE;
			}
			if (*(uint64_t*)(e+NOWDB_OFF_ORIGIN) ==
			    *(uint64_t*)(last+NOWDB_OFF_ORIGIN) &&
			    *(uint64_t*)(e+NOWDB_OFF_DESTIN) <
			    *(uint64_t*)(last+NOWDB_OFF_DESTIN)) {
				fprintf(stderr, "not sorted (destin)\n");
				NOWDB_IGNORE(nowdb_file_close(file));
				free(last); return FALSE;
			}
			if (*(uint64_t*)(e+NOWDB_OFF_ORIGIN) ==
			    *(uint64_t*)(last+NOWDB_OFF_ORIGIN) &&
			    *(uint64_t*)(e+NOWDB_OFF_DESTIN) ==
			    *(uint64_t*)(last+NOWDB_OFF_DESTIN) &&
			    *(uint64_t*)(e+NOWDB_OFF_STAMP) <
			    *(uint64_t*)(last+NOWDB_OFF_STAMP)) {
				fprintf(stderr, "%d: not sorted (timestamp)\n", i);
				NOWDB_IGNORE(nowdb_file_close(file));
				free(last); return FALSE;
			}
			memcpy(last,e,recsz);
			i+=file->recordsize;
		}
	}
	free(last);
	fprintf(stderr, "file is sorted\n");
	err = nowdb_file_close(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	return TRUE;
}

nowdb_bool_t checkFile(nowdb_store_t *store) {
	nowdb_err_t      err;
	ts_algo_list_t files;
	nowdb_file_t   *file;
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
		if (file->id != store->writer->id) {
			if (!checkSorted(file)) {
				nowdb_store_destroyFiles(store, &files);
				return FALSE;
			}
		}
	}
	nowdb_store_destroyFiles(store, &files);
	return TRUE;
}

int main() {
	nowdb_comprsc_t compare = &nowdb_sort_edge_compare;
	int rc = EXIT_SUCCESS;
	nowdb_store_t *store = NULL;
	struct timespec t1, t2;
	uint32_t recsz = nowdb_recSize(6);

	if (!nowdb_err_init()) {
		fprintf(stderr, "cannot init library\n");
		return EXIT_FAILURE;
	}
	store = xBootstrap("rsc/store30", NOWDB_CONT_EDGE,
                              compare, NOWDB_COMP_ZSTD, 1,
	                    recsz, NOWDB_MEGA, NOWDB_MEGA);
	if (store == NULL) {
		fprintf(stderr, "cannot bootstrap\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

	timestamp(&t1);
	if (!insertEdges(store, ONEANDHALF)) {
		fprintf(stderr, "cannot insert\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	timestamp(&t2);
	fprintf(stderr, "Running time: %luus\n", minus(&t2, &t1)/1000);

	if (!waitForSort(store)) {
		fprintf(stderr, "waitForSort failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	if (!checkFile(store)) {
		fprintf(stderr, "checkFile failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

cleanup:
	if (store != NULL) {
		if (!closeStore(store)) {
			fprintf(stderr, "cannot close store\n");
		}
		destroyStore(store);
		free(store);
	}
	nowdb_err_destroy();
	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED\n");
	} else {
		fprintf(stderr, "FAILED\n");
	}
	return rc;
}

