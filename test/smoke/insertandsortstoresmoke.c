/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Benchmarking plain file read
 * ========================================================================
 */
#include <nowdb/io/file.h>
#include <common/progress.h>
#include <common/cmd.h>
#include <common/bench.h>
#include <common/stores.h>

#define ONEANDHALF 16384

nowdb_bool_t insertEdges(nowdb_store_t *store, uint32_t count) {
	nowdb_err_t err;
	nowdb_edge_t e;

	for(uint32_t i=0; i<count; i++) {

		memset(&e,0,64);

		do e.origin = rand()%100; while(e.origin == 0);
		do e.destin = rand()%100; while(e.destin == 0);
		do e.edge   = rand()%10; while(e.edge == 0);
		do e.label  = rand()%10; while(e.label == 0);
		err = nowdb_time_now(&e.timestamp);
		if (err != NOWDB_OK) {
			fprintf(stderr, "insert error\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
		e.weight = (uint64_t)i;
		e.weight2  = 0;
		e.wtype[0] = NOWDB_TYP_UINT;
		e.wtype[1] = NOWDB_TYP_NOTHING;

		err = nowdb_store_insert(store, &e);
		if (err != NOWDB_OK) {
			fprintf(stderr, "insert error\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			return FALSE;
		}
	}
	/*
	fprintf(stderr, "inserted %u (last: %lu)\n",
	                           count, e.weight);
	*/
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
	}
	return TRUE;
}

nowdb_bool_t checkSorted(nowdb_file_t *file) {
	nowdb_err_t err;
	nowdb_edge_t *e=NULL;
	nowdb_edge_t last;
	char first = 1;

	err = nowdb_file_open(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return FALSE;
	}
	err = nowdb_file_rewind(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		NOWDB_IGNORE(nowdb_file_close(file));
		return FALSE;
	}
	fprintf(stderr, "%u == %u\n",
	        file->bufsize, file->blocksize);
	first = TRUE;
	for(;file->pos<file->size;) {
		err = nowdb_file_move(file);
		if (err != NOWDB_OK) {
			nowdb_err_print(err);
			nowdb_err_release(err);
			NOWDB_IGNORE(nowdb_file_close(file));
			return FALSE;
		}
		for(int i=0;i<file->bufsize;i+=file->recordsize) {
			e = (nowdb_edge_t*)(file->bptr+i);
			if (first) {
				memcpy(&last,e,64); first = 0; continue;
			}
			if (e->origin < last.origin) {
				fprintf(stderr, "not sorted (origin)\n");
				NOWDB_IGNORE(nowdb_file_close(file));
				return FALSE;
			}
			if (e->origin == last.origin &&
			    e->edge   <  last.edge) {
				fprintf(stderr, "not sorted (edge)\n");
				NOWDB_IGNORE(nowdb_file_close(file));
				return FALSE;
			}
			if (e->origin == last.origin &&
			    e->edge   == last.edge   &&
			    e->destin <  last.destin) {
				fprintf(stderr, "not sorted (destin)\n");
				NOWDB_IGNORE(nowdb_file_close(file));
				return FALSE;
			}
			if (e->origin == last.origin &&
			    e->edge   == last.edge   &&
			    e->destin == last.destin &&
			    e->label  <  last.label) {
				fprintf(stderr, "not sorted (label)\n");
				NOWDB_IGNORE(nowdb_file_close(file));
				return FALSE;
			}
			if (e->origin == last.origin &&
			    e->edge   == last.edge   &&
			    e->destin == last.destin &&
			    e->label  == last.label  &&
			    e->timestamp < last.timestamp) {
				fprintf(stderr, "not sorted (timestamp)\n");
				NOWDB_IGNORE(nowdb_file_close(file));
				return FALSE;
			}
			memcpy(&last,e,64);
		}
	}
	fprintf(stderr, "file is sorted\n");
	err = nowdb_file_close(file);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		NOWDB_IGNORE(nowdb_file_close(file));
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
		nowdb_store_destroyFiles(&files);
		return FALSE;
	}
	if (files.len != 2) {
		fprintf(stderr, "expecting 2 files: %d\n", files.len);
		nowdb_store_destroyFiles(&files);
		return FALSE;
	}
	for(runner=files.head; runner!=NULL; runner=runner->nxt) {
		file = runner->cont;
		if (file->id != store->writer->id) {
			if (!checkSorted(file)) {
				nowdb_store_destroyFiles(&files);
				return FALSE;
			}
		}
	}
	nowdb_store_destroyFiles(&files);
	return TRUE;
}

int main() {
	nowdb_comprsc_t compare = &nowdb_store_edge_compare;
	int rc = EXIT_SUCCESS;
	nowdb_store_t *store = NULL;
	struct timespec t1, t2;

	if (!nowdb_err_init()) {
		fprintf(stderr, "cannot init library\n");
		return EXIT_FAILURE;
	}
	store = xBootstrap("rsc/store30", compare, NOWDB_COMP_FLAT);
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
		nowdb_store_destroy(store);
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

