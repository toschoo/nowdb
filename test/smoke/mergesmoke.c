/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Test the tester
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <common/scopes.h>
#include <common/db.h>
#include <common/bench.h>

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

#define NEDGES   5
#define NPRODS   2
#define NCLIENTS 1

int testBuffer(nowdb_scope_t *scope, int h) {
	int rc = 0;
	int c = 0;
	nowdb_err_t     err;
	nowdb_index_t  *idx;
	ts_algo_list_t  files;
	nowdb_context_t *ctx;
	nowdb_reader_t *reader;
	nowdb_edge_t   *last, *cur;
	struct timespec t1, t2;

	timestamp(&t1);

	// get context
	err = nowdb_scope_getContext(scope, "sales", &ctx);
	if (err != NOWDB_OK) {
		fprintf(stderr, "context not found\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}

	// get index
	err = nowdb_scope_getIndexByName(scope, "cidx_ctx_oe", &idx);
	if (err != NOWDB_OK) {
		fprintf(stderr, "index not found\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}

	// get pending files
	ts_algo_list_init(&files);
	err = nowdb_store_getAllWaiting(&ctx->store, &files);
	if (err != NOWDB_OK) {
		fprintf(stderr, "get waiting failed\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}

	// count files
	fprintf(stderr, "we have %u files\n", files.len);
	
	// create buffer
	err = nowdb_reader_bufidx(&reader, &files, idx, NULL,
	                           NOWDB_ORD_ASC, NULL, NULL);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create buffer\n");
		nowdb_store_destroyFiles(&ctx->store, &files);
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}

	timestamp(&t2);
	fprintf(stderr, "bufidx created after %ldus\n",
	                          minus(&t2, &t1)/1000);

	// reader size
	fprintf(stderr, "bufreader has: %u\n",
	        reader->size/reader->recsize);

	if (reader->size/reader->recsize != h*HALFEDGE) {
		fprintf(stderr, "wrong size in bufreader: %u\n", reader->size);
		rc = -1; goto cleanup;
	}

	last = NULL;
	for(int i=0; i<reader->size; i+=reader->recsize) {
		if (last == NULL) {
			last = (nowdb_edge_t*)(reader->buf+i);
			cur = (nowdb_edge_t*)(reader->buf+i);
		} else {
			cur = (nowdb_edge_t*)(reader->buf+i);
			if (cur->origin < last->origin) {
				fprintf(stderr, "buffer is not ordered\n");
				rc = -1; break;
			}
		}
		// fprintf(stderr, "%lu\n", cur->origin);
	}
	if (rc != 0) goto cleanup;

	c=0;
	for(;;) {
		err = nowdb_reader_move(reader);
		if (err != NOWDB_OK) break;

		char *src = nowdb_reader_page(reader);
		if (src == NULL) {
			fprintf(stderr, "page is NULL\n");
			rc = -1; goto cleanup;
		}
		for(int i=0; i<NOWDB_IDX_PAGE; i+=reader->recsize) {
			if (memcmp(src+i, nowdb_nullrec,
			          reader->recsize) == 0) break;
			c++;
		}
	}
	if (err != NOWDB_OK) {
		if (err->errcode == nowdb_err_eof) {
			nowdb_err_release(err); err = NOWDB_OK;
		} else {
			nowdb_err_print(err);
			nowdb_err_release(err);
			rc = -1; goto cleanup;
		}
	}
	if (c != reader->size/reader->recsize) {
		fprintf(stderr, "counted: %d, expected: %u\n",
		                              c, reader->size);
		rc = -1; goto cleanup;
	}

cleanup:
	nowdb_reader_destroy(reader); free(reader);
	nowdb_store_destroyFiles(&ctx->store, &files);
	return rc;
}

int testRange(nowdb_scope_t *scope, int h) {
	int rc = 0;
	int c = 0;
	nowdb_err_t     err;
	nowdb_index_t  *idx;
	ts_algo_list_t  rfiles;
	nowdb_context_t *ctx;
	nowdb_reader_t *range=NULL;
	// nowdb_edge_t   *last, *cur;
	struct timespec t1, t2;

	timestamp(&t1);

	// get context
	err = nowdb_scope_getContext(scope, "sales", &ctx);
	if (err != NOWDB_OK) {
		fprintf(stderr, "context not found\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}

	// get index
	err = nowdb_scope_getIndexByName(scope, "cidx_ctx_oe", &idx);
	if (err != NOWDB_OK) {
		fprintf(stderr, "index not found\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}

	ts_algo_list_init(&rfiles);
	err = nowdb_store_getReaders(&ctx->store, &rfiles,
                                          NOWDB_TIME_DAWN,
                                          NOWDB_TIME_DUSK);
	if (err != NOWDB_OK) {
		fprintf(stderr, "get waiting failed\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		rc = -1; goto cleanup;
	}

	// count files
	fprintf(stderr, "we have %u readers\n",
	                           rfiles.len);

	// create frange
	err = nowdb_reader_frange(&range, &rfiles, idx, NULL,
	                                          NULL, NULL);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create range\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		rc = -1; goto cleanup;
	}
	timestamp(&t2);
	fprintf(stderr, "range created after %ldus\n",
	                        minus(&t2, &t1)/1000);

	for(;;) {
		err = nowdb_reader_move(range);
		if (err != NOWDB_OK) break;

		char *src = nowdb_reader_page(range);
		if (src == NULL) {
			fprintf(stderr, "page is NULL\n");
			rc = -1; goto cleanup;
		}
		// fprintf(stderr, "%p\n", src);
		for(int i=0; i<NOWDB_IDX_PAGE; i+=range->recsize) {

			if (memcmp(src+i, nowdb_nullrec,
			    range->recsize) == 0) break;

			/* check cont */
			if (range->cont != NULL) {
				int n = i/range->recsize;
				if (range->cont[0] == 0 &&
				    range->cont[1] == 0) break;
				if (n < 64) {
					uint64_t k = (1lu << n);
					if (!(range->cont[0] & k)) continue;
				} else  {
					uint64_t k = (1lu << (n-64));
					if (!(range->cont[1] & k)) continue;
				}
			}
			c++;
		}
	}
	if (err != NOWDB_OK) {
		if (err->errcode == nowdb_err_eof) {
			nowdb_err_release(err);
			err = NOWDB_OK;
		} else {
			fprintf(stderr, "error in range\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			rc = -1; goto cleanup;
		}
	}

	fprintf(stderr, "we counted: %d\n", c);
	if (c != h * HALFEDGE) {
		fprintf(stderr, "expected: %d\n", h*HALFEDGE);
		rc = -1; goto cleanup;
	}

cleanup:
	if (range != NULL) {
		nowdb_reader_destroy(range); free(range);
	}
	nowdb_store_destroyFiles(&ctx->store, &rfiles);
	return rc;
}

int testMerge(nowdb_scope_t *scope, int h) {
	int rc = 0;
	int c = 0;
	int c0 = 0, c1 = 0;
	nowdb_err_t     err;
	nowdb_index_t  *idx;
	ts_algo_list_t  pfiles;
	ts_algo_list_t  rfiles;
	nowdb_context_t *ctx;
	nowdb_reader_t *buffer=NULL;
	nowdb_reader_t *range=NULL;
	nowdb_reader_t *merge=NULL;
	// nowdb_edge_t   *last, *cur;
	struct timespec t1, t2;

	timestamp(&t1);

	// get context
	err = nowdb_scope_getContext(scope, "sales", &ctx);
	if (err != NOWDB_OK) {
		fprintf(stderr, "context not found\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}

	// get index
	err = nowdb_scope_getIndexByName(scope, "cidx_ctx_oe", &idx);
	if (err != NOWDB_OK) {
		fprintf(stderr, "index not found\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}

	// get pending files
	ts_algo_list_init(&pfiles);
	err = nowdb_store_getAllWaiting(&ctx->store, &pfiles);
	if (err != NOWDB_OK) {
		fprintf(stderr, "get waiting failed\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}

	ts_algo_list_init(&rfiles);
	err = nowdb_store_getReaders(&ctx->store, &rfiles,
                                          NOWDB_TIME_DAWN,
                                          NOWDB_TIME_DUSK);
	if (err != NOWDB_OK) {
		fprintf(stderr, "get waiting failed\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		rc = -1; goto cleanup;
	}

	// count files
	fprintf(stderr, "we have %u pending files and %u readers\n",
	                                    pfiles.len, rfiles.len);
	
	// create buffer
	err = nowdb_reader_bufidx(&buffer, &pfiles, idx, NULL,
	                           NOWDB_ORD_ASC, NULL, NULL);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create buffer\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		rc = -1; goto cleanup;
	}

	// create frange
	err = nowdb_reader_frange(&range, &rfiles, idx, NULL,
	                                          NULL, NULL);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create range\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		rc = -1; goto cleanup;
	}

	// create merge
	err = nowdb_reader_merge(&merge, 2, range, buffer);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create merge\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		rc = -1; goto cleanup;
	}
	timestamp(&t2);
	fprintf(stderr, "merge created after %ldus\n",
	                        minus(&t2, &t1)/1000);

	for(;;) {
		err = nowdb_reader_move(merge);
		if (err != NOWDB_OK) break;

		char *src = nowdb_reader_page(merge);
		if (src == NULL) {
			fprintf(stderr, "page is NULL\n");
			rc = -1; goto cleanup;
		}
		// fprintf(stderr, "%p\n", src);
		for(int i=0; i<NOWDB_IDX_PAGE; i+=merge->recsize) {

			if (memcmp(src+i, nowdb_nullrec,
			    merge->recsize) == 0) break;

			/* check cont */
			if (merge->cont != NULL) {
				int n = i/merge->recsize;
				if (merge->cont[0] == 0 &&
				    merge->cont[1] == 0) break;
				if (n < 64) {
					uint64_t k = (1lu << n);
					if (!(merge->cont[0] & k)) continue;
				} else  {
					uint64_t k = (1lu << (n-64));
					// fprintf(stderr, "%lu %d %lu\n", merge->cont[1], i, k);
					if (!(merge->cont[1] & k)) continue;
				}
			}

			c++;
			if (merge->cur == 0) c0++;
			if (merge->cur == 1) c1++;

			/*
			if (merge->cur == 1) {
				fprintf(stderr, "MERGE: %lu\n", *(uint64_t*)(src+i+8));
			}
			*/
		}
	}
	if (err != NOWDB_OK) {
		if (err->errcode == nowdb_err_eof) {
			nowdb_err_release(err);
			err = NOWDB_OK;
		} else {
			fprintf(stderr, "error in merge\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			rc = -1; goto cleanup;
		}
	}

	fprintf(stderr, "we counted: %d = %d + %d\n", c, c0, c1);
	if (c != h * HALFEDGE) {
		fprintf(stderr, "expected: %d\n", h*HALFEDGE);
		rc = -1; goto cleanup;
	}

cleanup:
	if (merge != NULL) {
		nowdb_reader_destroy(merge); free(merge);
	} else {
		if (buffer != NULL) {
			nowdb_reader_destroy(buffer); free(buffer);
		}
		if (range != NULL) {
			nowdb_reader_destroy(range); free(range);
		}
	}
	nowdb_store_destroyFiles(&ctx->store, &pfiles);
	nowdb_store_destroyFiles(&ctx->store, &rfiles);
	return rc;
}

int main() {
	int rc = EXIT_SUCCESS;
	nowdb_scope_t *scope = NULL;

	/*
	int o, d;
	nowdb_time_t tp;
	char *sql=NULL;
	*/

	srand(time(NULL) ^ (uint64_t)&printf);

	if (!nowdb_init()) {
		fprintf(stderr, "cannot init library\n");
		return EXIT_FAILURE;
	}
	if (initScopes() != 0) {
		fprintf(stderr, "cannot init scopes\n");
		return EXIT_FAILURE;
	}
	if (createDB(NEDGES, NPRODS, NCLIENTS) != 0) {
		fprintf(stderr, "cannot init scopes\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	scope = openDB();
	if (scope == NULL) {
		fprintf(stderr, "cannot open db\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

	// test bufreader
	fprintf(stderr, "BUFFER\n");
	if (testBuffer(scope, 1) != 0) {
		fprintf(stderr, "testBuffer failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}
	// range
	fprintf(stderr, "RANGE\n");
	if (testRange(scope, NEDGES-1) != 0) {
		fprintf(stderr, "testRange failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

	// test mergereader
	fprintf(stderr, "MERGE\n");
	if (testMerge(scope, NEDGES) != 0) {
		fprintf(stderr, "testMerge failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

cleanup:
	if (scope != NULL) {
		closeDB(scope);
	}
	closeScopes();
	nowdb_close();

	if (rc == EXIT_SUCCESS) {
		fprintf(stderr, "PASSED.\n");
	} else {
		fprintf(stderr, "FAILED.\n");
	}
	return rc;
}

