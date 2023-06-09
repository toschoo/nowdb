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

#define INDEX "_idx_buys_origin"

#define NEDGES   5
#define NPRODS   2
#define NCLIENTS 1

#define EDGE_OFF  24
#define LABEL_OFF 32
#define WEIGHT_OFF 40

#define LOOP_INIT(stm, atts) \
	uint32_t recsz = nowdb_recSize(atts); \
	uint32_t bufsz = (NOWDB_IDX_PAGE/recsz)*recsz; \
	uint32_t remsz = NOWDB_IDX_PAGE - bufsz;

#define LOOP_HEAD \
	for(int i=0; i<NOWDB_IDX_PAGE;) { \
		if (i%NOWDB_IDX_PAGE >= bufsz) { \
			i+=remsz; continue; \
		}

#define LOOP_END \
		i+=recsz; \
	}

void setRandomValue(char *e, uint32_t off) {
	uint64_t x;
	do x = rand()%100; while(x == 0);
	memcpy(e+off, &x, 8);
}

void setValue(char *e, uint32_t off, uint64_t v) {
	memcpy(e+off, &v, 8);
}

int edgecompare(char *one, char *two, uint32_t off) {
	if (*(uint64_t*)(one+off) < *(uint64_t*)(two+off)) return -1;
	if (*(uint64_t*)(one+off) > *(uint64_t*)(two+off)) return  1;
	return 0;
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

nowdb_eval_t _eval;

int testBuffer(nowdb_scope_t *scope, int h) {
	int rc = 0;
	int c = 0;
	nowdb_err_t     err;
	nowdb_index_t  *idx;
	ts_algo_list_t  files;
	nowdb_context_t *ctx;
	nowdb_reader_t *reader;
	char *last, *cur;
	struct timespec t1, t2;
	int have;

	LOOP_INIT(1,5)

	timestamp(&t1);

	// get context
	err = nowdb_scope_getContext(scope, "buys", &ctx);
	if (err != NOWDB_OK) {
		fprintf(stderr, "context not found\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}

	// get index
	err = nowdb_scope_getIndexByName(scope, INDEX, &idx);
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
	                  &_eval, NOWDB_ORD_ASC, NULL, NULL);
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
	int pgs = reader->size/NOWDB_IDX_PAGE;
	int rpp = NOWDB_IDX_PAGE/reader->recsize;
	have = rpp*pgs+(reader->size-pgs*NOWDB_IDX_PAGE)/reader->recsize;

	fprintf(stderr, "bufreader has: %u\n", have);

	if (have != h*HALFEDGE) {
		fprintf(stderr, "wrong size in bufreader: %u/%u=%u (%u)\n",
		                reader->size, reader->recsize,
		                reader->size/reader->recsize,
		                h*HALFEDGE);
		rc = -1; goto cleanup;
	}

	last = NULL;

	char x;
	for(int i=0; i<reader->size;) {
		if (i%NOWDB_IDX_PAGE >= bufsz) {
			i+=remsz; continue;
		}
		if (last == NULL) {
			last = reader->buf+i;
			cur  = last;
		} else {
			cur = reader->buf+i;
			x = edgecompare(cur, last, NOWDB_OFF_ORIGIN);
			if (x == -1) {
				fprintf(stderr, "buffer is not ordered\n");
				rc = -1; break;
			}
			if (x != 0) {
				last = cur;
			}
		}
		// fprintf(stderr, "%lu\n", cur->origin);
		i+=reader->recsize;
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
		LOOP_HEAD
			if (memcmp(src+i, nowdb_nullrec,
			          reader->recsize) == 0) break;
			c++;
		LOOP_END
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
	if (c != have) {
		fprintf(stderr, "counted: %d, expected: %u\n", c, have);
		rc = -1; goto cleanup;
	}

cleanup:
	nowdb_reader_destroy(reader); free(reader);
	nowdb_store_destroyFiles(&ctx->store, &files);
	return rc;
}

int testBKRange(nowdb_scope_t *scope, int h) {
	int rc = 0;
	int c = 0;
	nowdb_err_t     err;
	nowdb_index_t  *idx;
	ts_algo_list_t  files;
	nowdb_context_t *ctx;
	nowdb_reader_t *reader;
	char           *last, *cur;
	struct timespec t1, t2;
	LOOP_INIT(1,5)

	timestamp(&t1);

	// get context
	err = nowdb_scope_getContext(scope, "buys", &ctx);
	if (err != NOWDB_OK) {
		fprintf(stderr, "context not found\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}

	// get index
	err = nowdb_scope_getIndexByName(scope, INDEX, &idx);
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
	err = nowdb_reader_bkrange(&reader, &files, idx, NULL,
	                    &_eval, NOWDB_ORD_ASC, NULL, NULL);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create kbuffer\n");
		nowdb_store_destroyFiles(&ctx->store, &files);
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}

	timestamp(&t2);
	fprintf(stderr, "bkrange created after %ldus\n",
	                          minus(&t2, &t1)/1000);

	// reader size
	int pgs = reader->size/NOWDB_IDX_PAGE;
	int rpp = NOWDB_IDX_PAGE/reader->recsize;
	int have = rpp*pgs+(reader->size-pgs*NOWDB_IDX_PAGE)/reader->recsize;

	fprintf(stderr, "bkrange has: %d\n", have);

	if (have != h*HALFEDGE) {
		fprintf(stderr, "wrong size in bufreader: %u\n", reader->size);
		rc = -1; goto cleanup;
	}

	last = NULL;
	int ukeys=0;
	char x;
	for(int i=0; i<reader->size;) {
		if (i%NOWDB_IDX_PAGE >= bufsz) {
			i+=remsz; continue;
		}
		if (last == NULL) {
			last = reader->buf+i;
			cur  = last;
			ukeys++;
		} else {
			cur = reader->buf+i;
			x = edgecompare(cur, last, NOWDB_OFF_ORIGIN);
			if (x == -1) {
				fprintf(stderr, "buffer is not ordered\n");
				rc = -1; break;
			}
			if (x != 0) {
				last = cur;
				ukeys++;
			}
		}
		// fprintf(stderr, "%lu\n", cur->origin);
		i+=reader->recsize;
	}
	if (rc != 0) goto cleanup;

	fprintf(stderr, "uniqe keys: %d\n", ukeys);

	c=0;
	for(;;) {
		err = nowdb_reader_move(reader);
		if (err != NOWDB_OK) break;

		char *src = nowdb_reader_page(reader);
		if (src == NULL) {
			fprintf(stderr, "page is NULL\n");
			rc = -1; goto cleanup;
		}
		// fprintf(stdout, "%lu\n", *(uint64_t*)src);
		c++;
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
	fprintf(stderr, "counted: %d\n", c);

	if (c != ukeys) {
		fprintf(stderr, "counted: %d, expected: %d\n",
		                                     c, ukeys);
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
	struct timespec t1, t2;

	LOOP_INIT(1,5)

	timestamp(&t1);

	// get context
	err = nowdb_scope_getContext(scope, "buys", &ctx);
	if (err != NOWDB_OK) {
		fprintf(stderr, "context not found\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}

	// get index
	err = nowdb_scope_getIndexByName(scope, INDEX, &idx);
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
		LOOP_HEAD
			// fprintf(stderr, "in loop\n");

			if (memcmp(src+i, nowdb_nullrec,
			    range->recsize) == 0) break;

			/* check cont */
			if (range->cont != NULL) {
				int n = (i/range->recsize)/8; // range->recsize;
				int b = (i/range->recsize)%8; // range->recsize;
				uint8_t k=1;
				if (range->cont[n] & (k<<b)) c++;
			}
		LOOP_END
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

int testKRange(nowdb_scope_t *scope, int h) {
	int rc = 0;
	int c = 0;
	nowdb_err_t     err;
	nowdb_index_t  *idx;
	ts_algo_list_t  rfiles;
	nowdb_context_t *ctx;
	nowdb_reader_t *range=NULL;
	struct timespec t1, t2;

	timestamp(&t1);

	// get context
	err = nowdb_scope_getContext(scope, "buys", &ctx);
	if (err != NOWDB_OK) {
		fprintf(stderr, "context not found\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}

	// get index
	err = nowdb_scope_getIndexByName(scope, INDEX, &idx);
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
	err = nowdb_reader_krange(&range, &rfiles, idx, NULL,
	                                          NULL, NULL);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create krange\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		rc = -1; goto cleanup;
	}
	timestamp(&t2);
	fprintf(stderr, "krange created after %ldus\n",
	                         minus(&t2, &t1)/1000);

	/*
	last = NULL;
	int ukeys=0;
	char x;
	for(int i=0; i<reader->size;) {
		if (i%NOWDB_IDX_PAGE >= bufsz) {
			i+=remsz; continue;
		}
		if (last == NULL) {
			last = reader->buf+i;
			cur  = last;
			ukeys++;
		} else {
			cur = reader->buf+i;
			x = edgecompare(cur, last, NOWDB_OFF_ORIGIN);
			if (x == -1) {
				fprintf(stderr, "buffer is not ordered\n");
				rc = -1; break;
			}
			if (x != 0) {
				last = cur;
				ukeys++;
			}
		}
		// fprintf(stderr, "%lu\n", cur->origin);
		i+=reader->recsize;
	}
	if (rc != 0) goto cleanup;
	*/

	for(;;) {
		err = nowdb_reader_move(range);
		if (err != NOWDB_OK) break;

		char *src = nowdb_reader_page(range);
		if (src == NULL) {
			fprintf(stderr, "page is NULL\n");
			rc = -1; goto cleanup;
		}
		c++;
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
	// count unique keys!
	/*
	if (c != ukeys) {
		fprintf(stderr, "expected: %d\n", ukeys);
		rc = -1; goto cleanup;
	}
	*/

cleanup:
	if (range != NULL) {
		nowdb_reader_destroy(range); free(range);
	}
	nowdb_store_destroyFiles(&ctx->store, &rfiles);
	return rc;
}

int uniqueKey = 0;

int testMerge(nowdb_scope_t *scope, char type, int h) {
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
	struct timespec t1, t2;
	uint32_t mx;
	char *tmp=NULL;

	LOOP_INIT(1,5)

	tmp = calloc(1,recsz);
	if (tmp == NULL) {
		fprintf(stderr, "out-of-mem\n");
		return -1;
	}

	timestamp(&t1);

	// get context
	err = nowdb_scope_getContext(scope, "buys", &ctx);
	if (err != NOWDB_OK) {
		fprintf(stderr, "context not found\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		free(tmp);
		return -1;
	}

	// get index
	err = nowdb_scope_getIndexByName(scope, INDEX, &idx);
	if (err != NOWDB_OK) {
		fprintf(stderr, "index not found\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		free(tmp);
		return -1;
	}

	// get pending files
	ts_algo_list_init(&pfiles);
	err = nowdb_store_getAllWaiting(&ctx->store, &pfiles);
	if (err != NOWDB_OK) {
		fprintf(stderr, "get waiting failed\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		free(tmp);
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
	switch(type) {
	case 'k':
		err = nowdb_reader_bkrange(&buffer, &pfiles, idx, NULL,
		                    &_eval, NOWDB_ORD_ASC, NULL, NULL);
		break;
	default:
		err = nowdb_reader_bufidx(&buffer, &pfiles, idx, NULL,
		                    &_eval, NOWDB_ORD_ASC, NULL, NULL);
	}
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot create buffer\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		rc = -1; goto cleanup;
	}

	// create range
	switch(type) {
	case 'k':
		err = nowdb_reader_krange(&range, &rfiles, idx, NULL,
		                                          NULL, NULL);
		break;
	default:
		err = nowdb_reader_frange(&range, &rfiles, idx, NULL,
		                                          NULL, NULL);
	}
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

	if (type != 'k') uniqueKey = 0;

	// check order and count
	fprintf(stderr, "BUFISZE: %u\n", type=='k'?merge->recsize:
	                                           NOWDB_IDX_PAGE);
	for(;;) {
		err = nowdb_reader_move(merge);
		if (err != NOWDB_OK) break;

		char *src = nowdb_reader_page(merge);
		if (src == NULL) {
			fprintf(stderr, "page is NULL\n");
			rc = -1; goto cleanup;
		}
		mx = type == 'k'?merge->recsize:NOWDB_IDX_PAGE;
		for(int i=0; i<mx;) {

			if (type != 'k' && i%NOWDB_IDX_PAGE >= bufsz) {
				i+=remsz; continue;
			}

			if (memcmp(src+i, nowdb_nullrec,
			    merge->recsize) == 0) break;

			/* check cont */
			if (merge->cont != NULL) {
				int n = (i/merge->recsize)/8;
				int b = (i/merge->recsize)%8;
				uint8_t k = 1;
				if (!(merge->cont[n] & (k<<b))) {
					i+=merge->recsize;
					continue;
				}
			}

			if (memcmp(tmp, nowdb_nullrec,
			          merge->recsize) == 0) 
			{
				memcpy(tmp, src+i, merge->recsize);
				if (type == 'k') {
					c++;
					if (merge->cur == 0) c0++;
					if (merge->cur == 1) c1++;
				} else {
					uniqueKey++;
				}

			} else {
				if (edgecompare(tmp, src+i,
				    NOWDB_OFF_ORIGIN) == -1)
				{
					memcpy(tmp, src+i, merge->recsize);
					if (type == 'k') {
						c++;
						if (merge->cur == 0) c0++;
						if (merge->cur == 1) c1++;
					} else {
						uniqueKey++;
					}

				} else if (edgecompare(tmp, src+i,  
					   NOWDB_OFF_ORIGIN) == 1) {
					fprintf(stderr,
						"not in order: %lu > %lu\n",
						*(uint64_t*)(tmp+
					         NOWDB_OFF_ORIGIN),
					        *(uint64_t*)(src+i+
						 NOWDB_OFF_ORIGIN)); 
					return -1;
				}
			} 
			if (type != 'k') {
				c++;
				if (merge->cur == 0) c0++;
				if (merge->cur == 1) c1++;
			}

			/*
			fprintf(stderr, "%cMERGE [%05d]: %lu (%d)\n", type, c,
			 *(uint64_t*)(src+i), merge->cur);
			*/

			i+=merge->recsize;
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
	fprintf(stderr, "unique keys: %d\n", uniqueKey);
	if (type == 'k' && c != uniqueKey) {
		fprintf(stderr, "expected: %d\n", uniqueKey);
		rc = -1; goto cleanup;

	} else if (type != 'k' && c != h * HALFEDGE) {
		fprintf(stderr, "expected: %d\n", h*HALFEDGE);
		rc = -1; goto cleanup;
	}

cleanup:
	if (tmp != NULL) free(tmp);
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

void initEval(nowdb_scope_t *scope) {
	_eval.model = scope->model;
	_eval.text = scope->text;

	_eval.tlru = NULL;
}

void destroyEval() {
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

	initEval(scope);

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
	if (testMerge(scope, 'r', NEDGES) != 0) {
		fprintf(stderr, "testMerge failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

	// test bkrange
	fprintf(stderr, "BKRANGE\n");
	if (testBKRange(scope, 1) != 0) {
		fprintf(stderr, "testBKRange failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

	// test krange
	fprintf(stderr, "KRANGE\n");
	if (testKRange(scope, 1) != 0) {
		fprintf(stderr, "testKRange failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

	// test kmerge
	fprintf(stderr, "KMERGE\n");
	if (testMerge(scope, 'k', NEDGES) != 0) {
		fprintf(stderr, "testKMerge failed\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

cleanup:
	destroyEval();
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

