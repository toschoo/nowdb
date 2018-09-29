/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Reader: generic data access interface
 * ========================================================================
 */
#include <nowdb/reader/reader.h>

#include <stdarg.h>

static char *OBJECT = "reader";

/* ------------------------------------------------------------------------
 * Reader Types
 * ------------------------------------------------------------------------
 */
#define NOWDB_READER_FULLSCAN 1
#define NOWDB_READER_SEARCH   10
#define NOWDB_READER_FRANGE   100
#define NOWDB_READER_KRANGE   101
#define NOWDB_READER_CRANGE   102
#define NOWDB_READER_BUF      1000

#define BEETERR(x,f) \
	if (x == BEET_ERR_EOF) { \
		if (f) reader->eof = 1; \
		return nowdb_err_get(nowdb_err_eof, FALSE, OBJECT, NULL); \
	} else if (x != BEET_OK) { \
		return makeBeetError(x); \
	}

#define NOMEM(x) \
	err = nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT, x);

/* ------------------------------------------------------------------------
 * Helper: initialise an already allocated reader
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t initReader(nowdb_reader_t *reader) {
	reader->type = 0;
	reader->recsize = 0;
	reader->size = 0;
	reader->off = 0;
	reader->nodata = 0;
	reader->eof = 0;
	reader->ko  = 0;
	reader->buf = NULL;
	reader->tmp = NULL;
	reader->tmp2 = NULL;
	reader->page2 = NULL;
	reader->plru = NULL;
	reader->bplru = NULL;
	reader->page = NULL;
	reader->key = NULL;
	reader->start = NULL;
	reader->end = NULL;
	reader->filter = NULL;
	reader->iter  = NULL;
	reader->state = NULL;
	reader->current = NULL;
	reader->file = NULL;
	reader->cont = NULL;
	reader->ikeys = NULL;
	reader->from = NOWDB_TIME_DAWN;
	reader->to   = NOWDB_TIME_DUSK;

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: allocate and initialise a new reader
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t newReader(nowdb_reader_t **reader) {
	nowdb_err_t err;

	*reader = malloc(sizeof(nowdb_reader_t));
	if (*reader == NULL) return nowdb_err_get(nowdb_err_no_mem,
	                        FALSE, OBJECT, "allocating reader");
	err = initReader(*reader);
	if (err != NOWDB_OK) {
		free(*reader); return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: make beet error
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t makeBeetError(beet_err_t ber) {
	nowdb_err_t err, err2=NOWDB_OK;

	if (ber == BEET_OK) return NOWDB_OK;
	if (ber < BEET_OSERR_ERRNO) {
		err2 = nowdb_err_get(nowdb_err_beet, FALSE, OBJECT,
		                          (char*)beet_oserrdesc());
	}
	err = nowdb_err_get(nowdb_err_beet, FALSE, OBJECT,
	                         (char*)beet_errdesc(ber));
	if (err == NOWDB_OK) return err2; else err->cause = err2;
	return err;
}

/* ------------------------------------------------------------------------
 * Helper: rewind fullscan reader
 * ------------------------------------------------------------------------
 */
nowdb_err_t rewindFullscan(nowdb_reader_t *reader) {
	nowdb_err_t err;

	reader->current = reader->files->head;
	if (reader->current == NULL) return nowdb_err_get(nowdb_err_eof,
	                                           FALSE, OBJECT, NULL);
	reader->closeit = FALSE;
	reader->page = NULL; 
	reader->cont = NULL; 
	reader->off = 0; 
	reader->eof = 0; 

	if (((nowdb_file_t*)reader->current->cont)->state ==
	      nowdb_file_state_closed) {
		err = nowdb_file_open(reader->current->cont);
		if (err != NOWDB_OK) return err;
		reader->closeit = TRUE;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: rewind search reader
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t rewindSearch(nowdb_reader_t *reader) {
	beet_err_t  ber;
	nowdb_err_t err;

	if (reader->file != NULL) {
		if (reader->closeit) {
			err = nowdb_file_close(reader->file);
			if (err != NOWDB_OK) return err;
		}
	}

	reader->closeit = FALSE;
	reader->page = NULL; 
	reader->cont = NULL; 
	reader->file = NULL;
	reader->off = 0; 
	reader->eof = 0; 

	ber = beet_iter_reset(reader->iter);
	if (ber != BEET_OK) {
		return makeBeetError(ber);
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: rewind range reader
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t rewindRange(nowdb_reader_t *reader) {
	return rewindSearch(reader);
}

/* ------------------------------------------------------------------------
 * Helper: rewind buffer
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t rewindBuffer(nowdb_reader_t *reader) {
	reader->off = -1;
	reader->eof = 0; 
	reader->key = NULL;
	reader->cont= NULL;
	reader->page = NULL;
	if (reader->page2 != NULL) {
		memset(reader->page2, 0, NOWDB_IDX_PAGE);
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: rewind seq
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t rewindSeq(nowdb_reader_t *reader) {
	nowdb_err_t    err;

	reader->cur = 0;
	reader->cont = NULL;
	reader->key = NULL;
	reader->eof = 0; 

	for(int i=0; i<reader->nr; i++) {
		err = nowdb_reader_rewind(reader->sub[i]);
		if (err != NOWDB_OK) return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: rewind merge
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t rewindMerge(nowdb_reader_t *reader) {
	nowdb_err_t    err;

	reader->key = NULL;
	reader->cont = NULL;
	reader->eof = 0; 
	reader->moved = 0xffffffff;

	for(int i=0; i<reader->nr; i++) {
		err = nowdb_reader_rewind(reader->sub[i]);
		if (err != NOWDB_OK) return err;
		err = nowdb_reader_move(reader->sub[i]);
		if (err != NOWDB_OK) {
			if (err->errcode == nowdb_err_eof) {
				nowdb_err_release(err);
				continue;
			}
			return err;
		}
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Destroy
 * ------------------------------------------------------------------------
 */
void nowdb_reader_destroy(nowdb_reader_t *reader) {
	if (reader == NULL) return;

	if (reader->bplru != NULL) {
		nowdb_pplru_destroy(reader->bplru);
		free(reader->bplru);
		reader->bplru = NULL;
	}

	switch(reader->type) {

	case NOWDB_READER_FULLSCAN:
		reader->files = NULL;
		return;

	case NOWDB_READER_SEARCH:
		reader->files = NULL;
		if (reader->iter != NULL) {
			beet_iter_destroy(reader->iter);
			reader->iter = NULL;
		}
		if (reader->state != NULL) {
			beet_state_release(reader->state);
			beet_state_destroy(reader->state);
			reader->state = NULL;
		}
		if (reader->file != NULL) {
			NOWDB_IGNORE(nowdb_file_close(reader->file));
			reader->file = NULL;
		}
		return;

	case NOWDB_READER_FRANGE:
	case NOWDB_READER_KRANGE:
	case NOWDB_READER_CRANGE:
		reader->files = NULL;
		if (reader->iter != NULL) {
			beet_iter_destroy(reader->iter);
			reader->iter = NULL;
		}
		if (reader->file != NULL) {
			NOWDB_IGNORE(nowdb_file_close(reader->file));
			reader->file = NULL;
		}
		if (reader->plru != NULL) {
			nowdb_pplru_destroy(reader->plru);
			free(reader->plru);
			reader->plru = NULL;
		}
		if (reader->buf != NULL) {
			free(reader->buf); reader->buf = NULL;
		}
		return;

	case NOWDB_READER_BUF:
	case NOWDB_READER_BUFIDX:
	case NOWDB_READER_BKRANGE:
	case NOWDB_READER_BCRANGE:
		if (reader->buf != NULL) {
			free(reader->buf); reader->buf = NULL;
		}
		if (reader->tmp != NULL) {
			free(reader->tmp); reader->tmp = NULL;
		}
		if (reader->tmp2 != NULL) {
			free(reader->tmp2); reader->tmp2 = NULL;
		}
		if (reader->page2 != NULL) {
			free(reader->page2); reader->page2 = NULL;
		}
		return;

	case NOWDB_READER_SEQ:
	case NOWDB_READER_MERGE:
		if (reader->sub != NULL) {
			for(int i=0; i<reader->nr; i++) {
				if (reader->sub[i] != NULL) {
					nowdb_reader_destroy(reader->sub[i]);
					free(reader->sub[i]);
					reader->sub[i] = NULL;
				}
			}
			free(reader->sub); reader->sub = NULL;
		}
		if (reader->tmp != NULL) {
			free(reader->tmp); reader->tmp = NULL;
		}
		return;

	default:
		return;
	}
}

/* ------------------------------------------------------------------------
 * Helper: switch page
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t nextpage(nowdb_reader_t *reader) {
	nowdb_err_t err;

	err = nowdb_file_movePeriod(reader->current->cont,
                                 reader->from, reader->to);
	if (err == NOWDB_OK) {
		reader->page = ((nowdb_file_t*)reader->current->cont)->bptr;
		return NOWDB_OK;
	}

	if (err->errcode != nowdb_err_eof) return err;
	nowdb_err_release(err);

	if (reader->closeit) {
		err = nowdb_file_close(reader->current->cont);
		if (err != NOWDB_OK) return err;
	}

	reader->current = reader->current->nxt;
	if (reader->current == NULL) return nowdb_err_get(nowdb_err_eof,
	                                           FALSE, OBJECT, NULL);
	
	if (((nowdb_file_t*)reader->current->cont)->state ==
	      nowdb_file_state_closed) {
		err = nowdb_file_open(reader->current->cont);
		if (err != NOWDB_OK) return err;
		reader->closeit = TRUE;
	} else {
		reader->closeit = FALSE;
	}

	err = nowdb_file_rewind(reader->current->cont);
	if (err != NOWDB_OK) return err;

	err = nowdb_file_movePeriod(reader->current->cont,
	                         reader->from, reader->to);
	if (err != NOWDB_OK) return err;

	reader->page = ((nowdb_file_t*)reader->current->cont)->bptr;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: get fileid and pos from pageid
 * ------------------------------------------------------------------------
 */
static inline void getFilePos(nowdb_pageid_t  pge,
                              nowdb_fileid_t *fid,
                              uint32_t       *pos) {
	nowdb_pageid_t tmp;
	*fid = (uint32_t)(pge >> 32);
	tmp = pge << 32;
	*pos = (uint32_t)(tmp >> 32);
}

/* ------------------------------------------------------------------------
 * Helper: find file in list (not optimal)
 * ------------------------------------------------------------------------
 */
static inline nowdb_file_t *findfile(ts_algo_list_t *list,
                                     nowdb_fileid_t   fid) {
	ts_algo_list_node_t *runner;
	nowdb_file_t        *file;

	for(runner=list->head;runner!=NULL;runner=runner->nxt) {
		file = runner->cont;
		if (file->id == fid) return file;
	}
	return NULL;
}

/* ------------------------------------------------------------------------
 * Helper: switch page
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t getpage(nowdb_reader_t *reader, nowdb_pageid_t pge) {
	nowdb_err_t err;
	nowdb_fileid_t fid;
	uint32_t pos;
	char w=1;

	if (reader->plru != NULL) {
		err = nowdb_pplru_get(reader->plru, pge, &reader->page);
		if (err != NOWDB_OK) return err;
		if (reader->page != NULL) return NOWDB_OK;
	}

	getFilePos(pge, &fid, &pos);

	// fprintf(stderr, "file: %u/%u (%lu)\n", fid, pos, pge);

	if (reader->file != NULL) {
		if (reader->file->id != fid) {
			err = nowdb_file_close(reader->file);
			if (err != NOWDB_OK) return err;
			reader->file = NULL;
		}
	}
	if (reader->file == NULL) {
		reader->file = findfile(reader->files, fid);
		if (reader->file == NULL) {
			// fprintf(stderr, "file not found %u\n", fid);
			return nowdb_err_get(nowdb_err_key_not_found,
			                         FALSE, OBJECT, NULL);
		}
		err = nowdb_file_open(reader->file);
		if (err != NOWDB_OK) return err;
	}

	err = nowdb_file_position(reader->file, pos);
	if (err != NOWDB_OK) return err;

	/* this is sloppy */
	err = nowdb_file_worth(reader->file, reader->from, reader->to, &w);
	if (err != NOWDB_OK) return err;
	if (!w) {
		// fprintf(stderr, "###IGNORED###\n");
		return nowdb_err_get(nowdb_err_key_not_found,
	                                 FALSE, OBJECT, NULL);
	}

	err = nowdb_file_loadBlock(reader->file);
	if (err != NOWDB_OK) return err;

	reader->page = reader->file->bptr;

	if (reader->plru != NULL) {
		err = nowdb_pplru_add(reader->plru, pge, reader->page);
		if (err != NOWDB_OK) return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: move for fullscan
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t moveFullscan(nowdb_reader_t *reader) {
	nowdb_err_t err = NOWDB_OK;

	if (reader->eof) {
		return nowdb_err_get(nowdb_err_eof,
	                      FALSE, OBJECT, NULL);
	}
	if (reader->current == NULL) {
		reader->eof = 1;
		return nowdb_err_get(nowdb_err_eof,
	                      FALSE, OBJECT, NULL);
	}
	/* test getpage for fullscan ! */
	err = nextpage(reader);
	if (err != NOWDB_OK) {
		if (err->errcode == nowdb_err_eof) reader->eof = 1;
	}
	return err;
}

/* ------------------------------------------------------------------------
 * Helper: move for search
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t moveSearch(nowdb_reader_t *reader) {
	nowdb_err_t     err;
	nowdb_pageid_t *pge;
	beet_err_t      ber;

	if (reader->eof) return nowdb_err_get(nowdb_err_eof,
	                                FALSE, OBJECT, NULL);

	for(;;) {
		ber = beet_iter_move(reader->iter, (void**)&pge,
		                          (void**)&reader->cont);
		BEETERR(ber,1);

		/*
		fprintf(stderr, "content: %lu|%lu (%lu)\n",
		                           reader->cont[0],
		                           reader->cont[1],
		                                     *pge);
		*/

		err = getpage(reader, *pge);
		if (err == NOWDB_OK) break;
		if (err->errcode == nowdb_err_key_not_found) {
			nowdb_err_release(err); continue;
		}
		if (err->errcode == nowdb_err_eof) reader->eof = 1;
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: move for range, reading all pages
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t moveFRange(nowdb_reader_t *reader) {
	nowdb_err_t err;
	beet_err_t  ber;
	nowdb_pageid_t *pge;

	if (reader->eof) {
		return nowdb_err_get(nowdb_err_eof,
	                       FALSE, OBJECT, NULL);
	}
	if (reader->key == NULL) {
		ber = beet_iter_move(reader->iter, &reader->key, NULL);
		BEETERR(ber,1);
		ber = beet_iter_enter(reader->iter);
		BEETERR(ber,0);
	}
	for(;;) {
		ber = beet_iter_move(reader->iter, (void**)&pge,
		                          (void**)&reader->cont);
		while (ber == BEET_ERR_EOF) {
			// fprintf(stderr, "in inner loop\n");
			ber = beet_iter_leave(reader->iter);
			BEETERR(ber,0);
			
			ber = beet_iter_move(reader->iter,
			               &reader->key, NULL);
			BEETERR(ber,1);

			// fprintf(stderr, "key: %lu\n", *(uint64_t*)(reader->key+8));

			ber = beet_iter_enter(reader->iter);
			if (ber == BEET_ERR_EOF) continue;
			BEETERR(ber,0);

			ber = beet_iter_move(reader->iter,
			                    (void**)&pge,
			                    (void**)&reader->cont);
			if (ber == BEET_ERR_EOF) continue;
			BEETERR(ber,0);
			break;
		}
		BEETERR(ber,0);

		err = getpage(reader, *pge);
		// fprintf(stderr, "%lu: %p\n", *pge, reader->page);
		if (err == NOWDB_OK) break;
		if (err->errcode == nowdb_err_key_not_found) {
			nowdb_err_release(err); continue;
		}
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: move for range, reading keys only
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t moveKRange(nowdb_reader_t *reader) {
	beet_err_t  ber;
	ber = beet_iter_move(reader->iter, &reader->key, NULL);
	BEETERR(ber,1);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: move for range, reading keys and content (bitmap)
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t moveCRange(nowdb_reader_t *reader) {
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: move for buffer
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t moveBuf(nowdb_reader_t *reader) {
	if (reader->eof) {
		return nowdb_err_get(nowdb_err_eof, FALSE, OBJECT, NULL);
	}
	if (reader->size == 0) {
		reader->eof = 1;
		return nowdb_err_get(nowdb_err_eof, FALSE, OBJECT, NULL);
	}
	if (reader->off < 0) {
		reader->off = 0;
		return NOWDB_OK;
	}
	if (reader->off + NOWDB_IDX_PAGE >= reader->size) {
		reader->eof = 1;
		return nowdb_err_get(nowdb_err_eof, FALSE, OBJECT, NULL);
	}
	reader->off += NOWDB_IDX_PAGE;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: move for bufidx
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t moveBufIdx(nowdb_reader_t *reader) {
	char x;
	char *p;

	if (reader->eof) {
		return nowdb_err_get(nowdb_err_eof, FALSE, OBJECT, NULL);
	}
	if (reader->size == 0) {
		reader->eof = 1;
		return nowdb_err_get(nowdb_err_eof, FALSE, OBJECT, NULL);
	}
	if (reader->off < 0) {
		reader->off = 0;
	} else if (reader->off >= reader->size) {
		reader->eof = 1;
		return nowdb_err_get(nowdb_err_eof, FALSE, OBJECT, NULL);
	}
	if (reader->key != NULL) {
		if (reader->recsize == NOWDB_EDGE_SIZE) {
			nowdb_index_grabEdgeKeys(reader->ikeys,
		                       reader->buf+reader->off,
			                         reader->tmp2);
		        x = nowdb_index_edge_compare(reader->tmp,
			                             reader->tmp2,
			                             reader->ikeys);
		} else {
			nowdb_index_grabVertexKeys(reader->ikeys,
		                         reader->buf+reader->off,
			                           reader->tmp2);
		        x = nowdb_index_edge_compare(reader->tmp,
			                             reader->tmp2,
			                             reader->ikeys);
		}
		if (x != BEET_CMP_EQUAL) {
			memcpy(reader->tmp, reader->tmp2,
			             reader->ikeys->sz*8);
		}
	} else {
		if (reader->recsize == NOWDB_EDGE_SIZE) {
			nowdb_index_grabEdgeKeys(reader->ikeys,
		                       reader->buf+reader->off,
			                           reader->tmp);
		} else {
			nowdb_index_grabVertexKeys(reader->ikeys,
		                         reader->buf+reader->off,
			                            reader->tmp);
		}
		reader->key = reader->tmp;
	}
	p = reader->buf+reader->off;
	memset(reader->page2, 0, NOWDB_IDX_PAGE);
	for(int i=0; i<NOWDB_IDX_PAGE; i+=reader->recsize) {
		if (reader->off >= reader->size) break;
		if (nowdb_sort_edge_keys_compare(p,
		          reader->buf+reader->off,
		          reader->ikeys) != NOWDB_SORT_EQUAL)  break;

		if (reader->type == NOWDB_READER_BUFIDX  ||
		   (reader->type == NOWDB_READER_BKRANGE && i == 0)) {
			memcpy(reader->page2+i,
			       reader->buf+reader->off,
			       reader->recsize);
		}
		reader->off += reader->recsize;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: find next key
 * ------------------------------------------------------------------------
 */
#define KEYCMP(k1, k2) \
	reader->recsize == NOWDB_EDGE_SIZE ? \
	                   nowdb_index_edge_compare(k1, k2, reader->ikeys): \
	                   nowdb_index_vertex_compare(k1, k2, reader->ikeys)

static inline nowdb_err_t findNext(nowdb_reader_t *reader) {
	nowdb_err_t err;
	char x;
	void *key=NULL;
	char found = 0;

	if (reader->eof) {
		return nowdb_err_get(nowdb_err_eof, FALSE, OBJECT, NULL);
	}

	reader->cur = 0;

	for(int i=0; i<reader->nr; i++) {
		if (reader->sub[i]->eof) continue;
		found = 1;
		if (reader->key != NULL) {
			x = KEYCMP(reader->sub[i]->key,
			           reader->key);
		}
		if (reader->key == NULL || x != BEET_CMP_LESS) {
			if (key == NULL) {
				key = reader->sub[i]->key;
				reader->cur = i;
			} else {
				x = KEYCMP(reader->sub[i]->key, key);
			        if (x == BEET_CMP_LESS) {
					key = reader->sub[i]->key;
					reader->cur = i;
				}
			}
		}
	}
	if (key == NULL || !found) {
		reader->eof = 1;
		return nowdb_err_get(nowdb_err_eof,
	                       FALSE, OBJECT, NULL);
	}
	if (reader->key == NULL) {
		reader->tmp = calloc(reader->ikeys->sz, 8);
		if (reader->tmp == NULL) {
			NOMEM("allocating keys");
			return err;
		}
		reader->key = reader->tmp;
	}
	memcpy(reader->key, key, reader->ikeys->sz*8);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: move seq
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t moveSeq(nowdb_reader_t *reader) {
	nowdb_err_t err;

	if (reader->eof) return nowdb_err_get(nowdb_err_eof,
		                        FALSE, OBJECT, NULL);
	
	for(;;) {
		if (reader->cur >= reader->nr) {
			reader->eof = 1;
			return nowdb_err_get(nowdb_err_eof,
			               FALSE, OBJECT, NULL);
		}
		err = nowdb_reader_move(reader->sub[reader->cur]);
		if (err == NOWDB_OK) break;
		if (err->errcode == nowdb_err_eof) {
			nowdb_err_release(err);
			reader->cur++;
			continue;
		}
		return err;
	}
	reader->cont = reader->sub[reader->cur]->cont;
	reader->key  = reader->sub[reader->cur]->key;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: move merge
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t moveMerge(nowdb_reader_t *reader) {
	nowdb_err_t err;

	for(;;) {
		err = findNext(reader);
		if (err != NOWDB_OK) return err;

		if (reader->moved & (1 << reader->cur)) {
			reader->moved ^= (1 << reader->cur);
			break;
		}
		reader->moved |= (1 << reader->cur);
		err = nowdb_reader_move(reader->sub[reader->cur]);
		if (err != NOWDB_OK) {
			if (err->errcode == nowdb_err_eof) {
				nowdb_err_release(err);
				continue;
			}
			return err;
		}
	}

	/*
	fprintf(stderr, "FOUND    : %lu %d: %u (%lu / %lu) -- %p\n",
	                                    *(uint64_t*)reader->key,
	                                 reader->cur, reader->moved,
		             reader->sub[reader->cur]->cont == NULL?
	                        0:reader->sub[reader->cur]->cont[0],
		             reader->sub[reader->cur]->cont == NULL?
	                        0:reader->sub[reader->cur]->cont[1],
	                nowdb_reader_page(reader->sub[reader->cur]));
	*/

	reader->cont = reader->sub[reader->cur]->cont;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Move
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_move(nowdb_reader_t *reader) {
	if (reader == NULL) return nowdb_err_get(nowdb_err_invalid,
	                    FALSE, OBJECT, "reader object is NULL");

	reader->cont = NULL;
	reader->page = NULL;

	switch(reader->type) {
	case NOWDB_READER_FULLSCAN:
		return moveFullscan(reader);

	case NOWDB_READER_SEARCH:
		return moveSearch(reader);

	case NOWDB_READER_FRANGE:
		return moveFRange(reader);

	case NOWDB_READER_KRANGE:
		return moveKRange(reader);

	case NOWDB_READER_CRANGE:
		return moveCRange(reader);

	case NOWDB_READER_BUF:
		return moveBuf(reader);

	case NOWDB_READER_BUFIDX:
	case NOWDB_READER_BKRANGE:
		return moveBufIdx(reader);

	case NOWDB_READER_SEQ:
		return moveSeq(reader);

	case NOWDB_READER_MERGE:
		return moveMerge(reader);

	default:
		return nowdb_err_get(nowdb_err_panic, FALSE, OBJECT,
		                      "unkown reader type in move");
	}
}

/* ------------------------------------------------------------------------
 * Rewind
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_rewind(nowdb_reader_t *reader) {
	if (reader == NULL) return nowdb_err_get(nowdb_err_invalid,
	                    FALSE, OBJECT, "reader object is NULL");

	if (reader->nodata) return NOWDB_OK;

	switch(reader->type) {
	case NOWDB_READER_FULLSCAN:
		return rewindFullscan(reader);

	case NOWDB_READER_SEARCH:
		return rewindSearch(reader);

	case NOWDB_READER_FRANGE:
	case NOWDB_READER_KRANGE:
	case NOWDB_READER_CRANGE:
		return rewindRange(reader);

	case NOWDB_READER_BUF:
	case NOWDB_READER_BUFIDX:
	case NOWDB_READER_BKRANGE:
		return rewindBuffer(reader);

	case NOWDB_READER_SEQ:
		return rewindSeq(reader);

	case NOWDB_READER_MERGE:
		return rewindMerge(reader);

	default:
		return nowdb_err_get(nowdb_err_panic, FALSE, OBJECT,
		                    "unkown reader type in rewind");
	}
}

/* ------------------------------------------------------------------------
 * Page
 * ------------------------------------------------------------------------
 */
char *nowdb_reader_page(nowdb_reader_t *reader) {
	int sz, off=0;
	switch(reader->type) {
	case NOWDB_READER_FULLSCAN:
	case NOWDB_READER_SEARCH:
	case NOWDB_READER_FRANGE: return reader->page;

	case NOWDB_READER_KRANGE:
	case NOWDB_READER_CRANGE:
		for(int i=0; i<reader->ikeys->sz; i++) {
			sz = nowdb_sizeByOff(reader->recsize,
			                     reader->ikeys->off[i]);
			memcpy(reader->buf+reader->ikeys->off[i],
			           (char*)(reader->key)+off, sz);
			off+=sz;
		}
		return reader->buf;

	case NOWDB_READER_BUF:
		if (reader->off >= reader->size) return NULL;
		return reader->buf+reader->off;

	case NOWDB_READER_BUFIDX:
	case NOWDB_READER_BKRANGE:
		return reader->page2;

	case NOWDB_READER_SEQ:
	case NOWDB_READER_MERGE:
		if (reader->cur >= reader->nr) return NULL;
		return nowdb_reader_page(
		             reader->sub[reader->cur]);

	default:
		return NULL;
	}
}

/* ------------------------------------------------------------------------
 * Pageid
 * ------------------------------------------------------------------------
 */
nowdb_pageid_t nowdb_reader_pageid(nowdb_reader_t *reader);

/* ------------------------------------------------------------------------
 * Key
 * ------------------------------------------------------------------------
 */
char *nowdb_reader_key(nowdb_reader_t *reader);

/* ------------------------------------------------------------------------
 * SkipKey
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_skipKey(nowdb_reader_t *reader);

/* ------------------------------------------------------------------------
 * Read
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_read(nowdb_reader_t *reader,
                              char           *buf,
                              uint32_t        size,
                              uint32_t       *osize);

/* ------------------------------------------------------------------------
 * Set Period
 * ------------------------------------------------------------------------
 */
void nowdb_reader_setPeriod(nowdb_reader_t *reader,
                            nowdb_time_t     start,
                            nowdb_time_t       end) {
	reader->from = start;
	reader->to   = end;
}

/* ------------------------------------------------------------------------
 * Fullscan
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_fullscan(nowdb_reader_t **reader,
                                  ts_algo_list_t  *files,
                                  nowdb_filter_t  *filter) {
	nowdb_err_t err;
	if (reader == NULL) return nowdb_err_get(nowdb_err_invalid,
	        FALSE, OBJECT, "pointer to reader object is NULL");
	if (files == NULL) return nowdb_err_get(nowdb_err_invalid,
	                   FALSE, OBJECT, "files object is NULL");
	if (files->head == NULL) return nowdb_err_get(nowdb_err_eof,
	                                       FALSE, OBJECT, NULL);
	if (files->head->cont == NULL) return nowdb_err_get(
	     nowdb_err_invalid, FALSE, OBJECT, "empty list");
	err = newReader(reader);
	if (err != NOWDB_OK) return err;
	(*reader)->type = NOWDB_READER_FULLSCAN;
	(*reader)->filter = filter;
	(*reader)->recsize = ((nowdb_file_t*)files->head->cont)->recordsize;
	(*reader)->files = files;

	err = rewindFullscan(*reader);
	if (err != NOWDB_OK) {
		nowdb_reader_destroy(*reader); free(*reader);
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Index Search
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_search(nowdb_reader_t **reader,
                                ts_algo_list_t  *files,
                                nowdb_index_t   *index,
                                char            *key,
                                nowdb_filter_t  *filter) {
	nowdb_err_t err;
	beet_err_t  ber;

	if (reader == NULL) return nowdb_err_get(nowdb_err_invalid,
	        FALSE, OBJECT, "pointer to reader object is NULL");
	if (files == NULL) return nowdb_err_get(nowdb_err_invalid,
	                   FALSE, OBJECT, "files object is NULL");
	if (files->head == NULL) return nowdb_err_get(nowdb_err_eof,
	                                       FALSE, OBJECT, NULL);
	if (files->head->cont == NULL) return nowdb_err_get(
	     nowdb_err_invalid, FALSE, OBJECT, "empty list");
	err = newReader(reader);
	if (err != NOWDB_OK) return err;

	(*reader)->type = NOWDB_READER_SEARCH;
	(*reader)->filter = filter;
	(*reader)->recsize = ((nowdb_file_t*)files->head->cont)->recordsize;
	(*reader)->files = files;

	ber = beet_iter_alloc(index->idx, &(*reader)->iter);
	if (ber != BEET_OK) {
		nowdb_reader_destroy(*reader); free(*reader);
		return makeBeetError(ber);
	}

	ber = beet_state_alloc(index->idx, &(*reader)->state);
	if (ber != BEET_OK) {
		nowdb_reader_destroy(*reader); free(*reader);
		return makeBeetError(ber);
	}

	ber = beet_index_getIter(index->idx, (*reader)->state, key,
	                                     (*reader)->iter);
	if (ber == BEET_ERR_KEYNOF) {
		(*reader)->eof = 1;
		(*reader)->nodata = 1;
		return NOWDB_OK;

	} else if (ber != BEET_OK) {
		nowdb_reader_destroy(*reader); free(*reader);
		return makeBeetError(ber);
	}
	err = rewindSearch(*reader);
	if (err != NOWDB_OK) {
		nowdb_reader_destroy(*reader); free(*reader);
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Index Range scan
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t mkRange(nowdb_reader_t **reader,
                                  uint32_t         rtype,
                                  ts_algo_list_t  *files,
                                  nowdb_index_t   *index,
                                  nowdb_filter_t  *filter,
                                  void *start, void *end) {
	nowdb_err_t err;
	beet_err_t  ber;
	beet_range_t range, *rptr=NULL;
	beet_dir_t  dir=BEET_DIR_ASC;
	beet_compare_t cmp;
	void *rsc;

	if (reader == NULL) return nowdb_err_get(nowdb_err_invalid,
	        FALSE, OBJECT, "pointer to reader object is NULL");
	if (files == NULL) return nowdb_err_get(nowdb_err_invalid,
	                   FALSE, OBJECT, "files object is NULL");
	if (files->head == NULL) return nowdb_err_get(nowdb_err_eof,
	                                       FALSE, OBJECT, NULL);
	if (files->head->cont == NULL) return nowdb_err_get(
	     nowdb_err_invalid, FALSE, OBJECT, "empty list");

	err = newReader(reader);
	if (err != NOWDB_OK) return err;

	(*reader)->type = rtype;
	(*reader)->filter = filter;
	(*reader)->recsize = ((nowdb_file_t*)files->head->cont)->recordsize;
	(*reader)->files = files;
	(*reader)->ikeys = nowdb_index_getResource(index);
	(*reader)->eof = 0;
	(*reader)->ko  = rtype == NOWDB_READER_KRANGE;

	if (rtype == NOWDB_READER_FRANGE) {
		(*reader)->plru = calloc(1, sizeof(nowdb_pplru_t));
		if ((*reader)->plru == NULL) {
			nowdb_reader_destroy(*reader); free(*reader);
			NOMEM("allocating page lru");
			return err;
		}

		err = nowdb_pplru_init((*reader)->plru, 100000);
		if (err != NOWDB_OK) {
			free((*reader)->plru); (*reader)->plru = NULL;
			nowdb_reader_destroy(*reader); free(*reader);
			return err;
		}
	} else {
		(*reader)->buf = calloc(1, (*reader)->recsize);
		if ((*reader)->buf == NULL) {
			nowdb_reader_destroy(*reader); free(*reader);
			NOMEM("allocating buffer");
			return err;
		}
	}

	ber = beet_iter_alloc(index->idx, &(*reader)->iter);
	if (ber != BEET_OK) {
		nowdb_reader_destroy(*reader); free(*reader);
		return makeBeetError(ber);
	}

	(*reader)->start = start;
	(*reader)->end = end;

	if (start != NULL && end != NULL) {
		range.fromkey = start;
		range.tokey = end;
		rptr = &range;
		cmp = nowdb_index_getCompare(index);
		rsc = nowdb_index_getResource(index);
		dir = cmp(start, end, rsc) == BEET_CMP_GREATER?BEET_DIR_ASC:
		                                               BEET_DIR_DESC;
	}

	ber = beet_index_range(index->idx, rptr, dir,
	                             (*reader)->iter);
	if (ber == BEET_ERR_KEYNOF) {
		(*reader)->eof = 1;
		(*reader)->nodata = 1;
		return NOWDB_OK;

	} else if (ber != BEET_OK) {
		nowdb_reader_destroy(*reader); free(*reader);
		return makeBeetError(ber);
	}

	/*
	err = rewindRange(*reader);
	if (err != NOWDB_OK) {
		nowdb_reader_destroy(*reader); free(*reader);
		return err;
	}
	*/
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Index Full Range scan
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_frange(nowdb_reader_t **reader,
                                ts_algo_list_t  *files,
                                nowdb_index_t   *index,
                                nowdb_filter_t  *filter,
                                void *start, void *end) {
	return mkRange(reader, NOWDB_READER_FRANGE,
	                      files, index, filter,
	                                start, end);
}

/* ------------------------------------------------------------------------
 * Index Key Range scan
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_krange(nowdb_reader_t **reader,
                                ts_algo_list_t  *files,
                                nowdb_index_t   *index,
                                nowdb_filter_t  *filter,
                                void *start, void *end) {
	return mkRange(reader, NOWDB_READER_KRANGE,
	                      files, index, filter,
	                                start, end);
}

/* ------------------------------------------------------------------------
 * Index Count Range scan
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_crange(nowdb_reader_t **reader,
                                ts_algo_list_t  *files,
                                nowdb_index_t   *index,
                                nowdb_filter_t  *filter,
                                void *start, void *end) {
	return mkRange(reader, NOWDB_READER_CRANGE,
	                      files, index, filter,
	                                start, end);
}

/* ------------------------------------------------------------------------
 * Helper: compute size of all files
 * ------------------------------------------------------------------------
 */
static inline uint64_t countBytes(ts_algo_list_t *files) {
	ts_algo_list_node_t *runner;
	nowdb_file_t        *file;
	uint64_t total=0;

	for(runner=files->head; runner!=NULL; runner=runner->nxt) {
		file = runner->cont;
		total += file->size;
	}
	return total;
}

/* ------------------------------------------------------------------------
 * Helper: scan all files
 * ------------------------------------------------------------------------
 */
static nowdb_err_t fillbuf(nowdb_reader_t *reader) {
	nowdb_err_t err = NOWDB_OK;
	nowdb_reader_t *full;
	char *src;
	uint32_t x=0;
	char more = 1;

	err = nowdb_reader_fullscan(&full, reader->files, reader->filter);
	if (err != NOWDB_OK) return err;

	while (more) {
		err = nowdb_reader_move(full);
		if (err != NOWDB_OK) {
			if (err->errcode == nowdb_err_eof) {
				nowdb_err_release(err);
				more = 0; err = NOWDB_OK;
			}
			break;
		}
		src = nowdb_reader_page(full);
		for(int i=0; i<NOWDB_IDX_PAGE; i+=reader->recsize) {

			/* we hit the nullrecord */
			if (memcmp(src+i, nowdb_nullrec,
			          reader->recsize) == 0)
				break;

			/* apply filter */
			if (reader->filter != NULL &&
			    !nowdb_filter_eval(reader->filter, src+i))
				continue;

			memcpy(reader->buf+x, src+i, reader->recsize);
			x+=reader->recsize;
		}
	}
	reader->size = x;
	nowdb_reader_destroy(full); free(full);
	return err;
}

/* ------------------------------------------------------------------------
 * Buffer scan
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_buffer(nowdb_reader_t  **reader,
                                ts_algo_list_t   *files,
                                nowdb_filter_t   *filter) {
	nowdb_err_t err;
	uint64_t sz;

	if (reader == NULL) return nowdb_err_get(nowdb_err_invalid,
	        FALSE, OBJECT, "pointer to reader object is NULL");
	if (files == NULL) return nowdb_err_get(nowdb_err_invalid,
	                   FALSE, OBJECT, "files object is NULL");
	if (files->head == NULL) return nowdb_err_get(nowdb_err_eof,
	                                       FALSE, OBJECT, NULL);
	if (files->head->cont == NULL) return nowdb_err_get(
	     nowdb_err_invalid, FALSE, OBJECT, "empty list");

	/* count bytes */
	sz = countBytes(files);
	if (sz > NOWDB_GIGA) {
		return nowdb_err_get(nowdb_err_invalid, FALSE, OBJECT,
		                             "too many pending files");
	} else if (sz == 0) {
		sz = NOWDB_IDX_PAGE;
	} else {
		uint32_t tmp = sz / NOWDB_IDX_PAGE;
		tmp *= NOWDB_IDX_PAGE;
		if (tmp < sz) tmp += NOWDB_IDX_PAGE;
	}

	err = newReader(reader);
	if (err != NOWDB_OK) return err;

	(*reader)->type = NOWDB_READER_BUF;
	(*reader)->filter = filter;
	(*reader)->recsize = ((nowdb_file_t*)files->head->cont)->recordsize;
	(*reader)->files = files;

	/* alloc buffer */
	(*reader)->buf = calloc(1,sz);
	if ((*reader)->buf == NULL) {
		nowdb_reader_destroy(*reader); free(*reader);
		NOMEM("allocating buffer");
		return err;
	}

	/* fullscan files according to filter */
	err = fillbuf(*reader);
	if (err != NOWDB_OK) {
		nowdb_reader_destroy(*reader); free(*reader);
		return err;
	}
	return nowdb_reader_rewind(*reader);
}

/* ------------------------------------------------------------------------
 * Buffer simulating an index range scan
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_bufidx(nowdb_reader_t  **reader,
                                ts_algo_list_t   *files,
                                nowdb_index_t    *index,
                                nowdb_filter_t   *filter,
                                nowdb_ord_t        ord,
                                void *start,void  *end) {
	nowdb_err_t err;

	if (index == NULL) return nowdb_err_get(nowdb_err_invalid,
	                          FALSE, OBJECT, "index is NULL");

	err = nowdb_reader_buffer(reader, files, filter);
	if (err != NOWDB_OK) return err;

	(*reader)->type = NOWDB_READER_BUFIDX;
	(*reader)->start = start;
	(*reader)->end = end;
	(*reader)->ord = ord;
	(*reader)->eof = 0;

	(*reader)->page2 = calloc(1, NOWDB_IDX_PAGE);
	if ((*reader)->page2 == NULL) {
		NOMEM("allocating secondary page");
		return err;
	}

	(*reader)->ikeys = nowdb_index_getResource(index);
	(*reader)->tmp = calloc((*reader)->ikeys->sz, 8);
	if ((*reader)->tmp == NULL) {
		nowdb_reader_destroy(*reader); free(*reader);
		NOMEM("allocating key");
		return err;
	}
	(*reader)->tmp2 = calloc((*reader)->ikeys->sz, 8);
	if ((*reader)->tmp2 == NULL) {
		nowdb_reader_destroy(*reader); free(*reader);
		NOMEM("allocating key");
		return err;
	}
	nowdb_mem_sort((*reader)->buf,
		       (*reader)->size/(*reader)->recsize,
		       (*reader)->recsize,
		       (*reader)->recsize == NOWDB_EDGE_SIZE ?
		               &nowdb_sort_edge_keys_compare :
		               &nowdb_sort_vertex_keys_compare,
		       (*reader)->ikeys);

	return nowdb_reader_rewind(*reader);
}

/* ------------------------------------------------------------------------
 * KBuffer simulating an index range scan (keys only)
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_bkrange(nowdb_reader_t  **reader,
                                 ts_algo_list_t   *files,
                                 nowdb_index_t    *index,
                                 nowdb_filter_t   *filter,
                                 nowdb_ord_t        ord,
                                 void *start,void  *end) {
	nowdb_err_t err;

	err = nowdb_reader_bufidx(reader, files, index,
	                       filter, ord, start, end);
	if (err != NOWDB_OK) return err;

	(*reader)->type = NOWDB_READER_BKRANGE;
	(*reader)->ko  = 1;

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Helper: create higher-order reader
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t mkMulti(nowdb_reader_t **reader,
                                  int type, uint32_t nr,
                                  va_list args) {

	nowdb_err_t err;

	if (reader == NULL) return nowdb_err_get(nowdb_err_invalid,
	        FALSE, OBJECT, "pointer to reader object is NULL");

	err = newReader(reader);
	if (err != NOWDB_OK) return err;

	(*reader)->type = type;
	(*reader)->nr = nr;
	(*reader)->cur = 0;
	(*reader)->key = NULL;
	(*reader)->eof = 0;

	(*reader)->sub = calloc(nr, sizeof(nowdb_reader_t*));
	if ((*reader)->sub == NULL) {
		NOMEM("allocating subreaders");
		nowdb_reader_destroy(*reader); free(*reader);
		return err;
	}
	
	for(int i=0; i<nr; i++) {
		(*reader)->sub[i] = (nowdb_reader_t*)va_arg(args,
		                                 nowdb_reader_t*);
		if ((*reader)->sub[i] == NULL) {
			err = nowdb_err_get(nowdb_err_invalid, FALSE,
			                 OBJECT, "subreader is NULL");
		}
		if ((*reader)->sub[i]->ikeys == NULL &&
		    type == NOWDB_READER_MERGE) {
			err = nowdb_err_get(nowdb_err_invalid, FALSE,
			                OBJECT, "not a range reader");
		}
	}

	(*reader)->ikeys = (*reader)->sub[0]->ikeys;
	(*reader)->recsize = (*reader)->sub[0]->recsize;
	(*reader)->ko = (*reader)->sub[0]->ko;

	return nowdb_reader_rewind(*reader);
}
                   
/* ------------------------------------------------------------------------
 * Sequence Reader
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_seq(nowdb_reader_t **reader, uint32_t nr, ...) {
	va_list args;
	nowdb_err_t err;
	va_start(args,nr);
	err = mkMulti(reader, NOWDB_READER_SEQ, nr, args);
	va_end(args);
	return err;
}

/* ------------------------------------------------------------------------
 * Merge Reader
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_merge(nowdb_reader_t **reader, uint32_t nr, ...) {
	va_list args;
	nowdb_err_t err;
	va_start(args,nr);
	err = mkMulti(reader, NOWDB_READER_MERGE, nr, args);
	va_end(args);
	return err;
}
