/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Reader: generic data access interface
 * ========================================================================
 */
#include <nowdb/reader/reader.h>

static char *OBJECT = "reader";

/* ------------------------------------------------------------------------
 * Helper: initialise an new reader
 * ------------------------------------------------------------------------
 */
#define NOWDB_READER_FULLSCAN 1
#define NOWDB_READER_SEARCH   2
#define NOWDB_READER_RANGE    3
#define NOWDB_READER_BUF      4

/* ------------------------------------------------------------------------
 * Helper: initialise an already allocated reader
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t initReader(nowdb_reader_t *reader) {
	reader->type = 0;
	reader->recsize = 0;
	reader->size = 0;
	reader->off = 0;
	reader->buf = NULL;
	reader->page = NULL;
	reader->key = NULL;
	reader->start = NULL;
	reader->end = NULL;
	reader->filter = NULL;
	reader->index = NULL;
	reader->order= NULL;
	reader->current = NULL;

	ts_algo_list_init(&reader->files);

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
 * Helper: allocate and initialise a new reader
 * ------------------------------------------------------------------------
 */
nowdb_err_t rewindFullscan(nowdb_reader_t *reader) {
	reader->current = reader->files.head;
	if (reader->current == NULL) return nowdb_err_get(nowdb_err_eof,
	                                           FALSE, OBJECT, NULL);
	reader->page = NULL; 
	reader->off = 0; 
	return nowdb_file_open(reader->current->cont);
}

/* ------------------------------------------------------------------------
 * Helper: allocate and initialise a new reader
 * ------------------------------------------------------------------------
 */
static inline void destroyFiles(ts_algo_list_t *files) {
	ts_algo_list_node_t *runner, *tmp;
	runner = files->head;
	while(runner != NULL) {
		nowdb_file_destroy(runner->cont);
		free(runner->cont); tmp = runner->nxt;
		ts_algo_list_remove(files, runner);
		free(runner); runner = tmp;
	}
}

/* ------------------------------------------------------------------------
 * Helper: allocate and initialise a new reader
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t copyFiles(ts_algo_list_t *source,
                                    ts_algo_list_t *target) {
	nowdb_err_t err;
	ts_algo_list_node_t *runner;
	nowdb_file_t        *file;

	for(runner=source->head; runner!=NULL; runner=runner->nxt) {
		file = malloc(sizeof(nowdb_file_t));
		if (file == NULL) {
			destroyFiles(target);
			return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
			                       "allocating file descriptor");
		}
		err = nowdb_file_copy(runner->cont, file);
		if (err != NOWDB_OK) {
			destroyFiles(target); return err;
		}
		if (ts_algo_list_append(target, file) != TS_ALGO_OK) {
			destroyFiles(target);
			return nowdb_err_get(nowdb_err_no_mem, FALSE, OBJECT,
			                                      "list.append");
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
	switch(reader->type) {
	case NOWDB_READER_FULLSCAN:
		destroyFiles(&reader->files);
		return;

	case NOWDB_READER_SEARCH:
	case NOWDB_READER_RANGE:
	case NOWDB_READER_BUF:
		fprintf(stderr, "not implemented\n");
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

	err = nowdb_file_move(reader->current->cont);
	if (err == NOWDB_OK) return NOWDB_OK;

	if (err->errcode != nowdb_err_eof) return err;
	nowdb_err_release(err);

	err = nowdb_file_close(reader->current->cont);
	if (err != NOWDB_OK) return err;

	reader->current = reader->current->nxt;
	if (reader->current == NULL) return nowdb_err_get(nowdb_err_eof,
	                                           FALSE, OBJECT, NULL);
	err = nowdb_file_open(reader->current->cont);
	if (err != NOWDB_OK) return err;

	err = nowdb_file_rewind(reader->current->cont);
	if (err != NOWDB_OK) return err;

	return nowdb_file_move(reader->current->cont);
}

/* ------------------------------------------------------------------------
 * Helper: move for fullscan
 * ------------------------------------------------------------------------
 */
static inline nowdb_err_t moveFullscan(nowdb_reader_t *reader) {
	if (reader->current == NULL) return nowdb_err_get(nowdb_err_eof,
	                                           FALSE, OBJECT, NULL);
	return nextpage(reader);
}

/* ------------------------------------------------------------------------
 * Move
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_move(nowdb_reader_t *reader) {
	if (reader == NULL) return nowdb_err_get(nowdb_err_invalid,
	                    FALSE, OBJECT, "reader object is NULL");
	switch(reader->type) {
	case NOWDB_READER_FULLSCAN:
		return moveFullscan(reader);

	case NOWDB_READER_SEARCH:
	case NOWDB_READER_RANGE:
	case NOWDB_READER_BUF:
		return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT,
		                                               "move");
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
	switch(reader->type) {
	case NOWDB_READER_FULLSCAN:
		return rewindFullscan(reader);
	case NOWDB_READER_SEARCH:
	case NOWDB_READER_RANGE:
	case NOWDB_READER_BUF:
		return nowdb_err_get(nowdb_err_not_supp, FALSE, OBJECT,
		                                             "rewind");
	default:
		return nowdb_err_get(nowdb_err_panic, FALSE, OBJECT,
		                    "unkown reader type in rewind");
	}
}

/* ------------------------------------------------------------------------
 * Page
 * ------------------------------------------------------------------------
 */
char *nowb_reader_page(nowdb_reader_t *reader);

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
	err = copyFiles(files, &(*reader)->files);
	if (err != NOWDB_OK) {
		nowdb_reader_destroy(*reader); free(*reader);
		return err;
	}
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
                                nowdb_filter_t  *filter);

/* ------------------------------------------------------------------------
 * Index Range scan
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_range(nowdb_reader_t **reader,
                               ts_algo_list_t  *files,
                               nowdb_index_t   *index,
                               nowdb_filter_t  *filter,
                               void *start, void *end);

/* ------------------------------------------------------------------------
 * Buffer scan
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_buffer(nowdb_reader_t  **reader,
                                char             *buf,
                                uint32_t         *size,
                                nowdb_filter_t   *filter,
                                nowdb_ordering_t *order,
                                void *start,void  *end);

