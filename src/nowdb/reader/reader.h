/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Reader: generic data access interface
 * ========================================================================
 */
#ifndef nowdb_reader_decl
#define nowdb_reader_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/task/lock.h>
#include <nowdb/io/dir.h>
#include <nowdb/io/file.h>
#include <nowdb/reader/filter.h>

#include <tsalgo/list.h>
#include <tsalgo/tree.h>

typedef char nowdb_index_t;     /* just for the moment */
typedef char nowdb_ordering_t;  /* just for the moment */

/* ------------------------------------------------------------------------
 * Reader   
 * ------
 * Reader is a fundamental interface to get access to data.
 * It can be instantiated from different types of datasets,
 * but always provides the following services:
 * - destroy (of course)
 * - move    : moves the reader to the next page
 * - rewind  : resets the reader to the starting position
 * - read    : copies matching data into a target buffer
 * - page    : returns the current page
 * - pageid  : returns the identifier of the current page
 * - key     : returns the current index key (or NULL,
 *             when no index was selected)
 * ------------------------------------------------------------------------
 */
typedef struct {
	uint32_t                type; /* reader type                   */
	uint32_t             recsize; /* set according to first file   */
	ts_algo_list_t        *files; /* list of relevant files        */
	ts_algo_list_node_t *current; /* current file (fullscan)       */
	ts_algo_tree_t       readers; /* files for index-based readers */
	char                    *buf; /* for buffer-based readers      */
	uint32_t                size; /* size of buffer in bytes       */
	nowdb_filter_t       *filter; /* filter                        */
	nowdb_ordering_t      *order; /* ordering                      */
	nowdb_index_t         *index; /* index                         */
	nowdb_bool_t         closeit; /* close file after use          */
	char                   *page; /* pointer to current page       */
	uint32_t                 off; /* offset into win               */
	void                    *key; /* current key                   */
	void                  *start; /* start of range                */
	void                    *end; /* end of range                  */
} nowdb_reader_t;

/* ------------------------------------------------------------------------
 * Destroy
 * ------------------------------------------------------------------------
 */
void nowdb_reader_destroy(nowdb_reader_t *reader);

/* ------------------------------------------------------------------------
 * Move
 * ----
 * Moves to the next page. When move succeeds without error,
 * it is guaranteed that 'page' is not NULL.
 * It move reaches the end of the dataset, EOF is returned.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_move(nowdb_reader_t *reader);

/* ------------------------------------------------------------------------
 * Rewind
 * ------
 * Resets the reader to start position.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_rewind(nowdb_reader_t *reader);

/* ------------------------------------------------------------------------
 * Page
 * ----
 * Returns the current page
 * ------------------------------------------------------------------------
 */
char *nowdb_reader_page(nowdb_reader_t *reader);

/* ------------------------------------------------------------------------
 * Pageid
 * ------
 * Returns pageid of the current page
 * ------------------------------------------------------------------------
 */
nowdb_pageid_t nowdb_reader_pageid(nowdb_reader_t *reader);

/* ------------------------------------------------------------------------
 * Key
 * ----
 * Returns the current key or NULL if reader is not on an index
 * ------------------------------------------------------------------------
 */
char *nowdb_reader_key(nowdb_reader_t *reader);

/* ------------------------------------------------------------------------
 * SkipKey
 * -------
 * Do not continue reading this key, skip directly to the next one.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_skipKey(nowdb_reader_t *reader);

/* ------------------------------------------------------------------------
 * Read
 * ----
 * Copies the result set into a user-provided buffer.
 * Read will continue calling move internally until
 * - either the resultset is exhausted or
 * - osize = size
 * Parameters:
 * - reader :
 * - buf    : buffer provided by the user
 * - size   : size of that buffer (in bytes)
 * - osize  : bytes written by read.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_read(nowdb_reader_t *reader,
                              char           *buf,
                              uint32_t        size,
                              uint32_t       *osize);

/* ------------------------------------------------------------------------
 * Fullscan
 * --------
 * Instantiate a reader as fullscan.
 * Parameters:
 * - Reader: out parameter
 * - files : list of relevant files
 * - filter: select only relevant elements
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_fullscan(nowdb_reader_t **reader,
                                  ts_algo_list_t  *files,
                                  nowdb_filter_t  *filter);

/* ------------------------------------------------------------------------
 * Index Search
 * ------------
 * Instantiate a reader as index search.
 * Parameters:
 * - Reader: out parameter
 * - files : list of relevant files
 * - index : the index to search
 * - filter: select only relevant elements
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_search(nowdb_reader_t **reader,
                                ts_algo_list_t  *files,
                                nowdb_index_t   *index,
                                nowdb_filter_t  *filter);

/* ------------------------------------------------------------------------
 * Index Range scan
 * ----------------
 * Instantiate a reader as index range scan.
 * Parameters:
 * - Reader: out parameter
 * - files : list of relevant files
 * - index : the index to search
 * - filter: select only relevant elements
 * - start/end: indicate the range in terms of keys of that index
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_range(nowdb_reader_t **reader,
                               ts_algo_list_t  *files,
                               nowdb_index_t   *index,
                               nowdb_filter_t  *filter,
                               void *start, void *end);

/* ------------------------------------------------------------------------
 * Buffer scan
 * -----------
 * Instantiate a reader from a buffer.
 * Parameters:
 * - reader: out parameter
 * - buf   : the buffer to read
 * - size  : the size of the buffer in bytes
 * - filter: select only relevant elements
 * - order : ordering of the buffer (if any)
 * - start/end: range indicator; ignore if ordering is NULL.
 *              with ordering and range, the reader behaves
 *              like a range scanner.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_buffer(nowdb_reader_t  **reader,
                                char             *buf,
                                uint32_t         *size,
                                nowdb_filter_t   *filter,
                                nowdb_ordering_t *order,
                                void *start, void *end);
#endif
