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
#include <nowdb/fun/expr.h>
#include <nowdb/index/index.h>
#include <nowdb/mem/pplru.h>
#include <nowdb/sort/sort.h>

#include <tsalgo/list.h>
#include <tsalgo/tree.h>

/* ------------------------------------------------------------------------
 * Reader Types
 * ------------------------------------------------------------------------
 */
#define NOWDB_READER_FULLSCAN 1
#define NOWDB_READER_SEARCH   10
#define NOWDB_READER_FRANGE   100
#define NOWDB_READER_KRANGE   101
#define NOWDB_READER_CRANGE   102
#define NOWDB_READER_MRANGE   103
#define NOWDB_READER_BUF      1000
#define NOWDB_READER_BUFIDX   1001
#define NOWDB_READER_BKRANGE  1002
#define NOWDB_READER_BCRANGE  1003
#define NOWDB_READER_BMRANGE  1004
#define NOWDB_READER_SEQ      10001
#define NOWDB_READER_MERGE    10002
#define NOWDB_READER_JOIN     10003

/* ------------------------------------------------------------------------
 * Max sub readers
 * ------------------------------------------------------------------------
 */
#define NOWDB_READER_MAXSUB 64

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
typedef struct nowdb_reader_t {
	uint32_t                type; /* reader type                   */
	uint32_t             recsize; /* set according to first file   */
	ts_algo_list_t        *files; /* list of relevant files        */
	ts_algo_list_node_t *current; /* current file (fullscan)       */
	nowdb_file_t           *file; /* current file (search)         */
	ts_algo_tree_t       readers; /* files for index-based readers */
	nowdb_pplru_t          *plru; /* LRU cache for range reader    */
	nowdb_pplru_t         *bplru; /* black list                    */
	char                    *buf; /* for buffer-based readers      */
	uint32_t                size; /* size of buffer in bytes       */
	char                    *tmp; /* buffer for keys in bufreader  */
	char                   *tmp2; /* buffer for keys in bufreader  */
	char                  *page2; /* buffer for page in bufreader  */
	nowdb_expr_t          filter; /* filter                        */
	nowdb_eval_t           *eval; /* evaluation helper             */
	beet_iter_t             iter; /* iterator                      */
	beet_state_t           state; /* query state                   */
	nowdb_bool_t         closeit; /* close file after use          */
	char                   *page; /* pointer to current page       */
	int32_t                  off; /* offset into win               */
	nowdb_bitmap64_t       *cont; /* content of current page       */
	nowdb_index_keys_t    *ikeys; /* index keys                    */
	ts_algo_tree_t        **maps; /* Maps of keys for MRANGE       */
	void                    *key; /* current key                   */
	void                  *start; /* start of range                */
	void                    *end; /* end of range                  */
	nowdb_ord_t              ord; /* asc or desc                   */
	nowdb_time_t            from; /* start of period               */
	nowdb_time_t              to; /* end   of period               */
	struct nowdb_reader_t  **sub; /* subreaders                    */
	uint32_t                  nr; /* number of subreaders          */
	uint32_t                 cur; /* current subreader             */
	char                     eof; /* reached eof                   */
	char                  nodata; /* reader has not data           */
	char                      ko; /* key-only reader               */
	char                ownfiles; /* destroy files                 */
	nowdb_bitmap64_t       moved; /* sub has been moved            */
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
 * If move reaches the end of the dataset, EOF is returned.
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
 * Set Period
 * ------------------------------------------------------------------------
 */
void nowdb_reader_setPeriod(nowdb_reader_t *reader,
                            nowdb_time_t     start,
                            nowdb_time_t       end);

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
                                  ts_algo_list_t   *files,
                                  nowdb_expr_t    filter);

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
                                char            *key,
                                nowdb_expr_t filter);

/* ------------------------------------------------------------------------
 * Index Full Range scan
 * ---------------------
 * Instantiate a reader as full index range scan,
 * reading all pages of all keys in range.
 *
 * Parameters:
 * - Reader: out parameter
 * - files : list of relevant files
 * - index : the index to search
 * - filter: select only relevant elements
 * - start/end: indicate the range in terms of keys of that index
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_frange(nowdb_reader_t **reader,
                                ts_algo_list_t  *files,
                                nowdb_index_t   *index,
                                nowdb_expr_t     filter,
                                void *start, void *end);

/* ------------------------------------------------------------------------
 * Index Key Range scan
 * --------------------
 * Instantiate a reader as key index range scan,
 * reading all keys in range, but not their pages.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_krange(nowdb_reader_t **reader,
                                ts_algo_list_t  *files,
                                nowdb_index_t   *index,
                                nowdb_expr_t    filter,
                                void *start, void *end);

/* ------------------------------------------------------------------------
 * Index Count Range scan
 * ----------------------
 * Instantiate a reader as count index range scan,
 * reading all keys in range and their bitmap, but not the pages.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_crange(nowdb_reader_t **reader,
                                ts_algo_list_t  *files,
                                nowdb_index_t   *index,
                                nowdb_expr_t    filter,
                                void *start, void *end);

/* ------------------------------------------------------------------------
 * Index Map Range scan
 * --------------------
 * Instantiate a reader as map index range scan,
 * reading all keys in range, but only the bitmaps and pages
 * of those whose key matches with a key in the "map" (i.e. tree)
 * passed in as argument
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_mrange(nowdb_reader_t **reader,
                                ts_algo_list_t  *files,
                                nowdb_index_t   *index,
                                nowdb_expr_t    filter,
                                ts_algo_tree_t  **maps,
                                void *start, void *end);

/* ------------------------------------------------------------------------
 * Buffer scan
 * -----------
 * Instantiate a reader from a buffer.
 * Parameters:
 * - reader: out parameter
 * - files : the datafiles
 * - filter: select only relevant elements
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_buffer(nowdb_reader_t  **reader,
                                ts_algo_list_t   *files,
                                nowdb_expr_t     filter,
                                nowdb_eval_t     *eval);

/* ------------------------------------------------------------------------
 * Buffer simulating an index range scan
 * -------------------------------------
 * Parameters:
 * - reader: out parameter
 * - files : the datafiles
 * - index : order the data according to this index
 * - filter: select only relevant elements
 * - ord   : order of range
 * - start/end: range indicator; ignore if ordering is NULL.
 *              with ordering and range, the reader behaves
 *              like a range scanner.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_bufidx(nowdb_reader_t  **reader,
                                ts_algo_list_t   *files,
                                nowdb_index_t    *index,
                                nowdb_expr_t     filter,
                                nowdb_eval_t     *eval, 
                                nowdb_ord_t        ord,
                                void *start, void *end);

/* ------------------------------------------------------------------------
 * Buffer simulating an index range scan (keys only)
 * -------------------------------------
 * Parameters:
 * - reader: out parameter
 * - files : the datafiles
 * - index : order the data according to this index
 * - filter: select only relevant elements
 * - ord   : order of range
 * - start/end: range indicator; ignore if ordering is NULL.
 *              with ordering and range, the reader behaves
 *              like a range scanner.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_bkrange(nowdb_reader_t  **reader,
                                 ts_algo_list_t   *files,
                                 nowdb_index_t    *index,
                                 nowdb_expr_t     filter,
                                 nowdb_eval_t     *eval, 
                                 nowdb_ord_t        ord,
                                 void *start,void  *end);

/* ------------------------------------------------------------------------
 * Sequence reader ('va_list' convention)
 * ---------------
 * reader: the sequence reader
 * nr    : number of subreaders
 * ...   : the readers
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_seq(nowdb_reader_t **reader, uint32_t nr, ...); 

/* ------------------------------------------------------------------------
 * Sequence reader ('argv' convention)
 * ---------------
 * reader: the sequence reader
 * nr    : number of subreaders
 * sub   : array of readers
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_vseq(nowdb_reader_t **reader, uint32_t nr,
                              nowdb_reader_t **sub);

/* ------------------------------------------------------------------------
 * Merge reader ('va_list' convention)
 * ------------
 * reader: the merge reader
 * nr    : number of subreaders
 * ...   : the readers
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_merge(nowdb_reader_t **reader, uint32_t nr, ...);

/* ------------------------------------------------------------------------
 * Merge reader ('argv' convention)
 * ------------
 * reader: the merge reader
 * nr    : number of subreaders
 * sub   : array of readers
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_reader_vmerge(nowdb_reader_t **reader, uint32_t nr,
                                nowdb_reader_t **sub);
#endif
