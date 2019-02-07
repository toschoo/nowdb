/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Cursor: DQL executor
 * ========================================================================
 */
#ifndef nowdb_cursor_decl
#define nowdb_cursor_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/scope/scope.h>
#include <nowdb/fun/expr.h>
#include <nowdb/reader/reader.h>
#include <nowdb/reader/vrow.h>
#include <nowdb/qplan/plan.h>
#include <nowdb/query/row.h>
#include <nowdb/fun/group.h>

/* ------------------------------------------------------------------------
 * Pair of (store, files obtained from that store)
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_store_t   *store;
	ts_algo_list_t   files;
	ts_algo_list_t pending;
} nowdb_storefile_t;

/* ------------------------------------------------------------------------
 * Cursor 
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_target_t    target; /* main target                   */
	nowdb_reader_t      *rdr; /* the reader                    */
	nowdb_storefile_t    stf; /* should be a list of stf!      */
	nowdb_model_t     *model; /* model                         */
	nowdb_expr_t      filter; /* main filter                   */
	nowdb_row_t         *row; /* projection                    */
	nowdb_group_t     *group; /* grouping                      */
	nowdb_group_t     *nogrp; /* apply aggs without grouping   */
	nowdb_model_vertex_t  *v; /* type if this is not a join!   */
	nowdb_vrow_t       *wrow; /* vertex row for where          */
	nowdb_vrow_t       *prow; /* vertex row for projection     */
	nowdb_eval_t       *eval; /* evaluation helper             */
	char           *leftover; /* leftover from previous round  */
	uint64_t            pmap; /* property map of leftover      */
	uint32_t             off; /* offset in the current reader  */
	uint32_t           recsz; /* record size                   */
	char                *tmp; /* temporary buffer for group    */
	char               *tmp2; /* temporary buffer for row      */
	char            vrtx[32]; /* yet another temporary buffer  */
	char            *fromkey; /* range: fromkey                */
	char              *tokey; /* range:   tokey                */
	char             freesrc; /* free the source               */
	char               hasid; /* has id to identify model      */
	char            grouping; /* has id to identify model      */
	char                 eof; /* end of file was reached       */
} nowdb_cursor_t;

/* ------------------------------------------------------------------------
 * Create a new cursor 
 * -------------------
 * Expecting a scope and an execution plan.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_cursor_new(nowdb_scope_t  *scope,
                             ts_algo_list_t  *plan,
                             nowdb_cursor_t **cur);

/* ------------------------------------------------------------------------
 * Destroy a new cursor 
 * ------------------------------------------------------------------------
 */
void nowdb_cursor_destroy(nowdb_cursor_t *cur);

/* ------------------------------------------------------------------------
 * Open cursor 
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_cursor_open(nowdb_cursor_t *cur);

/* ------------------------------------------------------------------------
 * Fetch from cursor
 * -----------------
 * Copy records corresponding to the query into the user-provided buffer
 * of size sz (in bytes). The number of bytes written to the buffer is
 * communicated back through osz.
 * When the cursor is exhausted, fetch will return the error 'eof'.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_cursor_fetch(nowdb_cursor_t   *cur,
                              char *buf, uint32_t sz,
                                       uint32_t *osz,
                                     uint32_t *count);

/* ------------------------------------------------------------------------
 * Reset cursor to start position
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_cursor_rewind(nowdb_cursor_t *cur);

/* ------------------------------------------------------------------------
 * Check for eof
 * ------------------------------------------------------------------------
 */
char nowdb_cursor_eof(nowdb_cursor_t *cur);
#endif
