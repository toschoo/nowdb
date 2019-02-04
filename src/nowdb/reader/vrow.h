/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Vertex row: representation of all vertex properties
 *             needed to evaluate a filter as one single row
 * ========================================================================
 */
#ifndef nowdb_vrow_decl
#define nowdb_vrow_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/fun/expr.h>

#include <tsalgo/list.h>
#include <tsalgo/tree.h>

#include <stdint.h>
#include <stdio.h>

/* ------------------------------------------------------------------------
 * The Vertex Row
 * ------------------------------------------------------------------------
 */
typedef struct {
	nowdb_roleid_t  role;   /* this is the type we are handling   */
	nowdb_key_t    *prev;   /* previous vertex                    */
	uint32_t        size;   /* size of one vrow                   */
	uint32_t          np;   /* number of props                    */
	char        wantvrtx;   /* want vertex                        */
	char       forcecomp;   /* force completion on vertex switch  */
	nowdb_expr_t  filter;   /* the relevant filter                */
	nowdb_eval_t   *eval;   /* evaluation helper                  */
	ts_algo_tree_t *pspec;  /* all properties for the vertex type */
	ts_algo_tree_t *vrtx;   /* map of current vertices            */
	ts_algo_list_t *ready;  /* completed vertices                 */
} nowdb_vrow_t;

/* ------------------------------------------------------------------------
 * Create a vertex row for a given filter
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_vrow_fromFilter(nowdb_roleid_t  role,
                                  nowdb_vrow_t  **vrow,
                                  nowdb_expr_t  filter,
                                  nowdb_eval_t  *eval);

/* ------------------------------------------------------------------------
 * Create a vertex row for a given expression
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_vrow_new(nowdb_roleid_t role,
                           nowdb_vrow_t **vrow,
                           nowdb_eval_t *eval);

/* ------------------------------------------------------------------------
 * Set force completion
 * ------------------------------------------------------------------------
 */
void nowdb_vrow_autoComplete(nowdb_vrow_t *vrow);

/* ------------------------------------------------------------------------
 * Add expression to an existing vrow
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_vrow_addExpr(nowdb_vrow_t *vrow, 
                               nowdb_expr_t  expr);

/* ------------------------------------------------------------------------
 * Destroy the vertex row
 * ------------------------------------------------------------------------
 */
void nowdb_vrow_destroy(nowdb_vrow_t *vrow);

/* ------------------------------------------------------------------------
 * Try to add a vertex property
 * ----------------------------
 * if the property is relevant,
 * this vertex will be added (added=1),
 * otherwise it is rejected (added=0).
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_vrow_add(nowdb_vrow_t   *vrow,
                           nowdb_vertex_t *vertex,
                           char           *added);

/* ------------------------------------------------------------------------
 * Force completion of all leftovers
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_vrow_force(nowdb_vrow_t *vrow);

/* ------------------------------------------------------------------------
 * Try to evaluate
 * ---------------
 * vertices that are complete
 * (i.e. all relevant properties have been added)
 * are evaluated. If the evaluation fails for a given vertex,
 * it is removed from the vrow and false is returned;
 * if the evaluation succeeds, the *vertex is removed,
 * its vid is passed back through *vid and true is returned.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_vrow_eval(nowdb_vrow_t *vrow,
                            nowdb_key_t  *vid,
                            nowdb_bool_t *found);

/* ------------------------------------------------------------------------
 * Check for complete vertex
 * -------------------------
 * Checks whether there is a complete vertex (without evaluation).
 * If there is, its row is copied into row and
 * its size is passed to 'size'.
 * ------------------------------------------------------------------------
 */
nowdb_bool_t nowdb_vrow_complete(nowdb_vrow_t *vrow,
                                 uint32_t     *size,
                                 uint64_t     *pmap,
                                 char        **row);

/* ------------------------------------------------------------------------
 * Check whether vrow is empty
 * ------------------------------------------------------------------------
 */
nowdb_bool_t nowdb_vrow_empty(nowdb_vrow_t *vrow);

/* ------------------------------------------------------------------------
 * Show current vrows and their state (debugging)
 * ------------------------------------------------------------------------
 */
void nowdb_vrow_show(nowdb_vrow_t *vrow);
#endif
