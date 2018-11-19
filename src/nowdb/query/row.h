/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Projection services
 * ========================================================================
 */
#ifndef nowdb_row_decl
#define nowdb_row_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/model/model.h>
#include <nowdb/text/text.h>
#include <nowdb/scope/scope.h>
#include <nowdb/mem/ptlru.h>
#include <nowdb/fun/expr.h>
#include <nowdb/fun/group.h>
#include <nowdb/query/rowutl.h>

#include <stdint.h>

#include <tsalgo/list.h>

#define NOWDB_FIELD_SELECT 1
#define NOWDB_FIELD_ORDER  2
#define NOWDB_FIELD_GROUP  4
#define NOWDB_FIELD_AGG    8

typedef struct {
	uint32_t             sz; /* number of fields                  */
	uint32_t            cur; /* current field                     */
	uint32_t            fur; /* current function field            */
	uint32_t          dirty; /* we are in the middle of something */
	nowdb_expr_t    *fields; /* the projection fields             */
	nowdb_eval_t       eval; /* evaluation helper                 */
} nowdb_row_t;

/* ------------------------------------------------------------------------
 * initialise the row
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_row_init(nowdb_row_t       *row,
                           nowdb_scope_t   *scope,
                           ts_algo_list_t *fields);

/* ------------------------------------------------------------------------
 * destroy the row
 * ------------------------------------------------------------------------
 */
void nowdb_row_destroy(nowdb_row_t *row);

/* ------------------------------------------------------------------------
 * project
 * -------
 * write data from src
 * according to the projection rules defined in row
 * to the output buf.
 * The result buffer is self-contained an can be printed
 * or transformed to a string without the help of a row.
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_row_project(nowdb_row_t *row,
                              nowdb_group_t *group,
                              char *src, uint32_t recsz,
                              char *buf, uint32_t sz,
                              uint32_t *osz,
                              char *full,
                              char *count,
                              char *complete);

/* ------------------------------------------------------------------------
 * transform projected buffer to string
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_row_toString(char  *buf,
                               char **str);

/* ------------------------------------------------------------------------
 * Extract a row from the buffer
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_row_extractRow(char    *buf, uint32_t   sz,
		                 uint32_t row, uint32_t *idx);

/* ------------------------------------------------------------------------
 * Extract a field from the buffer
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_row_extractField(char      *buf, uint32_t   sz,
		                   uint32_t field, uint32_t *idx);
#endif
