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
// #include <nowdb/fun/expr.h>
#include <nowdb/fun/group.h>
#include <nowdb/query/rowutl.h>

#include <stdint.h>

#include <tsalgo/list.h>

#define NOWDB_FIELD_SELECT 1
#define NOWDB_FIELD_ORDER  2
#define NOWDB_FIELD_GROUP  4
#define NOWDB_FIELD_AGG    8

typedef struct {
	nowdb_target_t target; /* context or vertex (or neither)         */
	uint32_t          off; /* identifies the field for edges         */
	char            *name; /* name of a property                     */
	nowdb_key_t    propid; /* propid for this property               */
	nowdb_roleid_t roleid; /* to which type it belongs               */
	char               pk; /* is primary key                         */
	void           *value; /* constant value                         */
	uint16_t          typ; /* type of constant value                 */
	nowdb_bitmap8_t flags; /* what to do with the field              */
	uint32_t          agg; /* aggregate function to apply on the row */
} nowdb_field_t;

typedef struct {
	uint32_t             sz; /* number of fields                  */
	uint32_t            cur; /* current field                     */
	uint32_t            fur; /* current function field            */
	uint32_t           vcur; /* current vertex field              */
	uint32_t          dirty; /* we are in the middle of something */
	nowdb_field_t   *fields; /* the fields                        */
	nowdb_vertex_t    *vrtx; /* remember what we have seen        */
	nowdb_model_t    *model; /* the model to use for projection   */
	nowdb_text_t      *text; /* the text  to use for projection   */
	nowdb_ptlru_t     *tlru; /* private text lru cache            */
	nowdb_model_vertex_t *v; /* the current vertex model          */
	nowdb_model_prop_t  **p; /* the current property models       */
	nowdb_model_edge_t   *e; /* the current edge   model          */
	nowdb_model_vertex_t *o; /* the origin of the edge            */
	nowdb_model_vertex_t *d; /* the destin of the edge            */
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
 * switch group
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_row_group(nowdb_row_t *row);

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
