/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Model Types
 * ========================================================================
 *
 * ========================================================================
 */
#ifndef nowdb_model_types_decl
#define nowdb_model_types_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>

#include <tsalgo/tree.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------------
 * Model Type is numerical or text
 * ------------------------------------------------------------------------
 */
typedef uint8_t nowdb_model_type_t;

#define NOWDB_MODEL_NUM  0
#define NOWDB_MODEL_TEXT 1

/* ------------------------------------------------------------------------
 * Tablespace
 * ------------------------------------------------------------------------
 */
typedef struct {
	char    *name;
	uint32_t tsid;
} nowdb_model_ts_t;

/* ------------------------------------------------------------------------
 * Property Model
 * ------------------------------------------------------------------------
 */
typedef struct {
	char              *name; /* property name                  */
	nowdb_key_t      propid; /* property id (stored in vertex) */
	nowdb_roleid_t   roleid; /* the type to which it belongs   */
	uint32_t            pos; /* keep properties ordered        */
	nowdb_type_t      value; /* type of the property value     */
	nowdb_bool_t         pk; /* this one is PK                 */
} nowdb_model_prop_t;

/* ------------------------------------------------------------------------
 * Edge Property Model
 * ------------------------------------------------------------------------
 */
typedef struct {
	char          *name;
	nowdb_key_t  propid;
	nowdb_key_t  edgeid;
	nowdb_type_t  value;
	uint32_t        off;
} nowdb_model_pedge_t;

/* ------------------------------------------------------------------------
 * Vertex Model
 * ------------------------------------------------------------------------
 */
typedef struct {
	char             *name; /* name of the vertex  */
	nowdb_roleid_t  roleid; /* roleid              */
	nowdb_model_type_t vid; /* is the vid texutal? */
} nowdb_model_vertex_t;

/* ------------------------------------------------------------------------
 * Edge Model
 * ------------------------------------------------------------------------
 */
typedef struct {
	char               *name; /* name of the edge      */
	nowdb_key_t       edgeid; /* the edgeid (= edge)   */
	nowdb_roleid_t    origin; /* roleid of the origin  */
	nowdb_roleid_t    destin; /* roleid of the destin  */
	nowdb_model_pedge_t   op; /* origin pedge          */
	nowdb_model_pedge_t   dp; /* destin pedge          */
	nowdb_model_pedge_t   tp; /* stamp  pedge          */
	uint16_t             num; /* number of attributes  */
	uint32_t            ctrl; /* size of control block */
	uint32_t            size; /* size of edge          */
	nowdb_type_t      weight; /* to be removed         */
	nowdb_type_t     weight2; /* to be removed         */
	nowdb_model_type_t  edge; /* to be removed         */
	nowdb_model_type_t label; /* to be removed         */
} nowdb_model_edge_t;

#endif
