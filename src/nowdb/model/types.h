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
 * Property Model
 * ------------------------------------------------------------------------
 */
typedef struct {
	char              *name; /* property name                  */
	nowdb_key_t      propid; /* property id (stored in vertex) */
	nowdb_roleid_t   roleid; /* the role to which it belongs   */
	nowdb_model_type_t prop; /* is property textual?           */
	nowdb_type_t      value; /* type of the property value     */
} nowdb_model_prop_t;

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
	nowdb_type_t      weight; /* type of the weight    */
	nowdb_type_t     weight2; /* type of weight2       */
	nowdb_model_type_t  edge; /* is the edge  textual? */
	nowdb_model_type_t label; /* is the label textual? */
} nowdb_model_edge_t;

#endif
