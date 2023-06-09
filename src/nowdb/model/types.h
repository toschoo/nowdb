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
	nowdb_key_t      propid; /* property id (stored in vertex) */ // needed?
	nowdb_roleid_t   roleid; /* the type to which it belongs   */
	uint32_t            pos; /* keep properties ordered        */
	nowdb_type_t      value; /* type of the property value     */
	nowdb_bool_t         pk; /* this one is PK                 */
	nowdb_bool_t      stamp; /* this one is the timestamp      */
	nowdb_bool_t        inc; /* promise to keep increasing pk  */
	uint32_t            off; /* row offset                     */
} nowdb_model_prop_t;

/* ------------------------------------------------------------------------
 * Edge Property Model
 * to be removed
 * ------------------------------------------------------------------------
 */
typedef struct {
	char          *name;
	nowdb_key_t  propid; // needed?
	nowdb_key_t  edgeid;
	uint32_t        pos;
	nowdb_type_t  value;
	nowdb_bool_t origin;
	nowdb_bool_t destin;
	nowdb_bool_t  stamp;
	uint32_t        off;
} nowdb_model_pedge_t;

/* ------------------------------------------------------------------------
 * Vertex Model
 * ------------------------------------------------------------------------
 */
typedef struct {
	char               *name; /* name of the vertex    */
	nowdb_roleid_t    roleid; /* roleid                */
	nowdb_model_type_t   vid; /* is the vid textual?   */
	nowdb_bool_t     stamped; /* vertex is stamped     */
	uint16_t             num; /* number of attributes  */
	uint32_t            ctrl; /* size of control block */
	uint32_t            size; /* size of edge          */
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
	nowdb_bool_t     stamped; /* vertex is stamped     */
	uint16_t             num; /* number of attributes  */
	uint32_t            ctrl; /* size of control block */
	uint32_t            size; /* size of edge          */
} nowdb_model_edge_t;

#endif
