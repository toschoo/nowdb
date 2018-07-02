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

typedef uint8_t nowdb_model_type_t;

#define NOWDB_MODEL_NUM  0
#define NOWDB_MODEL_TEXT 1

typedef struct {
	char              *name;
	nowdb_key_t      propid;
	nowdb_roleid_t   roleid;
	nowdb_model_type_t prop;
	nowdb_type_t      value;
} nowdb_model_prop_t;

typedef struct {
	char             *name;
	nowdb_roleid_t  roleid;
	nowdb_model_type_t vid;
} nowdb_model_vertex_t;

typedef struct {
	char               *name;
	nowdb_key_t       edgeid;
	nowdb_roleid_t    origin;
	nowdb_roleid_t    destin;
	nowdb_type_t      weight;
	nowdb_type_t     weight2;
	nowdb_model_type_t  edge;
	nowdb_model_type_t label;
} nowdb_model_edge_t;

#endif

