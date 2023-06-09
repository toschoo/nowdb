/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * AST: Abstract Syntax Tree
 * ========================================================================
 */
#ifndef nowdb_ast_decl
#define nowdb_ast_decl

// #include <nowdb/reader/filter.h>
#include <nowdb/types/types.h>

#include <stdlib.h>
#include <stdint.h>

/* -----------------------------------------------------------------------
 * An AST may represent
 * --------------------
 * - data definition   statement
 * - data loading      statement
 * - data manipulation statement
 * - data query        statement
 * - miscellaneous     statement
 * -----------------------------------------------------------------------
 */
#define NOWDB_AST_DDL  1000
#define NOWDB_AST_DLL  2000
#define NOWDB_AST_DML  3000
#define NOWDB_AST_DQL  4000
#define NOWDB_AST_MISC 5000

/* -----------------------------------------------------------------------
 * DDL is either
 * - create
 * - alter
 * - drop
 * 
 * e.g.:
 *
 * +data definition
 * +--+create
 * +--+--+context (<context>)
 * +--+--+option
 * +--+--+--+more options
 * -----------------------------------------------------------------------
 */
#define NOWDB_AST_CREATE 1001
#define NOWDB_AST_ALTER  1002
#define NOWDB_AST_DROP   1003
#define NOWDB_AST_SHOW   1011
#define NOWDB_AST_DESC   1012

/* -----------------------------------------------------------------------
 * DLL is load
 * -----------------------------------------------------------------------
 */
#define NOWDB_AST_LOAD   2001

/* -----------------------------------------------------------------------
 * DML is either
 * - insert
 * - update
 * - delete
 * -----------------------------------------------------------------------
 */
#define NOWDB_AST_INSERT 3001
#define NOWDB_AST_UPDATE 3002
#define NOWDB_AST_DELETE 3003

/* -----------------------------------------------------------------------
 * DQL is more complex, it consists of the following root nodes
 * - from
 * - select
 * - where
 * - order
 * - group
 * 
 * e.g.:
 * 
 * +data query
 * +--+from
 * +--+--+context (myctx)
 * +--+select *
 * +--+where
 * +--+--+and
 * +--+--+--+just
 * +--+--+--+--+=
 * +--+--+--+--+--+field (ege)
 * +--+--+--+--+--+uint value (1)
 * +--+--+--+just
 * +--+--+--+--+=
 * +--+--+--+--+--+field (origin)
 * +--+--+--+--+--+uint value (100)
 * -----------------------------------------------------------------------
 */
#define NOWDB_AST_FROM   4001
#define NOWDB_AST_SELECT 4002
#define NOWDB_AST_WHERE  4003
#define NOWDB_AST_GROUP  4004
#define NOWDB_AST_ORDER  4005

/* -----------------------------------------------------------------------
 * DQL Where:
 * - and
 * - or
 * - just
 * - not
 * -----------------------------------------------------------------------
 */
#define NOWDB_AST_AND    4006
#define NOWDB_AST_OR     4007
#define NOWDB_AST_JUST   4008
#define NOWDB_AST_NOT    4009

/* -----------------------------------------------------------------------
 * DQL From and Select:
 * - join
 * - star (select *)
 * -----------------------------------------------------------------------
 */
#define NOWDB_AST_JOIN   4010
#define NOWDB_AST_STAR   4011

/* -----------------------------------------------------------------------
 * Micellaneous
 * ------------
 * +miscellaneous
 * +--+use (<scope>)
 * -----------------------------------------------------------------------
 */
#define NOWDB_AST_USE    5001
#define NOWDB_AST_FETCH  5002
#define NOWDB_AST_CLOSE  5003
#define NOWDB_AST_EXEC   5004
#define NOWDB_AST_LOCK   5005
#define NOWDB_AST_UNLOCK 5006

/* -----------------------------------------------------------------------
 * Generic targets
 * - target (scope, vertex, context, index)
 * - scope
 * - context
 * - vertex
 * - index
 * - on is for indices: create index xyz on ...
 * - type (vertex definition)
 * - edge (context definition)
 * -----------------------------------------------------------------------
 */
#define NOWDB_AST_TARGET   10100
#define NOWDB_AST_SCOPE    10101
#define NOWDB_AST_STORAGE  10102
#define NOWDB_AST_CONTEXT  10105
#define NOWDB_AST_VERTEX   10106
#define NOWDB_AST_INDEX    10107
#define NOWDB_AST_ON       10108
#define NOWDB_AST_TYPE     10109
#define NOWDB_AST_EDGE     10110
#define NOWDB_AST_PROC     10111
#define NOWDB_AST_MODULE   10112
#define NOWDB_AST_MUTEX    10113
#define NOWDB_AST_QUEUE    10114

/* -----------------------------------------------------------------------
 * Options
 * -----------------------------------------------------------------------
 */
#define NOWDB_AST_OPTION   10200
#define NOWDB_AST_ALLOCSZ  10201
#define NOWDB_AST_LARGESZ  10202
#define NOWDB_AST_SORTERS  10203
#define NOWDB_AST_SORT     10204
#define NOWDB_AST_COMP     10205
#define NOWDB_AST_ENCP     10206
#define NOWDB_AST_SIZING   10207
#define NOWDB_AST_DISK     10208
#define NOWDB_AST_STRESS   10209
#define NOWDB_AST_IGNORE   10210
#define NOWDB_AST_PK       10211
#define NOWDB_AST_ORIGIN   10212
#define NOWDB_AST_DESTIN   10213
#define NOWDB_AST_STAMP    10214
#define NOWDB_AST_INC      10215
#define NOWDB_AST_LANG     10220
#define NOWDB_AST_ERRORS   10221
#define NOWDB_AST_MODE     10222
#define NOWDB_AST_TIMEOUT  10223

/* -----------------------------------------------------------------------
 * IFEXISTS is a special option for create and drop:
 * - create if not exists
 * - drop   if     exists
 * -----------------------------------------------------------------------
 */
#define NOWDB_AST_IFEXISTS 10301

/* -----------------------------------------------------------------------
 * Data
 * - key-value list, e.g. insert into ... (a=b, c=d, ...)
 * - value list, e.g. insert into ... (a, b, c, ...)
 * - field, also field list, e.g. create index xyz on ctx (a,b,c)
 * - decl, e.g. create type (a text, b int, ...)
 * - fun, e.g. select count(*)
 * -----------------------------------------------------------------------
 */
#define NOWDB_AST_DATA     10300
#define NOWDB_AST_KEYVAL   10301
#define NOWDB_AST_VALLIST  10302
#define NOWDB_AST_FIELD    10303
#define NOWDB_AST_DECL     10305
#define NOWDB_AST_OFF      10306
#define NOWDB_AST_OP       10307
#define NOWDB_AST_FUN      10308
#define NOWDB_AST_PARAM    10309
#define NOWDB_AST_ALIAS    10310

/* -----------------------------------------------------------------------
 * Values
 * TODO: consider to use the types here directly (NOWDB_TYP_TEXT, etc.)
 * -----------------------------------------------------------------------
 */
#define NOWDB_AST_VALUE    10320
#define NOWDB_AST_TEXT     10321
#define NOWDB_AST_FLOAT    10322
#define NOWDB_AST_UINT     10323
#define NOWDB_AST_INT      10324
#define NOWDB_AST_DATE     10325
#define NOWDB_AST_TIME     10326
#define NOWDB_AST_BOOL     10327
#define NOWDB_AST_NULL     10328
#define NOWDB_AST_VTYPE    10329

/* -----------------------------------------------------------------------
 * Path and Location ('remote', 'local')
 * -----------------------------------------------------------------------
 */
#define NOWDB_AST_PATH     10009
#define NOWDB_AST_LOC      10010

/* -----------------------------------------------------------------------
 * Compare (a = b)
 * -----------------------------------------------------------------------
 */
#define NOWDB_AST_COMPARE  10500
#define NOWDB_AST_EQ 1
#define NOWDB_AST_LE 2
#define NOWDB_AST_GE 3
#define NOWDB_AST_LT 4
#define NOWDB_AST_GT 5
#define NOWDB_AST_NE 6
#define NOWDB_AST_IN 7

/* -----------------------------------------------------------------------
 * what the ast value represents:
 * - a string   (the standard case)
 * - an integer (usually a constant)
 * -----------------------------------------------------------------------
 */
#define NOWDB_AST_V_STRING  1
#define NOWDB_AST_V_INTEGER 2

/* -----------------------------------------------------------------------
 * An AST node
 * -----------------------------------------------------------------------
 */
typedef struct nowdb_ast_st {
	int                  ntype; /* node type                       */
	int                  stype; /* subtype                         */
	int                  vtype; /* value type                      */
        char                 isstr; /* value represents a string       */
	void                *value; /* value stored by the node        */
	int                  nKids; /* number of kids                  */
	struct nowdb_ast_st **kids; /* array of pointers to the kids   */
} nowdb_ast_t;

/* -----------------------------------------------------------------------
 * Create an AST node of type 'ntype' and subtype 'stype'
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_create(int ntype, int stype);

/* -----------------------------------------------------------------------
 * Initialise an already allocated AST node
 * -----------------------------------------------------------------------
 */
int nowdb_ast_init(nowdb_ast_t *n, int ntype, int stype);

/* -----------------------------------------------------------------------
 * Destroy AST node
 * -----------------------------------------------------------------------
 */
void nowdb_ast_destroy(nowdb_ast_t *n);

/* -----------------------------------------------------------------------
 * Destroy and free AST node
 * -----------------------------------------------------------------------
 */
void nowdb_ast_destroyAndFree(nowdb_ast_t *n);

/* -----------------------------------------------------------------------
 * Set value of that AST node
 * -----------------------------------------------------------------------
 */
void nowdb_ast_setValue(nowdb_ast_t *n, int vtype, void *val);

/* -----------------------------------------------------------------------
 * Set value of that AST node, set string indicator
 * -----------------------------------------------------------------------
 */
void nowdb_ast_setValueAsString(nowdb_ast_t *n, int vtype, void *val);

/* -----------------------------------------------------------------------
 * Add a kid to this AST node
 * -----------------------------------------------------------------------
 */
int nowdb_ast_add(nowdb_ast_t *n, nowdb_ast_t *k);

/* -----------------------------------------------------------------------
 * Add a kid to this AST node as parameter
 * -----------------------------------------------------------------------
 */
int nowdb_ast_addParam(nowdb_ast_t *n, nowdb_ast_t *k);

/* -----------------------------------------------------------------------
 * Show the AST (prints to stdout)
 * -----------------------------------------------------------------------
 */
void nowdb_ast_show(nowdb_ast_t *n);

/* -----------------------------------------------------------------------
 * AST Navigator
 * -----------------------------------------------------------------------
 */
#define NOWDB_AST_DDL_OPERATION 0

#define NOWDB_AST_CREATE_TARGET 0
#define NOWDB_AST_CREATE_OPTION 1

#define NOWDB_AST_OPTION_OPTION 0

#define NOWDB_AST_DROP_TARGET   0

#define NOWDB_AST_ALTER_TARGET  0
#define NOWDB_AST_ALTER_OPTION  0

#define NOWDB_AST_DLL_OPERATION 0

#define NOWDB_AST_LOAD_TARGET   0
#define NOWDB_AST_LOAD_OPTION   1

/* -----------------------------------------------------------------------
 * Get the operation from the current AST node,
 * e.g.: get "CREATE" from "DDL"
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_operation(nowdb_ast_t *node);

/* -----------------------------------------------------------------------
 * Get the target from the current AST node
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_target(nowdb_ast_t *node);

/* -----------------------------------------------------------------------
 * Get the on part from the current AST node (create index)
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_on(nowdb_ast_t *node);

/* -----------------------------------------------------------------------
 * Get an option from the current AST node
 * if option is 0, the first option found is returned,
 * otherwise an option with the specific subcode is searched.
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_option(nowdb_ast_t *node, int option);

/* -----------------------------------------------------------------------
 * Get field declaration from the current AST node
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_declare(nowdb_ast_t *node);

/* -----------------------------------------------------------------------
 * Get storage option from the current AST node
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_storage(nowdb_ast_t *node);

/* -----------------------------------------------------------------------
 * Get offset from field declaration
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_off(nowdb_ast_t *node);

/* -----------------------------------------------------------------------
 * Get the projection from the current AST node (DQL only)
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_select(nowdb_ast_t *node);

/* -----------------------------------------------------------------------
 * Get the group clause from the current AST node (DQL only)
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_group(nowdb_ast_t *node);

/* -----------------------------------------------------------------------
 * Get the order clause from the current AST node (DQL only)
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_order(nowdb_ast_t *node);

/* -----------------------------------------------------------------------
 * Get field list from the current AST node
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_field(nowdb_ast_t *node);

/* -----------------------------------------------------------------------
 * Get value list from the current AST node
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_value(nowdb_ast_t *node);

/* -----------------------------------------------------------------------
 * Get param list from fun
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_param(nowdb_ast_t *node);

/* -----------------------------------------------------------------------
 * Get next param from param
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_nextParam(nowdb_ast_t *node);

/* -----------------------------------------------------------------------
 * Get 'from' from the current AST node (DQL only)
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_from(nowdb_ast_t *node);

/* -----------------------------------------------------------------------
 * Get 'where' from the current AST node (DQL and DML only)
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_where(nowdb_ast_t *node);

/* -----------------------------------------------------------------------
 * Get a condition from the current AST node (DQL and DML only)
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_condition(nowdb_ast_t *node);

/* -----------------------------------------------------------------------
 * Get a compare from the current AST node (DQL and DML only)
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_compare(nowdb_ast_t *node);

/* -----------------------------------------------------------------------
 * Get an operand from the current AST node,
 * e.g. get the first/second operand from a comparison.
 * The 'i' parameter indicates which operand.
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_operand(nowdb_ast_t *node, int i);

/* -----------------------------------------------------------------------
 * Get user defined type from decl
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_utype(nowdb_ast_t *ast);

/* -----------------------------------------------------------------------
 * Convert AST type to generic NOWDB type
 * -----------------------------------------------------------------------
 */
nowdb_type_t nowdb_ast_type(uint32_t type);

/* -----------------------------------------------------------------------
 * Get the 'value' field of the ast as either
 * - uinsigend integer
 * - sigend integer
 * - string
 * -----------------------------------------------------------------------
 */
int nowdb_ast_getUInt(nowdb_ast_t *node, uint64_t *value);
int nowdb_ast_getInt(nowdb_ast_t *node, int64_t *value);
char *nowdb_ast_getString(nowdb_ast_t *node);

#endif
