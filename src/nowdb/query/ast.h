#ifndef nowdb_ast_decl
#define nowdb_ast_decl

#include <nowdb/reader/filter.h>

#include <stdlib.h>
#include <stdint.h>

#define NOWDB_AST_DDL  1000
#define NOWDB_AST_DLL  2000
#define NOWDB_AST_DML  3000
#define NOWDB_AST_DQL  4000
#define NOWDB_AST_MISC 5000

#define NOWDB_AST_CREATE 1001
#define NOWDB_AST_ALTER  1002
#define NOWDB_AST_DROP   1003

#define NOWDB_AST_LOAD   2001

#define NOWDB_AST_INSERT 3001
#define NOWDB_AST_UPDATE 3002

#define NOWDB_AST_FROM   4001
#define NOWDB_AST_SELECT 4002
#define NOWDB_AST_WHERE  4003
#define NOWDB_AST_AND    4004
#define NOWDB_AST_OR     4005
#define NOWDB_AST_JUST   4006
#define NOWDB_AST_NOT    4007
#define NOWDB_AST_GROUP  4008
#define NOWDB_AST_ORDER  4009
#define NOWDB_AST_JOIN   4010
#define NOWDB_AST_ALL    4011

#define NOWDB_AST_USE    5001

#define NOWDB_AST_TARGET   10100
#define NOWDB_AST_SCOPE    10101
#define NOWDB_AST_CONTEXT  10102
#define NOWDB_AST_VERTEX   10103
#define NOWDB_AST_INDEX    10104

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

#define NOWDB_AST_IFEXISTS 10301

#define NOWDB_AST_DATA     10300
#define NOWDB_AST_KEYVAL   10301
#define NOWDB_AST_VALLIST  10302
#define NOWDB_AST_FIELD    10303
#define NOWDB_AST_VALUE    10304

#define NOWDB_AST_PATH     10009
#define NOWDB_AST_LOC      10010

#define NOWDB_AST_COMPARE  10500

#define NOWDB_AST_EQ NOWDB_FILTER_EQ
#define NOWDB_AST_LE NOWDB_FILTER_LE
#define NOWDB_AST_GE NOWDB_FILTER_GE
#define NOWDB_AST_LT NOWDB_FILTER_LT
#define NOWDB_AST_GT NOWDB_FILTER_GT
#define NOWDB_AST_NE NOWDB_FILTER_NE

#define NOWDB_AST_V_STRING  1
#define NOWDB_AST_V_INTEGER 2

typedef struct nowdb_ast_st {
	int                  ntype;
	int                  stype;
	int                  vtype;
	void                *value;
	void                 *conv;
	int                  nKids;
	struct nowdb_ast_st **kids;
} nowdb_ast_t;

void freeme(void*);

nowdb_ast_t *nowdb_ast_create(int ntype, int stype);
int nowdb_ast_init(nowdb_ast_t *n, int ntype, int stype);
void nowdb_ast_destroy(nowdb_ast_t *n);
void nowdb_ast_destroyAndFree(nowdb_ast_t *n);
int nowdb_ast_setValue(nowdb_ast_t *n, int vtype, void *val);
int nowdb_ast_add(nowdb_ast_t *n, nowdb_ast_t *k);

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

nowdb_ast_t *nowdb_ast_operation(nowdb_ast_t *node);
nowdb_ast_t *nowdb_ast_target(nowdb_ast_t *node);
nowdb_ast_t *nowdb_ast_option(nowdb_ast_t *node, int option);

nowdb_ast_t *nowdb_ast_select(nowdb_ast_t *node);
nowdb_ast_t *nowdb_ast_from(nowdb_ast_t *node);
nowdb_ast_t *nowdb_ast_where(nowdb_ast_t *node);
nowdb_ast_t *nowdb_ast_condition(nowdb_ast_t *node);
nowdb_ast_t *nowdb_ast_compare(nowdb_ast_t *node);
nowdb_ast_t *nowdb_ast_operand(nowdb_ast_t *node, int i);

int nowdb_ast_getUInt(nowdb_ast_t *node, uint64_t *value);
int nowdb_ast_getInt(nowdb_ast_t *node, int64_t *value);
char *nowdb_ast_getString(nowdb_ast_t *node);

#endif
