#ifndef nowdbsql_ast_decl
#define nowdbsql_ast_decl

#define NOWDB_AST_DDL 1000
#define NOWDB_AST_DLL 2000
#define NOWDB_AST_DML 3000
#define NOWDB_AST_DQL 4000

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
#define NOWDB_AST_NOT    4006
#define NOWDB_AST_GROUP  4007
#define NOWDB_AST_ORDER  4008
#define NOWDB_AST_JOIN   4009

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
#define NOWDB_AST_THROUGHP 10209
#define NOWDB_AST_IGNORE   10210

#define NOWDB_AST_DATA     10300
#define NOWDB_AST_KEYVAL   10301
#define NOWDB_AST_VALLIST  10302

#define NOWDB_AST_PATH     10009
#define NOWDB_AST_LOC      10010

#define NOWDB_AST_V_STRING  1
#define NOWDB_AST_V_INTEGER 2

typedef struct nowdb_ast_st {
	int                  ntype;
	int                  stype;
	int                  vtype;
	void                *value;
	int                  nKids;
	struct nowdb_ast_st **kids;
} nowdb_ast_t;

void freeme(void*);

nowdb_ast_t *nowdb_ast_create(int ntype, int stype);
int nowdb_ast_init(nowdb_ast_t *n, int ntype, int stype);
void nowdb_ast_destroy(nowdb_ast_t *n);
int nowdb_ast_setValue(nowdb_ast_t *n, int vtype, void *val);
int nowdb_ast_add(nowdb_ast_t *n, nowdb_ast_t *k);

void nowdb_ast_show(nowdb_ast_t *n);

#endif
