#ifndef nowdb_sql_state_decl
#define nowdb_sql_state_decl

#include <nowdb/query/ast.h>

#define NOWDB_SQL_ERR_NO_MEM     1
#define NOWDB_SQL_ERR_SYNTAX     2
#define NOWDB_SQL_ERR_INCOMPLETE 3
#define NOWDB_SQL_ERR_LEX        4
#define NOWDB_SQL_ERR_PARSER     5
#define NOWDB_SQL_ERR_STACK      6
#define NOWDB_SQL_ERR_EOF        7
#define NOWDB_SQL_ERR_INPUT      8
#define NOWDB_SQL_ERR_PANIC     99
#define NOWDB_SQL_ERR_UNKNOWN  100

#define NOWDB_SQL_ERR_SIZE 128

#define NOWDB_SQL_COMMENT -17

typedef struct nowdbsql_stack_st nowdbsql_stack_t;

typedef struct {
	int             errcode;
	char            *errmsg;
	nowdbsql_stack_t *stack;
} nowdbsql_state_t;

const char *nowdbsql_err_desc(int err);
void nowdbsql_errmsg(nowdbsql_state_t *res, char *msg, char *token);

int nowdbsql_state_init(nowdbsql_state_t *res);
void nowdbsql_state_reinit(nowdbsql_state_t *res);
void nowdbsql_state_destroy(nowdbsql_state_t *res);

nowdb_ast_t *nowdbsql_state_ast(nowdbsql_state_t *res);

void nowdbsql_state_pushScope(nowdbsql_state_t *res, char *scope);
void nowdbsql_state_pushIndex(nowdbsql_state_t *res, char *index);
void nowdbsql_state_pushContext(nowdbsql_state_t *res, char *ctx);
void nowdbsql_state_pushVertex(nowdbsql_state_t *res, char *alias);
void nowdbsql_state_pushTable(nowdbsql_state_t *res, char *name,
                                                   char *alias);
void nowdbsql_state_pushComparison(nowdbsql_state_t *res, int comp);
void nowdbsql_state_pushOption(nowdbsql_state_t *res,
                              int option, char *value);
void nowdbsql_state_pushSizing(nowdbsql_state_t *res, int sizing);
void nowdbsql_state_pushStress(nowdbsql_state_t *res, int sizing);
void nowdbsql_state_pushDisk(nowdbsql_state_t *res, int sizing);
void nowdbsql_state_pushNocomp(nowdbsql_state_t *res);
void nowdbsql_state_pushNosort(nowdbsql_state_t *res);

void nowdbsql_state_pushDDL(nowdbsql_state_t *res);
void nowdbsql_state_pushCreate(nowdbsql_state_t *res);
void nowdbsql_state_pushDrop(nowdbsql_state_t *res);
void nowdbsql_state_pushAlter(nowdbsql_state_t *res);

void nowdbsql_state_pushDLL(nowdbsql_state_t *res);
void nowdbsql_state_pushLoad(nowdbsql_state_t *res, char *path);

void nowdbsql_state_pushMisc(nowdbsql_state_t *res);
void nowdbsql_state_pushUse(nowdbsql_state_t *res, char *name);

void nowdbsql_state_pushDQL(nowdbsql_state_t *res);
void nowdbsql_state_pushProjection(nowdbsql_state_t *res);
void nowdbsql_state_pushFrom(nowdbsql_state_t *res);
void nowdbsql_state_pushWhere(nowdbsql_state_t *res);

#endif
