/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Parser State
 * ========================================================================
 * The parser state maintains a stack containing partial parsing results
 * that are pushed onto the stack during parsing. The partial results are
 * ast nodes that completed at the end of the parsing process.
 * This may ease the parsing process in some situations,
 * but in fact we need it only in very few situations.
 * The parser (nowdbsql.y) should be improved, so that the stack is
 * in fact not necessary anymore.
 * ========================================================================
 */
#ifndef nowdb_sql_state_decl
#define nowdb_sql_state_decl

#include <nowdb/query/ast.h>

/* ------------------------------------------------------------------------
 * Error indicators
 * ------------------------------------------------------------------------
 */
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

/* ------------------------------------------------------------------------
 * Tokens that are never passed to the parser (negative values!)
 * ------------------------------------------------------------------------
 */
#define NOWDB_SQL_COMMENT -17
#define NOWDB_SQL_UNEXPECTED -1

/* ------------------------------------------------------------------------
 * Predeclaration of the stack
 * ------------------------------------------------------------------------
 */
typedef struct nowdbsql_stack_st nowdbsql_stack_t;

/* ------------------------------------------------------------------------
 * Parser state
 * ------------------------------------------------------------------------
 */
typedef struct {
	int             errcode;
	char            *errmsg;
	nowdbsql_stack_t *stack;
} nowdbsql_state_t;

/* ------------------------------------------------------------------------
 * Macro to check whether an error has occurred during parsing
 * ------------------------------------------------------------------------
 */
#define NOWDB_SQL_CHECKSTATE() \
	if (nowdbres->errcode != 0) { \
		return; \
	}

/* ------------------------------------------------------------------------
 * Macro to create an ast node during parsing
 * ------------------------------------------------------------------------
 */
#define NOWDB_SQL_CREATEAST(n, nt, st) \
	*n = nowdb_ast_create(nt, st); \
	if (*n == NULL) { \
		nowdbres->errcode = NOWDB_SQL_ERR_NO_MEM; \
		return; \
	}

/* ------------------------------------------------------------------------
 * Macro to add a kid to an existing ast node
 * ------------------------------------------------------------------------
 */
#define NOWDB_SQL_ADDKID(n, k) \
	if (nowdb_ast_add(n,k) != 0) { \
		nowdbres->errcode = NOWDB_SQL_ERR_NO_MEM; \
		return; \
	}

/* ------------------------------------------------------------------------
 * Get a description of the errorcode
 * ------------------------------------------------------------------------
 */
const char *nowdbsql_err_desc(int err);

/* ------------------------------------------------------------------------
 * Set an error message
 * ------------------------------------------------------------------------
 */
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

void nowdbsql_state_pushAst(nowdbsql_state_t *res, nowdb_ast_t *ast);

#endif
