/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Parser State
 * ========================================================================
 * The parser state maintains a stack containing partial parsing results
 * that are pushed onto the stack during parsing. The partial results are
 * ast nodes that were completed at the end of the parsing process.
 * This may ease the parsing process in some situations,
 * but in fact we currently use it only to store the final result.
 * ========================================================================
 */
#ifndef nowdb_sql_state_decl
#define nowdb_sql_state_decl

#include <nowdb/sql/ast.h>
#include <nowdb/scope/context.h>

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
#define NOWDB_SQL_ERR_BUFSIZE    8
#define NOWDB_SQL_ERR_INPUT      9
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
 * Macro to set a string, removing '
 * ------------------------------------------------------------------------
 */
#define NOWDB_SQL_SETSTRING(n, s) \
	size_t x; \
	char *z; \
	if (s != NULL) { \
		x = strnlen(s, 255); \
		if (x > 2) { \
			z = malloc(x-1); \
			if (z == NULL) { \
				nowdbres->errcode = NOWDB_SQL_ERR_NO_MEM; \
				return; \
			} \
			for(int i=0;i<x-1; i++) z[i] = s[i+1]; \
			z[x-2] = 0; \
			nowdb_ast_setValueAsString(n,NOWDB_AST_V_STRING,z); \
			free(s); \
		} \
	}

/* ------------------------------------------------------------------------
 * Create A DDL statement and push it to the stack
 * Parameters:
 * - C: an ast representing the DDL operation (CREATE, DROP, ALTER)
 * ------------------------------------------------------------------------
 */
#define NOWDB_SQL_MAKE_DDL(C) \
	NOWDB_SQL_CHECKSTATE(); \
	nowdb_ast_t *d; \
	NOWDB_SQL_CREATEAST(&d, NOWDB_AST_DDL, 0); \
	NOWDB_SQL_ADDKID(d, C); \
	nowdbsql_state_pushAst(nowdbres, d);

/* ------------------------------------------------------------------------
 * Create a DLL statement representing 'LOAD'
 * Parameters:
 * - S: string defining from where to load (path)
 * - T: ast representing the target
 * - O: ast representing options
 * - M: string representing a type or edge
 * ------------------------------------------------------------------------
 */
#define NOWDB_SQL_MAKE_LOAD(S,T,O,M) \
	NOWDB_SQL_CHECKSTATE(); \
	nowdb_ast_t *l; \
	nowdb_ast_t *d; \
	nowdb_ast_t *m; \
	NOWDB_SQL_CREATEAST(&l, NOWDB_AST_LOAD, 0); \
	nowdb_ast_setValue(l, NOWDB_AST_V_STRING, S); \
	NOWDB_SQL_ADDKID(l, T); \
	if (O != NULL) { \
		NOWDB_SQL_ADDKID(l, O); \
	} \
	if (M != NULL) { \
		NOWDB_SQL_CREATEAST(&m, NOWDB_AST_OPTION, NOWDB_AST_TYPE); \
		nowdb_ast_setValue(m, NOWDB_AST_V_STRING, M); \
		NOWDB_SQL_ADDKID(l, m); \
	} \
	NOWDB_SQL_CREATEAST(&d, NOWDB_AST_DLL, 0); \
	NOWDB_SQL_ADDKID(d, l); \
	nowdbsql_state_pushAst(nowdbres, d);

/* ------------------------------------------------------------------------
 * Create A DQL statement and push it to the stack
 * Parameters:
 * - P: ast representing projection (a.k.a SELECT)
 * - F: ast representing FROM
 * - W: ast representing WHERE
 * - G: ast representing GROUP BY
 * - O: ast representing ORDER BY
 * ------------------------------------------------------------------------
 */
#define NOWDB_SQL_MAKE_DQL(P,F,W,G,O) \
	NOWDB_SQL_CHECKSTATE(); \
	nowdb_ast_t *d; \
	NOWDB_SQL_CREATEAST(&d, NOWDB_AST_DQL, 0); \
	NOWDB_SQL_ADDKID(d, P); \
	NOWDB_SQL_ADDKID(d, F); \
	if (W != NULL) { \
		NOWDB_SQL_ADDKID(d, W); \
	} \
	if (G != NULL) { \
		NOWDB_SQL_ADDKID(d, G); \
	} \
	if (O != NULL) { \
		NOWDB_SQL_ADDKID(d, O); \
	} \
	nowdbsql_state_pushAst(nowdbres, d);

/* ------------------------------------------------------------------------
 * Make a MISC statement representing 'USE'
 * Parameters:
 * - S: string defining what to use
 * ------------------------------------------------------------------------
 */
#define NOWDB_SQL_MAKE_USE(S) \
	NOWDB_SQL_CHECKSTATE(); \
	nowdb_ast_t *u; \
	nowdb_ast_t *m; \
	NOWDB_SQL_CREATEAST(&u, NOWDB_AST_USE, 0); \
	nowdb_ast_setValue(u, NOWDB_AST_V_STRING, S); \
	NOWDB_SQL_CREATEAST(&m, NOWDB_AST_MISC, 0); \
	NOWDB_SQL_ADDKID(m, u); \
	nowdbsql_state_pushAst(nowdbres, m);

/* ------------------------------------------------------------------------
 * Create and add an option to an ast 
 * Parameters:
 * - C: the ast to add to
 * - o: the option subcode
 * - t: the value type
 * - v: the value
 * ------------------------------------------------------------------------
 */
#define NOWDB_SQL_ADD_OPTION(C,o,t,v) \
	NOWDB_SQL_CHECKSTATE(); \
	nowdb_ast_t *x; \
	NOWDB_SQL_CREATEAST(&x, NOWDB_AST_OPTION, o); \
	nowdb_ast_setValue(x, t, v); \
	NOWDB_SQL_ADDKID(C, x);

/* ------------------------------------------------------------------------
 * Make a 'CREATE' statement
 * Parameters:
 * - C: the ast representing the CREATE
 * - x: the target subcode
 * - t: the target identifier
 * - v: an option to be added
 * ------------------------------------------------------------------------
 */
#define NOWDB_SQL_MAKE_CREATE(C,x,I,O) \
	NOWDB_SQL_CHECKSTATE(); \
	nowdb_ast_t *t; \
	NOWDB_SQL_CREATEAST(&C, NOWDB_AST_CREATE, 0); \
	NOWDB_SQL_CREATEAST(&t, NOWDB_AST_TARGET, x); \
	nowdb_ast_setValue(t, NOWDB_AST_V_STRING, I); \
	NOWDB_SQL_ADDKID(C, t); \
	if (O != NULL) { \
		NOWDB_SQL_ADDKID(C,O); \
	}

/* ------------------------------------------------------------------------
 * Make a 'DROP' statement
 * Parameters:
 * - C: the ast representing the DROP
 * - x: the target subcode
 * - t: the target identifier
 * - v: an option to be added
 * ------------------------------------------------------------------------
 */
#define NOWDB_SQL_MAKE_DROP(C,x,I,O) \
	NOWDB_SQL_CHECKSTATE(); \
	nowdb_ast_t *t; \
	NOWDB_SQL_CREATEAST(&C, NOWDB_AST_DROP, 0); \
	NOWDB_SQL_CREATEAST(&t, NOWDB_AST_TARGET, x); \
	nowdb_ast_setValue(t, NOWDB_AST_V_STRING, I); \
	NOWDB_SQL_ADDKID(C, t); \
	if (O != NULL) { \
		NOWDB_SQL_ADDKID(C,O); \
	}

/* ------------------------------------------------------------------------
 * Make a and edge declaration for origin or destin
 * Parameters:
 * - C: the ast representing the edge
 * - x: the offset subcode
 * - I: the name of the user-defined type
 * ------------------------------------------------------------------------
 */
#define NOWDB_SQL_MAKE_EDGE_TYPE(E, x, I) \
	NOWDB_SQL_CHECKSTATE(); \
	nowdb_ast_t *o; \
	NOWDB_SQL_CREATEAST(&E, NOWDB_AST_DECL, NOWDB_AST_TYPE); \
	nowdb_ast_setValue(E, NOWDB_AST_V_STRING, I); \
	NOWDB_SQL_CREATEAST(&o, NOWDB_AST_OFF, x); \
	NOWDB_SQL_ADDKID(E,o);

/* ------------------------------------------------------------------------
 * Make a DML target statement
 * Parameters:
 * - S: ast representing the target
 * - x: target subcode (context, vertex)
 * - I: target name (if it is a context)
 * ------------------------------------------------------------------------
 */
#define NOWDB_SQL_MAKE_DMLTARGET(T,x,I) \
	NOWDB_SQL_CHECKSTATE(); \
	NOWDB_SQL_CREATEAST(&T, NOWDB_AST_TARGET, x); \
	if (I != NULL) { \
		nowdb_ast_setValue(T, NOWDB_AST_V_STRING, I); \
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

/* ------------------------------------------------------------------------
 * initialise state
 * ------------------------------------------------------------------------
 */
int nowdbsql_state_init(nowdbsql_state_t *res);

/* ------------------------------------------------------------------------
 * reinitialise state
 * ------------------------------------------------------------------------
 */
void nowdbsql_state_reinit(nowdbsql_state_t *res);

/* ------------------------------------------------------------------------
 * destroy state
 * ------------------------------------------------------------------------
 */
void nowdbsql_state_destroy(nowdbsql_state_t *res);

/* ------------------------------------------------------------------------
 * get result
 * ------------------------------------------------------------------------
 */
nowdb_ast_t *nowdbsql_state_getAst(nowdbsql_state_t *res);

/* ------------------------------------------------------------------------
 * push to stack
 * ------------------------------------------------------------------------
 */
void nowdbsql_state_pushAst(nowdbsql_state_t *res, nowdb_ast_t *ast);

#endif
