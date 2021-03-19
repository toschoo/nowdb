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
#define NOWDB_SQL_ERR_CLOSED    10
#define NOWDB_SQL_ERR_SIGNAL    11
#define NOWDB_SQL_ERR_PROTOCOL  12
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
 * Macro to add a kid as param to an existing ast node
 * ------------------------------------------------------------------------
 */
#define NOWDB_SQL_ADDPARAM(n, k) \
	if (nowdb_ast_addParam(n,k) != 0) { \
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
		x = strnlen(s, 257); \
		if (x >= 2) { \
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
 * Create An INSERT statement and push it to the stack
 * Parameters:
 * - T: ast representing target
 * - F: ast representing field list
 * - V: ast representing value list
 * ------------------------------------------------------------------------
 */
#define NOWDB_SQL_MAKE_INSERT(T,F,V) \
	NOWDB_SQL_CHECKSTATE(); \
	nowdb_ast_t *dml; \
	nowdb_ast_t *i; \
	NOWDB_SQL_CREATEAST(&i, NOWDB_AST_INSERT, 0); \
	NOWDB_SQL_ADDKID(i, T); \
	NOWDB_SQL_ADDKID(i, V); \
	if (F != NULL) { \
		NOWDB_SQL_ADDKID(i, F); \
	} \
	NOWDB_SQL_CREATEAST(&dml,NOWDB_AST_DML, 0); \
	NOWDB_SQL_ADDKID(dml, i); \
	nowdbsql_state_pushAst(nowdbres, dml);

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
 * Make a MISC statement representing 'FETCH'
 * Parameters:
 * - u: the integer identifying the cursor
 * ------------------------------------------------------------------------
 */
#define NOWDB_SQL_MAKE_FETCH(u) \
	NOWDB_SQL_CHECKSTATE(); \
	nowdb_ast_t *f; \
	nowdb_ast_t *m; \
	NOWDB_SQL_CREATEAST(&f, NOWDB_AST_FETCH, 0); \
	nowdb_ast_setValue(f, NOWDB_AST_V_STRING, u); \
	NOWDB_SQL_CREATEAST(&m, NOWDB_AST_MISC, 0); \
	NOWDB_SQL_ADDKID(m, f); \
	nowdbsql_state_pushAst(nowdbres, m);

/* ------------------------------------------------------------------------
 * Make a MISC statement representing 'CLOSE'
 * Parameters:
 * - u: the integer identifying the cursor
 * ------------------------------------------------------------------------
 */
#define NOWDB_SQL_MAKE_CLOSE(u) \
	NOWDB_SQL_CHECKSTATE(); \
	nowdb_ast_t *c; \
	nowdb_ast_t *m; \
	NOWDB_SQL_CREATEAST(&c, NOWDB_AST_CLOSE, 0); \
	nowdb_ast_setValue(c, NOWDB_AST_V_STRING, u); \
	NOWDB_SQL_CREATEAST(&m, NOWDB_AST_MISC, 0); \
	NOWDB_SQL_ADDKID(m, c); \
	nowdbsql_state_pushAst(nowdbres, m);

/* ------------------------------------------------------------------------
 * Make a MISC statement representing an isolated 'SELECT'
 * Parameters:
 * - p: project clause
 * ------------------------------------------------------------------------
 */
#define NOWDB_SQL_MAKE_SELECT(p) \
	NOWDB_SQL_CHECKSTATE(); \
	nowdb_ast_t *m; \
	NOWDB_SQL_CREATEAST(&m, NOWDB_AST_MISC, 0); \
	NOWDB_SQL_ADDKID(m, p); \
	nowdbsql_state_pushAst(nowdbres, m);

/* ------------------------------------------------------------------------
 * Make a MISC statement representing 'EXECUTE'
 * Parameters:
 * - p: parameters
 * ------------------------------------------------------------------------
 */
#define NOWDB_SQL_MAKE_EXEC(N,P) \
	NOWDB_SQL_CHECKSTATE(); \
	nowdb_ast_t *x; \
	nowdb_ast_t *m; \
	NOWDB_SQL_CREATEAST(&x, NOWDB_AST_EXEC, 0); \
	nowdb_ast_setValue(x, NOWDB_AST_V_STRING, N); \
	if (P != NULL) {\
		NOWDB_SQL_ADDPARAM(x, P); \
	} \
	NOWDB_SQL_CREATEAST(&m, NOWDB_AST_MISC, 0); \
	NOWDB_SQL_ADDKID(m, x); \
	nowdbsql_state_pushAst(nowdbres, m);

/* ------------------------------------------------------------------------
 * Make a MISC statement representing 'LOCK' or 'UNLOCK'
 * Parameters:
 * - p: parameters
 * ------------------------------------------------------------------------
 */
#define NOWDB_SQL_MAKE_LOCK(L,o,P) \
	NOWDB_SQL_CHECKSTATE(); \
	nowdb_ast_t *z; \
	nowdb_ast_t *l; \
	NOWDB_SQL_CREATEAST(&z, o, 0); \
	nowdb_ast_setValue(z, NOWDB_AST_V_STRING, L); \
	if (P != NULL) {\
		NOWDB_SQL_ADDKID(z, P); \
	} \
	NOWDB_SQL_CREATEAST(&l, NOWDB_AST_MISC, 0); \
	NOWDB_SQL_ADDKID(l, z); \
	nowdbsql_state_pushAst(nowdbres, l);

/* ------------------------------------------------------------------------
 * Make an option
 * - A: the ast to create
 * - o: the option subcode
 * - t: the value type
 * - v: the value
 * ------------------------------------------------------------------------
 */
#define NOWDB_SQL_MAKE_OPTION(A,o,t,v) \
	NOWDB_SQL_CHECKSTATE(); \
	NOWDB_SQL_CREATEAST(&A, NOWDB_AST_OPTION, o); \
	nowdb_ast_setValue(A, t, v);

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
 * - I: the target identifier
 * - O: an option to be added
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
 * Make a 'CREATE PROCEDURE' statement
 * Parameters:
 * - C: the ast representing the CREATE
 * - M: the module
 * - N: the name of the procedure
 * - L: the language
 * - r: the return type (0: procedure)
 * - P: parameter list
 * ------------------------------------------------------------------------
 */
#define NOWDB_SQL_MAKE_PROC(C, M, N, L, r, P) \
	NOWDB_SQL_CHECKSTATE(); \
	nowdb_ast_t *t; \
	nowdb_ast_t *m; \
	nowdb_ast_t *o; \
	nowdb_ast_t *o2; \
	NOWDB_SQL_CREATEAST(&C, NOWDB_AST_CREATE, 0); \
	NOWDB_SQL_CREATEAST(&t, NOWDB_AST_TARGET, NOWDB_AST_PROC); \
	NOWDB_SQL_CREATEAST(&m, NOWDB_AST_TARGET, NOWDB_AST_MODULE); \
	NOWDB_SQL_CREATEAST(&o, NOWDB_AST_OPTION, NOWDB_AST_LANG); \
	nowdb_ast_setValue(t, NOWDB_AST_V_STRING, N); \
	nowdb_ast_setValue(m, NOWDB_AST_V_STRING, M); \
	nowdb_ast_setValue(o, NOWDB_AST_V_STRING, L); \
	NOWDB_SQL_ADDKID(t, m); \
	NOWDB_SQL_ADDKID(C, t); \
	NOWDB_SQL_ADDKID(C, o); \
	if (r != 0) { \
		NOWDB_SQL_CREATEAST(&o2, NOWDB_AST_OPTION, NOWDB_AST_TYPE); \
		nowdb_ast_setValue(o2, NOWDB_AST_V_INTEGER, r); \
		NOWDB_SQL_ADDKID(C,o2); \
	}\
	if (P != NULL) { \
		NOWDB_SQL_ADDKID(C,P); \
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
 * - E: the ast representing the edge
 * - I: the name of the user-defined type
 * - T: the vertex type
 * ------------------------------------------------------------------------
 */
#define NOWDB_SQL_MAKE_EDGE_TYPE(E, I, T) \
	NOWDB_SQL_CHECKSTATE(); \
	nowdb_ast_t *o; \
	NOWDB_SQL_CREATEAST(&E, NOWDB_AST_DECL, NOWDB_AST_TYPE); \
	nowdb_ast_setValue(E, NOWDB_AST_V_STRING, I); \
	NOWDB_SQL_CREATEAST(&o, NOWDB_AST_TYPE, 0); \
	nowdb_ast_setValue(o, NOWDB_AST_V_STRING, T); \
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
