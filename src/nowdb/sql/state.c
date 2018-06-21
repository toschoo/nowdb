/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Parser State
 * ========================================================================
 */
#include <nowdb/sql/state.h>
#include <nowdb/sql/nowdbsql.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

/* ------------------------------------------------------------------------
 * Error description
 * ------------------------------------------------------------------------
 */
const char *nowdbsql_err_desc(int err) {
	switch(err) {
	case 0: return "OK";
	case NOWDB_SQL_ERR_NO_MEM: return "out-of-memory";
	case NOWDB_SQL_ERR_SYNTAX: return "syntax error";
	case NOWDB_SQL_ERR_INCOMPLETE: return "incomplete input";
	case NOWDB_SQL_ERR_LEX: return "lexer error";
	case NOWDB_SQL_ERR_PARSER: return "internal parser error";
	case NOWDB_SQL_ERR_STACK: return "stack overflow";
	case NOWDB_SQL_ERR_EOF: return "end of input";
	case NOWDB_SQL_ERR_PANIC: return "PANIC! This is a bug!";
	default: return "unknown error";
	}
}

#define MAX_TOKEN_SIZE \
	(NOWDB_SQL_ERR_SIZE-16)/2

/* ------------------------------------------------------------------------
 * Create an error message and store it in errmsg
 * ------------------------------------------------------------------------
 */
void nowdbsql_errmsg(nowdbsql_state_t *res, char *msg, char *token) {
	if (res->errmsg != NULL) free(res->errmsg);
	res->errmsg = malloc(NOWDB_SQL_ERR_SIZE);
	if (res->errmsg == NULL) return;
	if (token == NULL) {
		sprintf(res->errmsg, "%s at unknown position", msg);
	} else {
		size_t s = strnlen(token, MAX_TOKEN_SIZE+1);
		if (s == MAX_TOKEN_SIZE+1) {
			token[MAX_TOKEN_SIZE] = 0;
		}
		s = strnlen(msg, MAX_TOKEN_SIZE+1);
		if (s == MAX_TOKEN_SIZE+1) {
			msg[MAX_TOKEN_SIZE] = 0;
		}
		sprintf(res->errmsg, "%s near '%s'", msg, token);
	}
}

#define STACKSIZE 8

/* ------------------------------------------------------------------------
 * The stack
 * ------------------------------------------------------------------------
 */
struct nowdbsql_stack_st {
	int hi; /* current pointer */
	int lo; /* stack base      */
	nowdb_ast_t **chunks; /* the stack elements */
};

/* ------------------------------------------------------------------------
 * push to the stack
 * ------------------------------------------------------------------------
 */
static inline int push(nowdbsql_stack_t *stk,
                       nowdb_ast_t      *ast) {
	if (stk->hi >= STACKSIZE) return -1;
	if (stk->lo > stk->hi) return -1;
	stk->chunks[stk->hi] = ast;
	stk->hi++;
	return 0;
}

/* ------------------------------------------------------------------------
 * Clean current position (without freeing memory)
 * ------------------------------------------------------------------------
 */
static inline void clean(nowdbsql_stack_t *stack, int pos) {
	stack->chunks[pos] = NULL;
}

/* ------------------------------------------------------------------------
 * Clean everything (without freeing memory)
 * ------------------------------------------------------------------------
 */
static inline void resetStack(nowdbsql_stack_t *stack) {
	for(int i=stack->hi-1; i>=stack->lo; i--) {
		if (stack->chunks[i] != NULL) break;
		if (stack->hi > stack->lo) stack->hi--;
	}
}

/* ------------------------------------------------------------------------
 * Clean everything (releasing memory)
 * ------------------------------------------------------------------------
 */
static inline void cleanStack(nowdbsql_stack_t *stack) {
	stack->lo = 0;
	stack->hi = 0;
	for(int i=0; i<STACKSIZE; i++) {
		if (stack->chunks[i] != NULL) {
			nowdb_ast_destroy(stack->chunks[i]);
			free(stack->chunks[i]);
			stack->chunks[i] = NULL;
		}
	}
}

/* ------------------------------------------------------------------------
 * Destroy the stack
 * ------------------------------------------------------------------------
 */
static inline void destroyStack(nowdbsql_stack_t *stack) {
	if (stack->chunks != NULL) {
		cleanStack(stack);
		free(stack->chunks); stack->chunks = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Show what's on the stack
 * ------------------------------------------------------------------------
 */
static inline void showStack(nowdbsql_stack_t *stack) {
	fprintf(stderr, "stack:\n");
	for(int i=0; i<=stack->hi; i++) {
		if (stack->chunks[i] == NULL) {
			fprintf(stderr, "%d: NULL\n", i);
		} else {
			fprintf(stderr, "%d: %d\n", i,
			     stack->chunks[i]->ntype);
		}
	}
}

/* ------------------------------------------------------------------------
 * Make state ready for reuse
 * ------------------------------------------------------------------------
 */
void nowdbsql_state_reinit(nowdbsql_state_t *res) {
	if (res->stack != NULL) cleanStack(res->stack);
	res->errcode = 0;
	if (res->errmsg != NULL) {
		free(res->errmsg); res->errmsg = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Initialise parser state
 * ------------------------------------------------------------------------
 */
int nowdbsql_state_init(nowdbsql_state_t *res) {
	memset(res, 0, sizeof(nowdbsql_state_t));
	res->stack = calloc(1,sizeof(nowdbsql_stack_t));
	if (res->stack == NULL) return -1;
	res->stack->chunks = calloc(STACKSIZE, sizeof(nowdb_ast_t*));
	if (res->stack->chunks == NULL) {
		free(res->stack); res->stack = NULL; return -1;
	}
	return 0;
}

/* ------------------------------------------------------------------------
 * Destroy parser state
 * ------------------------------------------------------------------------
 */
void nowdbsql_state_destroy(nowdbsql_state_t *res) {
	if (res->stack != NULL) {
		destroyStack(res->stack);
		free(res->stack); res->stack = NULL;
	}
	res->errcode = 0;
	if (res->errmsg != NULL) {
		free(res->errmsg); res->errmsg = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Get first position from stack
 * ------------------------------------------------------------------------
 */
nowdb_ast_t *nowdbsql_state_getAst(nowdbsql_state_t *res) {
	nowdb_ast_t *n = res->stack->chunks[0];
	clean(res->stack, 0);
	return n;
}

/* ------------------------------------------------------------------------
 * Push to stack
 * ------------------------------------------------------------------------
 */
void nowdbsql_state_pushAst(nowdbsql_state_t *res, nowdb_ast_t *n) {
	if (push(res->stack, n) != 0) { \
		res->errcode = NOWDB_SQL_ERR_STACK; \
	}
}
