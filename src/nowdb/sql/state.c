#include <nowdb/sql/state.h>
#include <nowdb/sql/nowdbsql.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

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

#define STACKSIZE 256

struct nowdbsql_stack_st {
	int hi;
	int lo;
	nowdb_ast_t **chunks;
};

static inline int push(nowdbsql_stack_t *stk,
                       nowdb_ast_t      *ast) {
	if (stk->hi >= STACKSIZE) return -1;
	if (stk->lo > stk->hi) return -1;
	stk->chunks[stk->hi] = ast;
	stk->hi++;
	return 0;
}

static inline int hitType(nowdb_ast_t *ast, int *nTypes, int size) {
	for(int i=size-1; i>=0; i--) {
		if (ast != NULL && ast->ntype == nTypes[i]) return 1;
	}
	return 0;
}

static inline nowdb_ast_t *popType(nowdbsql_stack_t *stk,
                         int *nTypes, int size, int *pos) {
	for(int i=stk->hi; i>=stk->lo; i--) {
		if (hitType(stk->chunks[i], nTypes, size)) {
			*pos = i; return (stk->chunks[i]);
		}
	}
	return NULL;
}

static inline void clean(nowdbsql_stack_t *stack, int pos) {
	stack->chunks[pos] = NULL;
}

static inline void resetStack(nowdbsql_stack_t *stack) {
	for(int i=stack->hi-1; i>=stack->lo; i--) {
		if (stack->chunks[i] != NULL) break;
		if (stack->hi > stack->lo) stack->hi--;
	}
}

static inline void incStack(nowdbsql_stack_t *stack) {
	stack->lo++; 
	if (stack->hi < stack->lo) stack->hi = stack->lo+1;
}

static inline void decStack(nowdbsql_stack_t *stack) {
	stack->lo--; stack->hi = stack->lo;
}

static inline void limitStack(nowdbsql_stack_t *stack,
                                int *nTypes, int size) {
	for(int i=stack->hi; i>=0; i--) {
		if (hitType(stack->chunks[i], nTypes, size)) {
			stack->lo = i+1;
			if (stack->hi < stack->lo) {
				stack->hi = stack->lo+1;
			}
			break;
		}
	}
}

static inline void unlimitStack(nowdbsql_stack_t *stack) {
	stack->lo=0;
}

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

static inline void destroyStack(nowdbsql_stack_t *stack) {
	if (stack->chunks != NULL) {
		cleanStack(stack);
		free(stack->chunks); stack->chunks = NULL;
	}
}

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

void nowdbsql_state_reinit(nowdbsql_state_t *res) {
	if (res->stack != NULL) cleanStack(res->stack);
	res->errcode = 0;
	if (res->errmsg != NULL) {
		free(res->errmsg); res->errmsg = NULL;
	}
}

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

nowdb_ast_t *nowdbsql_state_ast(nowdbsql_state_t *res) {
	nowdb_ast_t *n = res->stack->chunks[0];
	clean(res->stack, 0);
	return n;
}

#define PUSHN(n) \
	if (push(res->stack, n) != 0) { \
		res->errcode = NOWDB_SQL_ERR_STACK; \
	}

#define CREATE(n, nt, st, vt, v) \
	*n = nowdb_ast_create(nt, st); \
	if (*n == NULL) { \
		res->errcode = NOWDB_SQL_ERR_NO_MEM; \
		return; \
	} \
	if (vt != 0) { \
		nowdb_ast_setValue(*n, vt, v); \
	}

#define POP(n, k, t, s, p) \
	*k = popType(res->stack, t, s, &p); \
	if (*k == NULL) { \
		if (n != NULL) { \
			nowdb_ast_destroy(n); free(n); \
		} \
		res->errcode = NOWDB_SQL_ERR_PARSER; \
		return; \
	}

#define TRYPOP(k, t, s, p) \
	*k = popType(res->stack, t, s, &p); \

#define ADDKID(n, k, p) \
	if (nowdb_ast_add(n,k) != 0) { \
		nowdb_ast_destroy(n); free(n); \
		res->errcode = NOWDB_SQL_ERR_PARSER; \
		return; \
	} \
	clean(res->stack, p)

#define RESET() \
	resetStack(res->stack)

#define INC() \
	incStack(res->stack);

#define DEC() \
	decStack(res->stack);

#define LIMIT(o, s) \
	limitStack(res->stack, o, s);

#define REOPEN() \
	unlimitStack(res->stack);

static inline void PUSH(nowdbsql_state_t *res, int nt, int st, int vt, void *v) {
	nowdb_ast_t *n;
	n = nowdb_ast_create(nt, st);
	if (n == NULL) {
		res->errcode = NOWDB_SQL_ERR_NO_MEM;
		return;
	}
	if (vt != 0) nowdb_ast_setValue(n, vt, v);
	PUSHN(n);
}

void nowdbsql_state_pushScope(nowdbsql_state_t *res,
                                        char *scope) {
	PUSH(res, NOWDB_AST_TARGET, NOWDB_AST_SCOPE, NOWDB_AST_V_STRING, scope);
}

void nowdbsql_state_pushIndex(nowdbsql_state_t *res,
                                        char *index) {
	/* missing: on, keys, etc. */
	PUSH(res, NOWDB_AST_TARGET, NOWDB_AST_INDEX, NOWDB_AST_V_STRING, index);
}

void nowdbsql_state_pushContext(nowdbsql_state_t *res,
                                            char *ctx) {
	PUSH(res, NOWDB_AST_TARGET, NOWDB_AST_CONTEXT, NOWDB_AST_V_STRING, ctx);
}

void nowdbsql_state_pushVertex(nowdbsql_state_t *res, char *alias) {
	if (alias == NULL) {
		PUSH(res, NOWDB_AST_TARGET, NOWDB_AST_VERTEX, 0, NULL);
	} else {
		PUSH(res, NOWDB_AST_TARGET, NOWDB_AST_VERTEX,
		          NOWDB_AST_V_STRING, alias);
	}
}

/* table or table spec ??? */
void nowdbsql_state_pushTable(nowdbsql_state_t *res, char *name, char *alias) {
	if (alias == NULL) 
		PUSH(res, NOWDB_AST_TARGET, NOWDB_AST_CONTEXT,
		          NOWDB_AST_V_STRING, name);
	else {
		fprintf(stderr, "PUSH TABLE NAME WITH ALIAS!!!\n");
		res->errcode = NOWDB_SQL_ERR_PARSER;
	}
}

void nowdbsql_state_pushField(nowdbsql_state_t *res, char *name) {
	// fprintf(stderr, "Field: '%s'\n", name);
	PUSH(res, NOWDB_AST_FIELD, 0,
	          NOWDB_AST_V_STRING, name);
}

void nowdbsql_state_pushValue(nowdbsql_state_t *res, int typ, char *name) {
	// fprintf(stderr, "VALUE: '%s' (%d)\n", name, typ);
	PUSH(res, NOWDB_AST_VALUE, typ,
	          NOWDB_AST_V_STRING, name);
}

void nowdbsql_state_pushComparison(nowdbsql_state_t *res,
                                                int comp) {
	switch(comp) {
	case NOWDB_SQL_EQ:
		PUSH(res, NOWDB_AST_COMPARE, NOWDB_AST_EQ, 0, NULL);
		break;
	case NOWDB_SQL_LE:
		PUSH(res, NOWDB_AST_COMPARE, NOWDB_AST_LE, 0, NULL);
		break;
	case NOWDB_SQL_GE:
		PUSH(res, NOWDB_AST_COMPARE, NOWDB_AST_GE, 0, NULL);
		break;
	case NOWDB_SQL_LT:
		PUSH(res, NOWDB_AST_COMPARE, NOWDB_AST_LT, 0, NULL);
		break;
	case NOWDB_SQL_GT:
		PUSH(res, NOWDB_AST_COMPARE, NOWDB_AST_GT, 0, NULL);
		break;
	case NOWDB_SQL_NE:
		PUSH(res, NOWDB_AST_COMPARE, NOWDB_AST_NE, 0, NULL);
		break;
	default:
		fprintf(stderr, "UNKNOWN COMPARISON: %d\n", comp);
		res->errcode = NOWDB_SQL_ERR_PARSER;
	}
}

void nowdbsql_state_pushSizing(nowdbsql_state_t *res,
                                            int size) {
	PUSH(res, NOWDB_AST_OPTION, 
	          NOWDB_AST_SIZING,
	          NOWDB_AST_V_INTEGER,
	     (void*)(uint64_t)size); /* this is an SQL constant! */
}

void nowdbsql_state_pushStress(nowdbsql_state_t *res,
                                            int size) {
	PUSH(res, NOWDB_AST_OPTION, 
	          NOWDB_AST_STRESS,
	          NOWDB_AST_V_INTEGER,
	       (void*)(uint64_t)size); /* this is an SQL constant! */
}

void nowdbsql_state_pushDisk(nowdbsql_state_t *res,
                                          int size) {
	PUSH(res, NOWDB_AST_OPTION, 
	          NOWDB_AST_DISK,
	          NOWDB_AST_V_INTEGER,
	     (void*)(uint64_t)size); /* this is an SQL constant! */
}

void nowdbsql_state_pushNocomp(nowdbsql_state_t *res) {
	PUSH(res, NOWDB_AST_OPTION, NOWDB_AST_COMP, 0, NULL);
}

void nowdbsql_state_pushNosort(nowdbsql_state_t *res) {
	PUSH(res, NOWDB_AST_OPTION, NOWDB_AST_SORT, 0, NULL);
}

void nowdbsql_state_pushOption(nowdbsql_state_t *res,
                             int option, char *value) {
	switch(option) {
	case NOWDB_SQL_ALLOCSIZE:
		PUSH(res, NOWDB_AST_OPTION, NOWDB_AST_ALLOCSZ,
		          NOWDB_AST_V_STRING, value);
		break;
	case NOWDB_SQL_LARGESIZE:
		PUSH(res, NOWDB_AST_OPTION, NOWDB_AST_LARGESZ,
		          NOWDB_AST_V_STRING, value);
		break;
	case NOWDB_SQL_SORTERS:
		PUSH(res, NOWDB_AST_OPTION, NOWDB_AST_SORTERS,
		          NOWDB_AST_V_STRING, value);
		break;
	case NOWDB_SQL_SORTING: break;
	case NOWDB_SQL_COMPRESSION:
		PUSH(res, NOWDB_AST_OPTION, NOWDB_AST_COMP,
		          NOWDB_AST_V_STRING, value);
		break;
	case NOWDB_SQL_ENCRYPTION:
		PUSH(res, NOWDB_AST_OPTION, NOWDB_AST_ENCP,
		          NOWDB_AST_V_STRING, value);
		break;
	case NOWDB_SQL_IGNORE:
		if (value == NULL) {
			PUSH(res, NOWDB_AST_OPTION, NOWDB_AST_IGNORE, 0, NULL);
		} else {
			PUSH(res, NOWDB_AST_OPTION, NOWDB_AST_IGNORE,
			          NOWDB_AST_V_STRING, value);
		}
		break;
	case NOWDB_SQL_USE:
		if (value == NULL) {
			PUSH(res, NOWDB_AST_OPTION, NOWDB_AST_USE, 0, NULL);
		} else {
			PUSH(res, NOWDB_AST_OPTION, NOWDB_AST_USE,
			          NOWDB_AST_V_STRING, value);
		}
		break;
	case NOWDB_SQL_EXISTS:
		PUSH(res, NOWDB_AST_OPTION, NOWDB_AST_IFEXISTS, 0, NULL);
		break;
	default:
		fprintf(stderr, "unknown option: %d, %s\n",
		                            option, value);
		if (value != NULL) free(value);
	}
}

void setDDl(int *ddl) {
	ddl[0] = NOWDB_AST_CREATE;
	ddl[1] = NOWDB_AST_ALTER;
	ddl[2] = NOWDB_AST_DROP;
}

void nowdbsql_state_pushDDL(nowdbsql_state_t *res) {
	int p;
	int ddl[3];
	nowdb_ast_t *n,*k;

	CREATE(&n, NOWDB_AST_DDL, 0, 0, NULL);
	setDDl(ddl);
	POP(n, &k, ddl, 3, p);
	ADDKID(n,k,p);
	RESET();
	PUSHN(n);
}

void nowdbsql_state_pushDLL(nowdbsql_state_t *res) {
	int p;
	int dll = NOWDB_AST_LOAD;
	nowdb_ast_t *n,*k;

	CREATE(&n, NOWDB_AST_DLL, 0, 0, NULL);
	POP(n, &k, &dll, 1, p);
	ADDKID(n,k,p);
	RESET();
	PUSHN(n);
}

void nowdbsql_state_pushMisc(nowdbsql_state_t *res) {
	int p;
	int misc = NOWDB_AST_USE;
	nowdb_ast_t *n,*k;

	CREATE(&n, NOWDB_AST_MISC, 0, 0, NULL);
	POP(n, &k, &misc, 1, p);
	ADDKID(n,k,p);
	RESET();
	PUSHN(n);
}

void nowdbsql_state_pushDrop(nowdbsql_state_t *res) {
	int p;
	int target = NOWDB_AST_TARGET;
	int option = NOWDB_AST_OPTION;
	nowdb_ast_t *n, *k;
	
	CREATE(&n, NOWDB_AST_DROP, 0, 0, NULL);
	POP(n, &k, &target, 1, p);
	ADDKID(n,k,p);
	TRYPOP(&k, &option, 1, p);
	while(k!=NULL) {
		ADDKID(n,k,p);
		TRYPOP(&k,&option,1,p);
	}
	RESET();
	PUSHN(n);
}

void nowdbsql_state_pushCreate(nowdbsql_state_t *res) {
	int p;
	int target = NOWDB_AST_TARGET;
	int option = NOWDB_AST_OPTION;
	nowdb_ast_t *n, *k;

	CREATE(&n, NOWDB_AST_CREATE, 0, 0, NULL);
	POP(n, &k, &target, 1, p);
	ADDKID(n,k,p);
	TRYPOP(&k, &option, 1, p);
	while(k!=NULL) {
		ADDKID(n,k,p);
		TRYPOP(&k,&option,1,p);
	}
	RESET();
	PUSHN(n);
}

void nowdbsql_state_pushAlter(nowdbsql_state_t *res) {
	int p;
	int target = NOWDB_AST_TARGET;
	int option = NOWDB_AST_OPTION;
	nowdb_ast_t *n, *k;

	CREATE(&n, NOWDB_AST_ALTER, 0, 0, NULL);
	POP(n, &k, &target, 1, p);
	ADDKID(n,k,p);
	TRYPOP(&k, &option, 1, p);
	while(k!=NULL) {
		ADDKID(n,k,p);
		TRYPOP(&k,&option,1,p);
	}
	RESET();
	PUSHN(n);
}

void nowdbsql_state_pushLoad(nowdbsql_state_t *res, char *path) {
	int p;
	int target = NOWDB_AST_TARGET;
	int option = NOWDB_AST_OPTION;
	nowdb_ast_t *n, *k;
	
	CREATE(&n, NOWDB_AST_LOAD, 0, NOWDB_AST_V_STRING, path);
	POP(n, &k, &target, 1, p);
	ADDKID(n,k,p);
	TRYPOP(&k, &option, 1, p);
	while(k!=NULL) {
		ADDKID(n,k,p);
		TRYPOP(&k,&option,1,p);
	}
	RESET();
	PUSHN(n);
}

void nowdbsql_state_pushUse(nowdbsql_state_t *res, char *name) {
	nowdb_ast_t *n;
	CREATE(&n, NOWDB_AST_USE, 0, NOWDB_AST_V_STRING, name);
	PUSHN(n);
}

void nowdbsql_state_pushDQL(nowdbsql_state_t *res) {
	int p;
	int select = NOWDB_AST_SELECT;
	int   from = NOWDB_AST_FROM;
	int  where = NOWDB_AST_WHERE;
	nowdb_ast_t *n, *k;
	
	REOPEN();

	CREATE(&n, NOWDB_AST_DQL, 0, 0, NULL);

	POP(n, &k, &select, 1, p);
	ADDKID(n,k,p);

	POP(n, &k, &from, 1, p);
	ADDKID(n,k,p);

	TRYPOP(&k, &where, 1, p);
	if (k!=NULL) ADDKID(n,k,p);

	RESET();
	PUSHN(n);
}

void nowdbsql_state_pushProjection(nowdbsql_state_t *res) {
	nowdb_ast_t *n;

	CREATE(&n, NOWDB_AST_SELECT, NOWDB_AST_ALL, 0, NULL);
	RESET();
	PUSHN(n);
	INC();
}

void nowdbsql_state_pushFrom(nowdbsql_state_t *res) {
	int p;
	int target = NOWDB_AST_TARGET;
	nowdb_ast_t *n, *k;

	CREATE(&n, NOWDB_AST_FROM, 0, 0, NULL);
	POP(n, &k, &target, 1, p);
	ADDKID(n,k,p);
	TRYPOP(&k, &target, 1, p);
	while(k!=NULL) {
		ADDKID(n,k,p);
		TRYPOP(&k,&target,1,p);
	}
	RESET();
	PUSHN(n);
	INC();
}

void setCondition(int *cond) {
	cond[0] = NOWDB_AST_JUST;
	cond[1] = NOWDB_AST_AND;
	cond[2] = NOWDB_AST_OR;
	cond[3] = NOWDB_AST_NOT;
}

void nowdbsql_state_pushWhere(nowdbsql_state_t *res) {
	int p;
	int from = NOWDB_AST_FROM;
	int cond[4];
	nowdb_ast_t *n, *k;

	setCondition(cond);

	LIMIT(&from, 1);

	CREATE(&n, NOWDB_AST_WHERE, 0, 0, NULL);

	POP(n, &k, cond, 4, p);
	ADDKID(n,k,p);

	RESET();
	PUSHN(n);
	INC();
}

void nowdbsql_state_pushAndOr(nowdbsql_state_t *res, int what, char neg) {
	int p;
	int from = NOWDB_AST_FROM;
	int cond[4];
	nowdb_ast_t *n, *k;

	setCondition(cond);

	LIMIT(&from, 1);

	CREATE(&n, what, 0, 0, NULL);

	POP(n, &k, cond, 4, p);
	ADDKID(n,k,p);

	TRYPOP(&k, cond, 4, p);
	if (k != NULL) ADDKID(n,k,p);

	RESET();
	PUSHN(n);
	INC();
}

void nowdbsql_state_pushCondition(nowdbsql_state_t *res, char neg) {
	int p,p1;
	int comp  = NOWDB_AST_COMPARE;
	int op[2];
	nowdb_ast_t *n, *k, *g;

	if (neg) {
		CREATE(&n, NOWDB_AST_NOT, 0, 0, NULL);
	} else {
		CREATE(&n, NOWDB_AST_JUST, 0, 0, NULL);
	}

	POP(n, &k, &comp, 1, p1);

	op[0] = NOWDB_AST_FIELD;
	op[1] = NOWDB_AST_VALUE;

	TRYPOP(&g, op, 2, p);
	if (g!=NULL) {
		// fprintf(stderr, "%d %s\n", g->ntype, (char*)g->value);
		ADDKID(k,g,p);
	}
	TRYPOP(&g, op, 2, p);
	if (g!=NULL) {
		// fprintf(stderr, "%d %s\n", g->ntype, (char*)g->value);
		ADDKID(k,g,p);
	}

	ADDKID(n,k,p1);
	RESET();
	PUSHN(n);
	INC();
}
