/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * AST: Abstract Syntax Tree
 * ========================================================================
 */
#include <nowdb/sql/ast.h>

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>

/* -----------------------------------------------------------------------
 * Macro for allocating an ast node
 * -----------------------------------------------------------------------
 */
#define ASTCALLOC(k) \
	if (k > 0) { \
		n->kids = calloc(k, sizeof(nowdb_ast_t*)); \
		if (n->kids == NULL) return -1; \
	} \
	n->nKids = k; \
	return 0

/* -----------------------------------------------------------------------
 * Unknown AST error
 * -----------------------------------------------------------------------
 */
#define UNDEFINED(k) \
	fprintf(stderr, "unkdefined: %d\n", k); \
	free(n); return -1;

/* -----------------------------------------------------------------------
 * Macro to recursively add a kid
 * -----------------------------------------------------------------------
 */
#define ADDKID(x) \
	if (n->kids[x] == NULL) { \
		n->kids[x] = k; \
	} else { \
		nowdb_ast_add(n->kids[x], k); \
	} \
	return 0

/* -----------------------------------------------------------------------
 * Create a new AST node
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_create(int ntype, int stype) {
	nowdb_ast_t *n;

	n = calloc(1,sizeof(nowdb_ast_t));
	if (n == NULL) return NULL;

	if (nowdb_ast_init(n, ntype, stype) != 0) {
		free(n); return NULL;
	}
	return n;
}

/* -----------------------------------------------------------------------
 * Intialise an already allocated AST node
 * -----------------------------------------------------------------------
 */
int nowdb_ast_init(nowdb_ast_t *n, int ntype, int stype) {

	n->ntype = ntype;
	n->stype = stype;

	switch(ntype) {

	case NOWDB_AST_DDL:
	case NOWDB_AST_DLL:
	case NOWDB_AST_DML: ASTCALLOC(1);
	case NOWDB_AST_DQL: ASTCALLOC(5); 
	case NOWDB_AST_MISC: ASTCALLOC(1); 

	case NOWDB_AST_CREATE: ASTCALLOC(5);
	case NOWDB_AST_ALTER: ASTCALLOC(2);
	case NOWDB_AST_DROP:  ASTCALLOC(2);
	case NOWDB_AST_SHOW:  ASTCALLOC(0);
	case NOWDB_AST_DESC:  ASTCALLOC(0);

	case NOWDB_AST_LOAD:  ASTCALLOC(2); 

	case NOWDB_AST_INSERT: ASTCALLOC(3);
	case NOWDB_AST_DELETE: ASTCALLOC(2);
	case NOWDB_AST_UPDATE: UNDEFINED(ntype);

	case NOWDB_AST_FROM:
	case NOWDB_AST_SELECT: ASTCALLOC(2);
	case NOWDB_AST_WHERE: ASTCALLOC(1);
	case NOWDB_AST_AND:
	case NOWDB_AST_OR: ASTCALLOC(2);
	case NOWDB_AST_NOT:    
	case NOWDB_AST_JUST: ASTCALLOC(1);
	case NOWDB_AST_GROUP:  
	case NOWDB_AST_ORDER: ASTCALLOC(1); 
	case NOWDB_AST_JOIN: UNDEFINED(ntype); 

	case NOWDB_AST_COMPARE: ASTCALLOC(2);

	case NOWDB_AST_FIELD: ASTCALLOC(1);
	case NOWDB_AST_VALUE: ASTCALLOC(1);
	case NOWDB_AST_DECL: ASTCALLOC(3);
	case NOWDB_AST_DESTIN: ASTCALLOC(1);
	case NOWDB_AST_FUN: ASTCALLOC(2);
	case NOWDB_AST_OP: ASTCALLOC(2);
	case NOWDB_AST_TYPE: ASTCALLOC(0);

	case NOWDB_AST_USE: ASTCALLOC(0);
	case NOWDB_AST_FETCH: ASTCALLOC(0);
	case NOWDB_AST_CLOSE: ASTCALLOC(0);
	case NOWDB_AST_EXEC: ASTCALLOC(1);
	case NOWDB_AST_LOCK: ASTCALLOC(2);
	case NOWDB_AST_UNLOCK: ASTCALLOC(2);

	case NOWDB_AST_TARGET: ASTCALLOC(2);
	case NOWDB_AST_ALIAS: ASTCALLOC(0);
	case NOWDB_AST_OPTION: ASTCALLOC(1);
	case NOWDB_AST_PATH:   ASTCALLOC(0);
	case NOWDB_AST_DATA:   ASTCALLOC(1);
	case NOWDB_AST_ON:   ASTCALLOC(0);
	case NOWDB_AST_STORAGE: ASTCALLOC(0);

	default:
		return -1;
	}
	return 0;
}

/* -----------------------------------------------------------------------
 * Convert the AST ntype/stype to a nice string
 * -----------------------------------------------------------------------
 */
static inline char *tellType(int ntype, int stype) {
	switch(ntype) {
	case 0: return "empty";
	case NOWDB_AST_DDL: return "data definition";
	case NOWDB_AST_DLL: return "data loading";
	case NOWDB_AST_DML: return "data manipulation";
	case NOWDB_AST_DQL: return "data query";
	case NOWDB_AST_MISC: return "miscellaneous";

	case NOWDB_AST_CREATE: return "create";
	case NOWDB_AST_ALTER: return "alter";
	case NOWDB_AST_DROP:  return "drop";

	case NOWDB_AST_SHOW:  return "show";
	case NOWDB_AST_DESC:  return "describe";

	case NOWDB_AST_LOAD:  return "load";

	case NOWDB_AST_INSERT: return "insert";
	case NOWDB_AST_UPDATE: return "update";
	case NOWDB_AST_DELETE: return "delete";

	case NOWDB_AST_FROM: return "from";
	case NOWDB_AST_SELECT: 
		if (stype == NOWDB_AST_STAR) return "select *";
		else return "select";

	case NOWDB_AST_WHERE: return "where";
	case NOWDB_AST_AND: return "and";
	case NOWDB_AST_OR: return "or";
	case NOWDB_AST_NOT: return "not";
	case NOWDB_AST_JUST: return "just";
	case NOWDB_AST_GROUP:  return "group";
	case NOWDB_AST_ORDER:  return "order";
	case NOWDB_AST_JOIN: return "join";

	case NOWDB_AST_FIELD: 
		if (stype == NOWDB_AST_PARAM) {
			return "field parameter";
		} else {
			return "field"; 
		}

	case NOWDB_AST_FUN: return "function"; 
	case NOWDB_AST_OP: return "operator"; 

	case NOWDB_AST_VALUE:
		switch(stype) {
		case NOWDB_AST_TEXT: return "text value"; 
		case NOWDB_AST_FLOAT: return "float value"; 
		case NOWDB_AST_UINT: return "uint value"; 
		case NOWDB_AST_INT: return "int value"; 
		case NOWDB_AST_BOOL: return "bool value"; 
		case NOWDB_AST_NULL: return "null value"; 
		default: return "unknown type of value";
		}

	case NOWDB_AST_DECL: 
		switch(stype) {
		case NOWDB_AST_TEXT: return "text field"; 
		case NOWDB_AST_FLOAT: return "float field"; 
		case NOWDB_AST_UINT: return "uint field"; 
		case NOWDB_AST_INT: return "int field"; 
		case NOWDB_AST_DATE: return "date field"; 
		case NOWDB_AST_TIME: return "time field"; 
		case NOWDB_AST_BOOL: return "bool field"; 
		case NOWDB_AST_TYPE: return "user defined field"; 
		default: return "unknown field type";
		}

	case NOWDB_AST_TYPE: return "user defined type";

	case NOWDB_AST_USE: return "use";
	case NOWDB_AST_FETCH: return "fetch";
	case NOWDB_AST_CLOSE: return "close";
	case NOWDB_AST_EXEC: return "execute";
	case NOWDB_AST_LOCK: return "lock";
	case NOWDB_AST_UNLOCK: return "unlock";

	case NOWDB_AST_TARGET:
		switch(stype) {
		case NOWDB_AST_SCOPE: return "scope";
		case NOWDB_AST_STORAGE: return "storage";
		case NOWDB_AST_VERTEX: return "vertex";
		case NOWDB_AST_CONTEXT: return "context";
		case NOWDB_AST_INDEX: return "index";
		case NOWDB_AST_TYPE: return "type";
		case NOWDB_AST_EDGE: return "edge";
		case NOWDB_AST_PROC: return "procedure";
		case NOWDB_AST_MODULE: return "module";
		case NOWDB_AST_MUTEX: return "lock";
		case NOWDB_AST_QUEUE: return "queue";
		default: return "unknown target";
		}

	case NOWDB_AST_ALIAS: return "alias";

	case NOWDB_AST_STORAGE: return "storage";

	case NOWDB_AST_ON:
		switch(stype) {
		case NOWDB_AST_VERTEX: return "on vertex";
		case NOWDB_AST_CONTEXT: return "on context";
		default: return "on unknown target";
		}

	case NOWDB_AST_OPTION: 
		switch(stype) {
		case NOWDB_AST_SIZING: return "option sizing";
		case NOWDB_AST_ALLOCSZ: return "option alloc size";
		case NOWDB_AST_LARGESZ: return "option large size";
		case NOWDB_AST_SORTERS: return "option sorters";
		case NOWDB_AST_SORT: return "option sort";
		case NOWDB_AST_COMP: return "option comp";
		case NOWDB_AST_ENCP: return "option encp";
		case NOWDB_AST_STRESS: return "option stress";
		case NOWDB_AST_DISK: return "option disk";
		case NOWDB_AST_IFEXISTS: return "if (not) exists";
		case NOWDB_AST_IGNORE: return "option ignore";
		case NOWDB_AST_USE: return "option use";
		case NOWDB_AST_PK: return "primary key";
		case NOWDB_AST_STAMP: return "timestamp";
		case NOWDB_AST_ORIGIN: return "origin";
		case NOWDB_AST_DESTIN: return "destination";
		case NOWDB_AST_INC: return "increment";
		case NOWDB_AST_TYPE: return "as type";
		case NOWDB_AST_LANG: return "language";
		case NOWDB_AST_ERRORS: return "error file";
		case NOWDB_AST_MODE: return "option mode";
		case NOWDB_AST_TIMEOUT: return "option timeout";
		default: return "unknown option";
		}

	case NOWDB_AST_COMPARE: 
		switch(stype) {
		case NOWDB_AST_EQ: return "=";
		case NOWDB_AST_LE: return "<=";
		case NOWDB_AST_GE: return ">=";
		case NOWDB_AST_LT: return "<";
		case NOWDB_AST_GT: return ">";
		case NOWDB_AST_NE: return "!=";
		case NOWDB_AST_IN: return "in";
		default: return "unknown compare";
		}

	case NOWDB_AST_DATA:
		switch(stype) {
		case NOWDB_AST_KEYVAL: return "data as pairs";
		case NOWDB_AST_VALLIST: return "data as list";
		default: return "data in unknown format";
		}

	case NOWDB_AST_PATH:   return "path";

	default: fprintf(stderr, "%d|%d is unknown\n", ntype, stype); return "unknown";
	}
}

/* -----------------------------------------------------------------------
 * Destroy an AST recursively
 * -----------------------------------------------------------------------
 */
static void destroy(nowdb_ast_t *n, char r) {
	if (n == NULL) return;
	if (n->value != NULL) {
		if (n->vtype == NOWDB_AST_V_STRING) {
			free(n->value);
		}
		n->value = NULL;
	}
	for(int i=0;r && i<n->nKids;i++) {
		if (n->kids[i] != NULL) {
			nowdb_ast_destroy(n->kids[i]);
			free(n->kids[i]);
		}
	}
	if (n->kids != NULL) {
		free(n->kids); n->kids = NULL;
	}
}

/* -----------------------------------------------------------------------
 * Destroy an AST recursively
 * -----------------------------------------------------------------------
 */
void nowdb_ast_destroy(nowdb_ast_t *n) {
	destroy(n,1);
}

/* -----------------------------------------------------------------------
 * Destroy an AST non-recursively
 * -----------------------------------------------------------------------
 */
void nowdb_ast_destroyNR(nowdb_ast_t *n) {
	destroy(n,0);
}

/* -----------------------------------------------------------------------
 * Destroy an AST recursively and free it
 * -----------------------------------------------------------------------
 */
void nowdb_ast_destroyAndFree(nowdb_ast_t *n) {
	if (n == NULL) return;
	nowdb_ast_destroy(n);
	free(n);
}

/* -----------------------------------------------------------------------
 * Set the value
 * -----------------------------------------------------------------------
 */
void nowdb_ast_setValue(nowdb_ast_t *n,
                  int vtype, void *val) 
{
	n->vtype = vtype;
	n->value = val; /* note that lemon does not free
	                 * this pointer, since it is used
	                 * in an action, so we can just
	                 * take it over and free it later! */
}

/* -----------------------------------------------------------------------
 * Set the value
 * -----------------------------------------------------------------------
 */
void nowdb_ast_setValueAsString(nowdb_ast_t *n,
                          int vtype, void *val) 
{
	n->isstr = 1;
	n->vtype = vtype;
	n->value = val;
}

/* -----------------------------------------------------------------------
 * Add kid to a DDL node
 * -----------------------------------------------------------------------
 */
static inline int ad4l(nowdb_ast_t *n,
                       nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_CREATE:
	case NOWDB_AST_ALTER: 
	case NOWDB_AST_DROP:
	case NOWDB_AST_SHOW:
	case NOWDB_AST_DESC: ADDKID(0);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to a DLL node
 * -----------------------------------------------------------------------
 */
static inline int ad3ll(nowdb_ast_t *n,
                       nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_LOAD: ADDKID(0);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to a DML node
 * -----------------------------------------------------------------------
 */
static inline int ad3ml(nowdb_ast_t *n,
                       nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_INSERT: ADDKID(0);
	case NOWDB_AST_UPDATE: return -1;
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to a DQL node
 * -----------------------------------------------------------------------
 */
static inline int ad3ql(nowdb_ast_t *n,
                        nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_SELECT: ADDKID(1);
	case NOWDB_AST_JOIN:
	case NOWDB_AST_FROM: ADDKID(0);
	case NOWDB_AST_WHERE:
	case NOWDB_AST_AND:
	case NOWDB_AST_OR: ADDKID(2);
	case NOWDB_AST_GROUP: ADDKID(3);
	case NOWDB_AST_ORDER: ADDKID(4);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to a miscellaneous node
 * -----------------------------------------------------------------------
 */
static inline int addmisc(nowdb_ast_t *n,
                          nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_USE: ADDKID(0);
	case NOWDB_AST_FETCH: ADDKID(0);
	case NOWDB_AST_CLOSE: ADDKID(0);
	case NOWDB_AST_EXEC: ADDKID(0);
	case NOWDB_AST_LOCK: ADDKID(0);
	case NOWDB_AST_UNLOCK: ADDKID(0);
	case NOWDB_AST_SELECT: ADDKID(0);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to a create node
 * -----------------------------------------------------------------------
 */
static inline int addcreate(nowdb_ast_t *n,
                            nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_TARGET: ADDKID(0);
	case NOWDB_AST_OPTION: ADDKID(1);
	case NOWDB_AST_ON: ADDKID(2);
	case NOWDB_AST_FIELD: ADDKID(3);
	case NOWDB_AST_DECL: ADDKID(3);
	case NOWDB_AST_STORAGE: ADDKID(4);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to an alter node
 * -----------------------------------------------------------------------
 */
static inline int addalter(nowdb_ast_t *n,
                           nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_TARGET: ADDKID(0);
	case NOWDB_AST_OPTION: ADDKID(1);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to a drop node
 * -----------------------------------------------------------------------
 */
static inline int ad3rop(nowdb_ast_t *n,
                          nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_TARGET: ADDKID(0);
	case NOWDB_AST_OPTION: ADDKID(1);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to a load node
 * -----------------------------------------------------------------------
 */
static inline int addload(nowdb_ast_t *n,
                          nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_TARGET: ADDKID(0);
	case NOWDB_AST_OPTION: ADDKID(1);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to an insert node
 * -----------------------------------------------------------------------
 */
static inline int addins(nowdb_ast_t *n,
                         nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_TARGET: ADDKID(0);
	case NOWDB_AST_VALUE: ADDKID(1);
	case NOWDB_AST_FIELD: ADDKID(2);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to a delete node
 * -----------------------------------------------------------------------
 */
static inline int ad3el(nowdb_ast_t *n,
                        nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_TARGET: ADDKID(0);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to an option node
 * -----------------------------------------------------------------------
 */
static inline int addopt(nowdb_ast_t *n,
                         nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_OPTION: ADDKID(0);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to a data node
 * -----------------------------------------------------------------------
 */
static inline int ad3ata(nowdb_ast_t *n,
                         nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_DATA: ADDKID(1);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to a select node
 * -----------------------------------------------------------------------
 */
static inline int addsel(nowdb_ast_t *n,
                         nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_FIELD: ADDKID(0);
	case NOWDB_AST_VALUE: ADDKID(0);
	case NOWDB_AST_FUN: ADDKID(0);
	case NOWDB_AST_OP: ADDKID(0);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to a group node
 * -----------------------------------------------------------------------
 */
static inline int addgroup(nowdb_ast_t *n,
                           nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_FIELD: ADDKID(0);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to a order node
 * -----------------------------------------------------------------------
 */
static inline int addorder(nowdb_ast_t *n,
                           nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_FIELD: ADDKID(0);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to a from node
 * -----------------------------------------------------------------------
 */
static inline int addfrom(nowdb_ast_t *n,
                          nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_TARGET: ADDKID(0);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to a where node
 * -----------------------------------------------------------------------
 */
static inline int addwhere(nowdb_ast_t *n,
                           nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_OP: ADDKID(0);
	case NOWDB_AST_FIELD: ADDKID(0);
	case NOWDB_AST_VALUE: ADDKID(0);
	case NOWDB_AST_FUN: ADDKID(0);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to a bool node
 * -----------------------------------------------------------------------
 */
static inline int addbool(nowdb_ast_t *n,
                          nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_COMPARE:
	case NOWDB_AST_AND:
	case NOWDB_AST_OR:
	case NOWDB_AST_NOT:
	case NOWDB_AST_JUST:
		if (n->kids[0] == NULL) {
			ADDKID(0);
		} else {
			ADDKID(1);
		}

	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to a not node
 * -----------------------------------------------------------------------
 */
static inline int addnot(nowdb_ast_t *n,
                         nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_COMPARE:
	case NOWDB_AST_AND:
	case NOWDB_AST_OR:
	case NOWDB_AST_NOT:
	case NOWDB_AST_JUST: ADDKID(0);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to a compare node
 * -----------------------------------------------------------------------
 */
static inline int addcompare(nowdb_ast_t *n,
                             nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_FIELD:
	case NOWDB_AST_VALUE:
		if (n->kids[0] == NULL) {
			ADDKID(0);
		} else {
			ADDKID(1);
		}
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to field
 * -----------------------------------------------------------------------
 */
static inline int ad3ecl(nowdb_ast_t *n,
                         nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_TYPE: ADDKID(0);
	case NOWDB_AST_OPTION: ADDKID(1);
	case NOWDB_AST_DECL: ADDKID(2);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to field (field list)
 * -----------------------------------------------------------------------
 */
static inline int addfield(nowdb_ast_t *n,
                           nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_FIELD: ADDKID(0);
	case NOWDB_AST_FUN: ADDKID(0);
	case NOWDB_AST_OP: ADDKID(0);
	case NOWDB_AST_VALUE: ADDKID(0);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to fun (list)
 * -----------------------------------------------------------------------
 */
static inline int addfun(nowdb_ast_t *n,
                         nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_FIELD:
	case NOWDB_AST_VALUE:
	case NOWDB_AST_OP:
	case NOWDB_AST_FUN: ADDKID(1);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to fun (parameter)
 * -----------------------------------------------------------------------
 */
static inline int addparfun(nowdb_ast_t *n,
                            nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_FIELD:
	case NOWDB_AST_VALUE:
	case NOWDB_AST_OP:
	case NOWDB_AST_FUN: ADDKID(0);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to operator (list)
 * -----------------------------------------------------------------------
 */
static inline int addop(nowdb_ast_t *n,
                        nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_FIELD:
	case NOWDB_AST_VALUE:
	case NOWDB_AST_FUN:
	case NOWDB_AST_OP: ADDKID(1);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add parameter (operator list)
 * -----------------------------------------------------------------------
 */
static inline int addparop(nowdb_ast_t *n,
                           nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_FIELD:
	case NOWDB_AST_VALUE:
	case NOWDB_AST_FUN:
	case NOWDB_AST_OP: ADDKID(0);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to value (value list)
 * -----------------------------------------------------------------------
 */
static inline int addval(nowdb_ast_t *n,
                         nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_FIELD:
	case NOWDB_AST_FUN:
	case NOWDB_AST_OP:
	case NOWDB_AST_VALUE: ADDKID(0);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to exec (parameters)
 * -----------------------------------------------------------------------
 */
static inline int addexec(nowdb_ast_t *n,
                          nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_FIELD:
	case NOWDB_AST_FUN:
	case NOWDB_AST_OP:
	case NOWDB_AST_VALUE: ADDKID(0);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to exec (parameters)
 * -----------------------------------------------------------------------
 */
static inline int addlock(nowdb_ast_t *n,
                          nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_TARGET: ADDKID(0);
	case NOWDB_AST_OPTION: ADDKID(1);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to target (module)
 * -----------------------------------------------------------------------
 */
static inline int addtrg(nowdb_ast_t *n,
                         nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_TARGET: ADDKID(0);
	case NOWDB_AST_ALIAS:  ADDKID(1);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to any node
 * -----------------------------------------------------------------------
 */
int nowdb_ast_add(nowdb_ast_t *n, nowdb_ast_t *k) {
	switch(n->ntype) {

	case NOWDB_AST_DDL: return ad4l(n,k);
	case NOWDB_AST_DLL: return ad3ll(n,k);
	case NOWDB_AST_DML: return ad3ml(n,k);
	case NOWDB_AST_DQL: return ad3ql(n,k); 
	case NOWDB_AST_MISC: return addmisc(n,k); 

	case NOWDB_AST_CREATE: return addcreate(n,k);
	case NOWDB_AST_ALTER: return addalter(n,k);
	case NOWDB_AST_DROP:  return ad3rop(n,k);

	case NOWDB_AST_LOAD:  return addload(n,k);

	case NOWDB_AST_INSERT: return addins(n,k);
	case NOWDB_AST_DELETE: return ad3el(n,k);
	case NOWDB_AST_UPDATE: return -1;

	case NOWDB_AST_FROM: return addfrom(n,k);
	case NOWDB_AST_SELECT: return addsel(n,k);
	case NOWDB_AST_WHERE: return addwhere(n,k);
	case NOWDB_AST_AND:
	case NOWDB_AST_OR:  return addbool(n,k);
	case NOWDB_AST_NOT: 
	case NOWDB_AST_JUST: return addnot(n,k);
	case NOWDB_AST_GROUP: return addgroup(n,k);
	case NOWDB_AST_ORDER: return addorder(n,k);
	case NOWDB_AST_JOIN:  return -1;

	case NOWDB_AST_COMPARE: return addcompare(n,k);

	case NOWDB_AST_EXEC: return addexec(n,k);

	case NOWDB_AST_LOCK:
	case NOWDB_AST_UNLOCK: return addlock(n,k);

	case NOWDB_AST_TARGET: return addtrg(n,k);
	case NOWDB_AST_ON:       return -1;
	case NOWDB_AST_OPTION:   return addopt(n,k);
	case NOWDB_AST_SIZING:   return -1;
	case NOWDB_AST_PATH:     return -1;
	case NOWDB_AST_DATA: return ad3ata(n,k);

	case NOWDB_AST_FIELD: return addfield(n,k);
	case NOWDB_AST_DECL: return ad3ecl(n,k);
	case NOWDB_AST_FUN: return addfun(n,k);
	case NOWDB_AST_OP: return addop(n,k);
	case NOWDB_AST_VALUE: return addval(n,k);

	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Add kid to any node as parameter
 * -----------------------------------------------------------------------
 */
int nowdb_ast_addParam(nowdb_ast_t *n, nowdb_ast_t *k) {
	switch(n->ntype) {
	case NOWDB_AST_EXEC:
	case NOWDB_AST_OP: return addparop(n,k);
	case NOWDB_AST_FUN: return addparfun(n,k);
	default: return -1;
	}
}

/* -----------------------------------------------------------------------
 * Helper: show ast
 * -----------------------------------------------------------------------
 */
static void showme(nowdb_ast_t *n, char *before) {
	char *nxtbefore;
	int s;

	if (n->value != NULL) {
		switch(n->vtype) {
		case NOWDB_AST_V_STRING:
			fprintf(stdout, "%s%s (%s)\n", before,
			         tellType(n->ntype, n->stype),
			                     (char*)n->value);
			break;
		case NOWDB_AST_V_INTEGER:
			fprintf(stdout, "%s%s (%lu)\n", before,
			          tellType(n->ntype, n->stype),
			                   (uint64_t)n->value);
			break;
		default:
			fprintf(stdout, "%s%s (unknown type)\n",
		 	  before, tellType(n->ntype, n->stype));
		}
	} else {
		fprintf(stdout, "%s%s\n", before,
		   tellType(n->ntype, n->stype));
	}
	if (n->kids == NULL) return;

 	s = strlen(before) + 4;
	nxtbefore = malloc(s);
	if (nxtbefore == NULL) {
		fprintf(stderr, "FAILED: no memory\n");
		return;
	}
	sprintf(nxtbefore, "+--%s", before);
	for(int i=0;i<n->nKids;i++) {
		if (n->kids[i] != NULL) {
			fprintf(stderr, "%s[%d]...\n", before, i);
			showme(n->kids[i],nxtbefore);
		}
	}
	free(nxtbefore);
}

/* -----------------------------------------------------------------------
 * Show ast
 * -----------------------------------------------------------------------
 */
void nowdb_ast_show(nowdb_ast_t *n) {
	showme(n, "+");
}

/* -----------------------------------------------------------------------
 * Get operation
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_operation(nowdb_ast_t *ast) {
	if (ast == NULL) return NULL;

	switch(ast->ntype) {
	case NOWDB_AST_DDL:
	case NOWDB_AST_DML:
	case NOWDB_AST_DLL: return ast->kids[0];

	case NOWDB_AST_DQL: return NULL;

	case NOWDB_AST_MISC: return ast->kids[0];

	default: return NULL;
	}
}

/* -----------------------------------------------------------------------
 * Get target
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_target(nowdb_ast_t *ast) {
	if (ast == NULL) return NULL;

	switch(ast->ntype) {
	case NOWDB_AST_DDL:
	case NOWDB_AST_DLL: return nowdb_ast_target(
		                   nowdb_ast_operation(ast));

	case NOWDB_AST_CREATE:
	case NOWDB_AST_ALTER:
	case NOWDB_AST_DROP: return ast->kids[0];

	case NOWDB_AST_LOAD: return ast->kids[0];

	case NOWDB_AST_DML:
	case NOWDB_AST_DQL: return nowdb_ast_target(
	                           nowdb_ast_from(ast));

	case NOWDB_AST_FROM: return ast->kids[0];
	case NOWDB_AST_INSERT: return ast->kids[0];
	case NOWDB_AST_UPDATE: return ast->kids[0];
	case NOWDB_AST_DELETE: return ast->kids[0];

	case NOWDB_AST_LOCK:
	case NOWDB_AST_UNLOCK: return ast->kids[0];

	case NOWDB_AST_TARGET: return ast->kids[0]; // sub-target

	default: return NULL;
	}
}

/* -----------------------------------------------------------------------
 * Get 'on'
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_on(nowdb_ast_t *ast) {
	if (ast == NULL) return NULL;
	switch(ast->ntype) {
	case NOWDB_AST_DDL: return nowdb_ast_on(
	                           nowdb_ast_operation(ast));

	case NOWDB_AST_CREATE: return ast->kids[2];

	default: return NULL;
	}
}

/* -----------------------------------------------------------------------
 * Get field
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_field(nowdb_ast_t *ast) {
	if (ast == NULL) return NULL;
	switch(ast->ntype) {
	case NOWDB_AST_DDL: return nowdb_ast_field(
	                           nowdb_ast_operation(ast));

	case NOWDB_AST_CREATE: return ast->kids[3];
	case NOWDB_AST_SELECT: return ast->kids[0];
	case NOWDB_AST_WHERE: return ast->kids[0]; // should not be 'field'
	case NOWDB_AST_INSERT: return ast->kids[2];
	case NOWDB_AST_GROUP: return ast->kids[0];
	case NOWDB_AST_ORDER: return ast->kids[0];
	case NOWDB_AST_FIELD: return ast->kids[0];
	case NOWDB_AST_FUN: return ast->kids[1];
	case NOWDB_AST_OP: return ast->kids[1];
	case NOWDB_AST_VALUE: return ast->kids[0];
	default: return NULL;
	}
}

/* -----------------------------------------------------------------------
 * Get value
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_value(nowdb_ast_t *ast) {
	if (ast == NULL) return NULL;
	switch(ast->ntype) {
	case NOWDB_AST_INSERT: return ast->kids[1];
	case NOWDB_AST_VALUE: return ast->kids[0];
	default: return NULL;
	}
}

/* -----------------------------------------------------------------------
 * Get param
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_param(nowdb_ast_t *ast) {
	if (ast == NULL) return NULL;
	switch(ast->ntype) {
	case NOWDB_AST_FUN: return ast->kids[0];
	case NOWDB_AST_OP: return ast->kids[0];
	case NOWDB_AST_EXEC: return ast->kids[0];
	case NOWDB_AST_VALUE: return ast->kids[0];
	case NOWDB_AST_FIELD: return ast->kids[0];
	default: return NULL;
	}
}

/* -----------------------------------------------------------------------
 * Get next param
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_nextParam(nowdb_ast_t *ast) {
	if (ast == NULL) return NULL;
	switch(ast->ntype) {
	case NOWDB_AST_FUN: return ast->kids[1];
	case NOWDB_AST_OP: return ast->kids[1];
	case NOWDB_AST_EXEC: return ast->kids[0];
	case NOWDB_AST_VALUE: return ast->kids[0];
	case NOWDB_AST_FIELD: return ast->kids[0];
	default: return NULL;
	}
}

/* -----------------------------------------------------------------------
 * Get select
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_select(nowdb_ast_t *ast) {
	if (ast->ntype != NOWDB_AST_DQL) return NULL;
	return ast->kids[1];
}

/* -----------------------------------------------------------------------
 * Get group
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_group(nowdb_ast_t *ast) {
	if (ast->ntype != NOWDB_AST_DQL) return NULL;
	return ast->kids[3];
}

/* -----------------------------------------------------------------------
 * Get order
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_order(nowdb_ast_t *ast) {
	if (ast->ntype != NOWDB_AST_DQL) return NULL;
	return ast->kids[4];
}

/* -----------------------------------------------------------------------
 * Get from
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_from(nowdb_ast_t *ast) {
	if (ast->ntype != NOWDB_AST_DQL) return NULL;
	return ast->kids[0];
}

/* -----------------------------------------------------------------------
 * Get where
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_where(nowdb_ast_t *ast) {
	switch(ast->ntype) {
	case NOWDB_AST_DQL: return ast->kids[2];
	case NOWDB_AST_DML: return NULL;
	default: return NULL;
	}
}

/* -----------------------------------------------------------------------
 * Get condition
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_condition(nowdb_ast_t *ast) {
	switch(ast->ntype) {
	case NOWDB_AST_WHERE: 
	case NOWDB_AST_AND: 
	case NOWDB_AST_OR: return ast->kids[0];
	default: return NULL; 
	}
}

/* -----------------------------------------------------------------------
 * Get operand
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_operand(nowdb_ast_t *ast, int i) {
	switch(ast->ntype) {
	case NOWDB_AST_WHERE: 
	case NOWDB_AST_AND:
	case NOWDB_AST_OR:
	case NOWDB_AST_NOT:
	case NOWDB_AST_JUST:
	case NOWDB_AST_COMPARE: 
		if (i == 1) return ast->kids[0];
		if (ast->ntype == NOWDB_AST_NOT  ||
		    ast->ntype == NOWDB_AST_JUST ||
		    ast->ntype == NOWDB_AST_WHERE) return NULL;
		return ast->kids[1];
	default: return NULL; 
	}
}

/* -----------------------------------------------------------------------
 * Get option
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_option(nowdb_ast_t *ast, int option) {
	if (ast == NULL) return NULL;

	switch(ast->ntype) {
	case NOWDB_AST_DDL:
	case NOWDB_AST_DLL: return nowdb_ast_option(
		                   nowdb_ast_operation(ast), option);
	case NOWDB_AST_CREATE:
	case NOWDB_AST_ALTER:
	case NOWDB_AST_DROP: 
		return nowdb_ast_option(ast->kids[1], option);

	case NOWDB_AST_LOAD: 
		return nowdb_ast_option(ast->kids[1], option);

	case NOWDB_AST_DECL:
		return nowdb_ast_option(ast->kids[1], option);

	case NOWDB_AST_LOCK:
	case NOWDB_AST_UNLOCK:
		return nowdb_ast_option(ast->kids[1], option);

	case NOWDB_AST_OPTION:
		if (option == 0) return ast;
		if (ast->stype == option) return ast;
		return nowdb_ast_option(ast->kids[0], option);

	case NOWDB_AST_DML:
	case NOWDB_AST_DQL: return NULL;

	default: return NULL;
	}
}

/* -----------------------------------------------------------------------
 * Get field declaration from the current AST node
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_declare(nowdb_ast_t *ast) {
	if (ast == NULL) return NULL;

	switch(ast->ntype) {
	case NOWDB_AST_DDL: return nowdb_ast_decl(
	                     nowdb_ast_operation(ast));
	case NOWDB_AST_CREATE:
	case NOWDB_AST_ALTER: return ast->kids[3];

	case NOWDB_AST_DECL: return ast->kids[2];

	default: return NULL;
	}
}

/* -----------------------------------------------------------------------
 * Get field offset from the current AST node
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_off(nowdb_ast_t *ast) {
	if (ast == NULL) return NULL;

	switch(ast->ntype) {
	case NOWDB_AST_DECL: return ast->kids[0];
	default: return NULL;
	}
}

/* -----------------------------------------------------------------------
 * Get storage from the current AST node
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_storage(nowdb_ast_t *ast) {
	if (ast == NULL) return NULL;

	switch(ast->ntype) {
	case NOWDB_AST_CREATE: return ast->kids[4];
	default: return NULL;
	}
}

/* -----------------------------------------------------------------------
 * AST user type
 * -----------------------------------------------------------------------
 */
nowdb_ast_t *nowdb_ast_utype(nowdb_ast_t *ast) {
	if (ast == NULL) return NULL;
	switch(ast->ntype) {
	case NOWDB_AST_DECL: return ast->kids[0];
	default: return NULL;
	}
}

/* -----------------------------------------------------------------------
 * AST type to NOWDB type
 * -----------------------------------------------------------------------
 */
nowdb_type_t nowdb_ast_type(uint32_t type) {
	switch(type) {
	case 0: return NOWDB_TYP_NOTHING;
	case NOWDB_AST_TEXT: return NOWDB_TYP_TEXT;
	case NOWDB_AST_FLOAT: return NOWDB_TYP_FLOAT;
	case NOWDB_AST_UINT: return NOWDB_TYP_UINT;
	case NOWDB_AST_INT: return NOWDB_TYP_INT;
	case NOWDB_AST_DATE: return NOWDB_TYP_DATE;
	case NOWDB_AST_TIME: return NOWDB_TYP_TIME;
	case NOWDB_AST_BOOL: return NOWDB_TYP_BOOL;
	case NOWDB_AST_NULL: return NOWDB_TYP_NOTHING;
	default: return NOWDB_TYP_NOTHING;
	}
	return NOWDB_TYP_NOTHING;
}

/* -----------------------------------------------------------------------
 * Get value as UInt
 * -----------------------------------------------------------------------
 */
int nowdb_ast_getUInt(nowdb_ast_t *node, uint64_t *value) {
	char *end;
	if (node->value == NULL) return -1;
	if (value == NULL) return -1;
	if (node->vtype == NOWDB_AST_V_INTEGER) {
		*value = (uint64_t)(node->value);
		return 0;
	}
	*value = strtoull(node->value, &end, 10);
	if (*end != 0) return -1;
	return 0;
}

/* -----------------------------------------------------------------------
 * Get value as Int
 * -----------------------------------------------------------------------
 */
int nowdb_ast_getInt(nowdb_ast_t *node, int64_t *value) {
	char *end;
	if (node->value == NULL) return -1;
	if (value == NULL) return -1;
	if (node->vtype == NOWDB_AST_V_INTEGER) {
		*value = (int64_t)(node->value);
		return 0;
	}
	*value = strtoll(node->value, &end, 10);
	if (*end != 0) return -1;
	return 0;
}

/* -----------------------------------------------------------------------
 * Get value as string
 * -----------------------------------------------------------------------
 */
char *nowdb_ast_getString(nowdb_ast_t *node) {
	return node->value;
}
