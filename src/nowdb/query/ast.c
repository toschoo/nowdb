#include <nowdb/query/ast.h>

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>

#define ASTCALLOC(k) \
	n->kids = calloc(k, sizeof(nowdb_ast_t*)); \
	if (n->kids == NULL) return -1; \
	n->nKids = k; \
	return 0

#define UNDEFINED(k) \
	fprintf(stderr, "unkdefined: %d\n", k); \
	free(n); return -1;

nowdb_ast_t *nowdb_ast_create(int ntype, int stype) {
	nowdb_ast_t *n;

	n = calloc(1,sizeof(nowdb_ast_t));
	if (n == NULL) return NULL;

	if (nowdb_ast_init(n, ntype, stype) != 0) {
		free(n); return NULL;
	}
	return n;
}

int nowdb_ast_init(nowdb_ast_t *n, int ntype, int stype) {

	n->ntype = ntype;
	n->stype = stype;

	switch(ntype) {

	case NOWDB_AST_DDL:
	case NOWDB_AST_DLL:
	case NOWDB_AST_DML: ASTCALLOC(1);
	case NOWDB_AST_DQL: ASTCALLOC(5); 
	case NOWDB_AST_MISC: ASTCALLOC(1); 

	case NOWDB_AST_CREATE: ASTCALLOC(2);
	case NOWDB_AST_ALTER: ASTCALLOC(2);
	case NOWDB_AST_DROP:  ASTCALLOC(2);

	case NOWDB_AST_LOAD:  ASTCALLOC(2); 

	case NOWDB_AST_INSERT: ASTCALLOC(2);
	case NOWDB_AST_UPDATE: UNDEFINED(ntype);

	case NOWDB_AST_FROM:
	case NOWDB_AST_SELECT:
	case NOWDB_AST_WHERE:
	case NOWDB_AST_AND:
	case NOWDB_AST_OR:     
	case NOWDB_AST_NOT:    
	case NOWDB_AST_GROUP:  
	case NOWDB_AST_ORDER: ASTCALLOC(2); 
	case NOWDB_AST_JOIN: UNDEFINED(ntype); 

	case NOWDB_AST_USE: ASTCALLOC(0);

	case NOWDB_AST_TARGET:   ASTCALLOC(0);
	case NOWDB_AST_OPTION:   ASTCALLOC(1);
	case NOWDB_AST_PATH:     ASTCALLOC(0);
	case NOWDB_AST_DATA:     ASTCALLOC(1);

	default:
		return -1;
	}
	return 0;
}

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

	case NOWDB_AST_LOAD:  return "load";

	case NOWDB_AST_INSERT: return "insert";
	case NOWDB_AST_UPDATE: return "update";

	case NOWDB_AST_FROM: return "from";
	case NOWDB_AST_SELECT: 
		if (stype == NOWDB_AST_ALL) return "select *";
		else return "select";

	case NOWDB_AST_WHERE: return "where";
	case NOWDB_AST_AND: return "and";
	case NOWDB_AST_OR: return "or";
	case NOWDB_AST_NOT: return "not";
	case NOWDB_AST_GROUP:  return "group";
	case NOWDB_AST_ORDER:  return "order";
	case NOWDB_AST_JOIN: return "join";

	case NOWDB_AST_USE: return "use";

	case NOWDB_AST_TARGET:
		switch(stype) {
		case NOWDB_AST_SCOPE: return "scope";
		case NOWDB_AST_VERTEX: return "vertex";
		case NOWDB_AST_CONTEXT: return "context";
		case NOWDB_AST_INDEX: return "index";
		default: return "unknown target";
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
			default: return "unknown compare";
		}

	case NOWDB_AST_DATA:
		switch(stype) {
			case NOWDB_AST_KEYVAL: return "data as pairs";
			case NOWDB_AST_VALLIST: return "data as list";
			default: return "data in unknown format";
		}

	case NOWDB_AST_PATH:   return "path";

	default: return "unknown";
	}
}

void nowdb_ast_destroy(nowdb_ast_t *n) {
	if (n == NULL) return;
	if (n->value != NULL) {
		if (n->vtype == NOWDB_AST_V_STRING) {
			free(n->value);
		}
		n->value = NULL;
	}
	for(int i=0;i<n->nKids;i++) {
		nowdb_ast_destroy(n->kids[i]); free(n->kids[i]);
	}
	if (n->kids != NULL) {
		free(n->kids); n->kids = NULL;
	}
}

int nowdb_ast_setValue(nowdb_ast_t *n,
                  int vtype, void *val) 
{
	n->vtype = vtype;
	n->value = val; /* note that lemon does not free
	                 * this pointer, since it is used
	                 * in an action, so we can just
	                 * take it over and free it later! */
	return 0;
}

#define ADDKID(x) \
	if (n->kids[x] == NULL) { \
		n->kids[x] = k; \
	} else { \
		nowdb_ast_add(n->kids[x], k); \
	} \
	return 0

static inline int ad4l(nowdb_ast_t *n,
                       nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_CREATE:
	case NOWDB_AST_ALTER: 
	case NOWDB_AST_DROP: ADDKID(0);
	default: return -1;
	}
}

static inline int ad3ll(nowdb_ast_t *n,
                       nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_LOAD: ADDKID(0);
	default: return -1;
	}
}

static inline int ad3ml(nowdb_ast_t *n,
                       nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_INSERT: ADDKID(0);
	case NOWDB_AST_UPDATE: return -1;
	default: return -1;
	}
}

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

static inline int addmisc(nowdb_ast_t *n,
                          nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_USE: ADDKID(0);
	default: return -1;
	}
}

static inline int addcreate(nowdb_ast_t *n,
                            nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_TARGET: ADDKID(0);
	case NOWDB_AST_OPTION: ADDKID(1);
	default: return -1;
	}
}

static inline int addalter(nowdb_ast_t *n,
                           nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_TARGET: ADDKID(0);
	case NOWDB_AST_OPTION: ADDKID(1);
	default: return -1;
	}
}

static inline int ad3rop(nowdb_ast_t *n,
                          nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_TARGET: ADDKID(0);
	case NOWDB_AST_OPTION: ADDKID(1);
	default: return -1;
	}
}

static inline int addload(nowdb_ast_t *n,
                          nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_TARGET: ADDKID(0);
	case NOWDB_AST_OPTION: ADDKID(1);
	default: return -1;
	}
}

static inline int addins(nowdb_ast_t *n,
                         nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_TARGET: ADDKID(0);
	case NOWDB_AST_DATA: ADDKID(1);
	default: return -1;
	}
}

static inline int addopt(nowdb_ast_t *n,
                         nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_OPTION: ADDKID(0);
	default: return -1;
	}
}

static inline int ad3ata(nowdb_ast_t *n,
                         nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_DATA: ADDKID(1);
	default: return -1;
	}
}

static inline int addsel(nowdb_ast_t *n,
                         nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_FIELD: ADDKID(0);
	default: return -1;
	}
}

static inline int addfrom(nowdb_ast_t *n,
                          nowdb_ast_t *k) {
	switch(k->ntype) {
	case NOWDB_AST_TARGET: ADDKID(0);
	default: return -1;
	}
}

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
	case NOWDB_AST_UPDATE: return -1;

	case NOWDB_AST_FROM: return addfrom(n,k);
	case NOWDB_AST_SELECT: return addsel(n,k);
	case NOWDB_AST_WHERE:   return -1;
	case NOWDB_AST_AND:     return -1;
	case NOWDB_AST_OR:      return -1;
	case NOWDB_AST_NOT:     return -1;
	case NOWDB_AST_GROUP:   return -1;
	case NOWDB_AST_ORDER:   return -1;
	case NOWDB_AST_JOIN:  return -1;

	case NOWDB_AST_TARGET:   return -1;
	case NOWDB_AST_OPTION:   return addopt(n,k);
	case NOWDB_AST_SIZING:   return -1;
	case NOWDB_AST_PATH:     return -1;
	case NOWDB_AST_DATA: return ad3ata(n,k);

	default: return -1;
	}
}

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
		if (n->kids[i] != NULL) showme(n->kids[i],nxtbefore);
	}
	free(nxtbefore);
}

void nowdb_ast_show(nowdb_ast_t *n) {
	showme(n, "+");
}

nowdb_ast_t *nowdb_ast_operation(nowdb_ast_t *ast) {
	if (ast == NULL) return NULL;

	switch(ast->ntype) {
	case NOWDB_AST_DDL:
	case NOWDB_AST_DLL: return ast->kids[0];

	case NOWDB_AST_DML:
	case NOWDB_AST_DQL: return NULL;

	case NOWDB_AST_MISC: return ast->kids[0];

	default: return NULL;
	}
}

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

	default: return NULL;
	}
}

nowdb_ast_t *nowdb_ast_select(nowdb_ast_t *ast) {
	if (ast->ntype != NOWDB_AST_DQL) return NULL;
	return ast->kids[1];
}

nowdb_ast_t *nowdb_ast_from(nowdb_ast_t *ast) {
	if (ast->ntype != NOWDB_AST_DQL) return NULL;
	return ast->kids[0];
}

nowdb_ast_t *nowdb_ast_where(nowdb_ast_t *ast) {
	return NULL;
}

nowdb_ast_t *nowdb_ast_option(nowdb_ast_t *ast, int option) {
	if (ast == NULL) return NULL;

	switch(ast->ntype) {
	case NOWDB_AST_DDL:
	case NOWDB_AST_DLL: return nowdb_ast_option(
		                   nowdb_ast_operation(ast), option);
	case NOWDB_AST_CREATE:
	case NOWDB_AST_ALTER:
	case NOWDB_AST_DROP: return ast->kids[1];

	case NOWDB_AST_LOAD: return ast->kids[1];

	case NOWDB_AST_OPTION:
		if (option == 0) return ast;
		if (ast->stype == option) return ast;
		return nowdb_ast_option(ast->kids[0], option);

	case NOWDB_AST_DML:
	case NOWDB_AST_DQL: return NULL;

	default: return NULL;
	}
}

int nowdb_ast_getUInt(nowdb_ast_t *node, uint64_t *value) {
	char *end;
	if (node->value == NULL) return -1;
	if (value == NULL) return -1;
	if (node->vtype == NOWDB_AST_V_INTEGER) {
		*value = (uint64_t)(node->value);
		return 0;
	}
	*value = strtoul(node->value, &end, 10);
	if (*end != 0) return -1;
	return 0;
}

int nowdb_ast_getInt(nowdb_ast_t *node, int64_t *value) {
	char *end;
	if (node->value == NULL) return -1;
	if (value == NULL) return -1;
	if (node->vtype == NOWDB_AST_V_INTEGER) {
		*value = (int64_t)(node->value);
		return 0;
	}
	*value = strtol(node->value, &end, 10);
	if (*end != 0) return -1;
	return 0;
}

char *nowdb_ast_getString(nowdb_ast_t *node) {
	return node->value;
}

