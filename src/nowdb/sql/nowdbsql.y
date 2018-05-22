%token_prefix NOWDB_SQL_
%name nowdb_sql_parse

%include {
#define UNUSED_VAR_SILENCER() \
	if (nowdbres->errcode != 0) ;
#ifndef NDEBUG
#define NDEBUG
#endif
#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include <nowdb/scope/context.h>
#include <nowdb/query/ast.h>
#include <nowdb/sql/nowdbsql.h>
#include <nowdb/sql/state.h>
}

%token_type {char*}
%token_destructor {
	/* fprintf(stderr, "freeing token '%s'\n", (char*)$$); */
	UNUSED_VAR_SILENCER();
	free($$);
}

%type condition {nowdb_ast_t*}
%destructor condition {nowdb_ast_destroyAndFree($$);}

%type more_conditions {nowdb_ast_t*}
%destructor more_conditions {nowdb_ast_destroyAndFree($$);}

%type comparison {nowdb_ast_t*}
%destructor comparison {nowdb_ast_destroyAndFree($$);}

%type operand {nowdb_ast_t*}
%destructor operand {nowdb_ast_destroyAndFree($$);}

%extra_argument { nowdbsql_state_t *nowdbres }

%parse_accept {
	// fprintf(stderr, "OK\n");
}

%parse_failure {
	nowdbres->errcode = NOWDB_SQL_ERR_SYNTAX;
}

%syntax_error {
	nowdbres->errcode = NOWDB_SQL_ERR_SYNTAX;
	nowdbsql_errmsg(nowdbres, "syntax error", (char*)yyminor); 
	// fprintf(stderr, "near '%s': syntax error\n", (char*)yyminor);
}

sql ::= ddl SEMICOLON. {
	nowdbsql_state_pushDDL(nowdbres);
}
sql ::= dll SEMICOLON. {
	nowdbsql_state_pushDLL(nowdbres);
}
sql ::= dql SEMICOLON. {
	nowdbsql_state_pushDQL(nowdbres);
}
sql ::= misc SEMICOLON. {
	nowdbsql_state_pushMisc(nowdbres);
}
ddl ::= CREATE target. {
	nowdbsql_state_pushCreate(nowdbres);
}
ddl ::= CREATE target IF NOT EXISTS. {
	nowdbsql_state_pushOption(nowdbres, NOWDB_SQL_EXISTS, NULL);
	nowdbsql_state_pushCreate(nowdbres);
}
ddl ::= CREATE context_target. {
	nowdbsql_state_pushCreate(nowdbres);
}
ddl ::= CREATE context_target IF NOT EXISTS. {
	nowdbsql_state_pushOption(nowdbres, NOWDB_SQL_EXISTS, NULL);
	nowdbsql_state_pushCreate(nowdbres);
}
ddl ::= CREATE context_target context_spec. {
	nowdbsql_state_pushCreate(nowdbres);
}
ddl ::= CREATE context_target IF NOT EXISTS context_spec. {
	nowdbsql_state_pushOption(nowdbres, NOWDB_SQL_EXISTS, NULL);
	nowdbsql_state_pushCreate(nowdbres);
}
ddl ::= CREATE context_target context_options. {
	nowdbsql_state_pushCreate(nowdbres);
}
ddl ::= CREATE context_target IF NOT EXISTS SET context_options. {
	nowdbsql_state_pushOption(nowdbres, NOWDB_SQL_EXISTS, NULL);
	nowdbsql_state_pushCreate(nowdbres);
}

ddl ::= DROP target. {
	nowdbsql_state_pushDrop(nowdbres);
}
ddl ::= DROP target IF EXISTS. {
	nowdbsql_state_pushOption(nowdbres, NOWDB_SQL_EXISTS, NULL);
	nowdbsql_state_pushDrop(nowdbres);
}
ddl ::= DROP context_target. {
	nowdbsql_state_pushDrop(nowdbres);
}
ddl ::= DROP context_target IF EXISTS. {
	nowdbsql_state_pushOption(nowdbres, NOWDB_SQL_EXISTS, NULL);
	nowdbsql_state_pushDrop(nowdbres);
}

ddl ::= ALTER context_only SET context_options. {
	nowdbsql_state_pushAlter(nowdbres);
}

misc ::= USE IDENTIFIER(I). {
	nowdbsql_state_pushUse(nowdbres, I);
}

target ::= SCOPE IDENTIFIER(I). {
	nowdbsql_state_pushScope(nowdbres, I);
}

/* qualified identifier ! */
target ::= INDEX IDENTIFIER(I). {
	nowdbsql_state_pushIndex(nowdbres, I);
}

/* qualified identifier ! */
context_target ::= CONTEXT IDENTIFIER(I). {
	nowdbsql_state_pushContext(nowdbres, I);
}

/* qualified identifier ! */
context_target ::= sizing CONTEXT IDENTIFIER(I). {
	nowdbsql_state_pushContext(nowdbres, I);
}

context_only ::= CONTEXT IDENTIFIER(I). {
	nowdbsql_state_pushContext(nowdbres, I);
}

context_options ::= context_option.
context_options ::= context_option COMMA context_options.

context_option ::= ALLOCSIZE EQ INTEGER(I). {
	nowdbsql_state_pushOption(nowdbres, NOWDB_SQL_ALLOCSIZE, I);
}
context_option ::= LARGESIZE EQ INTEGER(I). {
	nowdbsql_state_pushOption(nowdbres, NOWDB_SQL_LARGESIZE, I);
}
context_option ::= SORTERS EQ INTEGER(I). {
	nowdbsql_state_pushOption(nowdbres, NOWDB_SQL_SORTERS, I);
}
context_option ::= COMPRESSION EQ STRING(I). {
	nowdbsql_state_pushOption(nowdbres, NOWDB_SQL_COMPRESSION, I);
}
context_option ::= ENCRYPTION EQ STRING(I). {
	nowdbsql_state_pushOption(nowdbres, NOWDB_SQL_ENCRYPTION, I);
}

context_spec ::= with_stress with_disk without_comp without_sort.
context_spec ::= with_stress with_disk.
context_spec ::= with_stress.
context_spec ::= with_disk.
context_spec ::= without_comp.
context_spec ::= without_sort.
context_spec ::= without_comp without_sort.
context_spec ::= with_stress without_comp without_sort.
context_spec ::= with_stress without_sort.
context_spec ::= with_stress without_comp.
context_spec ::= with_disk without_comp without_sort.
context_spec ::= with_disk without_comp.
context_spec ::= with_disk without_sort.

sizing ::= TINY. {
	nowdbsql_state_pushSizing(nowdbres, NOWDB_CONFIG_SIZE_TINY);
}
sizing ::= SMALL. {
	nowdbsql_state_pushSizing(nowdbres, NOWDB_CONFIG_SIZE_SMALL);
}
sizing ::= MEDIUM. {
	nowdbsql_state_pushSizing(nowdbres, NOWDB_CONFIG_SIZE_MEDIUM);
}
sizing ::= BIG. {
	nowdbsql_state_pushSizing(nowdbres, NOWDB_CONFIG_SIZE_BIG);
}
sizing ::= LARGE. {
	nowdbsql_state_pushSizing(nowdbres, NOWDB_CONFIG_SIZE_LARGE);
}
sizing ::= HUGE. {
	nowdbsql_state_pushSizing(nowdbres, NOWDB_CONFIG_SIZE_HUGE);
}

with_stress ::= WITH MODERATE STRESS. {
	nowdbsql_state_pushStress(nowdbres, NOWDB_CONFIG_INSERT_MODERATE);
}
with_stress ::= WITH CONSTANT STRESS. {
	nowdbsql_state_pushStress(nowdbres, NOWDB_CONFIG_INSERT_CONSTANT);
}
with_stress ::= WITH INSANE STRESS. {
	nowdbsql_state_pushStress(nowdbres, NOWDB_CONFIG_INSERT_INSANE);
}

with_disk ::= ON disk_spec.

without_comp ::= WITHOUT COMPRESSION. {
	nowdbsql_state_pushNocomp(nowdbres);
}
without_sort ::= WITHOUT SORTING. {
	nowdbsql_state_pushNosort(nowdbres);
}
disk_spec ::= HDD. {
	nowdbsql_state_pushDisk(nowdbres, NOWDB_CONFIG_DISK_HDD);
}
disk_spec ::= SSD. {
	nowdbsql_state_pushDisk(nowdbres, NOWDB_CONFIG_DISK_SSD);
}
disk_spec ::= RAID. {
	nowdbsql_state_pushDisk(nowdbres, NOWDB_CONFIG_DISK_RAID);
}
dll ::= LOAD STRING(S) INTO dml_target. {
	nowdbsql_state_pushLoad(nowdbres, S);
}
dll ::= LOAD STRING(S) INTO dml_target header_clause. {
	nowdbsql_state_pushLoad(nowdbres, S);
}
header_clause ::= IGNORE HEADER. {
	nowdbsql_state_pushOption(nowdbres, NOWDB_SQL_IGNORE, NULL);
}
header_clause ::= USE HEADER. {
	nowdbsql_state_pushOption(nowdbres, NOWDB_SQL_USE, NULL);
}

dml_target ::= IDENTIFIER(I). {
	nowdbsql_state_pushContext(nowdbres, I);
}
dml_target ::= VERTEX. {
	nowdbsql_state_pushVertex(nowdbres, NULL);
}

/* MINIMAL VIABLE SQL */
dql ::= projection_clause from_clause. {
	nowdbsql_state_pushDQL(nowdbres);
}
dql ::= projection_clause from_clause where_clause.

projection_clause ::= SELECT STAR. {
	nowdbsql_state_pushProjection(nowdbres);
}

from_clause ::= FROM table_list. {
	nowdbsql_state_pushFrom(nowdbres);
}

table_list ::= table_spec.
table_list ::= table_spec COMMA table_list.

table_spec ::= VERTEX. {
	nowdbsql_state_pushVertex(nowdbres, NULL);
}
table_spec ::= VERTEX AS IDENTIFIER(A). {
	nowdbsql_state_pushVertex(nowdbres, A);
}
table_spec ::= IDENTIFIER(I). {
	nowdbsql_state_pushTable(nowdbres, I, NULL);
}
table_spec ::= IDENTIFIER(T) AS IDENTIFIER(A). {
	nowdbsql_state_pushTable(nowdbres, T, A);
}

where_clause ::= WHERE condition(C). {
	NOWDB_SQL_CHECKSTATE();
	nowdbsql_state_pushAst(nowdbres,C);
	nowdbsql_state_pushWhere(nowdbres);
}
where_clause ::= WHERE condition(C) more_conditions(M). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_ADDKID(M,C);
	nowdbsql_state_pushAst(nowdbres, M);
	nowdbsql_state_pushWhere(nowdbres);
}

where_clause ::= WHERE LPAR condition(C) more_conditions(M) RPAR. {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_ADDKID(M,C);
	nowdbsql_state_pushAst(nowdbres, M);
	nowdbsql_state_pushWhere(nowdbres);
}

where_clause ::= WHERE LPAR condition(C) more_conditions(M1) RPAR more_conditions(M2). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_ADDKID(M1,C);
	NOWDB_SQL_ADDKID(M2,M1);
	nowdbsql_state_pushAst(nowdbres, M2);
	nowdbsql_state_pushWhere(nowdbres);
}

condition(C) ::= operand(O1) comparison(O) operand(O2) . {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_ADDKID(O,O1);
	NOWDB_SQL_ADDKID(O,O2);
	NOWDB_SQL_CREATEAST(&C, NOWDB_AST_JUST, 0);
	NOWDB_SQL_ADDKID(C,O);
}
condition(C) ::= NOT operand(O1) comparison(O) operand(O2) . {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_ADDKID(O,O1);
	NOWDB_SQL_ADDKID(O,O2);
	NOWDB_SQL_CREATEAST(&C, NOWDB_AST_NOT, 0);
	NOWDB_SQL_ADDKID(C,O);
}
condition(C) ::= LPAR operand(O1) comparison(O) operand(O2) RPAR. {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_ADDKID(O,O1);
	NOWDB_SQL_ADDKID(O,O2);
	NOWDB_SQL_CREATEAST(&C, NOWDB_AST_JUST, 0);
	NOWDB_SQL_ADDKID(C,O);
}

condition(C) ::= NOT LPAR operand(O1) comparison(O) operand(O2) RPAR. {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_ADDKID(O,O1);
	NOWDB_SQL_ADDKID(O,O2);
	NOWDB_SQL_CREATEAST(&C, NOWDB_AST_NOT, 0);
	NOWDB_SQL_ADDKID(C,O);
}
 
more_conditions(M) ::= AND condition(C). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&M, NOWDB_AST_AND, 0);
	NOWDB_SQL_ADDKID(M,C);
}

more_conditions(M) ::= AND condition(C) more_conditions(M2). {
	nowdb_ast_t *ast;
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&ast, NOWDB_AST_AND, 0);
	if (M2->ntype == NOWDB_AST_OR) {
		NOWDB_SQL_ADDKID(ast,C);
		NOWDB_SQL_ADDKID(M2,ast);
		M = M2;
		
	} else {
		NOWDB_SQL_ADDKID(M2,C);
		NOWDB_SQL_ADDKID(ast,M2);
		M = ast;
	}
}

more_conditions(M) ::= OR condition(C). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&M, NOWDB_AST_OR, 0);
	NOWDB_SQL_ADDKID(M,C);
}

more_conditions(M) ::= OR condition(C) more_conditions(M2). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&M, NOWDB_AST_OR, 0);
	if (M2->ntype == NOWDB_AST_AND) {
		NOWDB_SQL_ADDKID(M2,C);
		NOWDB_SQL_ADDKID(M,M2);
	} else {
		NOWDB_SQL_ADDKID(M,C);
		NOWDB_SQL_ADDKID(M,M2);
	}
}

more_conditions(M) ::= AND LPAR condition(C) more_conditions(M2) RPAR. {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&M, NOWDB_AST_AND, 0);
	NOWDB_SQL_ADDKID(M2, C);
	NOWDB_SQL_ADDKID(M, M2);
}

more_conditions(M) ::= AND LPAR condition(C) more_conditions(M2) RPAR more_conditions (M3). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&M, NOWDB_AST_AND, 0);
	NOWDB_SQL_ADDKID(M2, C);
	NOWDB_SQL_ADDKID(M3, M2);
	NOWDB_SQL_ADDKID(M, M3);
}

more_conditions ::= OR LPAR condition more_conditions RPAR.
more_conditions ::= AND NOT LPAR condition more_conditions RPAR.
more_conditions ::= OR NOT LPAR condition more_conditions RPAR.

comparison(C) ::= EQ. {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&C, NOWDB_AST_COMPARE, NOWDB_AST_EQ);
}

comparison(C) ::= LE. {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&C, NOWDB_AST_COMPARE, NOWDB_AST_LE);
}
comparison(C) ::= GE. {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&C, NOWDB_AST_COMPARE, NOWDB_AST_GE);
}
comparison(C) ::= LT. {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&C, NOWDB_AST_COMPARE, NOWDB_AST_LT);
}
comparison(C) ::= GT. {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&C, NOWDB_AST_COMPARE, NOWDB_AST_GT);
}
comparison(C) ::= NE. {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&C, NOWDB_AST_COMPARE, NOWDB_AST_NE);
}

operand(O) ::= field(F). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&O, NOWDB_AST_FIELD, 0);
	nowdb_ast_setValue(O, NOWDB_AST_V_STRING, F);
}

operand(O) ::= value(V). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&O, NOWDB_AST_VALUE, 0);
	nowdb_ast_setValue(O, NOWDB_AST_V_STRING, V);
}

field(F) ::= IDENTIFIER(I). {
	F=I;
}
value(V) ::= STRING(S). {
	V=S;
}
value(V) ::= INTEGER(I). {
	V=I;
}

