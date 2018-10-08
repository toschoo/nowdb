/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * SQL Parser
 * ========================================================================
 * This parser definition file is intended for use with
 * the nowlemon parser generator (forked from sqlite lemon).
 * It creates an abstract syntax tree (ast),
 * which is used to execute sql statements and to create a cursor.
 * urgent TODO:
 * ------------
 * - expression in select and where
 * - qualified names
 * - aliases
 * - joins
 * - NULL
 * - TRUE and FALSE
 * - alter
 * - insert and update
 * - make load more versatile (csv, json, binary, avron)
 * - multi-line comments
 * ========================================================================
 */
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
#include <nowdb/sql/ast.h>
#include <nowdb/sql/nowdbsql.h>
#include <nowdb/sql/state.h>
}

/* ------------------------------------------------------------------------
 * Tokens are always plain strings
 * ------------------------------------------------------------------------
 */
%token_type {char*}
%token_destructor {
	// fprintf(stderr, "freeing token '%s'\n", (char*)$$);
	UNUSED_VAR_SILENCER();
	free($$);
}

/* ------------------------------------------------------------------------
 * Everything else is an ast node
 * ------------------------------------------------------------------------
 */
%type condition {nowdb_ast_t*}
%destructor condition {nowdb_ast_destroyAndFree($$);}

%type context_option {nowdb_ast_t*}
%destructor context_option {nowdb_ast_destroyAndFree($$);}

%type comparison {nowdb_ast_t*}
%destructor comparison {nowdb_ast_destroyAndFree($$);}

%type context_options {nowdb_ast_t*}
%destructor context_options {nowdb_ast_destroyAndFree($$);}

%type operand {nowdb_ast_t*}
%destructor operand {nowdb_ast_destroyAndFree($$);}

%type value {nowdb_ast_t*}
%destructor value {nowdb_ast_destroyAndFree($$);}

%type expr {nowdb_ast_t*}
%destructor expr {nowdb_ast_destroyAndFree($$);}

%type sizing {nowdb_ast_t*}
%destructor sizing {nowdb_ast_destroyAndFree($$);}

%type target {nowdb_ast_t*}
%destructor target {nowdb_ast_destroyAndFree($$);}

%type create_clause {nowdb_ast_t*}
%destructor create_clause {nowdb_ast_destroyAndFree($$);}

%type drop_clause {nowdb_ast_t*}
%destructor drop_clause {nowdb_ast_destroyAndFree($$);}

%type projection_clause {nowdb_ast_t*}
%destructor projection_clause {nowdb_ast_destroyAndFree($$);}

%type from_clause {nowdb_ast_t*}
%destructor from_clause {nowdb_ast_destroyAndFree($$);}

%type where_clause {nowdb_ast_t*}
%destructor where_clause {nowdb_ast_destroyAndFree($$);}

%type group_clause {nowdb_ast_t*}
%destructor group_clause {nowdb_ast_destroyAndFree($$);}

%type order_clause {nowdb_ast_t*}
%destructor order_clause {nowdb_ast_destroyAndFree($$);}

%type stress_spec {nowdb_ast_t*}
%destructor stress_spec {nowdb_ast_destroyAndFree($$);}

%type disk_spec {nowdb_ast_t*}
%destructor disk_spec {nowdb_ast_destroyAndFree($$);}

%type field_list {nowdb_ast_t*}
%destructor field_list {nowdb_ast_destroyAndFree($$);}

%type pj_list {nowdb_ast_t*}
%destructor pj_list {nowdb_ast_destroyAndFree($$);}

%type pj {nowdb_ast_t*}
%destructor pj {nowdb_ast_destroyAndFree($$);}

%type val_list {nowdb_ast_t*}
%destructor val_list {nowdb_ast_destroyAndFree($$);}

%type fun {nowdb_ast_t*}
%destructor fun {nowdb_ast_destroyAndFree($$);}

%type table_list {nowdb_ast_t*}
%destructor table_list {nowdb_ast_destroyAndFree($$);}

%type table_spec {nowdb_ast_t*}
%destructor table_spec {nowdb_ast_destroyAndFree($$);}

%type dml_target {nowdb_ast_t*}
%destructor dml_target {nowdb_ast_destroyAndFree($$);}

%type index_target {nowdb_ast_t*}
%destructor index_target {nowdb_ast_destroyAndFree($$);}

%type header_clause {nowdb_ast_t*}
%destructor header_clause {nowdb_ast_destroyAndFree($$);}

%type field_decl {nowdb_ast_t*}
%destructor field_decl {nowdb_ast_destroyAndFree($$);}

%type field_decl_list {nowdb_ast_t*}
%destructor field_decl_list {nowdb_ast_destroyAndFree($$);}

%type edge_field_decl {nowdb_ast_t*}
%destructor edge_field_decl {nowdb_ast_destroyAndFree($$);}

%type edge_field_decl_list {nowdb_ast_t*}
%destructor edge_field_decl_list {nowdb_ast_destroyAndFree($$);}

%type type {int}

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

/* ------------------------------------------------------------------------
 * An SQL statement is either
 * a DDL, DLL, DML, DQL or a miscellaneous statement
 * ------------------------------------------------------------------------
 */
sql ::= ddl SEMICOLON. 

sql ::= dll SEMICOLON.

sql ::= dml SEMICOLON.

sql ::= dql SEMICOLON. 

sql ::= misc SEMICOLON. 

/* ------------------------------------------------------------------------
 * DDL
 * ------------------------------------------------------------------------
 */
ddl ::= create_clause(C). {
	NOWDB_SQL_MAKE_DDL(C);
}
ddl ::= create_clause(C) IF NOT EXISTS. {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_ADD_OPTION(C, NOWDB_AST_IFEXISTS, 0, NULL);
	NOWDB_SQL_MAKE_DDL(C);
}
ddl ::= drop_clause(C). {
	NOWDB_SQL_MAKE_DDL(C);
}
ddl ::= drop_clause(C) IF EXISTS. {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_ADD_OPTION(C, NOWDB_AST_IFEXISTS, 0, NULL);
	NOWDB_SQL_MAKE_DDL(C);
}

/* ------------------------------------------------------------------------
 * DLL
 * ------------------------------------------------------------------------
 */
dll ::= LOAD STRING(S) INTO dml_target(T). {
	NOWDB_SQL_MAKE_LOAD(S,T,NULL,NULL)
}
dll ::= LOAD STRING(S) INTO dml_target(T) header_clause(H). {
	NOWDB_SQL_MAKE_LOAD(S,T,H,NULL)
}

dll ::= LOAD STRING(S) INTO dml_target(T) header_clause(H) AS IDENTIFIER(I). {
	NOWDB_SQL_MAKE_LOAD(S,T,H,I)
}

dll ::= LOAD STRING(S) INTO dml_target(T) header_clause(H) AS EDGE(E). {
	NOWDB_SQL_MAKE_LOAD(S,T,H,E)
}

dll ::= LOAD STRING(S) INTO dml_target(T) AS EDGE(E). {
	NOWDB_SQL_MAKE_LOAD(S,T,NULL,E)
}

/* ------------------------------------------------------------------------
 * DML
 * ------------------------------------------------------------------------
 */
dml ::= INSERT INTO dml_target(T) LPAR field_list(F) RPAR LPAR val_list(V) RPAR. {
	NOWDB_SQL_MAKE_INSERT(T,F,V);
}

dml ::= INSERT INTO dml_target(T) LPAR val_list(V) RPAR. {
	NOWDB_SQL_MAKE_INSERT(T,NULL,V);
}

/* ------------------------------------------------------------------------
 * DQL
 * ------------------------------------------------------------------------
 */
dql ::= projection_clause(P) from_clause(F). {
	NOWDB_SQL_MAKE_DQL(P,F,NULL,NULL,NULL);
}

dql ::= projection_clause(P) from_clause(F) where_clause(W). {
	NOWDB_SQL_MAKE_DQL(P,F,W,NULL,NULL);
}

dql ::= projection_clause(P) from_clause(F) where_clause(W) group_clause(G). {
	NOWDB_SQL_MAKE_DQL(P,F,W,G,NULL);
}

dql ::= projection_clause(P) from_clause(F) where_clause(W) order_clause(O). {
	NOWDB_SQL_MAKE_DQL(P,F,W,NULL,O);
}

dql ::= projection_clause(P) from_clause(F) where_clause(W) group_clause(G) order_clause(O). {
	NOWDB_SQL_MAKE_DQL(P,F,W,G,O);
}

dql ::= projection_clause(P) from_clause(F) group_clause(G). {
	NOWDB_SQL_MAKE_DQL(P,F,NULL,G,NULL);
}

dql ::= projection_clause(P) from_clause(F) order_clause(O). {
	NOWDB_SQL_MAKE_DQL(P,F,NULL,NULL,O);
}

dql ::= projection_clause(P) from_clause(F) group_clause(G) order_clause(O). {
	NOWDB_SQL_MAKE_DQL(P,F,NULL,G,O);
}

/* ------------------------------------------------------------------------
 * Miscellaneous (use)
 * ------------------------------------------------------------------------
 */
misc ::= USE IDENTIFIER(I). {
	NOWDB_SQL_MAKE_USE(I);
}
misc ::= FETCH UINTEGER(I). {
	NOWDB_SQL_MAKE_FETCH(I);	
}
misc ::= CLOSE UINTEGER(I). {
	NOWDB_SQL_MAKE_CLOSE(I);	
}

/* val_list shall be expression list! */
misc ::= EXECUTE IDENTIFIER(I) LPAR RPAR. {
	NOWDB_SQL_MAKE_EXEC(I,NULL);
}

misc ::= EXECUTE IDENTIFIER(I) LPAR val_list(V) RPAR. {
	NOWDB_SQL_MAKE_EXEC(I,V);
}

/* ------------------------------------------------------------------------
 *  Create Clause
 * ------------------------------------------------------------------------
 */
create_clause(C) ::= CREATE CONTEXT IDENTIFIER(I). {
	NOWDB_SQL_MAKE_CREATE(C,NOWDB_AST_CONTEXT,I,NULL);
}

create_clause(C) ::= CREATE CONTEXT IDENTIFIER(I) SET context_options(O). {
	NOWDB_SQL_MAKE_CREATE(C,NOWDB_AST_CONTEXT,I,NULL);
	NOWDB_SQL_ADDKID(C, O)
}

create_clause(C) ::= CREATE sizing(S) CONTEXT IDENTIFIER(I). {
	NOWDB_SQL_MAKE_CREATE(C,NOWDB_AST_CONTEXT,I,NULL);
	NOWDB_SQL_ADDKID(C, S)
}

create_clause(C) ::= CREATE sizing(S) CONTEXT IDENTIFIER(I) SET context_options(O). {
	NOWDB_SQL_MAKE_CREATE(C,NOWDB_AST_CONTEXT,I,NULL);
	NOWDB_SQL_ADDKID(O, S)
	NOWDB_SQL_ADDKID(C, O)
}

create_clause(C) ::= CREATE SCOPE IDENTIFIER(I). {
	NOWDB_SQL_MAKE_CREATE(C,NOWDB_AST_SCOPE,I,NULL);
}

create_clause(C) ::= CREATE INDEX IDENTIFIER(I) ON index_target(T) LPAR field_list(F) RPAR. {
	NOWDB_SQL_MAKE_CREATE(C,NOWDB_AST_INDEX,I,NULL);
	NOWDB_SQL_ADDKID(C, T);
	NOWDB_SQL_ADDKID(C, F);
}

create_clause(C) ::= CREATE sizing(S) INDEX IDENTIFIER(I) ON index_target(T) LPAR field_list(F) RPAR. {
	NOWDB_SQL_MAKE_CREATE(C,NOWDB_AST_INDEX,I,NULL);
	NOWDB_SQL_ADDKID(C, S);
	NOWDB_SQL_ADDKID(C, T);
	NOWDB_SQL_ADDKID(C, F);
}

create_clause(C) ::= CREATE TYPE IDENTIFIER(I). {
	NOWDB_SQL_MAKE_CREATE(C,NOWDB_AST_TYPE,I,NULL);
}

create_clause(C) ::= CREATE TYPE IDENTIFIER(I) LPAR field_decl_list(L) RPAR. {
	NOWDB_SQL_MAKE_CREATE(C,NOWDB_AST_TYPE,I,NULL);
	NOWDB_SQL_ADDKID(C,L);
}

create_clause(C) ::= CREATE EDGE IDENTIFIER(I) LPAR edge_field_decl_list(L) RPAR. {
	NOWDB_SQL_MAKE_CREATE(C, NOWDB_AST_EDGE, I, NULL);
	NOWDB_SQL_ADDKID(C,L);
}

create_clause(C) ::= CREATE PROCEDURE IDENTIFIER(M) DOT IDENTIFIER(N) LPAR RPAR LANGUAGE IDENTIFIER(L). {
	NOWDB_SQL_MAKE_PROC(C, M, N, L, 0, NULL);
}

/* field_decl_list also allows PK! */
create_clause(C) ::= CREATE PROCEDURE IDENTIFIER(M) DOT IDENTIFIER(N) LPAR field_decl_list(P) RPAR LANGUAGE IDENTIFIER(L). {
	NOWDB_SQL_MAKE_PROC(C, M, N, L, 0, P);
}

index_target(T) ::= IDENTIFIER(I). {
	NOWDB_SQL_CREATEAST(&T, NOWDB_AST_ON, NOWDB_AST_CONTEXT);
	nowdb_ast_setValue(T, NOWDB_AST_V_STRING, I);
}

index_target(T) ::= VERTEX. {
	NOWDB_SQL_CREATEAST(&T, NOWDB_AST_ON, NOWDB_AST_VERTEX);
}

field_decl(F) ::= IDENTIFIER(I) type(T). {
	NOWDB_SQL_CREATEAST(&F, NOWDB_AST_DECL, T);
	nowdb_ast_setValue(F, NOWDB_AST_V_STRING, I);
}

field_decl(F) ::= IDENTIFIER(I) type(T) PK. {
	NOWDB_SQL_CREATEAST(&F, NOWDB_AST_DECL, T);
	nowdb_ast_setValue(F, NOWDB_AST_V_STRING, I);
	NOWDB_SQL_ADD_OPTION(F, NOWDB_AST_PK, 0, NULL);
}

field_decl_list(L) ::= field_decl(F). {
	L=F;
}

field_decl_list(L) ::= field_decl(F) COMMA field_decl_list(L2). {
	NOWDB_SQL_ADDKID(F,L2);
	L=F;
}

edge_field_decl(E) ::= ORIGIN IDENTIFIER(I). {
	NOWDB_SQL_MAKE_EDGE_TYPE(E,NOWDB_OFF_ORIGIN,I);
}

edge_field_decl(E) ::= DESTINATION IDENTIFIER(I). {
	NOWDB_SQL_MAKE_EDGE_TYPE(E,NOWDB_OFF_DESTIN,I);
}

edge_field_decl(E) ::= LABEL type(T). {
	NOWDB_SQL_CREATEAST(&E, NOWDB_AST_DECL, T);
	nowdb_ast_setValue(E, NOWDB_AST_V_INTEGER,
	         (void*)(uint64_t)NOWDB_OFF_LABEL);
}
edge_field_decl(E) ::= WEIGHT type(T). {
	NOWDB_SQL_CREATEAST(&E, NOWDB_AST_DECL, T);
	nowdb_ast_setValue(E, NOWDB_AST_V_INTEGER,
	        (void*)(uint64_t)NOWDB_OFF_WEIGHT);
}

edge_field_decl(E) ::= WEIGHT2 type(T). {
	NOWDB_SQL_CREATEAST(&E, NOWDB_AST_DECL, T);
	nowdb_ast_setValue(E, NOWDB_AST_V_INTEGER,
	        (void*)(uint64_t)NOWDB_OFF_WEIGHT2);
}

edge_field_decl_list(L) ::= edge_field_decl(E). {
	L=E;
}

edge_field_decl_list(L) ::= edge_field_decl(E) COMMA edge_field_decl_list(L2). {
	NOWDB_SQL_ADDKID(E,L2);
	L=E;
}

type(T) ::= TEXT. {
	T=NOWDB_AST_TEXT;
}
type(T) ::= TIME. {
	T=NOWDB_AST_TIME;
}
type(T) ::= DATE. {
	T=NOWDB_AST_DATE;
}
type(T) ::= FLOAT. {
	T=NOWDB_AST_FLOAT;
}
type(T) ::= INT. {
	T=NOWDB_AST_INT;
}
type(T) ::= UINT. {
	T=NOWDB_AST_UINT;
}
type(T) ::= BOOL. {
	T=NOWDB_AST_BOOL;
}
type ::= LONGTEXT.

/* ------------------------------------------------------------------------
 *  Drop Clause
 * ------------------------------------------------------------------------
 */
drop_clause(C) ::= DROP CONTEXT IDENTIFIER(I). {
	NOWDB_SQL_MAKE_DROP(C,NOWDB_AST_CONTEXT,I,NULL);
}

drop_clause(C) ::= DROP SCOPE IDENTIFIER(I). {
	NOWDB_SQL_MAKE_DROP(C,NOWDB_AST_SCOPE,I,NULL);
}

drop_clause(C) ::= DROP INDEX IDENTIFIER(I). {
	NOWDB_SQL_MAKE_DROP(C,NOWDB_AST_INDEX,I,NULL);
}

drop_clause(C) ::= DROP TYPE IDENTIFIER(I). {
	NOWDB_SQL_MAKE_DROP(C,NOWDB_AST_TYPE,I,NULL);
}

drop_clause(C) ::= DROP EDGE IDENTIFIER(I). {
	NOWDB_SQL_MAKE_DROP(C,NOWDB_AST_EDGE,I,NULL);
}

drop_clause(C) ::= DROP PROCEDURE IDENTIFIER(I). {
	NOWDB_SQL_MAKE_DROP(C,NOWDB_AST_PROC,I,NULL);
}

/* ------------------------------------------------------------------------
 * Context Options
 * ------------------------------------------------------------------------
 */
context_options(O) ::= context_option(C). {
	NOWDB_SQL_CHECKSTATE();
	O = C;
}

context_options(O) ::= context_option(O1) COMMA context_options(O2). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_ADDKID(O1,O2);
	O = O1;
}

context_option(O) ::= ALLOCSIZE EQ UINTEGER(I). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&O, NOWDB_AST_OPTION, NOWDB_AST_ALLOCSZ);
	nowdb_ast_setValue(O, NOWDB_AST_V_STRING, I);
}
context_option(O) ::= LARGESIZE EQ UINTEGER(I). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&O, NOWDB_AST_OPTION, NOWDB_AST_LARGESZ);
	nowdb_ast_setValue(O, NOWDB_AST_V_STRING, I);
}
context_option(O) ::= SORTERS EQ UINTEGER(I). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&O, NOWDB_AST_OPTION, NOWDB_AST_SORTERS);
	nowdb_ast_setValue(O, NOWDB_AST_V_STRING, I);
}
context_option(O) ::= COMPRESSION EQ STRING(I). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&O, NOWDB_AST_OPTION, NOWDB_AST_COMP);
	nowdb_ast_setValue(O, NOWDB_AST_V_STRING, I);
}
context_option(O) ::= ENCRYPTION EQ STRING(I). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&O, NOWDB_AST_OPTION, NOWDB_AST_ENCP);
	nowdb_ast_setValue(O, NOWDB_AST_V_STRING, I);
}

context_option(O) ::= STRESS EQ stress_spec(S). {
	NOWDB_SQL_CHECKSTATE();
	O=S;
}

context_option(O) ::= SIZE EQ sizing(S). {
	NOWDB_SQL_CHECKSTATE();
	O=S;
}

context_option(O) ::= DISK EQ disk_spec(S). {
	NOWDB_SQL_CHECKSTATE();
	O=S;
}

sizing(S) ::= TINY. {
	NOWDB_SQL_CREATEAST(&S, NOWDB_AST_OPTION, NOWDB_AST_SIZING);
	nowdb_ast_setValue(S, NOWDB_AST_V_INTEGER,
	                   (void*)(uint64_t)NOWDB_CONFIG_SIZE_TINY);
}
sizing(S) ::= SMALL. {
	NOWDB_SQL_CREATEAST(&S, NOWDB_AST_OPTION, NOWDB_AST_SIZING);
	nowdb_ast_setValue(S, NOWDB_AST_V_INTEGER,
	                   (void*)(uint64_t)NOWDB_CONFIG_SIZE_SMALL);
}
sizing(S) ::= MEDIUM. {
	NOWDB_SQL_CREATEAST(&S, NOWDB_AST_OPTION, NOWDB_AST_SIZING);
	nowdb_ast_setValue(S, NOWDB_AST_V_INTEGER,
	                   (void*)(uint64_t)NOWDB_CONFIG_SIZE_MEDIUM);
}
sizing(S) ::= BIG. {
	NOWDB_SQL_CREATEAST(&S, NOWDB_AST_OPTION, NOWDB_AST_SIZING);
	nowdb_ast_setValue(S, NOWDB_AST_V_INTEGER,
	                   (void*)(uint64_t)NOWDB_CONFIG_SIZE_BIG);
}
sizing(S) ::= LARGE. {
	NOWDB_SQL_CREATEAST(&S, NOWDB_AST_OPTION, NOWDB_AST_SIZING);
	nowdb_ast_setValue(S, NOWDB_AST_V_INTEGER,
	                   (void*)(uint64_t)NOWDB_CONFIG_SIZE_LARGE);
}
sizing(S) ::= HUGE. {
	NOWDB_SQL_CREATEAST(&S, NOWDB_AST_OPTION, NOWDB_AST_SIZING);
	nowdb_ast_setValue(S, NOWDB_AST_V_INTEGER,
	                   (void*)(uint64_t)NOWDB_CONFIG_SIZE_HUGE);
}

stress_spec(O) ::= MODERATE. {
	NOWDB_SQL_CREATEAST(&O, NOWDB_AST_OPTION, NOWDB_AST_STRESS);
	nowdb_ast_setValue(O, NOWDB_AST_V_INTEGER,
	                   (void*)(uint64_t)NOWDB_CONFIG_INSERT_MODERATE);
}

stress_spec(O) ::= CONSTANT. {
	NOWDB_SQL_CREATEAST(&O, NOWDB_AST_OPTION, NOWDB_AST_STRESS);
	nowdb_ast_setValue(O, NOWDB_AST_V_INTEGER,
	                   (void*)(uint64_t)NOWDB_CONFIG_INSERT_CONSTANT);
}

stress_spec(O) ::= INSANE. {
	NOWDB_SQL_CREATEAST(&O, NOWDB_AST_OPTION, NOWDB_AST_STRESS);
	nowdb_ast_setValue(O, NOWDB_AST_V_INTEGER,
	                   (void*)(uint64_t)NOWDB_CONFIG_INSERT_INSANE);
}

disk_spec(O) ::= HDD. {
	NOWDB_SQL_CREATEAST(&O, NOWDB_AST_OPTION, NOWDB_AST_DISK);
	nowdb_ast_setValue(O, NOWDB_AST_V_INTEGER,
	                   (void*)(uint64_t)NOWDB_SQL_HDD);
}
disk_spec(O) ::= SSD. {
	NOWDB_SQL_CREATEAST(&O, NOWDB_AST_OPTION, NOWDB_AST_DISK);
	nowdb_ast_setValue(O, NOWDB_AST_V_INTEGER,
	                   (void*)(uint64_t)NOWDB_SQL_SSD);
}
disk_spec(O) ::= RAID. {
	NOWDB_SQL_CREATEAST(&O, NOWDB_AST_OPTION, NOWDB_AST_DISK);
	nowdb_ast_setValue(O, NOWDB_AST_V_INTEGER,
	                   (void*)(uint64_t)NOWDB_SQL_RAID);
}

/*
switch_spec(O) ::= ON. {
	NOWDB_SQL_CREATEAST(&O, NOWDB_AST_OPTION, NOWDB_AST_ON);
}
switch_spec(O) ::= OFF. {
	NOWDB_SQL_CREATEAST(&O, NOWDB_AST_OPTION, NOWDB_AST_OFF);
}
*/

/* ------------------------------------------------------------------------
 * Header Clause
 * ------------------------------------------------------------------------
 */
header_clause(O) ::= IGNORE HEADER. {
	NOWDB_SQL_CREATEAST(&O, NOWDB_AST_OPTION, NOWDB_AST_IGNORE);
}
header_clause(O) ::= USE HEADER. {
	NOWDB_SQL_CREATEAST(&O, NOWDB_AST_OPTION, NOWDB_AST_USE);
}

/* ------------------------------------------------------------------------
 * DML target
 * ------------------------------------------------------------------------
 */
dml_target(T) ::= IDENTIFIER(I). {
	NOWDB_SQL_MAKE_DMLTARGET(T, NOWDB_AST_CONTEXT, I)
}
dml_target(T) ::= VERTEX. {
	NOWDB_SQL_MAKE_DMLTARGET(T, NOWDB_AST_VERTEX, NULL)
}

/* ------------------------------------------------------------------------
 * projection clause
 * ------------------------------------------------------------------------
 */
projection_clause(P) ::= SELECT STAR. {
	NOWDB_SQL_CREATEAST(&P, NOWDB_AST_SELECT, NOWDB_AST_ALL);
}

projection_clause(P) ::= SELECT pj_list(F). {
	NOWDB_SQL_CREATEAST(&P, NOWDB_AST_SELECT, 0);
	NOWDB_SQL_ADDKID(P,F);
}

/* ------------------------------------------------------------------------
 * from clause 
 * ------------------------------------------------------------------------
 */
from_clause(F) ::= FROM table_list(T). {
	NOWDB_SQL_CREATEAST(&F, NOWDB_AST_FROM, 0);
	NOWDB_SQL_ADDKID(F, T);
}

/* ------------------------------------------------------------------------
 * group clause
 * ------------------------------------------------------------------------
 */
group_clause(G) ::= GROUP BY field_list(F). {
	NOWDB_SQL_CREATEAST(&G, NOWDB_AST_GROUP, 0);
	NOWDB_SQL_ADDKID(G,F);
}

/* ------------------------------------------------------------------------
 * order clause
 * ------------------------------------------------------------------------
 */
order_clause(O) ::= ORDER BY field_list(F). {
	NOWDB_SQL_CREATEAST(&O, NOWDB_AST_ORDER, 0);
	NOWDB_SQL_ADDKID(O,F);
}

/* ------------------------------------------------------------------------
 * projectable
 * ------------------------------------------------------------------------
 */
pj_list(L) ::= pj(F). {
	NOWDB_SQL_CHECKSTATE();
	L = F;
}
 
pj_list(L) ::= pj(F) COMMA pj_list(PL). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_ADDKID(F,PL);
	L = F;
}

pj(P) ::= field(F). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&P, NOWDB_AST_FIELD, 0);
	nowdb_ast_setValue(P, NOWDB_AST_V_STRING, F);
}

pj(P) ::= value(V). {
        P=V;
}

pj(P) ::= fun(F). {
	P=F;
}

fun(F) ::= IDENTIFIER(I) LPAR STAR RPAR. {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&F, NOWDB_AST_FUN, 0);
	nowdb_ast_setValue(F, NOWDB_AST_V_STRING, I);
}

fun(F) ::= IDENTIFIER(I) LPAR field(L) RPAR. {
	NOWDB_SQL_CREATEAST(&F, NOWDB_AST_FUN, 0);
	nowdb_ast_setValue(F, NOWDB_AST_V_STRING, I);
	nowdb_ast_t *a;
	NOWDB_SQL_CREATEAST(&a, NOWDB_AST_FIELD, NOWDB_AST_PARAM);
	nowdb_ast_setValue(a, NOWDB_AST_V_STRING, L);
	NOWDB_SQL_ADDKID(F,a);
}

fun(F) ::= IDENTIFIER(I) LPAR RPAR. {
	NOWDB_SQL_CREATEAST(&F, NOWDB_AST_FUN, 0);
	nowdb_ast_setValue(F, NOWDB_AST_V_STRING, I);
}

/* ------------------------------------------------------------------------
 * value list
 * ------------------------------------------------------------------------
 */
val_list(L) ::= value(V). {
	L=V;
}

val_list(L) ::= value(V) COMMA val_list(L2). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_ADDKID(V,L2);
	L = V;
}

/* ------------------------------------------------------------------------
 * Table list and table spec
 * ------------------------------------------------------------------------
 */
table_list(T) ::= table_spec(S). {
	T=S;
}

table_list(T) ::= table_spec(S) COMMA table_list(L). {
	NOWDB_SQL_ADDKID(S,L);
	T=S;
}

table_spec(T) ::= VERTEX. {
	NOWDB_SQL_CREATEAST(&T, NOWDB_AST_TARGET, NOWDB_AST_VERTEX);
}

table_spec(T) ::= IDENTIFIER(I). {
	NOWDB_SQL_CREATEAST(&T, NOWDB_AST_TARGET, NOWDB_AST_CONTEXT);
	nowdb_ast_setValue(T, NOWDB_AST_V_STRING, I);
}

table_spec(T) ::= VERTEX AS IDENTIFIER(I). {
	NOWDB_SQL_CREATEAST(&T, NOWDB_AST_TARGET, NOWDB_AST_TYPE);
	nowdb_ast_setValue(T, NOWDB_AST_V_STRING, I);
}

/* ------------------------------------------------------------------------
 * where clause and expr
 * ------------------------------------------------------------------------
 */
%left OR.
%left AND.
%right NOT.

where_clause(W) ::= WHERE expr(E). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&W, NOWDB_AST_WHERE, 0);
	NOWDB_SQL_ADDKID(W, E);
}

expr(E) ::= condition(C). {
	E=C;
}

expr(E) ::= expr(A) AND expr(B). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&E, NOWDB_AST_AND, 0);
	NOWDB_SQL_ADDKID(E,A);
	NOWDB_SQL_ADDKID(E,B);
}

expr(E) ::= expr(A) OR expr(B). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&E, NOWDB_AST_OR, 0);
	NOWDB_SQL_ADDKID(E,A);
	NOWDB_SQL_ADDKID(E,B);
}

expr(E) ::= LPAR expr(A) RPAR. {
	NOWDB_SQL_CHECKSTATE();
	E=A;
} 

expr(E) ::= NOT expr(A). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&E, NOWDB_AST_NOT, 0);
	NOWDB_SQL_ADDKID(E,A);
}

condition(C) ::= operand(O1) comparison(O) operand(O2) . {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_ADDKID(O,O1);
	NOWDB_SQL_ADDKID(O,O2);
	NOWDB_SQL_CREATEAST(&C, NOWDB_AST_JUST, 0);
	NOWDB_SQL_ADDKID(C,O);
}

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
	O=V;
}

field_list(L) ::= field(F). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&L, NOWDB_AST_FIELD, 0);
	nowdb_ast_setValue(L, NOWDB_AST_V_STRING, F);
}
 
field_list(L) ::= field(F) COMMA field_list(FL). {
	NOWDB_SQL_CREATEAST(&L, NOWDB_AST_FIELD, 0);
	nowdb_ast_setValue(L, NOWDB_AST_V_STRING, F);
	NOWDB_SQL_ADDKID(L,FL);
}

field(F) ::= IDENTIFIER(I). {
	F=I;
}
field(F) ::= ORIGIN(O). {
	F=O;
}
field(F) ::= DESTINATION(D). {
	F=D;
}
field(F) ::= EDGE(E). {
	F=E;
}
field(F) ::= LABEL(L). {
	F=L;
}
field(F) ::= TIMESTAMP(T). {
	F=T;
}
field(F) ::= WEIGHT(T). {
	F=T;
}
field(F) ::= WEIGHT2(T). {
	F=T;
}

value(V) ::= STRING(S). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&V, NOWDB_AST_VALUE, NOWDB_AST_TEXT);
	NOWDB_SQL_SETSTRING(V, S);
}
value(V) ::= UINTEGER(I). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&V, NOWDB_AST_VALUE, NOWDB_AST_UINT);
	nowdb_ast_setValue(V, NOWDB_AST_V_STRING, I);
}
value(V) ::= INTEGER(I). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&V, NOWDB_AST_VALUE, NOWDB_AST_INT);
	nowdb_ast_setValue(V, NOWDB_AST_V_STRING, I);
}
value(V) ::= FLOAT(F). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&V, NOWDB_AST_VALUE, NOWDB_AST_FLOAT);
	nowdb_ast_setValue(V, NOWDB_AST_V_STRING, F);
}
value(V) ::= TRUE(T). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&V, NOWDB_AST_VALUE, NOWDB_AST_BOOL);
	nowdb_ast_setValue(V, NOWDB_AST_V_STRING, T);
}
value(V) ::= FALSE(F). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&V, NOWDB_AST_VALUE, NOWDB_AST_BOOL);
	nowdb_ast_setValue(V, NOWDB_AST_V_STRING, F);
}
