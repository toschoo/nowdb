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
 * - expression in where
 * - qualified names
 * - aliases
 * - joins
 * - NULL
 * - alter
 * - update and delete
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

%stack_overflow {
     fprintf(stderr,"Giving up.  Parser stack overflow\n");
}

%stack_size 1000

/* ------------------------------------------------------------------------
 * Everything else is an ast node
 * ------------------------------------------------------------------------
 */
%type condition {nowdb_ast_t*}
%destructor condition {nowdb_ast_destroyAndFree($$);}

%type storage_option {nowdb_ast_t*}
%destructor storage_option {nowdb_ast_destroyAndFree($$);}

%type comparison {nowdb_ast_t*}
%destructor comparison {nowdb_ast_destroyAndFree($$);}

%type storage_options {nowdb_ast_t*}
%destructor storage_options {nowdb_ast_destroyAndFree($$);}

%type load_options {nowdb_ast_t*}
%destructor load_options {nowdb_ast_destroyAndFree($$);}

%type load_option {nowdb_ast_t*}
%destructor load_option {nowdb_ast_destroyAndFree($$);}

%type lock_options {nowdb_ast_t*}
%destructor lock_options {nowdb_ast_destroyAndFree($$);}

%type lock_option {nowdb_ast_t*}
%destructor lock_option {nowdb_ast_destroyAndFree($$);}

%type operand {nowdb_ast_t*}
%destructor operand {nowdb_ast_destroyAndFree($$);}

%type value {nowdb_ast_t*}
%destructor value {nowdb_ast_destroyAndFree($$);}

%type expr {nowdb_ast_t*}
%destructor expr {nowdb_ast_destroyAndFree($$);}

%type prj {nowdb_ast_t*}
%destructor prj {nowdb_ast_destroyAndFree($$);}

%type case {nowdb_ast_t*}
%destructor case {nowdb_ast_destroyAndFree($$);}

%type sizing {nowdb_ast_t*}
%destructor sizing {nowdb_ast_destroyAndFree($$);}

%type target {nowdb_ast_t*}
%destructor target {nowdb_ast_destroyAndFree($$);}

%type create_clause {nowdb_ast_t*}
%destructor create_clause {nowdb_ast_destroyAndFree($$);}

%type drop_clause {nowdb_ast_t*}
%destructor drop_clause {nowdb_ast_destroyAndFree($$);}

%type storage_clause {nowdb_ast_t*}
%destructor storage_clause {nowdb_ast_destroyAndFree($$);}

%type show_clause {nowdb_ast_t*}
%destructor show_clause {nowdb_ast_destroyAndFree($$);}

%type desc_clause {nowdb_ast_t*}
%destructor desc_clause {nowdb_ast_destroyAndFree($$);}

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

%type field {nowdb_ast_t*}
%destructor field {nowdb_ast_destroyAndFree($$);}

%type expr_list {nowdb_ast_t*}
%destructor expr_list {nowdb_ast_destroyAndFree($$);}

%type prj_list {nowdb_ast_t*}
%destructor prj_list {nowdb_ast_destroyAndFree($$);}

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

ddl ::= show_clause(C). {
	NOWDB_SQL_MAKE_DDL(C);
}

ddl ::= desc_clause(C). {
	NOWDB_SQL_MAKE_DDL(C);
}

show_clause(C) ::= SHOW IDENTIFIER(I). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&C, NOWDB_AST_SHOW, 0);
	nowdb_ast_setValue(C, NOWDB_AST_V_STRING, I);
}

desc_clause(C) ::= DESC IDENTIFIER(I). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&C, NOWDB_AST_DESC, 0);
	nowdb_ast_setValue(C, NOWDB_AST_V_STRING, I);
}

/* desc and show procedures */
/* show locks */
/* show and desc queues */

/* ------------------------------------------------------------------------
 * DLL
 * ------------------------------------------------------------------------
 */
dll ::= LOAD STRING(S) INTO dml_target(T). {
	NOWDB_SQL_MAKE_LOAD(S,T,NULL,NULL)
}
dll ::= LOAD STRING(S) INTO dml_target(T) SET load_options(O). {
	NOWDB_SQL_MAKE_LOAD(S,T,O,NULL)
}
dll ::= LOAD STRING(S) INTO dml_target(T) header_clause(H). {
	NOWDB_SQL_MAKE_LOAD(S,T,H,NULL)
}
dll ::= LOAD STRING(S) INTO dml_target(T) header_clause(H) SET load_options(O). {
	NOWDB_SQL_ADDKID(H,O)
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
dml ::= INSERT INTO dml_target(T) LPAR field_list(F) RPAR LPAR expr_list(V) RPAR. {
	NOWDB_SQL_MAKE_INSERT(T,F,V);
}

dml ::= INSERT INTO dml_target(T) LPAR field_list(F) RPAR VALUES LPAR expr_list(V) RPAR. {
	NOWDB_SQL_MAKE_INSERT(T,F,V);
}


dml ::= INSERT INTO dml_target(T) VALUES LPAR expr_list(V) RPAR. {
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
misc ::= projection_clause(P). {
	NOWDB_SQL_MAKE_SELECT(P);
}

misc ::= EXECUTE IDENTIFIER(I) LPAR RPAR. {
	NOWDB_SQL_MAKE_EXEC(I,NULL);
}

misc ::= EXECUTE IDENTIFIER(I) LPAR expr_list(V) RPAR. {
	NOWDB_SQL_MAKE_EXEC(I,V);
}

misc ::= LOCK IDENTIFIER(I). {
	NOWDB_SQL_MAKE_LOCK(I, NOWDB_AST_LOCK, NULL);
}

misc ::= LOCK IDENTIFIER(I) FOR IDENTIFIER(M). {
	nowdb_ast_t *m;
	NOWDB_SQL_MAKE_OPTION(m, NOWDB_AST_MODE, NOWDB_AST_V_STRING, M);
	NOWDB_SQL_MAKE_LOCK(I, NOWDB_AST_LOCK, m);
}

misc ::= LOCK IDENTIFIER(I) FOR IDENTIFIER(M) SET lock_options(O). {
	NOWDB_SQL_ADD_OPTION(O, NOWDB_AST_MODE, NOWDB_AST_V_STRING, M);
	NOWDB_SQL_MAKE_LOCK(I, NOWDB_AST_LOCK, O);
}

misc ::= UNLOCK IDENTIFIER(I). {
	NOWDB_SQL_MAKE_LOCK(I, NOWDB_AST_UNLOCK, NULL);
}

/* ------------------------------------------------------------------------
 *  Create Clause
 * ------------------------------------------------------------------------
 */
create_clause(C) ::= CREATE STORAGE IDENTIFIER(I). {
	NOWDB_SQL_MAKE_CREATE(C,NOWDB_AST_STORAGE,I,NULL);
}

create_clause(C) ::= CREATE STORAGE IDENTIFIER(I) SET storage_options(O). {
	NOWDB_SQL_MAKE_CREATE(C,NOWDB_AST_STORAGE,I,NULL);
	NOWDB_SQL_ADDKID(C, O)
}

create_clause(C) ::= CREATE sizing(S) STORAGE IDENTIFIER(I). {
	NOWDB_SQL_MAKE_CREATE(C,NOWDB_AST_STORAGE,I,NULL);
	NOWDB_SQL_ADDKID(C, S)
}

create_clause(C) ::= CREATE sizing(S) STORAGE IDENTIFIER(I) SET storage_options(O). {
	NOWDB_SQL_MAKE_CREATE(C,NOWDB_AST_STORAGE,I,NULL);
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

/* could be interesting...
create_clause(C) ::= CREATE TYPE IDENTIFIER(I). {
	NOWDB_SQL_MAKE_CREATE(C,NOWDB_AST_TYPE,I,NULL);
}
*/

create_clause(C) ::= CREATE TYPE IDENTIFIER(I) LPAR field_decl_list(L) RPAR. {
	NOWDB_SQL_MAKE_CREATE(C,NOWDB_AST_TYPE,I,NULL);
	NOWDB_SQL_ADDKID(C,L);
}

create_clause(C) ::= CREATE TYPE IDENTIFIER(I) LPAR field_decl_list(L) RPAR storage_clause(S). {
	NOWDB_SQL_MAKE_CREATE(C,NOWDB_AST_TYPE,I,S);
	NOWDB_SQL_ADDKID(C,L);
}

create_clause(C) ::= CREATE EDGE IDENTIFIER(I) LPAR edge_field_decl(O) COMMA edge_field_decl(D) RPAR. {
	NOWDB_SQL_ADDKID(O,D);
	NOWDB_SQL_MAKE_CREATE(C, NOWDB_AST_EDGE, I, NULL);
	NOWDB_SQL_ADDKID(C,O);
}

create_clause(C) ::= CREATE EDGE IDENTIFIER(I) LPAR edge_field_decl(O) COMMA edge_field_decl(D) RPAR storage_clause(S). {
	NOWDB_SQL_ADDKID(O,D);
	NOWDB_SQL_MAKE_CREATE(C, NOWDB_AST_EDGE, I, S);
	NOWDB_SQL_ADDKID(C,O);
}

create_clause(C) ::= CREATE STAMPED EDGE IDENTIFIER(I) LPAR edge_field_decl_list(L) RPAR. {
	NOWDB_SQL_MAKE_CREATE(C, NOWDB_AST_EDGE, I, NULL);
	NOWDB_SQL_ADDKID(C,L);
}

create_clause(C) ::= CREATE STAMPED EDGE IDENTIFIER(I) LPAR edge_field_decl_list(L) RPAR storage_clause(S). {
	NOWDB_SQL_MAKE_CREATE(C, NOWDB_AST_EDGE, I, S);
	NOWDB_SQL_ADDKID(C,L);
}

create_clause(C) ::= CREATE PROCEDURE IDENTIFIER(M) DOT IDENTIFIER(N) LPAR RPAR LANGUAGE IDENTIFIER(L). {
	NOWDB_SQL_MAKE_PROC(C, M, N, L, 0, NULL);
}

/* field_decl_list also allows PK! */
create_clause(C) ::= CREATE PROCEDURE IDENTIFIER(M) DOT IDENTIFIER(N) LPAR field_decl_list(P) RPAR LANGUAGE IDENTIFIER(L). {
	NOWDB_SQL_MAKE_PROC(C, M, N, L, 0, P);
}

create_clause(C) ::= CREATE LOCK IDENTIFIER(I). {
	NOWDB_SQL_MAKE_CREATE(C, NOWDB_AST_MUTEX, I, NULL);
}

storage_clause(S) ::= STORAGE EQ IDENTIFIER(I). {
	NOWDB_SQL_CREATEAST(&S, NOWDB_AST_STORAGE, 0);
	nowdb_ast_setValue(S, NOWDB_AST_V_STRING, I);
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

field_decl_list(L) ::= field_decl_list(L2) COMMA field_decl(F). {
	NOWDB_SQL_ADDKID(L2,F);
	L=L2;
}

edge_field_decl(E) ::= ORIGIN IDENTIFIER(I). {
	NOWDB_SQL_MAKE_EDGE_TYPE(E,NOWDB_OFF_ORIGIN,I);
}

edge_field_decl(E) ::= DESTINATION IDENTIFIER(I). {
	NOWDB_SQL_MAKE_EDGE_TYPE(E,NOWDB_OFF_DESTIN,I);
}

edge_field_decl(F) ::= IDENTIFIER(I) type(T). {
	NOWDB_SQL_CREATEAST(&F, NOWDB_AST_DECL, T);
	nowdb_ast_setValue(F, NOWDB_AST_V_STRING, I);
}

edge_field_decl_list(L) ::= edge_field_decl(E). {
	L=E;
}

edge_field_decl_list(L) ::= edge_field_decl_list(L2) COMMA edge_field_decl(E). {
	NOWDB_SQL_ADDKID(L2,E);
	L=L2;
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
drop_clause(C) ::= DROP STORAGE IDENTIFIER(I). {
	NOWDB_SQL_MAKE_DROP(C,NOWDB_AST_STORAGE,I,NULL);
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

drop_clause(C) ::= DROP LOCK IDENTIFIER(I). {
	NOWDB_SQL_MAKE_DROP(C,NOWDB_AST_MUTEX,I,NULL);
}

/* ------------------------------------------------------------------------
 * Storage Options
 * ------------------------------------------------------------------------
 */
storage_options(O) ::= storage_option(C). {
	NOWDB_SQL_CHECKSTATE();
	O = C;
}

storage_options(O) ::= storage_option(O1) COMMA storage_options(O2). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_ADDKID(O1,O2);
	O = O1;
}

storage_option(O) ::= ALLOCSIZE EQ UINTEGER(I). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&O, NOWDB_AST_OPTION, NOWDB_AST_ALLOCSZ);
	nowdb_ast_setValue(O, NOWDB_AST_V_STRING, I);
}
storage_option(O) ::= LARGESIZE EQ UINTEGER(I). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&O, NOWDB_AST_OPTION, NOWDB_AST_LARGESZ);
	nowdb_ast_setValue(O, NOWDB_AST_V_STRING, I);
}
storage_option(O) ::= SORTERS EQ UINTEGER(I). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&O, NOWDB_AST_OPTION, NOWDB_AST_SORTERS);
	nowdb_ast_setValue(O, NOWDB_AST_V_STRING, I);
}
storage_option(O) ::= COMPRESSION EQ STRING(I). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&O, NOWDB_AST_OPTION, NOWDB_AST_COMP);
	nowdb_ast_setValue(O, NOWDB_AST_V_STRING, I);
}
storage_option(O) ::= ENCRYPTION EQ STRING(I). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&O, NOWDB_AST_OPTION, NOWDB_AST_ENCP);
	nowdb_ast_setValue(O, NOWDB_AST_V_STRING, I);
}

storage_option(O) ::= STRESS EQ stress_spec(S). {
	NOWDB_SQL_CHECKSTATE();
	O=S;
}

storage_option(O) ::= SIZE EQ sizing(S). {
	NOWDB_SQL_CHECKSTATE();
	O=S;
}

storage_option(O) ::= DISK EQ disk_spec(S). {
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
 * Error Clause
 * ------------------------------------------------------------------------
 */
load_options(L) ::= load_option(O). {
	L=O;
}
load_options(L) ::= load_option(O) COMMA load_options(L2). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_ADDKID(O,L2);
	L=O;
}
load_option(O) ::= ERRORS EQ STRING(S). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&O, NOWDB_AST_OPTION, NOWDB_AST_ERRORS);
	nowdb_ast_setValue(O, NOWDB_AST_V_STRING, S);
}

/* ------------------------------------------------------------------------
 * LOCK options
 * ------------------------------------------------------------------------
 */
lock_options(L) ::= lock_option(O). {
	L=O;
}

lock_options(L) ::= lock_option(O) COMMA lock_options(L2). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_ADDKID(O,L2);
	L=O;
}

lock_option(O) ::= TIMEOUT EQ UINTEGER(I). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&O, NOWDB_AST_OPTION, NOWDB_AST_TIMEOUT);
	nowdb_ast_setValue(O, NOWDB_AST_V_STRING, I);
}

/* ------------------------------------------------------------------------
 * DML target
 * ------------------------------------------------------------------------
 */
dml_target(T) ::= IDENTIFIER(I). {
	NOWDB_SQL_MAKE_DMLTARGET(T, NOWDB_AST_CONTEXT, I)
}

/* ------------------------------------------------------------------------
 * projection clause
 * ------------------------------------------------------------------------
 */
projection_clause(P) ::= SELECT STAR. {
	NOWDB_SQL_CREATEAST(&P, NOWDB_AST_SELECT, NOWDB_AST_STAR);
}

projection_clause(P) ::= SELECT prj_list(F). {
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
 * expression
 * ------------------------------------------------------------------------
 */
%left OR.
%left AND.
%right NOT.
/* %left MATCH LIKE_KW BETWEEN IN ISNULL NOTNULL NE EQ. */
%left IS.
%left EQ NE.
%left GT LE LT GE.
%left IN.
/* %right ESCAPE. */
/* %left BITAND BITOR LSHIFT RSHIFT. */
%left PLUS MINUS.
%left STAR DIV PER.
%left POW.
/* %left CONCAT. */
/* %left COLLATE. */
/* %right BITNOT. */

prj_list(L) ::= prj(P). {
	NOWDB_SQL_CHECKSTATE();
	L = P;
}
 
prj_list(L) ::= prj_list(PL) COMMA prj(P). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_ADDKID(PL,P);
	L = PL;
}

prj(P) ::= expr(E). {
	P=E;
}

/* currently we ignore the alias! */
prj(P) ::= expr(E) AS IDENTIFIER. {
	P=E;
}

expr_list(L) ::= expr(F). {
	NOWDB_SQL_CHECKSTATE();
	L = F;
}
 
expr_list(L) ::= expr_list(PL) COMMA expr(F). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_ADDKID(PL,F);
	L = PL;
}

expr(P) ::= value(V). {
        P=V;
}

expr(P) ::= field(F). {
	P=F;
}

expr(P) ::= LPAR expr(E) RPAR. {
	P=E;
}

expr(P) ::= expr(O1) PLUS|MINUS(OP) expr(O2). {
	NOWDB_SQL_CREATEAST(&P, NOWDB_AST_OP, 2);
	nowdb_ast_setValue(P, NOWDB_AST_V_STRING, OP);
	NOWDB_SQL_ADDPARAM(P,O1);
	NOWDB_SQL_ADDPARAM(P,O2);
}

expr(P) ::= expr(O1) STAR|DIV|PER(OP) expr(O2). {
	NOWDB_SQL_CREATEAST(&P, NOWDB_AST_OP, 2);
	nowdb_ast_setValue(P, NOWDB_AST_V_STRING, OP);
	NOWDB_SQL_ADDPARAM(P,O1);
	NOWDB_SQL_ADDPARAM(P,O2);
}

expr(P) ::= expr(O1) POW(OP) expr(O2). {
	NOWDB_SQL_CREATEAST(&P, NOWDB_AST_OP, 2);
	nowdb_ast_setValue(P, NOWDB_AST_V_STRING, OP);
	NOWDB_SQL_ADDPARAM(P,O1);
	NOWDB_SQL_ADDPARAM(P,O2);
}

expr(E) ::= expr(A) AND(OP) expr(B). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&E, NOWDB_AST_OP, 2);
	nowdb_ast_setValue(E, NOWDB_AST_V_STRING, OP);
	NOWDB_SQL_ADDPARAM(E,A);
	NOWDB_SQL_ADDPARAM(E,B);
}

expr(E) ::= expr(A) OR(OP) expr(B). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&E, NOWDB_AST_OP, 2);
	nowdb_ast_setValue(E, NOWDB_AST_V_STRING, OP);
	NOWDB_SQL_ADDPARAM(E,A);
	NOWDB_SQL_ADDPARAM(E,B);
}

expr(E) ::= NOT(OP) expr(A). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&E, NOWDB_AST_OP, 1);
	nowdb_ast_setValue(E, NOWDB_AST_V_STRING, OP);
	NOWDB_SQL_ADDPARAM(E,A);
}

expr(E) ::= expr(A) IS(OP) NULL. {
	NOWDB_SQL_CREATEAST(&E, NOWDB_AST_OP, 1);
	nowdb_ast_setValue(E, NOWDB_AST_V_STRING, OP);
	NOWDB_SQL_ADDPARAM(E,A);
}

expr(E) ::= expr(A) IS NOT NULL. {
	NOWDB_SQL_CREATEAST(&E, NOWDB_AST_OP, 1);
	nowdb_ast_setValue(E, NOWDB_AST_V_STRING, strdup("ISN"));
	NOWDB_SQL_ADDPARAM(E,A);
}

expr(E) ::= expr(A) EQ|NE(OP) expr(B). {
	NOWDB_SQL_CREATEAST(&E, NOWDB_AST_OP, 2);
	nowdb_ast_setValue(E, NOWDB_AST_V_STRING, OP);
	NOWDB_SQL_ADDPARAM(E,A);
	NOWDB_SQL_ADDPARAM(E,B);
}

expr(E) ::= expr(A) GT|LT|LE|GE(OP) expr(B). {
	NOWDB_SQL_CREATEAST(&E, NOWDB_AST_OP, 2);
	nowdb_ast_setValue(E, NOWDB_AST_V_STRING, OP);
	NOWDB_SQL_ADDPARAM(E,A);
	NOWDB_SQL_ADDPARAM(E,B);
}

expr(E) ::= expr(N) IN(OP) LPAR val_list(V) RPAR. {
	NOWDB_SQL_CHECKSTATE()
	NOWDB_SQL_CREATEAST(&E, NOWDB_AST_OP, 2);
	nowdb_ast_setValue(E, NOWDB_AST_V_STRING, OP);
	NOWDB_SQL_ADDPARAM(E,N);
	NOWDB_SQL_ADDPARAM(E,V);
}

expr(E) ::= CASE case(C) END. {
	E=C;
}

case(C) ::= WHEN(OP) expr(E1) THEN expr(E2). {
	NOWDB_SQL_CHECKSTATE()
	NOWDB_SQL_CREATEAST(&C, NOWDB_AST_OP, 2);
	nowdb_ast_setValue(C, NOWDB_AST_V_STRING, OP);
	NOWDB_SQL_ADDPARAM(C,E1);
	NOWDB_SQL_ADDPARAM(C,E2);
	nowdb_ast_t *C2;
	nowdb_ast_t *E3;
	NOWDB_SQL_CREATEAST(&C2, NOWDB_AST_OP, 1);
	nowdb_ast_setValue(C2, NOWDB_AST_V_STRING, strdup("ELSE"));
	NOWDB_SQL_CREATEAST(&E3, NOWDB_AST_VALUE, NOWDB_AST_NULL);
	NOWDB_SQL_ADDPARAM(C2,E3);
	NOWDB_SQL_ADDPARAM(C,C2);
}

case(C) ::= WHEN(OP) expr(E1) THEN expr(E2) case(C2). {
	NOWDB_SQL_CHECKSTATE()
	NOWDB_SQL_CREATEAST(&C, NOWDB_AST_OP, 3);
	nowdb_ast_setValue(C, NOWDB_AST_V_STRING, OP);
	NOWDB_SQL_ADDPARAM(C,E1);
	NOWDB_SQL_ADDPARAM(C,E2);
	NOWDB_SQL_ADDPARAM(C,C2);
} 

case(C) ::= WHEN(O1) expr(E1) THEN expr(E2) ELSE(O2) expr(E3). {
	NOWDB_SQL_CHECKSTATE()
	NOWDB_SQL_CREATEAST(&C, NOWDB_AST_OP, 2);
	nowdb_ast_setValue(C, NOWDB_AST_V_STRING, O1);
	NOWDB_SQL_ADDPARAM(C,E1);
	NOWDB_SQL_ADDPARAM(C,E2);
	nowdb_ast_t *C2;
	NOWDB_SQL_CREATEAST(&C2, NOWDB_AST_OP, 1);
	nowdb_ast_setValue(C2, NOWDB_AST_V_STRING, O2);
	NOWDB_SQL_ADDPARAM(C2,E3);
	NOWDB_SQL_ADDPARAM(C,C2);
}

expr(P) ::= fun(F). {
	P=F;
}

fun(F) ::= IDENTIFIER(I) LPAR STAR RPAR. {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&F, NOWDB_AST_FUN, 0);
	nowdb_ast_setValue(F, NOWDB_AST_V_STRING, I);
}

fun(F) ::= IDENTIFIER(I) LPAR expr_list(L) RPAR. {
	NOWDB_SQL_CREATEAST(&F, NOWDB_AST_FUN, 0);
	nowdb_ast_setValue(F, NOWDB_AST_V_STRING, I);
	NOWDB_SQL_ADDPARAM(F,L);
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

val_list(L) ::= val_list(L2) COMMA value(V). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_ADDKID(L2,V);
	L = L2;
}

/* ------------------------------------------------------------------------
 * Table list and table spec
 * ------------------------------------------------------------------------
 */
table_list(T) ::= table_spec(S). {
	T=S;
}

table_list(T) ::= table_list(L) COMMMA table_spec(S). {
	NOWDB_SQL_ADDKID(L,S);
	T=L;
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
where_clause(W) ::= WHERE expr(E). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&W, NOWDB_AST_WHERE, 0);
	NOWDB_SQL_ADDKID(W, E);
}

field_list(L) ::= field(F). {
	L=F;
}
 
field_list(L) ::= field_list(FL) COMMA field(F). {
	NOWDB_SQL_ADDKID(FL,F);
	L=FL;
}

field(F) ::= IDENTIFIER(I). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&F, NOWDB_AST_FIELD, 0);
	nowdb_ast_setValue(F, NOWDB_AST_V_STRING, I);
}
field(F) ::= ORIGIN(I). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&F, NOWDB_AST_FIELD, 0);
	nowdb_ast_setValue(F, NOWDB_AST_V_STRING, I);
}
field(F) ::= DESTINATION(I). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&F, NOWDB_AST_FIELD, 0);
	nowdb_ast_setValue(F, NOWDB_AST_V_STRING, I);
}
field(F) ::= TIMESTAMP(I). {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&F, NOWDB_AST_FIELD, 0);
	nowdb_ast_setValue(F, NOWDB_AST_V_STRING, I);
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
value(V) ::= NULL. {
	NOWDB_SQL_CHECKSTATE();
	NOWDB_SQL_CREATEAST(&V, NOWDB_AST_VALUE, NOWDB_AST_NULL);
}
