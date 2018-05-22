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

where_clause ::= WHERE condition. {
	nowdbsql_state_pushWhere(nowdbres);
}
where_clause ::= WHERE condition more_conditions. {
	nowdbsql_state_pushWhere(nowdbres);
}

condition ::= operand comparison operand . {
	nowdbsql_state_pushCondition(nowdbres, 0);
}
condition ::= NOT operand comparison operand . {
	nowdbsql_state_pushCondition(nowdbres, 1);
}
condition ::= LPAR operand comparison operand RPAR. {
	nowdbsql_state_pushCondition(nowdbres, 0);
}
condition ::= NOT LPAR operand comparison operand RPAR. {
	nowdbsql_state_pushCondition(nowdbres, 1);
}
 
more_conditions ::= AND condition. {
	nowdbsql_state_pushAndOr(nowdbres, NOWDB_AST_AND, 0);
}
more_conditions ::= AND condition more_conditions. {
	nowdbsql_state_pushAndOr(nowdbres, NOWDB_AST_AND, 0);
}
more_conditions ::= OR condition. {
	nowdbsql_state_pushAndOr(nowdbres, NOWDB_AST_OR, 0);
}
more_conditions ::= OR condition more_conditions. {
	nowdbsql_state_pushAndOr(nowdbres, NOWDB_AST_OR, 0);
}

more_conditions ::= AND LPAR condition more_conditions RPAR.
more_conditions ::= AND LPAR condition more_conditions RPAR more_conditions.
more_conditions ::= OR LPAR condition more_conditions RPAR.
more_conditions ::= AND NOT LPAR condition more_conditions RPAR.
more_conditions ::= OR NOT LPAR condition more_conditions RPAR.

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

comparison ::= EQ. {
	nowdbsql_state_pushComparison(nowdbres, NOWDB_SQL_EQ);
}
comparison ::= LE. {
	nowdbsql_state_pushComparison(nowdbres, NOWDB_SQL_LE);
}
comparison ::= GE. {
	nowdbsql_state_pushComparison(nowdbres, NOWDB_SQL_GE);
}
comparison ::= LT. {
	nowdbsql_state_pushComparison(nowdbres, NOWDB_SQL_LT);
}
comparison ::= GT. {
	nowdbsql_state_pushComparison(nowdbres, NOWDB_SQL_GT);
}
comparison ::= NE. {
	nowdbsql_state_pushComparison(nowdbres, NOWDB_SQL_NE);
}

operand ::= field.
operand ::= value.

field ::= IDENTIFIER(I). {
	nowdbsql_state_pushField(nowdbres, I);
}
value ::= STRING(S). {
	nowdbsql_state_pushValue(nowdbres, NOWDB_AST_V_STRING, S);
}
value ::= INTEGER(I). {
	nowdbsql_state_pushValue(nowdbres, NOWDB_AST_V_INTEGER, I);
}

