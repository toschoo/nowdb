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
	// nowdbsql_errmsg(nowdbres, "syntax error", NULL); 
	// nowdbsql_errmsg(nowdbres, "syntax error", TOKEN); 
	fprintf(stderr, "FAILED!\n");
}

%syntax_error {
	nowdbres->errcode = NOWDB_SQL_ERR_SYNTAX;
	nowdbsql_errmsg(nowdbres, "syntax error", (char*)yyminor); 
	// fprintf(stderr, "near '%s': syntax error\n", (char*)yyminor);
	// fprintf(stderr, "incomplete input\n");
}

sql ::= ddl SEMICOLON. {
	nowdbsql_state_pushDDL(nowdbres);
}
sql ::= dll SEMICOLON. {
	nowdbsql_state_pushDLL(nowdbres);
}
ddl ::= CREATE target. {
	nowdbsql_state_pushCreate(nowdbres);
}
ddl ::= CREATE context_target. {
	nowdbsql_state_pushCreate(nowdbres);
}
ddl ::= CREATE context_target context_spec. {
	nowdbsql_state_pushCreate(nowdbres);
}
ddl ::= CREATE context_target SET context_options. {
	nowdbsql_state_pushCreate(nowdbres);
}

ddl ::= DROP target. {
	nowdbsql_state_pushDrop(nowdbres);
}

ddl ::= DROP context_target. {
	nowdbsql_state_pushDrop(nowdbres);
}

ddl ::= ALTER context_only SET context_options. {
	nowdbsql_state_pushAlter(nowdbres);
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

context_option ::= ALLOCSIZE EQUAL INTEGER(I). {
	nowdbsql_state_pushOption(nowdbres, NOWDB_SQL_ALLOCSIZE, I);
}
context_option ::= LARGESIZE EQUAL INTEGER(I). {
	nowdbsql_state_pushOption(nowdbres, NOWDB_SQL_LARGESIZE, I);
}
context_option ::= SORTERS EQUAL INTEGER(I). {
	nowdbsql_state_pushOption(nowdbres, NOWDB_SQL_SORTERS, I);
}
context_option ::= COMPRESSION EQUAL STRING(I). {
	nowdbsql_state_pushOption(nowdbres, NOWDB_SQL_COMPRESSION, I);
}
context_option ::= ENCRYPTION EQUAL STRING(I). {
	nowdbsql_state_pushOption(nowdbres, NOWDB_SQL_ENCRYPTION, I);
}

context_spec ::= with_throughput with_disk without_comp without_sort.
context_spec ::= with_throughput with_disk.
context_spec ::= with_throughput.
context_spec ::= with_disk.
context_spec ::= without_comp.
context_spec ::= without_sort.
context_spec ::= without_comp without_sort.
context_spec ::= with_throughput without_comp without_sort.
context_spec ::= with_throughput without_sort.
context_spec ::= with_throughput without_comp.
context_spec ::= with_disk without_comp without_sort.
context_spec ::= with_disk without_comp.
context_spec ::= with_disk without_sort.

sizing ::= TINY. {
	nowdbsql_state_pushSizing(nowdbres, NOWDB_SQL_TINY);
}
sizing ::= SMALL. {
	nowdbsql_state_pushSizing(nowdbres, NOWDB_SQL_SMALL);
}
sizing ::= MEDIUM. {
	nowdbsql_state_pushSizing(nowdbres, NOWDB_SQL_MEDIUM);
}
sizing ::= BIG. {
	nowdbsql_state_pushSizing(nowdbres, NOWDB_SQL_BIG);
}
sizing ::= LARGE. {
	nowdbsql_state_pushSizing(nowdbres, NOWDB_SQL_LARGE);
}
sizing ::= HUGE. {
	nowdbsql_state_pushSizing(nowdbres, NOWDB_SQL_HUGE);
}

with_throughput ::= WITH stress_spec THROUGHPUT.
with_disk ::= ON disk_spec.

without_comp ::= WITHOUT COMPRESSION. {
	nowdbsql_state_pushNocomp(nowdbres);
}
without_sort ::= WITHOUT SORTING. {
	nowdbsql_state_pushNosort(nowdbres);
}
disk_spec ::= HDD. {
	nowdbsql_state_pushDisk(nowdbres, NOWDB_SQL_HDD);
}
disk_spec ::= SSD. {
	nowdbsql_state_pushDisk(nowdbres, NOWDB_SQL_SSD);
}
disk_spec ::= RAID. {
	nowdbsql_state_pushDisk(nowdbres, NOWDB_SQL_RAID);
}
stress_spec ::= MODERATE. {
	nowdbsql_state_pushThroughput(nowdbres, NOWDB_SQL_MODERATE);
}
stress_spec ::= STRESS. {
	nowdbsql_state_pushThroughput(nowdbres, NOWDB_SQL_STRESS);
}
stress_spec ::= INSANE. {
	nowdbsql_state_pushThroughput(nowdbres, NOWDB_SQL_INSANE);
}
dll ::= LOAD STRING(S) INTO dml_target. {
	nowdbsql_state_pushLoad(nowdbres, S);
}
dll ::= LOAD STRING(S) INTO dml_target ignore_header. {
	nowdbsql_state_pushLoad(nowdbres, S);
}
ignore_header ::= IGNORE HEADER. {
	nowdbsql_state_pushOption(nowdbres, NOWDB_SQL_IGNORE, NULL);
}

dml_target ::= IDENTIFIER(I). {
	nowdbsql_state_pushContext(nowdbres, I);
}
dml_target ::= VERTEX. {
	nowdbsql_state_pushVertex(nowdbres);
}
