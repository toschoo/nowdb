#ifndef nowdbsql_parser_decl
#define nowdbsql_parser_decl

#include <nowdb/query/ast.h>
#include <nowdb/sql/state.h>
#include <nowdb/sql/lex.h>
#include <nowdb/sql/nowdbsql.h>

#include <stdio.h>

typedef struct {
	void            *lp; /* lemon parser  */
	yyscan_t         sc; /* scanner       */
	nowdbsql_state_t st; /* parser state  */
	FILE            *fd; /* input file    */
	char        *errmsg; /* error message */
} nowdbsql_parser_t;

int nowdbsql_parser_init(nowdbsql_parser_t *p, FILE *fd);
void nowdbsql_parser_destroy(nowdbsql_parser_t *p);

const char *nowdbsql_parser_errmsg(nowdbsql_parser_t *p);

int nowdbsql_parser_run(nowdbsql_parser_t *p,
                        nowdb_ast_t    **ast);

int nowdbsql_parser_buffer(nowdbsql_parser_t *p, char *buf, int size);

#endif

