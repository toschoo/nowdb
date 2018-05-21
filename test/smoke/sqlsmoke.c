#include <nowdb/query/ast.h>
#include <nowdb/sql/parser.h>

#include <stdio.h>
#include <stdlib.h>

int main() {
	nowdbsql_parser_t parser;
	nowdb_ast_t *ast;
	int rc;

	if (nowdbsql_parser_init(&parser, stdin) != 0) {
		fprintf(stderr, "cannot initialise parser\n");
		return EXIT_FAILURE;
	}
	for(;;) {
		rc = nowdbsql_parser_run(&parser, &ast);
		if (rc == NOWDB_SQL_ERR_EOF) {
			rc = 0; break;
		}
		if (rc != 0) break;
		if (ast == NULL) {
			fprintf(stderr, "no error / no result :-(\n");
			rc = NOWDB_SQL_ERR_UNKNOWN; break;
		}
		nowdb_ast_show(ast);
		nowdb_ast_destroy(ast);
		free(ast); ast = NULL;
	}
	if (rc != 0) {
		fprintf(stderr, "ERROR: %s\n",
		        nowdbsql_parser_errmsg(&parser));
		nowdbsql_parser_destroy(&parser);
		return EXIT_FAILURE;
	}
	nowdbsql_parser_destroy(&parser);
	return EXIT_SUCCESS;
}
