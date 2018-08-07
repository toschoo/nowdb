#include <nowdb/sql/ast.h>
#include <nowdb/sql/parser.h>
#include <common/bench.h>

#include <stdio.h>
#include <stdlib.h>

int run(nowdbsql_parser_t *parser, uint32_t *count) {
	uint32_t cnt = 0;
	nowdb_ast_t *ast;
	int rc;

	for(;;) {
		rc = nowdbsql_parser_run(parser, &ast);
		if (rc == NOWDB_SQL_ERR_EOF) {
			rc = 0; break;
		}
		if (rc != 0) break;
		if (ast == NULL) {
			fprintf(stderr, "no error / no result :-(\n");
			rc = NOWDB_SQL_ERR_UNKNOWN; break;
		}
		nowdb_ast_destroy(ast);
		free(ast); ast = NULL;
		cnt++;
	}
	if (rc != 0) {
		fprintf(stderr, "ERROR: %s\n", nowdbsql_err_desc(rc));
		return rc;
	}
	*count = cnt; return 0;
}

int main() {
	nowdbsql_parser_t parser;
	struct timespec t1, t2;
	uint32_t count;
	int rc;

	if (nowdbsql_parser_init(&parser, stdin) != 0) {
		fprintf(stderr, "cannot initialise parser\n");
		return EXIT_FAILURE;
	}

	getc(stdin);
	timestamp(&t1);
	rc = run(&parser, &count);
	timestamp(&t2);
	if (rc == 0) {
		fprintf(stdout, "Parsed %u in %ldus\n", count,
		                        minus(&t2, &t1)/1000);
	}
	nowdbsql_parser_destroy(&parser);
	if (rc == 0) return EXIT_SUCCESS;
	return EXIT_FAILURE;
}
