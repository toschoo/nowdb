/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * SQL Smoke:
 * parses an sql statement passed in through stdin
 * creates a plan from the ast
 * and prints it
 * ========================================================================
 */
#include <nowdb/sql/ast.h>
#include <nowdb/sql/parser.h>
#include <nowdb/qplan/plan.h>
#include <nowdb/scope/scope.h>

#include <tsalgo/list.h>

#include <stdio.h>
#include <stdlib.h>

nowdb_scope_t *global_scope=NULL;

int main() {
	ts_algo_list_t plan;
	nowdbsql_parser_t parser;
	nowdb_ast_t *ast;
	int rc, r=EXIT_SUCCESS;
	nowdb_err_t err;

	if (!nowdb_init()) {
		fprintf(stderr, "cannot init library\n");
		return EXIT_FAILURE;
	}
	err = nowdb_scope_new(&global_scope, "/opt/dbs/retail", 1);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot init scope\n");
		nowdb_close();
		return EXIT_FAILURE;
	}
	err = nowdb_scope_open(global_scope);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot open scope\n");
		nowdb_scope_destroy(global_scope); free(global_scope);
		nowdb_close();
		return EXIT_FAILURE;
	}
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
		ts_algo_list_init(&plan);
		err = nowdb_plan_fromAst(global_scope, ast, &plan);
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot create plan\n");
			nowdb_ast_destroy(ast);
			free(ast); ast = NULL;
			nowdb_err_print(err); nowdb_err_release(err);
			r=EXIT_FAILURE; break;
		}
		nowdb_plan_show(&plan, stderr);
		nowdb_plan_destroy(&plan,1);
		ts_algo_list_destroy(&plan);
		nowdb_ast_destroy(ast);
		free(ast); ast = NULL;
	}
	if (rc != 0) {
		fprintf(stderr, "ERROR: %s\n",
		        nowdbsql_parser_errmsg(&parser));
		r = EXIT_FAILURE;
	}
	nowdbsql_parser_destroy(&parser);
	if (global_scope != NULL) {
		err = nowdb_scope_close(global_scope);
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot close scope\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
		}
		nowdb_scope_destroy(global_scope); free(global_scope);
	}
	nowdb_close();
	return r;
}
