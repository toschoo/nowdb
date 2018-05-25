/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tool to interact with scope using SQL
 * ========================================================================
 */
#include <nowdb/scope/scope.h>
#include <nowdb/query/ast.h>
#include <nowdb/query/stmt.h>
#include <nowdb/query/cursor.h>
#include <nowdb/sql/parser.h>
#include <nowdb/io/dir.h>
#include <common/progress.h>
#include <common/cmd.h>
#include <common/bench.h>

#include <stdlib.h>
#include <stdio.h>

/* -----------------------------------------------------------------------
 * get a little help for my friends
 * -----------------------------------------------------------------------
 */
void helptxt(char *progname) {
	fprintf(stderr, "%s <path-to-base> [options]\n", progname);
	fprintf(stderr, "all options are in the format -opt value\n");
}

/* -----------------------------------------------------------------------
 * options
 * -------
 * global_count: how many edges we will insert
 * global_report: report back every n edges
 * global_context: the context in which to insert
 * global_nocomp: don't compress
 * global_nosort: don't sort
 * -----------------------------------------------------------------------
 */
int global_count = 0;
nowdb_scope_t *global_scope = NULL;

/* -----------------------------------------------------------------------
 * get options
 * -----------------------------------------------------------------------
 */
int parsecmd(int argc, char **argv) {
	int err = 0;

	global_count = ts_algo_args_findUint(
	            argc, argv, 2, "count", 1000, &err);
	if (err != 0) {
		fprintf(stderr, "command line error: %d\n", err);
		return -1;
	}
	return 0;
}

/* -----------------------------------------------------------------------
 * Print vertices
 * -----------------------------------------------------------------------
 */
void printVertex(nowdb_vertex_t *buf, uint32_t sz) {
	for(uint32_t i=0; i<sz; i++) {
		fprintf(stdout, "%lu(%u).%lu: %lu (%u)\n",
		                 buf[i].vertex, buf[i].role,
		                 buf[i].property,
		                 buf[i].value, buf[i].vtype);
	}
}

/* -----------------------------------------------------------------------
 * Print edges
 * -----------------------------------------------------------------------
 */
void printEdge(nowdb_edge_t *buf, uint32_t sz) {
	for(uint32_t i=0; i<sz; i++) {
		fprintf(stdout, "%lu -[%lu]-> %lu #%lu @%ld: ",
		                buf[i].origin, buf[i].edge, buf[i].destin,
		                buf[i].label, buf[i].timestamp);
		fprintf(stdout, "%lu (%u), %lu (%u)\n",
		                buf[i].weight, buf[i].wtype[0],
		                buf[i].weight2, buf[i].wtype[1]);
	}
}

/* -----------------------------------------------------------------------
 * Print report
 * -----------------------------------------------------------------------
 */
int printReport(nowdb_qry_result_t *res) {
	nowdb_qry_report_t *rep;
	if (res->result == NULL) {
		fprintf(stderr, "NO REPORT!\n");
		return -1;
	}
	rep = res->result;
	fprintf(stderr, "%lu rows loaded with %lu errors in %ldus\n",
	                rep->affected,
	                rep->errors,
	                rep->runtime);
	free(res->result); res->result = NULL;
	return 0;
}

char buf[8192];

/* -----------------------------------------------------------------------
 * Process Cursor
 * -----------------------------------------------------------------------
 */
int processCursor(nowdb_cursor_t *cur) {
	uint32_t osz;
	uint64_t total=0;
	nowdb_err_t err=NOWDB_OK;

	err = nowdb_cursor_open(cur);
	if (err != NOWDB_OK) {
		if (err->errcode == nowdb_err_eof) {
			nowdb_err_release(err);
			err = NOWDB_OK;
		}
		goto cleanup;
	}

	for(;;) {
		err = nowdb_cursor_fetch(cur, buf, 8192, &osz);
		if (err != NOWDB_OK) {
			if (err->errcode == nowdb_err_eof) {
				nowdb_err_release(err);
				err = NOWDB_OK;
			}
			break;
		}
		total += osz/cur->recsize;
		if (cur->recsize == 32) {
			printVertex((nowdb_vertex_t*)buf, osz/32);
		} else if (cur->recsize == 64) {
			printEdge((nowdb_edge_t*)buf, osz/64);
		}
	}
	fprintf(stderr, "Read: %lu\n", total);

cleanup:
	nowdb_cursor_destroy(cur); free(cur);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

/* -----------------------------------------------------------------------
 * Handle ast
 * -----------------------------------------------------------------------
 */
int handleAst(nowdb_path_t path, nowdb_ast_t *ast) {
	nowdb_err_t err;
	nowdb_qry_result_t res;

	err = nowdb_stmt_handle(ast, global_scope, NULL, path, &res);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot handle ast: \n");
		nowdb_ast_show(ast);
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	switch(res.resType) {
	case NOWDB_QRY_RESULT_NOTHING: break;
	case NOWDB_QRY_RESULT_REPORT: return printReport(&res);
	case NOWDB_QRY_RESULT_SCOPE: global_scope = res.result; break;
	case NOWDB_QRY_RESULT_CURSOR: return processCursor(res.result);

	case NOWDB_QRY_RESULT_PLAN:
		fprintf(stderr, 
		"I have a plan, but no plan what to do with it\n");
		return -1;
	default:
		fprintf(stderr, "unknown result :-(\n");
		return -1;
	}
	return 0;
}

/* -----------------------------------------------------------------------
 * Process queries
 * -----------------------------------------------------------------------
 */
int queries(nowdb_path_t path, FILE *stream) {
	int rc = 0;
	nowdbsql_parser_t parser;
	nowdb_ast_t       *ast;

	if (nowdbsql_parser_init(&parser, stream) != 0) {
		fprintf(stderr, "cannot init parser\n");
		return -1;
	}

	for(;;) {
		rc = nowdbsql_parser_run(&parser, &ast);
		if (rc == NOWDB_SQL_ERR_EOF) {
			rc = 0; break;
		}
		if (rc != 0) {
			fprintf(stderr, "parsing error\n");
			break;
		}
		if (ast == NULL) {
			fprintf(stderr, "no error, no ast :-(\n");
			rc = NOWDB_SQL_ERR_UNKNOWN; break;
		}
		rc = handleAst(path, ast);
		if (rc != 0) {
			nowdb_ast_destroy(ast); free(ast);
			fprintf(stderr, "cannot handle ast\n"); break;
		}
		nowdb_ast_destroy(ast); free(ast); ast = NULL;
	}
	if (rc != 0) {
		fprintf(stderr, "ERROR: %s\n",
		nowdbsql_parser_errmsg(&parser));
	}
	nowdbsql_parser_destroy(&parser);
	return rc;
}

/* -----------------------------------------------------------------------
 * main
 * -----------------------------------------------------------------------
 */
int main(int argc, char **argv) {
	int rc = EXIT_SUCCESS;
	nowdb_err_t      err;
	nowdb_path_t path;

	if (argc < 2) {
		helptxt(argv[0]);
		return EXIT_FAILURE;
	}
	path = argv[1];
	if (strnlen(path, 4096) == 4096) {
		fprintf(stderr, "path too long (max: 4096)\n");
		return EXIT_FAILURE;
	}
	if (path[0] == '-') {
		fprintf(stderr, "invalid path\n");
		helptxt(argv[0]);
		return EXIT_FAILURE;
	}
	if (parsecmd(argc, argv) != 0) {
		helptxt(argv[0]);
		return EXIT_FAILURE;
	}
	if (!nowdb_err_init()) {
		fprintf(stderr, "cannot init library\n");
		return EXIT_FAILURE;
	}
	if (queries(path, stdin) != 0) {
		fprintf(stderr, "cannot run queries\n");
		rc = EXIT_FAILURE; goto cleanup;
	}

cleanup:
	if (global_scope != NULL) {
		err = nowdb_scope_close(global_scope);
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot close scope\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
		}
		nowdb_scope_destroy(global_scope); free(global_scope);
	}
	nowdb_err_destroy();
	return rc;
}
