/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Simple tool to interact with scope using SQL
 * ========================================================================
 */
#include <nowdb/scope/scope.h>
#include <nowdb/sql/ast.h>
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
	fprintf(stderr, "global_typed: present typed version of untyped edges\n");
	fprintf(stderr, "global_timing: timing information\n");
}

/* -----------------------------------------------------------------------
 * options
 * -------
 * -----------------------------------------------------------------------
 */
int global_count = 0;
char global_timing = 0;
char global_typed = 0;
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

	global_typed = ts_algo_args_findBool(
	            argc, argv, 2, "typed", 0, &err);
	if (err != 0) {
		fprintf(stderr, "command line error: %d\n", err);
		return -1;
	}

	global_timing = ts_algo_args_findBool(
	            argc, argv, 2, "timing", 0, &err);
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
 * Print rows
 * -----------------------------------------------------------------------
 */
void printRow(char *buf, uint32_t sz) {
	// fprintf(stderr, "printing row\n");
	nowdb_row_print(buf, sz, stdout);
}

/* -----------------------------------------------------------------------
 * Save a lot of code
 * -----------------------------------------------------------------------
 */
#define HANDLEERR(s) \
	if (err != NOWDB_OK) { \
		fprintf(stderr, "%s:\n", s); \
		nowdb_err_print(err); \
		nowdb_err_release(err); \
		return; \
	}

/* -----------------------------------------------------------------------
 * Print edges typed
 * -----------------------------------------------------------------------
 */
void printTypedEdge(nowdb_edge_t *buf, uint32_t sz) {
	char tmp[32];
	nowdb_model_t *m;
	nowdb_text_t  *t;
	nowdb_model_edge_t *edge=NULL;
	nowdb_model_vertex_t *origin=NULL;
	nowdb_model_vertex_t *destin=NULL;
	nowdb_type_t typ;
	nowdb_err_t err;
	char *str;
	void *w;

	m = global_scope->model;
	t = global_scope->text;
	for(uint32_t i=0; i<sz; i++) {
		if (edge == NULL || edge->edgeid != buf[i].edge) {
			err = nowdb_model_getEdgeById(m,
			             buf[i].edge, &edge);
			HANDLEERR("unknown edge");
			err = nowdb_model_getVertexById(m,
			            edge->origin, &origin);
			HANDLEERR("unknown vertex (origin)");
			err = nowdb_model_getVertexById(m,
			            edge->destin, &destin);
			HANDLEERR("unknown vertex (destin)");
		}
		fprintf(stdout, "%s;", edge->name);
		if (origin->vid == NOWDB_MODEL_TEXT) {
			err = nowdb_text_getText(t, buf[i].origin, &str);
			HANDLEERR("unknown text (origin)");
			fprintf(stdout, "%s;", str); free(str);
		} else {
			fprintf(stdout, "%lu;", buf[i].origin);
		}
		if (destin->vid == NOWDB_MODEL_TEXT) {
			err = nowdb_text_getText(t, buf[i].destin, &str);
			HANDLEERR("unknown text (destin)");
			fprintf(stdout, "%s;", str); free(str);
		} else {
			fprintf(stdout, "%lu;", buf[i].destin);
		}
		if (edge->label == NOWDB_MODEL_TEXT) {
			err = nowdb_text_getText(t, buf[i].label, &str);
			HANDLEERR("unknown text (label)");
			fprintf(stdout, "%s;", str); free(str);
		} else {
			fprintf(stdout, "%lu;", buf[i].label);
		}
		err = nowdb_time_toString(buf[i].timestamp,
		                          NOWDB_TIME_FORMAT,
		                          tmp, 32);
		HANDLEERR("cannot convert timestamp");
		fprintf(stdout, "%s;", tmp);

		for(int z=0;z<2;z++) {
			if (z == 0) {
				typ = buf[i].wtype[0];
				w = &buf[i].weight;
			} else {
				typ = buf[i].wtype[1];
				w = &buf[i].weight2;
			}
			switch(typ) {
			case NOWDB_TYP_TEXT:
				err = nowdb_text_getText(t,
				       *(uint64_t*)w, &str);
				HANDLEERR("unknown text (weight)");
				fprintf(stdout, "%s;", str); 
				free(str); break;
			case NOWDB_TYP_TIME:
			case NOWDB_TYP_DATE:
				err = nowdb_time_toString(*(int64_t*)w,
				            NOWDB_TIME_FORMAT, tmp, 32);
				HANDLEERR("cannot convert time (weight)");
				fprintf(stdout, "%s;", tmp); break;
			case NOWDB_TYP_UINT:
				fprintf(stdout, "%lu;", *(uint64_t*)w); break;
			case NOWDB_TYP_INT:
				fprintf(stdout, "%ld;", *(int64_t*)w); break;
			case NOWDB_TYP_FLOAT:
				fprintf(stdout, "%.4f;", *(double*)w); break;
			default:
				fprintf(stdout, ";");

			}
		}
		fprintf(stdout, "\n");
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

#define BUFSIZE 0x10000
char buf[BUFSIZE];

/* -----------------------------------------------------------------------
 * Process Cursor
 * -----------------------------------------------------------------------
 */
int processCursor(nowdb_cursor_t *cur) {
	uint32_t osz;
	uint32_t cnt;
	uint64_t total=0;
	char eof=0;
	nowdb_err_t err=NOWDB_OK;

	struct timespec t1, t2;

	err = nowdb_cursor_open(cur);
	if (err != NOWDB_OK) {
		if (err->errcode == nowdb_err_eof) {
			fprintf(stderr, "Read: 0\n");
			nowdb_err_release(err);
			err = NOWDB_OK;
		}
		goto cleanup;
	}

	for(;;) {
		nowdb_timestamp(&t1);
		err = nowdb_cursor_fetch(cur, buf, BUFSIZE, &osz, &cnt);
		if (err != NOWDB_OK) {
			if (err->errcode == nowdb_err_eof) {
				nowdb_err_release(err);
				err = NOWDB_OK;
				eof = 1;
				if (cnt == 0) break;
			} else break;
		}
		nowdb_timestamp(&t2);
		/*
		fprintf(stderr, "FETCH (%u): %ldus\n",
		 cnt, nowdb_time_minus(&t2, &t1)/1000);
		*/
		total += cnt;
		// fprintf(stderr, "%lu\n", total);
		if (cur->recsize == 32) {
			if (cur->row != NULL && cur->hasid) {
				printRow(buf, osz);
			} else {
				printVertex((nowdb_vertex_t*)buf, osz/32);
			}
		} else if (cur->recsize == 64) {
			if (global_typed) {
				printTypedEdge((nowdb_edge_t*)buf, osz/64);
			} else if (cur->row != NULL && cur->hasid) {
				printRow(buf, osz);
			} else {
				printEdge((nowdb_edge_t*)buf, osz/64);
			}
		}
		if (eof) break;
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
	struct timespec t1, t2;

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
		timestamp(&t1);

		rc = handleAst(path, ast);
		if (rc != 0) {
			nowdb_ast_destroy(ast); free(ast);
			fprintf(stderr, "cannot handle ast\n"); break;
		}
		nowdb_ast_destroy(ast); free(ast); ast = NULL;

		timestamp(&t2);
		if (global_timing) {
			fprintf(stderr, "overall: %luus\n",
			   nowdb_time_minus(&t2, &t1)/1000);
		}
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
	if (!nowdb_init()) {
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
	nowdb_close();
	return rc;
}
