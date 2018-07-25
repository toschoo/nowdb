/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Advanced testing with scopes
 * ========================================================================
 */
#include <common/scopes.h>

#include <nowdb/store/storewrk.h>
#include <nowdb/scope/scope.h>
#include <nowdb/sql/ast.h>
#include <nowdb/query/stmt.h>
#include <nowdb/query/cursor.h>
#include <nowdb/sql/parser.h>
#include <nowdb/io/dir.h>
#include <common/progress.h>
#include <common/bench.h>

#include <stdlib.h>
#include <stdio.h>

static nowdbsql_parser_t _parser;

int initScopes() {
	if (nowdbsql_parser_init(&_parser, stdin) != 0) {
		fprintf(stderr, "cannot init parser\n");
		return -1;
	}
	return 0;
}

void closeScopes() {
	nowdbsql_parser_destroy(&_parser);
}

int doesExistScope(nowdb_path_t path) {
	return nowdb_path_exists(path, NOWDB_DIR_TYPE_DIR);
}

nowdb_scope_t *mkScope(nowdb_path_t path) {
	nowdb_err_t err;
	nowdb_scope_t *scope;

	err = nowdb_scope_new(&scope, path, 1);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot allocate scope\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return NULL;
	}
	return scope;
}

nowdb_scope_t *createScope(nowdb_path_t path) {
	nowdb_err_t err;
	nowdb_scope_t *scope;

	scope = mkScope(path);
	if (scope == NULL) return NULL;

	fprintf(stderr, "creating %s\n", path);

	err = nowdb_scope_create(scope);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_scope_destroy(scope); free(scope);
		return NULL;
	}
	return scope;
}

int dropScope(nowdb_path_t path) {
	nowdb_scope_t *scope;
	nowdb_err_t err;

	fprintf(stderr, "dropping %s\n", path);
	scope = mkScope(path);
	if (scope == NULL) return -1;
	
	err = nowdb_scope_drop(scope);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_scope_destroy(scope);
		free(scope);
		return -1;
	}
	nowdb_scope_destroy(scope);
	free(scope);
	return 0;
}

int openScope(nowdb_scope_t *scope) {
	nowdb_err_t err;

	fprintf(stderr, "opening %s\n", scope->path);
	err = nowdb_scope_open(scope);
	if (err != NOWDB_OK) {
		nowdb_err_print(err); nowdb_err_release(err);
		return -1;
	}
	return 0;
}

int closeScope(nowdb_scope_t *scope) {
	nowdb_err_t err;

	fprintf(stderr, "closing %s\n", scope->path);
	err = nowdb_scope_close(scope);
	if (err != NOWDB_OK) {
		nowdb_err_print(err); nowdb_err_release(err);
		return -1;
	}
	return 0;
}

static int handleAst(nowdb_scope_t    *scope,
                     nowdb_ast_t        *ast,
                     nowdb_qry_result_t *res) {
	nowdb_err_t err;

	err = nowdb_stmt_handle(ast, scope, NULL, NULL, res);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot handle ast: \n");
		nowdb_ast_show(ast);
		nowdb_err_print(err);
		nowdb_err_release(err);
		return -1;
	}
	return 0;
}

static nowdb_ast_t *parseStmt(char *stmt) {
	nowdb_ast_t *ast;
	int rc;

	rc = nowdbsql_parser_buffer(&_parser, stmt, strlen(stmt), &ast);
	if (rc == NOWDB_SQL_ERR_EOF) return ast;
	if (rc != 0) {
		fprintf(stderr, "parsing error: %s\n",
		     nowdbsql_parser_errmsg(&_parser));
		return NULL;
	}
	if (ast == NULL) {
		fprintf(stderr, "no error, no ast :-(\n");
		return NULL;
	}
	return ast;
}

/* -----------------------------------------------------------------------
 * Print report
 * -----------------------------------------------------------------------
 */
static int printReport(nowdb_qry_result_t *res) {
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

static int handleStmt(nowdb_scope_t    *scope,
                      char              *stmt,
                      nowdb_qry_result_t *res) {
	nowdb_ast_t *ast;
	int rc;

	ast = parseStmt(stmt);
	if (ast == NULL) return -1;

	rc = handleAst(scope, ast, res);
	nowdb_ast_destroy(ast); free(ast);
	if (rc != 0) return -1;

	return 0;
}

int execStmt(nowdb_scope_t *scope,
             char           *stmt) {
	nowdb_qry_result_t res;

	if (handleStmt(scope, stmt, &res) != 0) return -1;

	switch(res.resType) {

	case NOWDB_QRY_RESULT_NOTHING: return 0;
	case NOWDB_QRY_RESULT_REPORT: return printReport(&res);

	default:
		fprintf(stderr, "unexpected result :-(\n");
		return -1;
	}
	return 0;
}

nowdb_cursor_t *openCursor(nowdb_scope_t *scope, char *stmt) {
	nowdb_qry_result_t res;
	nowdb_err_t err;

	if (handleStmt(scope, stmt, &res) != 0) return NULL;

	switch(res.resType) {

	case NOWDB_QRY_RESULT_CURSOR: 
		if (res.result == NULL) return NULL;
		err = nowdb_cursor_open(res.result);
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot open cursor\n");
			nowdb_err_print(err); nowdb_err_release(err);
			nowdb_cursor_destroy(res.result); free(res.result);
			return NULL;
		}
		return res.result;

	default:
		fprintf(stderr, "unexpected result :-(\n");
		return NULL;
	}
	return NULL;
}

void closeCursor(nowdb_cursor_t *cur) {
	if (cur == NULL) return;
	nowdb_cursor_destroy(cur);
	free(cur);
}

#define DELAY 250000000

int waitscope(nowdb_scope_t *scope, char *context) {
	nowdb_context_t *ctx;
	nowdb_err_t err;
	int rc = 0;
	int mx = 100;
	int len;

	for(int m=0; m<mx; m++) {
		err = nowdb_scope_getContext(scope, context, &ctx);
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot get context '%s'\n",
			                                   context);
			nowdb_err_print(err);
			nowdb_err_release(err);
			rc = -1; break;
		}

		err = nowdb_lock_read(&ctx->store.lock);
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot lock store\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			rc = -1; break;
		}
		len = ctx->store.waiting.len;
		err = nowdb_unlock_read(&ctx->store.lock);
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot unlock store\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			rc = -1; break;
		}
		if (len == 0) break;
		fprintf(stdout, "\b\b\b\b\b\b\b\b");
		fprintf(stdout, "%08d", len);
		fflush(stdout);
		for(int i=1;i<len;i+=2) {
			err = nowdb_store_sortNow(&ctx->store.sortwrk);
			if (err != NOWDB_OK) {
				fprintf(stderr,
				"\ncannot send sort message\n");
				nowdb_err_print(err);
				nowdb_err_release(err);
				rc = -1; break;
			}
		}
		err = nowdb_task_sleep(DELAY);
		if (err != NOWDB_OK) {
			fprintf(stderr, "\ncannot sleep\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			rc = -1; break;
		}
	}
	fprintf(stdout, "\n");
	return rc;
}
