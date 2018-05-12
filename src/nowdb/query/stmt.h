#ifndef nowdb_query_stmt_decl
#define nowdb_query_stmt_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/io/dir.h>
#include <nowdb/scope/scope.h>
#include <nowdb/query/ast.h>

#define NOWDB_QRY_RESULT_NOTHING 1
#define NOWDB_QRY_RESULT_REPORT  2
#define NOWDB_QRY_RESULT_PLAN    3
#define NOWDB_QRY_RESULT_SCOPE   4

typedef struct {
	int  resType;
	void *result;
} nowdb_qry_result_t;

nowdb_err_t nowdb_stmt_handle(nowdb_ast_t *ast,
                          nowdb_scope_t *scope,
                          nowdb_path_t    base,
                      nowdb_qry_result_t *res);


#endif
