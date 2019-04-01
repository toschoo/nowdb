#include <nowdb/nowclient.h>
#include <lauxlib.h>
#include <lualib.h>
#include <stdlib.h> 
#include <string.h> 
#include <stdio.h> 
#include <stdint.h> 

#define PUSHERR(x,m) \
	lua_pushinteger(lu, x); \
	lua_pushstring(lu, m);

#define EXITERR(x,m) \
	PUSHERR(x,m); \
	return 2;

#define CONNECTION() \
	if (!lua_isinteger(lu, 1)) { \
		EXITERR(NOWDB_ERR_INVALID, "not a valid connection"); \
	} \
	nowdb_con_t c = (nowdb_con_t)lua_tointeger(lu,1); \
	if (c == NULL) { \
		EXITERR(NOWDB_ERR_INVALID, "not a valid connection"); \
	}

#define RESULT() \
	if (!lua_isinteger(lu, 1)) { \
		EXITERR(NOWDB_ERR_INVALID, "not a valid result"); \
	} \
	nowdb_result_t r = (nowdb_result_t)lua_tointeger(lu,1); \
	if (r == NULL) { \
		EXITERR(NOWDB_ERR_INVALID, "not a valid result"); \
	}

#define STRING(n) \
	if (!lua_isstring(lu, n)) { \
		EXITERR(NOWDB_ERR_INVALID, "not a string"); \
	} \
	char *str = strdup(lua_tostring(lu,n)); \
	if (str == NULL) { \
		EXITERR(NOWDB_ERR_NOMEM, "cannot duplicate string"); \
	}

#define INTEGER(n) \
	if (!lua_isinteger(lu, n)) { \
		EXITERR(NOWDB_ERR_INVALID, "not an integer"); \
	} \
	int64_t i = lua_tointeger(lu,n);

/* ------------------------------------------------------------------------
 * connect:
 * - srv, port, user, pwd: string
 * - return: 
 *   ok: err code
 *   connection: integer (if ok) or
 *   errmessage: string 
 * ------------------------------------------------------------------------
 */
static int connect(lua_State *lu) {
	char *srv  = NULL;
	char *port = NULL;
	nowdb_con_t con;
	int rc;

	if (!lua_isstring(lu, 1)) {
		EXITERR(NOWDB_ERR_INVALID, "server is not a string");
	}
	srv = strdup(lua_tostring(lu,1));
	if (srv == NULL) {
		EXITERR(NOWDB_ERR_NOMEM, "cannot duplicate string");
	}
	if (!lua_isstring(lu, 2)) {
		EXITERR(NOWDB_ERR_INVALID, "port is not a string");
	}
	port = strdup(lua_tostring(lu,2));
	if (port == NULL) {
		free(srv);
		EXITERR(NOWDB_ERR_NOMEM, "cannot duplicate string");
	}

	// ignore usr and pwd

	rc = nowdb_connect(&con, srv, port, NULL, NULL, 0);
	free(srv); free(port);
	if (rc != NOWDB_OK) {
		EXITERR(rc, "cannot connect");
	}

	lua_pushinteger(lu, NOWDB_OK);
	lua_pushinteger(lu, (uint64_t)con);

	return 2;
}

static int close(lua_State *lu) {

	CONNECTION()

	int rc = nowdb_connection_close(c);
	if (rc != 0) {
		nowdb_connection_destroy(c);
	}
	return 0;
}

static int errcode(lua_State *lu) {

	RESULT();

	int rc = nowdb_result_errcode(r);
	lua_pushinteger(lu, (int64_t)rc);

	return 1;
}

static int errdetails(lua_State *lu) {

	RESULT();

	const char *msg = nowdb_result_details(r);
	lua_pushstring(lu, msg);

	return 1;
}

static int rtype(lua_State *lu) {

	RESULT()

	int t = nowdb_result_type(r);
	lua_pushinteger(lu, t);

	return 1;
}

static int stoplib(lua_State *lu) {
	fprintf(stderr, "unloading\n");
	nowdb_client_close();
	return 0;
}

static int execute(lua_State *lu) {
	char *stmt = NULL;
	nowdb_result_t res;
	int rc;

	CONNECTION();
	STRING(2); stmt = str;

	rc = nowdb_exec_statement(c, stmt, &res); free(stmt);
	if (rc != NOWDB_OK) {
		EXITERR(rc, nowdb_err_explain(rc));
	}
	lua_pushinteger(lu, NOWDB_OK);
	lua_pushinteger(lu, (uint64_t)res);

	return 2;
}

static int release(lua_State *lu) {
	int rc = -1;

	RESULT();

	if (nowdb_result_type(r) == NOWDB_RESULT_CURSOR) {
		rc = nowdb_cursor_close((nowdb_cursor_t)r);
	}
	if (rc != NOWDB_OK) {
		nowdb_result_destroy(r);
	}
	return 0;
}

static int curid(lua_State *lu) {

	RESULT();

	lua_pushinteger(lu, nowdb_cursor_id((nowdb_cursor_t)r));
	return 1;
}

static int fetch(lua_State *lu) {
	int rc;

	RESULT();

	rc = nowdb_cursor_fetch((nowdb_cursor_t)r);
	if (rc != NOWDB_OK) {
		EXITERR(rc, nowdb_err_explain(rc));
	}
	lua_pushinteger(lu, NOWDB_OK);
	return 1;
}

static int getrow(lua_State *lu) {

	RESULT();

	nowdb_row_t row = nowdb_cursor_row((nowdb_cursor_t)r);
	lua_pushinteger(lu, (uint64_t)row);
	return 1;
}

static int nextrow(lua_State *lu) {
	int rc;

	RESULT();

	rc = nowdb_row_next((nowdb_row_t)r);
	lua_pushinteger(lu, (int64_t)rc);
	return 1;
}

static int countfields(lua_State *lu) {
	int rc;

	RESULT();

	rc = nowdb_row_count((nowdb_row_t)r);
	lua_pushinteger(lu, (int64_t)rc);
	return 1;
}

static int field(lua_State *lu) {
	int   t;
	void *v;

	RESULT();
	INTEGER(2);

	v = nowdb_row_field((nowdb_row_t)r, (int)i, &t);
	lua_pushinteger(lu, (int64_t)t);

	switch(t) {
	case NOWDB_TYP_TEXT:
		lua_pushstring(lu, (char*)v); break;

	case NOWDB_TYP_TIME:
	case NOWDB_TYP_DATE:
	case NOWDB_TYP_INT:
	case NOWDB_TYP_UINT:
		lua_pushinteger(lu, *(int64_t*)v); break;

	case NOWDB_TYP_FLOAT:
		lua_pushnumber(lu, *(double*)v); break;

	case NOWDB_TYP_BOOL:
		lua_pushboolean(lu, *(int64_t*)v); break;

	case NOWDB_TYP_NOTHING:
	default:
		lua_pushinteger(lu, 0ll);
	}

	return 2;
}

static const struct luaL_Reg luaifc[] = {
	{"cnow_connect", connect},
	{"cnow_close", close},
	// {"cnow_use", use},
	{"cnow_execute", execute},
	{"cnow_errcode", errcode},
	{"cnow_errdetails", errdetails},
	{"cnow_stop", stoplib},
	{NULL,NULL}
};

int luaopen_libnowluaifc(lua_State *lu) {
	fprintf(stderr, "loading\n");
	// nowdb_client_init();
	luaL_newlib(lu, luaifc);
	lua_register(lu, "cnow_connect", connect);
	lua_register(lu, "cnow_close", close);
	lua_register(lu, "cnow_execute", execute);
	lua_register(lu, "cnow_errcode", errcode);
	lua_register(lu, "cnow_errdetails", errdetails);
	lua_register(lu, "cnow_rtype", rtype);
	lua_register(lu, "cnow_curid", curid);
	lua_register(lu, "cnow_fetch", fetch);
	lua_register(lu, "cnow_getrow", getrow);
	lua_register(lu, "cnow_nextrow", nextrow);
	lua_register(lu, "cnow_countfields", countfields);
	lua_register(lu, "cnow_field", field);
	lua_register(lu, "cnow_release", release);
	lua_register(lu, "cnow_stop", stoplib);
	return 1;
}

