/* ========================================================================
 * (c) Tobias Schoofs, 2019
 * 
 * This file is part of the NOWDB Client Library.
 *
 * The NOWDB Client Library is free software;
 * you can redistribute it and/or modify it under the terms
 * of the GNU Lesser General Public License 
 * as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * The NOWDB Client Library
 * is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with the NOWDB CLIENT Library; if not, see
 * <http://www.gnu.org/licenses/>.
 *  
 * ========================================================================
 * NOWDB Client LIBRARY
 * Lua Interface
 * ========================================================================
 * TODO: report
 * ------------------------------------------------------------------------ 
 */
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
	c = (nowdb_con_t)lua_tointeger(lu,1); \
	if (c == NULL) { \
		EXITERR(NOWDB_ERR_INVALID, "not a valid connection"); \
	}

#define RESULT() \
	if (!lua_isinteger(lu, 1)) { \
		EXITERR(NOWDB_ERR_INVALID, "not a valid result"); \
	} \
	r = (nowdb_result_t)lua_tointeger(lu,1); \
	if (r == NULL) { \
		EXITERR(NOWDB_ERR_INVALID, "not a valid result"); \
	}

#define STRING(n) \
	if (!lua_isstring(lu, n)) { \
		EXITERR(NOWDB_ERR_INVALID, "not a string"); \
	} \
	str = strdup(lua_tostring(lu,n)); \
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
	const char *srv  = NULL;
	const char *port = NULL;
	nowdb_con_t con;
	int rc;

	if (!lua_isstring(lu, 1)) {
		EXITERR(NOWDB_ERR_INVALID, "server is not a string");
	}
	srv = lua_tostring(lu,1);
	if (srv == NULL) {
		EXITERR(NOWDB_ERR_NOMEM, "cannot get string");
	}
	if (!lua_isstring(lu, 2)) {
		EXITERR(NOWDB_ERR_INVALID, "port is not a string");
	}
	port = lua_tostring(lu,2);
	if (port == NULL) {
		EXITERR(NOWDB_ERR_NOMEM, "cannot get string");
	}

	// ignore usr and pwd

	rc = nowdb_connect(&con, (char*)srv, (char*)port, NULL, NULL, 0);
	if (rc != NOWDB_OK) {
		EXITERR(rc, "cannot connect");
	}

	lua_pushinteger(lu, NOWDB_OK);
	lua_pushinteger(lu, (uint64_t)con);

	return 2;
}

/* ------------------------------------------------------------------------
 * Close connection
 * ------------------------------------------------------------------------
 */
static int close(lua_State *lu) {
        nowdb_con_t c;

	CONNECTION()

	int rc = nowdb_connection_close(c);
	if (rc != 0) {
		nowdb_connection_destroy(c);
	}
	return 0;
}

/* ------------------------------------------------------------------------
 * Error code
 * receives a result
 * returns the error code
 * ------------------------------------------------------------------------
 */
static int errcode(lua_State *lu) {
	nowdb_result_t r;

	RESULT();

	int rc = nowdb_result_errcode(r);
	lua_pushinteger(lu, (int64_t)rc);

	return 1;
}

/* ------------------------------------------------------------------------
 * Error details
 * receives a result
 * returns the error message
 * ------------------------------------------------------------------------
 */
static int errdetails(lua_State *lu) {
	nowdb_result_t r;

	RESULT();

	const char *msg = nowdb_result_details(r);
	if (msg == NULL) {
		lua_pushstring(lu, "no details available");
	} else {
		lua_pushstring(lu, msg);
	}

	return 1;
}

/* ------------------------------------------------------------------------
 * Result type
 * receives a result and returns the type
 * ------------------------------------------------------------------------
 */
static int rtype(lua_State *lu) {
	nowdb_result_t r;

	RESULT()

	int t = nowdb_result_type(r);
	lua_pushinteger(lu, t);

	return 1;
}

/* ------------------------------------------------------------------------
 * Unload library
 * ------------------------------------------------------------------------
 */
static int stoplib(lua_State *lu) {
	nowdb_client_close();
	return 0;
}

/* ------------------------------------------------------------------------
 * Execute
 * - receives a connection and a statement (string)
 * - returns either ok and a result
 * - or error and error message
 * ------------------------------------------------------------------------
 */
static int execute(lua_State *lu) {
        nowdb_con_t c;
	char *str = NULL;
	nowdb_result_t res;
	int rc;

	CONNECTION();
	STRING(2);

	rc = nowdb_exec_statement(c, str, &res);
	if (rc != NOWDB_OK) {
		EXITERR(rc, nowdb_err_explain(rc));
	}
	lua_pushinteger(lu, NOWDB_OK);
	lua_pushinteger(lu, (uint64_t)res);

	return 2;
}

/* ------------------------------------------------------------------------
 * Release Result
 * - receives a result and releases it, does not return anything
 * ------------------------------------------------------------------------
 */
static int release(lua_State *lu) {
	nowdb_result_t r;
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

/* ------------------------------------------------------------------------
 * Cursor id
 * - receives a result and returns it curid
 * ------------------------------------------------------------------------
 */
static int curid(lua_State *lu) {
	nowdb_result_t r;

	RESULT();

	lua_pushinteger(lu, nowdb_cursor_id((nowdb_cursor_t)r));
	return 1;
}

/* ------------------------------------------------------------------------
 * Fetch
 * - receives a cursor and returns either ok or errcode and error message
 * ------------------------------------------------------------------------
 */
static int fetch(lua_State *lu) {
	nowdb_result_t r;
	int rc;

	RESULT();

	rc = nowdb_cursor_fetch((nowdb_cursor_t)r);
	if (rc != NOWDB_OK) {
		EXITERR(rc, nowdb_err_explain(rc));
	}
	lua_pushinteger(lu, NOWDB_OK);
	return 1;
}

/* ------------------------------------------------------------------------
 * Getrow
 * - receives a cursor and returns its row (NULL if no row is available)
 * ------------------------------------------------------------------------
 */
static int getrow(lua_State *lu) {
	nowdb_result_t r;

	RESULT();

	nowdb_row_t row = nowdb_cursor_row((nowdb_cursor_t)r);
	lua_pushinteger(lu, (uint64_t)row);
	return 1;
}

/* ------------------------------------------------------------------------
 * Nextrow
 * - receives a row and returns the return code (0 or -1)
 * ------------------------------------------------------------------------
 */
static int nextrow(lua_State *lu) {
	nowdb_result_t r;
	int rc;

	RESULT();

	rc = nowdb_row_next((nowdb_row_t)r);
	lua_pushinteger(lu, (int64_t)rc);
	return 1;
}

/* ------------------------------------------------------------------------
 * Countfields
 * - receives a cursor or row and returns the return code (0 or -1)
 * ------------------------------------------------------------------------
 */
static int countfields(lua_State *lu) {
	nowdb_result_t r;
	int rc;

	RESULT();

	rc = nowdb_row_count((nowdb_row_t)r);
	lua_pushinteger(lu, (int64_t)rc);
	return 1;
}

/* ------------------------------------------------------------------------
 * Field
 * - receives a row and an integer n and
 *   returns the type and value of the nth field
 *   (null if no such field is available)
 * ------------------------------------------------------------------------
 */
static int field(lua_State *lu) {
	nowdb_result_t r;
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
		lua_pushboolean(lu, (int)*(char*)v); break;

	case NOWDB_TYP_NOTHING:
	default:
		lua_pushinteger(lu, 0ll);
	}

	return 2;
}

/* ------------------------------------------------------------------------
 * init lib and register functions
 * ------------------------------------------------------------------------
 */
int luaopen_libnowluaifc(lua_State *lu) {
	nowdb_client_init();
	// luaL_newlib(lu, luaifc);
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
