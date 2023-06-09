/* ========================================================================
 * (c) Tobias Schoofs, 2019
 * 
 * This file is part of the NOWDB Stored Procedure Library.
 *
 * The NOWDB Stored Procedure Library is free software;
 * you can redistribute it and/or modify it under the terms
 * of the GNU Lesser General Public License 
 * as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * The NOWDB Stored Procedure Library
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
 * NOWDB Stored Procedure LIBRARY
 * Lua Interface
 * ========================================================================
 * TODO: report
 * ------------------------------------------------------------------------ 
 */
#include <nowdb/ifc/luaproc.h>
#include <nowdb/nowproc.h>
#include <nowdb/task/task.h>

/* ------------------------------------------------------------------------
 * Stack operation macros
 * ------------------------------------------------------------------------
 */
#define PUSHERR(x,m) \
	lua_pushinteger(lu, x); \
	lua_pushstring(lu, m);

#define EXITERR(x,m) \
	PUSHERR(x,m); \
	return 2;

#define RESULT() \
	if (!lua_isinteger(lu, 1)) { \
		EXITERR(nowdb_err_lua, "not a valid result"); \
	} \
	nowdb_dbresult_t r = (nowdb_dbresult_t)lua_tointeger(lu,1); \
	if (r == NULL) { \
		EXITERR(nowdb_err_lua, "not a valid result"); \
	} \

#define DB() \
	if (!lua_isinteger(lu, 1)) { \
		EXITERR(nowdb_err_lua, "not a valid database"); \
	} \
	nowdb_db_t db = (nowdb_db_t)lua_tointeger(lu,1); \
	if (db == NULL) { \
		EXITERR(nowdb_err_lua, "not a valid database"); \
	}

#define STRING(n) \
	if (!lua_isstring(lu, n)) { \
		EXITERR(nowdb_err_lua, "not a string"); \
	} \
	str = (char*)lua_tostring(lu,n); \
	if (str == NULL) { \
		EXITERR(nowdb_err_no_mem, "cannot duplicate string"); \
	}

#define INTEGER(n) \
	if (!lua_isinteger(lu, n)) { \
		EXITERR(nowdb_err_lua, "not an integer"); \
	} \
	i = lua_tointeger(lu,n);

#define BOOL(n) \
	if (!lua_isboolean(lu, n)) { \
		EXITERR(nowdb_err_lua, "not a boolean"); \
	} \
	b = (int64_t)lua_toboolean(lu,n);

#define PUSHRESULT(r) \
	lua_pushinteger(lu, (int64_t)r);

#define PUSHOK(r) \
	lua_pushinteger(lu, (int64_t)NOWDB_OK);

/* ------------------------------------------------------------------------
 * Create 'success' result
 * Note that when success fails, we return NULL,
 * so there is no risk that we call a method on an invalid pointer
 * ------------------------------------------------------------------------
 */
static int now2lua_success(lua_State *lu) {
	nowdb_dbresult_t r;

	r = nowdb_dbresult_success();
	PUSHRESULT(r)

	return 1;
}

/* ------------------------------------------------------------------------
 * Create error result with return code / error msg passed in
 * Note that when makeError fails, we return NULL,
 * so there is no risk that we call a method on an invalid pointer
 * ------------------------------------------------------------------------
 */
static int now2lua_error(lua_State *lu) {
	nowdb_dbresult_t r;
	int64_t i;
	char *str;

	INTEGER(1)
	STRING(2)

	r = nowdb_dbresult_makeError(i, str);
	PUSHRESULT(r)
	return 1;
}

/* ------------------------------------------------------------------------
 * Make empty row result
 * returns OK and the row
 * ------------------------------------------------------------------------
 */
static int now2lua_makeRow(lua_State *lu) {
	nowdb_dbrow_t r = nowdb_dbresult_makeRow();
	if (r == NULL) {
		EXITERR(nowdb_err_no_mem, "cannot create row")
	}
	PUSHOK()
	PUSHRESULT(r)
	return 2;
}

/* ------------------------------------------------------------------------
 * Get row capacity (returns the capacity only)
 * ------------------------------------------------------------------------
 */
static int now2lua_rowCapacity(lua_State *lu) {
	RESULT()
	lua_pushinteger(lu, (int64_t)nowdb_dbresult_rowCapacity(
	                                      (nowdb_dbrow_t)r));
	return 1;
}

/* ------------------------------------------------------------------------
 * Add a value to a row
 * receives the row, the type and the value
 * returns ok and, on error, an error message.
 * ------------------------------------------------------------------------
 */
static int now2lua_add2Row(lua_State *lu) {
	void *v=NULL;
	int64_t n;
	int64_t b;
	int64_t i;
	double  d;
	char *str;

	RESULT()
	INTEGER(2)

	switch(i) {
	case NOWDB_TYP_NOTHING: v = NULL; break;
	case NOWDB_TYP_TEXT: STRING(3); v=str; break;
	case NOWDB_TYP_BOOL: BOOL(3); v=&b; break;
	case NOWDB_TYP_FLOAT: 
		if (!lua_isnumber(lu, 3)) {
			EXITERR(nowdb_err_lua, "not a number");
		}
		d = lua_tonumber(lu, 3); v = &d; break;

	case NOWDB_TYP_TIME:
	case NOWDB_TYP_DATE:
	case NOWDB_TYP_INT:
	case NOWDB_TYP_UINT:
		if (!lua_isinteger(lu, 3)) {
			EXITERR(nowdb_err_lua, "not an integer");
		}
		n = lua_tointeger(lu, 3); v = &n; break;
		
	default:
		EXITERR(nowdb_err_lua, "unknown type");
	}

	if (nowdb_dbresult_add2Row((nowdb_dbrow_t)r,i,v)) {
		EXITERR(nowdb_err_no_mem, "out-of-memory");
	}
	PUSHOK()
	return 1;
}

/* ------------------------------------------------------------------------
 * Add EOR to row
 * receives the row and returns ok and, on error, the error message
 * ------------------------------------------------------------------------
 */
static int now2lua_closeRow(lua_State *lu) {
	RESULT()
	if (nowdb_dbresult_closeRow((nowdb_dbrow_t)r)) {
		EXITERR(nowdb_err_no_mem, "out-of-memory");
	}
	PUSHOK()
	return 1;
}

/* ------------------------------------------------------------------------
 * result type
 * receives the result returns the result type
 * ------------------------------------------------------------------------
 */
static int now2lua_rtype(lua_State *lu) {
	RESULT()
	lua_pushinteger(lu, (int64_t)nowdb_dbresult_type(r));
	return 1;
}

/* ------------------------------------------------------------------------
 * error code 
 * receives the result returns the error code
 * ------------------------------------------------------------------------
 */
static int now2lua_errcode(lua_State *lu) {
	RESULT()
	lua_pushinteger(lu, (int64_t)nowdb_dbresult_errcode(r));
	return 1;
}

/* ------------------------------------------------------------------------
 * error details
 * receives the result and returns the error message
 * ------------------------------------------------------------------------
 */
static int now2lua_errdetails(lua_State *lu) {
	RESULT()
	char *s = nowdb_dbresult_details(r);
	if (s == NULL) {
		lua_pushstring(lu, "no details available");
	} else {
		lua_pushstring(lu, s); free(s);
	}
	return 1;
}

/* ------------------------------------------------------------------------
 * fetch
 * receives a cursor and returns either OK or -1
 * ------------------------------------------------------------------------
 */
static int now2lua_fetch(lua_State *lu) {
	RESULT()
	if (nowdb_dbcur_fetch((nowdb_dbcur_t)r) != 0) {
		lua_pushinteger(lu, (int64_t)(-1));
		return 1;
	}
	lua_pushinteger(lu, (int64_t)(NOWDB_OK));
	return 1;
}

/* ------------------------------------------------------------------------
 * open
 * receives a cursor and returns either OK or -1
 * ------------------------------------------------------------------------
 */
static int now2lua_open(lua_State *lu) {
	RESULT()
	if (nowdb_dbcur_open(r) != 0) {
		lua_pushinteger(lu, (int64_t)(-1));
		return 1;
	}
	lua_pushinteger(lu, (int64_t)(NOWDB_OK));
	return 1;
}

/* ------------------------------------------------------------------------
 * nextrow
 * receives a cursor or row and returns either OK or -1
 * ------------------------------------------------------------------------
 */
static int now2lua_nextrow(lua_State *lu) {
	RESULT()
	if (nowdb_dbrow_next((nowdb_dbrow_t)r) != 0) {
		lua_pushinteger(lu, (int64_t)(-1));
		return 1;
	}
	lua_pushinteger(lu, (int64_t)(NOWDB_OK));
	return 1;
}

/* ------------------------------------------------------------------------
 * countfields
 * receives a cursor or row and returns the number of fields in the row
 * ------------------------------------------------------------------------
 */
static int now2lua_countfields(lua_State *lu) {
	RESULT()
	lua_pushinteger(lu, (int64_t)nowdb_dbrow_count((nowdb_dbrow_t)r));
	return 1;
}

/* ------------------------------------------------------------------------
 * field
 * receives a row and an integer n,
 * returns the type and the value of the nth field
 * (null if no such field is available)
 * ------------------------------------------------------------------------
 */
static int now2lua_field(lua_State *lu) {
	int t=0,i;

	RESULT()
	INTEGER(2)

	void *v = nowdb_dbrow_field((nowdb_dbrow_t)r, i, &t);

	if (v == NULL) {
		lua_pushinteger(lu, 0ll);
		lua_pushinteger(lu, 0ll);
	}

	lua_pushinteger(lu, (uint64_t)t);

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
 * execute
 * receives the database pointer and a statement (a string)
 * executes the statement against the database
 * and returns either OK and the result or
 *                    and error code and an error message
 * ------------------------------------------------------------------------
 */
static int now2lua_execute(lua_State *lu) {
	char *str, *stmt = NULL;
	nowdb_dbresult_t r=NULL;
	int rc;

	DB();
	STRING(2); stmt = str;

	rc = nowdb_dbexec_statement(db, stmt, &r);
	if (rc != 0 && r == NULL) {
		EXITERR(nowdb_err_no_mem, "out-of-memory");
	}
	lua_pushinteger(lu, (int64_t)NOWDB_OK);
	lua_pushinteger(lu, (uint64_t)r);

	return 2;
}

/* ------------------------------------------------------------------------
 * release
 * receives a result and releases this result
 * does not return anything.
 * ------------------------------------------------------------------------
 */
static int now2lua_release(lua_State *lu) {

	RESULT();

	if (nowdb_dbresult_type(r) == NOWDB_DBRESULT_CURSOR)
		nowdb_dbcur_close((nowdb_dbcur_t)r);
	else 
		nowdb_dbresult_destroy(r,1);
	return 0;
}

/* ------------------------------------------------------------------------
 * sleep
 * receives a time period in nanoseconds
 * and stops execution at least until the period is elapsed.
 * ------------------------------------------------------------------------
 */
static int now2lua_sleep(lua_State *lu) {
	nowdb_time_t i;
	nowdb_err_t err;

	INTEGER(1);

	err = nowdb_task_sleep(i);
	if (err != NOWDB_OK) {
		PUSHERR(err->errcode, err->info);
		nowdb_err_release(err);
		return 2;	
	}
	PUSHOK();
	return 1;
}

/* ------------------------------------------------------------------------
 * register all functions
 * ------------------------------------------------------------------------
 */
nowdb_err_t nowdb_registerNow2Lua(lua_State *lu) {
	lua_register(lu, "now2lua_success", now2lua_success);
	lua_register(lu, "now2lua_error", now2lua_error);
	lua_register(lu, "now2lua_makerow", now2lua_makeRow);
	lua_register(lu, "now2lua_rowcapacity", now2lua_rowCapacity);
	lua_register(lu, "now2lua_add2row", now2lua_add2Row);
	lua_register(lu, "now2lua_closerow", now2lua_closeRow);
	lua_register(lu, "now2lua_rtype", now2lua_rtype);
	lua_register(lu, "now2lua_execute", now2lua_execute);
	lua_register(lu, "now2lua_errcode", now2lua_errcode);
	lua_register(lu, "now2lua_errdetails", now2lua_errdetails);
	lua_register(lu, "now2lua_open", now2lua_open);
	lua_register(lu, "now2lua_fetch", now2lua_fetch);
	lua_register(lu, "now2lua_nextrow", now2lua_nextrow);
	lua_register(lu, "now2lua_countfields", now2lua_countfields);
	lua_register(lu, "now2lua_field", now2lua_field);
	lua_register(lu, "now2lua_release", now2lua_release);
	lua_register(lu, "now2lua_sleep", now2lua_sleep);
	return NOWDB_OK;
}
