/* ========================================================================
 * (c) Tobias Schoofs, 2019
 * ========================================================================
 * Lua Interface
 * ========================================================================
 */

#include <nowdb/ifc/luaproc.h>
#include <nowdb/nowproc.h>

/* ------------------------------------------------------------------------
 * LUA Interface
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
	}

#define STRING(n) \
	if (!lua_isstring(lu, n)) { \
		EXITERR(nowdb_err_lua, "not a string"); \
	} \
	char *str = strdup(lua_tostring(lu,n)); \
	if (str == NULL) { \
		EXITERR(nowdb_err_no_mem, "cannot duplicate string"); \
	}

#define INTEGER(n) \
	if (!lua_isinteger(lu, n)) { \
		EXITERR(nowdb_err_lua, "not an integer"); \
	} \
	int64_t i = lua_tointeger(lu,n);

#define BOOL(n) \
	if (!lua_isboolean(lu, n)) { \
		EXITERR(nowdb_err_lua, "not a boolean"); \
	} \
	int64_t b = (int64_t)lua_toboolean(lu,n);

#define PUSHRESULT(r) \
	lua_pushinteger(lu, (uint64_t)r);

#define PUSHOK(r) \
	lua_pushinteger(lu, (uint64_t)NOWDB_OK);

static int now2lua_success(lua_State *lu) {
	nowdb_dbresult_t r;

	r = nowdb_dbresult_success();
	PUSHRESULT(r)

	return 1;
}

static int now2lua_error(lua_State *lu) {
	nowdb_dbresult_t r;

	INTEGER(1)
	STRING(2)

	r = nowdb_dbresult_makeError(i, str); free(str);
	PUSHRESULT(r)

	return 1;
}

// report

// row
// now2lua_row
// now2lua_add2row
// now2lua_closerow

static int now2lua_makeRow(lua_State *lu) {
	nowdb_dbrow_t r = nowdb_dbresult_makeRow();
	if (r == NULL) {
		EXITERR(nowdb_err_no_mem, "cannot create row")
	}
	PUSHOK()
	PUSHRESULT(r)
	return 2;
}

static int now2lua_rowCapacity(lua_State *lu) {
	RESULT()
	lua_pushinteger(lu, (int64_t)nowdb_dbresult_rowCapacity(
	                                      (nowdb_dbrow_t)r));
	return 1;
}

static int now2lua_add2Row(lua_State *lu) {
	void *v=NULL;
	int64_t n;
	double  d;

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

static int now2lua_closeRow(lua_State *lu) {
	RESULT()
	if (nowdb_dbresult_closeRow((nowdb_dbrow_t)r)) {
		EXITERR(nowdb_err_no_mem, "out-of-memory");
	}
	PUSHOK()
	return 1;
}

// result type
// now2lua_rtype
static int now2lua_rtype(lua_State *lu) {
	RESULT()
	lua_pushinteger(lu, (int64_t)nowdb_dbresult_type(r));
	return 1;
}
// now2lua_errcode
// now2lua_errdetails
// now2lua_curid
// now2lua_fetch
// now2lua_nextrow
// now2lua_countfields
// now2lua_field
// now2lua_release

// execute

nowdb_err_t nowdb_registerNow2Lua(lua_State *lu) {
	lua_register(lu, "now2lua_success", now2lua_success);
	lua_register(lu, "now2lua_error", now2lua_error);
	lua_register(lu, "now2lua_makerow", now2lua_makeRow);
	lua_register(lu, "now2lua_rowcapacity", now2lua_rowCapacity);
	lua_register(lu, "now2lua_add2row", now2lua_add2Row);
	lua_register(lu, "now2lua_closerow", now2lua_closeRow);
	lua_register(lu, "now2lua_rtype", now2lua_rtype);
	return NOWDB_OK;
}
