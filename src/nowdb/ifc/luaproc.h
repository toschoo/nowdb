/* ========================================================================
 * (c) Tobias Schoofs, 2019
 * ========================================================================
 * Lua Interface
 * ========================================================================
 * Interface to the Lua language used by proc.
 * This header contains only the register function.
 * The functions that are actually registered are defined
 * in the body luaproc.c
 * ========================================================================
 */
#ifndef nowdb_luaproc_decl
#define nowdb_luaproc_decl

#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/nowproc.h>

#include <lualib.h>
#include <lauxlib.h>

nowdb_err_t nowdb_registerNow2Lua();

#endif


