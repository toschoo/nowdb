/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * NoWDB Daemon (a.k.a. "server")
 * ========================================================================
 */
#ifndef nowdb_version_decl
#define nowdb_version_decl

#define NOWDB_VERSION_STRING "0.1.0.1"

extern char *nowdb_version_string;

int nowdb_version();
int nowdb_major();
int nowdb_minor();
int nowdb_build();

#endif
