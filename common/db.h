/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * Standard database
 * ========================================================================
 */
#ifndef COM_DB_DECL
#define COM_DB_DECL

#include <nowdb/scope/scope.h>
#include <nowdb/query/cursor.h>
#include <nowdb/io/dir.h>
#include <common/scopes.h>
#include <common/bench.h>

#define HALFEDGE  8192
#define FULLEDGE 16384

#define HALFVRTX  8192
#define FULLVRTX 16384

#define CTXNAME "sales"

int createDB(int edges, int clients, int products);
int dropDB();
nowdb_scope_t *openDB();
void closeDB(nowdb_scope_t *scope);

int64_t countResults(nowdb_scope_t *scope,
                     const char    *stmt);

int edgecount(int h);

int checkEdgeResult(nowdb_scope_t *scope,
                    int origin, int destin,
                    nowdb_time_t tstp,
                    int64_t result);

char *getRandomSQL(int          halves,
                   int          *origin,
                   int          *destin,
                   nowdb_time_t *tp,
                   char         *frm);
#endif

