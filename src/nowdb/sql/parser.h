/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * SQL Parser Interface
 * ========================================================================
 * The parser wraps the lexer and parser.
 * TODO:
 * - improve error handling
 * - improve debugging facilities (tracer)
 * - implement the parser-buffer interface,
 *   to pass strings in instead of a stream.
 * ========================================================================
 */
#ifndef nowdbsql_parser_decl
#define nowdbsql_parser_decl

#include <nowdb/sql/ast.h>
#include <nowdb/sql/state.h>
#include <nowdb/sql/lex.h>
#include <nowdb/sql/nowdbsql.h>

#include <stdio.h>
#include <signal.h>

/* ------------------------------------------------------------------------
 * Parser interface
 * ------------------------------------------------------------------------
 */
typedef struct {
	void            *lp; /* lemon parser                 */
	yyscan_t         sc; /* scanner                      */
	nowdbsql_state_t st; /* parser state                 */
	FILE            *fd; /* input stream                 */
	int            sock; /* input socket                 */
	char           *buf; /* buffer for socket            */
	char        *errmsg; /* error message                */
	sigset_t       sigs; /* signal set for stream parser */ 
	char      streaming; /* streaming mode               */
} nowdbsql_parser_t;

/* ------------------------------------------------------------------------
 * Init the parser
 * ------------------------------------------------------------------------
 */
int nowdbsql_parser_init(nowdbsql_parser_t *p, FILE *fd);

/* ------------------------------------------------------------------------
 * Init the parser for socket
 * ------------------------------------------------------------------------
 */
int nowdbsql_parser_initStreaming(nowdbsql_parser_t *p, int fd);

/* ------------------------------------------------------------------------
 * Destroy the parser
 * ------------------------------------------------------------------------
 */
void nowdbsql_parser_destroy(nowdbsql_parser_t *p);

/* ------------------------------------------------------------------------
 * Get current error message (if any)
 * ------------------------------------------------------------------------
 */
const char *nowdbsql_parser_errmsg(nowdbsql_parser_t *p);

/* ------------------------------------------------------------------------
 * Run the parser
 * --------------
 * runs the parser on the input stream and
 * produces an ast of one sql statement;
 * the user is responsible for the memory management of the result.
 * On error, the parser shall be destroyed and initialised again.
 * ------------------------------------------------------------------------
 */
int nowdbsql_parser_run(nowdbsql_parser_t *p,
                        nowdb_ast_t    **ast);


/* ------------------------------------------------------------------------
 * Run the parser on a socket
 * --------------------------
 * runs the parser on the input socket and
 * produces an ast of one sql statement;
 * the user is responsible for the memory management of the result.
 * On error, the parser shall be destroyed and initialised again.
 * ------------------------------------------------------------------------
 */
int nowdbsql_parser_runStream(nowdbsql_parser_t *p,
                              nowdb_ast_t    **ast);

/* ------------------------------------------------------------------------
 * Parse SQL from input buffer
 * ---------------------------
 * runs the parser on the char buffer passed in;
 * produces an ast of one sql statement;
 * the user is responsible for the memory management of the result.
 * On error, the parser shall be destroyed and initialised again.
 * ------------------------------------------------------------------------
 */
int nowdbsql_parser_buffer(nowdbsql_parser_t *p, char *buf, int size,
                           nowdb_ast_t **ast);

#endif

