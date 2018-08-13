/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================a
 */
#include <nowdb/sql/parser.h>
#include <nowdb/sql/nowdbsql.h>
#include <nowdb/sql/lex.h>
#include <nowdb/sql/state.h>

#define BUFSIZE 8192

/* -----------------------------------------------------------------------
 * Announce the token seen in the stream on stderr.
 * We should think of a tracer (e.g. NDEBUG).
 * -----------------------------------------------------------------------
 */
/*
static void announce(int token, yyscan_t sc) {
	fprintf(stderr, "%d: %s\n", token, yyget_text(sc));
}
*/

/* -----------------------------------------------------------------------
 * The code corresponding to these functions is generated
 * by the lemon parser generator.
 * -----------------------------------------------------------------------
 */
void nowdb_sql_parse(void *parser, int token, char *minor,
                                  nowdbsql_state_t *res);
void *nowdb_sql_parseAlloc(void *fun);
void *nowdb_sql_parseFree(void *parser, void *fun);
void nowdb_sql_parseInit(void *parser);

/* -----------------------------------------------------------------------
 * Initialise the parser
 * -----------------------------------------------------------------------
 */
int nowdbsql_parser_init(nowdbsql_parser_t *p, FILE *fd) {
	p->fd = fd;
	p->errmsg = NULL;
	p->buf = NULL;
	p->streaming = 0;

	/* initialise our internal state */
	if (nowdbsql_state_init(&p->st) != 0) return -1;

	/* allocate the lemon-generated parser */
	p->lp = nowdb_sql_parseAlloc(malloc);
	if (p->lp == NULL) {
		nowdbsql_state_destroy(&p->st);
		return -1;
	}
	/* initialise the lemon-generated parser */
	nowdb_sql_parseInit(p->lp);

	/* initialise the lexer */
	yylex_init(&p->sc);
	yyset_in(fd, p->sc);

	return 0;
}

/* -----------------------------------------------------------------------
 * Initialise the parser for sockets
 * -----------------------------------------------------------------------
 */
int nowdbsql_parser_initSock(nowdbsql_parser_t *p, int fd) {
	p->sock = fd;
	p->errmsg = NULL;
	p->streaming = 1;

	/* allocate buffer */
	p->buf = malloc(BUFSIZE);
	if (p->buf == NULL) return NOWDB_SQL_ERR_NO_MEM;

	/* initialise our internal state */
	if (nowdbsql_state_init(&p->st) != 0) return -1;

	/* allocate the lemon-generated parser */
	p->lp = nowdb_sql_parseAlloc(malloc);
	if (p->lp == NULL) {
		nowdbsql_state_destroy(&p->st);
		free(p->buf); p->buf = NULL;
		return -1;
	}
	/* initialise the lemon-generated parser */
	nowdb_sql_parseInit(p->lp);

	/* initialise the lexer */
	yylex_init(&p->sc);

	return 0;
}

/* -----------------------------------------------------------------------
 * Destroy the parser
 * -----------------------------------------------------------------------
 */
void nowdbsql_parser_destroy(nowdbsql_parser_t *p) {
	if (p->errmsg != NULL) {
		free(p->errmsg); p->errmsg = NULL;
	}
	if (p->lp != NULL) {
		nowdb_sql_parseFree(p->lp, free);
		p->lp = NULL;
	}
	if (p->buf != NULL) {
		free(p->buf); p->buf = NULL;
	}

	yylex_destroy(p->sc); p->sc = NULL;
	nowdbsql_state_destroy(&p->st);
	
	p->fd = NULL;
}

/* -----------------------------------------------------------------------
 * Helper: get the error message from the internal state
 * -----------------------------------------------------------------------
 */
static inline void getErrMsg(nowdbsql_parser_t *p) {
	if (p->errmsg != NULL) {
		free(p->errmsg); p->errmsg = NULL;
	}
	if (p->st.errmsg != NULL) {
		p->errmsg = strdup(p->st.errmsg);
	} else {
		p->errmsg = malloc(32);
		if (p->errmsg == NULL) return;
		strcpy(p->errmsg, "unknown error");
	}
}

/* -----------------------------------------------------------------------
 * Helper: soft reinit:
 * - initialise the lemon parser
 * - initialise our internal state
 * -----------------------------------------------------------------------
 */
static inline void reinitSOFT(nowdbsql_parser_t *p) {
	if (p->lp != NULL) nowdb_sql_parseInit(p->lp);
	nowdbsql_state_reinit(&p->st);
}

/* -----------------------------------------------------------------------
 * Helper: hard reinit:
 * - destroy and allocate the lemon parser
 * - initialise the lemon parser
 * - initialise our internal state
 * -----------------------------------------------------------------------
 */
static inline void reinitHARD(nowdbsql_parser_t *p) {
	nowdb_sql_parseFree(p->lp,free);
	p->lp = nowdb_sql_parseAlloc(malloc);
	reinitSOFT(p);
}

/* -----------------------------------------------------------------------
 * Run the parser
 * -----------------------------------------------------------------------
 */
int nowdbsql_parser_run(nowdbsql_parser_t *p,
                        nowdb_ast_t    **ast) {
	int have = 0;
	int err = 0;
	int nToken;
	char  *str;

	/* no parser :-( */
	if (p->lp == NULL) return NOWDB_SQL_ERR_NO_MEM;

	/* as long as there are tokens ... */
	while((nToken = yylex(p->sc)) != 0) {

		// announce(nToken, p->sc);

		/* token error */
		if (nToken == NOWDB_SQL_UNEXPECTED) {
			p->st.errcode = NOWDB_SQL_ERR_LEX;
			// write error message to state
			nowdbsql_errmsg(&p->st, "unexpected character",
			                            yyget_text(p->sc));
			break;
		}

		/* start of line-comment */
		if (nToken == NOWDB_SQL_COMMENT) continue;

		/* now we have something */
		have=1;

		/* duplicate the symbol to avoid
		   mix-up with the lex and lemon memory */
		str = strdup(yyget_text(p->sc));
		if (str == NULL) {
			p->st.errcode = NOWDB_SQL_ERR_NO_MEM;
			nowdbsql_errmsg(&p->st, "out of memory",
			                     yyget_text(p->sc));
			break;
		}

		/* pass the token to the parser */
		nowdb_sql_parse(p->lp, nToken, str, &p->st);
		if (p->st.errcode != 0) break;

		/* the token was a semicolon: we are done */
		if (nToken == NOWDB_SQL_SEMICOLON) break;
	}
	/* error */
	if (p->st.errcode != 0) {
		err = p->st.errcode;
		getErrMsg(p);
		reinitHARD(p);
		*ast = NULL; return err;
	}

	/*
	if (p->streaming) {
		if (feof(p->fd)) return NOWDB_SQL_ERR_EOF;
	}
	*/

	/* terminate the parser */
	if(have) nowdb_sql_parse(p->lp , 0, NULL, &p->st);
	else {
		reinitHARD(p);
		*ast = NULL; return NOWDB_SQL_ERR_EOF;
	}
	/* get the result */
	*ast = nowdbsql_state_getAst(&p->st);
	/* get ready for the next round */
	reinitSOFT(p);
	return 0;
}

/* -----------------------------------------------------------------------
 * Get error message
 * -----------------------------------------------------------------------
 */
const char *nowdbsql_parser_errmsg(nowdbsql_parser_t *p) {
	return p->errmsg;
}

/* -----------------------------------------------------------------------
 * Parse buffers
 * -----------------------------------------------------------------------
 */
int nowdbsql_parser_buffer(nowdbsql_parser_t *p,
                           char *buf,  int size,
                           nowdb_ast_t   **ast) {
	int rc;
	YY_BUFFER_STATE pst;

	pst = yy_scan_bytes(buf, size, p->sc);
	if (pst == NULL) return NOWDB_SQL_ERR_NO_MEM;

	rc = nowdbsql_parser_run(p, ast);

	yy_delete_buffer(pst, p->sc);
	return rc;
}

/* -----------------------------------------------------------------------
 * Parse socket
 * -----------------------------------------------------------------------
 */
int nowdbsql_parser_runSocket(nowdbsql_parser_t *p, nowdb_ast_t **ast) {
	int x;
	int size;

	for(;;) {
		if (p->fd == NULL) { // || feof(p->fd)) {
			if (read(p->sock, &size, 4) != 4)
				return NOWDB_SQL_ERR_EOF;
			if (size > BUFSIZE) return NOWDB_SQL_ERR_BUFSIZE;
			if (p->fd != NULL) {
				fclose(p->fd); p->fd = NULL;
			}
			fprintf(stderr, "waiting\n");
			size = read(p->sock, p->buf, BUFSIZE);
			if (size == 0) return NOWDB_SQL_ERR_EOF;
			p->fd = fmemopen(p->buf, size, "r");
			if (p->fd == NULL) return NOWDB_SQL_ERR_INPUT;
			setbuf(p->fd, NULL);
			yyset_in(p->fd, p->sc);
		}
		x = nowdbsql_parser_run(p, ast);
		/*
		if (x == 0 && *ast == NULL) {
			fclose(p->fd); p->fd = NULL;
			continue;
		}
		*/
		// if (x != 0 && x != NOWDB_SQL_ERR_EOF) return x;
		if (x != 0) return x;
		break;
	}
	return 0;
}
