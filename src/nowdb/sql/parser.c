#include <nowdb/sql/parser.h>

#include <nowdb/sql/nowdbsql.h>
#include <nowdb/sql/lex.h>
#include <nowdb/sql/state.h>

/*
static void announce(int token, yyscan_t sc) {
	fprintf(stdout, "%d: %s\n", token, yyget_text(sc));
}
*/

void nowdb_sql_parse(void *parser, int token, char *minor,
                                  nowdbsql_state_t *res);
void *nowdb_sql_parseAlloc(void *fun);
void *nowdb_sql_parseFree(void *parser, void *fun);
void nowdb_sql_parseInit(void *parser);

int nowdbsql_parser_init(nowdbsql_parser_t *p, FILE *fd) {
	p->fd = fd;
	p->errmsg = NULL;

	if (nowdbsql_state_init(&p->st) != 0) return -1;
	p->lp = nowdb_sql_parseAlloc(malloc);
	if (p->lp == NULL) {
		nowdbsql_state_destroy(&p->st);
		return -1;
	}
	nowdb_sql_parseInit(p->lp);

	yylex_init(&p->sc);
	yyset_in(fd, p->sc);

	return 0;
}

void nowdbsql_parser_destroy(nowdbsql_parser_t *p) {
	if (p->errmsg != NULL) {
		free(p->errmsg); p->errmsg = NULL;
	}
	if (p->lp != NULL) {
		nowdb_sql_parseFree(p->lp, free);
		p->lp = NULL;
	}

	yylex_destroy(p->sc); p->sc = NULL;
	nowdbsql_state_destroy(&p->st);
	
	p->fd = NULL;
}

static inline void getErrMsg(nowdbsql_parser_t *p) {
	if (p->errmsg != NULL) {
		free(p->errmsg); p->errmsg = NULL;
	}
	if (p->st.errmsg != NULL) {
		size_t s = strlen(p->st.errmsg);
		p->errmsg = malloc(s+1);
		if (p->errmsg == NULL) return;
		strcpy(p->errmsg, p->st.errmsg);

	} else {
		p->errmsg = malloc(32);
		if (p->errmsg == NULL) return;
		strcpy(p->errmsg, "unknown error");
	}
}

static inline void reinitSOFT(nowdbsql_parser_t *p) {
	if (p->lp != NULL) nowdb_sql_parseInit(p->lp);
	nowdbsql_state_reinit(&p->st);
}

static inline void reinitHARD(nowdbsql_parser_t *p) {
	nowdb_sql_parseFree(p->lp,free);
	p->lp = nowdb_sql_parseAlloc(malloc);
	reinitSOFT(p);
}

int nowdbsql_parser_run(nowdbsql_parser_t *p,
                        nowdb_ast_t    **ast) {
	int have = 0;
	int err = 0;
	int nToken;
	char  *str;

	if (p->lp == NULL) return NOWDB_SQL_ERR_NO_MEM;

	while((nToken = yylex(p->sc)) != 0) {

		// announce(nToken, p->sc);

		if (nToken == NOWDB_SQL_UNEXPECTED) {
			p->st.errcode = NOWDB_SQL_ERR_LEX;
			// write error message to state
			break;
		}
		if (nToken == NOWDB_SQL_COMMENT) continue;

		have=1;

		str = strdup(yyget_text(p->sc));
		if (str == NULL) {
			p->st.errcode = NOWDB_SQL_ERR_NO_MEM; break;
		}
		nowdb_sql_parse(p->lp, nToken, str, &p->st);
		if (p->st.errcode != 0) break;
		if (nToken == NOWDB_SQL_SEMICOLON) break;
	}
	if (p->st.errcode != 0) {
		err = p->st.errcode;
		getErrMsg(p);
		reinitHARD(p);
		*ast = NULL; return err;
	}
	if(have) nowdb_sql_parse(p->lp , 0, NULL, &p->st);
	else {
		reinitHARD(p);
		*ast = NULL; return NOWDB_SQL_ERR_EOF;
	}
	*ast = nowdbsql_state_ast(&p->st);
	reinitSOFT(p);
	return 0;
}

const char *nowdbsql_parser_errmsg(nowdbsql_parser_t *p) {
	return p->errmsg;
}

int nowdbsql_parser_buffer(nowdbsql_parser_t *p, char *buf, int size);


