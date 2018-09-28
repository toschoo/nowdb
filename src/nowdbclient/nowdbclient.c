/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * 
 * This file is part of the NOWDB CLIENT Library.
 *
 * The NOWDB CLIENT Library is free software; you can redistribute it
 * and/or modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * The NOWDB CLIENT Library
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
 * NOWDB CLIENT LIBRARY
 * ========================================================================
 */
#include <nowdb/nowclient.h>
#include <nowdb/query/rowutl.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <errno.h>
#include <ctype.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netdb.h>

#define BUFSIZE 0x12000
#define MAXROW  0x1000

#ifdef _NOWDB_LE_
#define SESOPBIN "SQLBE0  "
#else
#define SESOPBIN "SQLLE0  "
#endif
#define SESOPTXT "SQLTX0  "

#define NOWDB_NOTHING  0
#define NOWDB_TEXT     1
#define NOWDB_DATE     2
#define NOWDB_TIME     3
#define NOWDB_FLOAT    4
#define NOWDB_INT      5
#define NOWDB_UINT     6
#define NOWDB_COMPLEX  7
#define NOWDB_LONGTEXT 8
#define NOWDB_BOOL     9

#define EOROW 0xa

#define NOWDB_STATUS    0x21
#define NOWDB_REPORT    0x22
#define NOWDB_ROW       0x23
#define NOWDB_CURSOR    0x24

#define NOWDB_DELIM     0x3b

#define ACK             0x4f
#define NOK             0x4e

#define DISC "disconnect"
#define USE "use"

/* ------------------------------------------------------------------------
 * Connection handle
 * ------------------------------------------------------------------------
 */
struct nowdb_con_t {
	char *node;  /* the node                    */
	char *serv;  /* the service                 */
	char *user;  /* connected user              */
	char *pw;    /* pw (do we need to store it? */
	char *buf;   /* temporary buffer            */
	int   sock;  /* socket                      */
	int   flags; /* session properties          */
};

/* ------------------------------------------------------------------------
 * Multi-purpose result
 * ------------------------------------------------------------------------
 */
struct nowdb_result_t {
	uint64_t cur;      /* cursor id (if result is cursor)         */
	nowdb_con_t con;   /* connection from where it came           */
	char *buf;         /* pointer to the content buffer           */
	char *mybuf;       /* my own private buffer                   */
	uint64_t affected; /* report: rows affected                   */
	uint64_t errors;   /* report: errors                          */
	uint64_t runtime;  /* report: runtime                         */
	int rtype;         /* result type (status, row, cursor, etc.) */
	int status;        /* status: ok or nok                       */
	int sz;            /* content size                            */
	int off;           /* offset into a row                       */
	int lo;            /* size of leftover (in case of rows)      */
	short err;         /* error code                              */
};

/* ------------------------------------------------------------------------
 * Destroy the connection
 * ------------------------------------------------------------------------
 */
static void destroyCon(struct nowdb_con_t *con) {
	if (con == NULL) return;
	if (con->node != NULL) {
		free(con->node); con->node = NULL;
	}
	if (con->serv != NULL) {
		free(con->serv); con->serv = NULL;
	}
	if (con->user != NULL) {
		free(con->user); con->user = NULL;
	}
	if (con->pw != NULL) {
		free(con->pw); con->pw= NULL;
	}
	if (con->buf != NULL) {
		free(con->buf); con->buf = NULL;
	}
}

/* ------------------------------------------------------------------------
 * Get address from a service
 * TODO: implement
 * ------------------------------------------------------------------------
 */
static int getAddress(struct nowdb_con_t *con,
                      struct addrinfo    **as) {
	int x;
	struct addrinfo hints;

	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;

	x = getaddrinfo(con->node, con->serv, &hints, as);
	if (x != 0) {
		/* return code needs to be diversified */
		fprintf(stderr, "cannot get address info: %d\n", x);
		return NOWDB_ERR_ADDR;
	}
	if (*as == NULL) return NOWDB_ERR_ADDR;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Connect
 * ------------------------------------------------------------------------
 */
static int tryConnect(struct nowdb_con_t *con,
                      struct addrinfo    *as) 
{
	struct addrinfo *runner;

	for(runner=as; runner!=NULL; runner=runner->ai_next) {
		if ((con->sock = socket(runner->ai_family,
		                        runner->ai_socktype,
		                        runner->ai_protocol)) == -1) 
		{
			perror("cannot create socket");
			continue;
		}
		if (connect(con->sock, runner->ai_addr,
		                       runner->ai_addrlen) != -1)
			break;

		close(con->sock);
	}
	if (runner == NULL) return NOWDB_ERR_NOCON;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Just send the buffer
 * ------------------------------------------------------------------------
 */
static inline int sendbuf(struct nowdb_con_t *con, int sz) {
	int x;
	x = write(con->sock, con->buf, sz);
       	if (x != sz) {
		fprintf(stderr, "cannot write socket: %d\n", x);
		perror("cannot write to socket");
		return NOWDB_ERR_NOWRITE;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Send bytes
 * ------------------------------------------------------------------------
 */
static inline int sendbytes(struct nowdb_con_t *con, char *bytes, int sz) {
	memcpy(con->buf, &sz, 4);
	memcpy(con->buf+4, bytes, sz);
	return sendbuf(con, sz+4);
}

/* ------------------------------------------------------------------------
 * generic read
 * ------------------------------------------------------------------------
 */
static inline int readN(int sock, char *buf, int sz) {
	size_t t=0;
	size_t x;

	while(t<sz) {
		x = read(sock, buf+t, sz-t);
       		if (x <= 0) {
			fprintf(stderr, "cannot read socket: %d/%zu\n",
			                                        sz, x);
			perror("cannot read socket");
			return NOWDB_ERR_NOREAD;
		}
		t+=x;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Read type and status flags
 * ------------------------------------------------------------------------
 */
static inline int readStatus(struct nowdb_con_t *con) {
	return readN(con->sock, con->buf, 2);
}

/* ------------------------------------------------------------------------
 * Read content type
 * ------------------------------------------------------------------------
 */
static inline int readSize(struct nowdb_con_t *con, int *sz) {
	int x;
	x = readN(con->sock, (char*)sz, 4);
	if (x != 0) return x;
	if (*sz > BUFSIZE-MAXROW) {
		fprintf(stderr, "SIZE: %d\n", *sz);
		return NOWDB_ERR_TOOBIG;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Read complete result 
 * ------------------------------------------------------------------------
 */
static inline int readResult(struct nowdb_con_t    *con,
                             struct nowdb_result_t *res) {
	int x;

	res->buf = con->buf;

	// read status (type and status)
	x = readStatus(con);
	if (x != NOWDB_OK) return x;

	// set type
	res->rtype = (int)con->buf[0];

	// fprintf(stderr, "type: %x\n", res->rtype);

	// get status
	res->status = -1;
	if (con->buf[1] == ACK) {
		res->status = 0;
		if (res->rtype == NOWDB_STATUS) return NOWDB_OK;

	// NOK only comes with a status report
	} else if (res->rtype == NOWDB_STATUS) {
		if (con->buf[1] != NOK) return NOWDB_ERR_PROTO;

		// in case of NOK, read error code
		x = readN(con->sock, (char*)&res->err, 2);
		if (x != NOWDB_OK) return x;

		// we provide no further information on EOF
		if (res->err == NOWDB_ERR_EOF) {
			con->buf[0] = 0; return 0;
		}
	}

	// in case of report, read three integers
	if (res->rtype == NOWDB_REPORT) {
		x = readN(con->sock, con->buf, 24);
       		if (x != NOWDB_OK) return x;
		memcpy(&res->affected, con->buf, 8);
		memcpy(&res->errors, con->buf+8, 8);
		memcpy(&res->runtime, con->buf+16, 8);
		return NOWDB_OK;
	}

	// in case of cursor, read cursor id
	if (res->rtype == NOWDB_CURSOR) { // readN
		x = readN(con->sock, (char*)&res->cur, 8);
		if (x != NOWDB_OK) return x;
	}

	// read size
	x = readSize(con, &res->sz);
	if (x != NOWDB_OK) return x;
	// fprintf(stderr, "size is %d\n", res->sz);

	// read the content
	x = readN(con->sock, con->buf, res->sz);
       	if (x != NOWDB_OK) return x;

	// we handle some results as strings
	con->buf[res->sz] = 0;

	// done
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Announce preferred session options
 * ------------------------------------------------------------------------
 */
static int sendSessionOpts(struct nowdb_con_t *con) {
	int sz;
	char *ops;

	if (con->flags & NOWDB_FLAGS_TEXT) {
		ops = SESOPTXT;
	} else {
		ops = SESOPBIN;
	}
		
	sz = strlen(ops);

	memcpy(con->buf, ops, sz);
	if (write(con->sock, con->buf, sz) != sz) {
		perror("cannot write to socket");
		return NOWDB_ERR_NOWRITE;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Finally put the pieces together to connect
 * ------------------------------------------------------------------------
 */
int nowdb_connect(nowdb_con_t *con,
                  char *node, char *srv,
                  char *user, char *pw,
                  int flags) 
{
	int  err;
	size_t s1, s2;
	struct addrinfo *as=NULL;

	if (node == NULL) return NOWDB_ERR_INVALID;
	s1 = strnlen(node, 4097);
	if (s1 > 4096) return NOWDB_ERR_INVALID;

	if (srv == NULL) return NOWDB_ERR_INVALID;
	s2 = strnlen(srv, 4097);
	if (s2 > 4096) return NOWDB_ERR_INVALID;

	*con = calloc(1,sizeof(struct nowdb_con_t));
	if (*con == NULL) return NOWDB_ERR_NOMEM;

	(*con)->sock = -1;
	(*con)->flags= flags;

	(*con)->node = malloc(s1+1);
	if ((*con)->node == NULL) {
		free(*con); *con = NULL;
		return NOWDB_ERR_NOMEM;
	}
	strcpy((*con)->node, node);

	(*con)->serv = malloc(s2+1);
	if ((*con)->serv == NULL) {
		destroyCon(*con); free(*con); *con = NULL;
		return NOWDB_ERR_NOMEM;
	}
	strcpy((*con)->serv, srv);

	(*con)->buf = malloc(BUFSIZE+4);
	if ((*con)->buf == NULL) {
		destroyCon(*con); free(*con); *con = NULL;
		return NOWDB_ERR_NOMEM;
	}

	err = getAddress(*con, &as);
	if (err != NOWDB_OK) {
		destroyCon(*con); free(*con); *con = NULL;
		return err;
	}

	err = tryConnect(*con, as);
	if (err != NOWDB_OK) {
		if (as != NULL) freeaddrinfo(as);
		destroyCon(*con); free(*con); *con = NULL;
		return err;
	}

	if (as != NULL) freeaddrinfo(as);

	err = sendSessionOpts(*con);
	if (err != NOWDB_OK) {
		destroyCon(*con); free(*con); *con = NULL;
		return err;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Close the connection
 * ------------------------------------------------------------------------
 */
int nowdb_connection_close(nowdb_con_t con) {

	// a friendly protocol would send a DISCONNECT message

	if (close(con->sock) != 0) {
		perror("cannot close socket");
		return NOWDB_ERR_NOCLOSE;
	}
	destroyCon(con); free(con);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Destroy connection (only if we could not close the connection)
 * ------------------------------------------------------------------------
 */
void nowdb_connection_destroy(nowdb_con_t con) {
	destroyCon(con); free(con);
}

/* ------------------------------------------------------------------------
 * Create a result
 * ------------------------------------------------------------------------
 */
static nowdb_result_t mkResult(nowdb_con_t con) {
	nowdb_result_t r = calloc(1, sizeof(struct nowdb_result_t));
	if (r == NULL) return NULL;
	r->con = con;
	return r;
}

/* ------------------------------------------------------------------------
 * Destroy result
 * ------------------------------------------------------------------------
 */
void nowdb_result_destroy(nowdb_result_t res) {
	if (res == NULL) return;
	if (res->mybuf != NULL) {
		free(res->mybuf); res->mybuf = NULL;
	}
	free(res);
}

/* ------------------------------------------------------------------------
 * Get result type
 * ------------------------------------------------------------------------
 */
int nowdb_result_type(nowdb_result_t res) {
	return res->rtype;
}

/* ------------------------------------------------------------------------
 * Get status
 * ------------------------------------------------------------------------
 */
int nowdb_result_status(nowdb_result_t res) {
	return res->status;
}

/* ------------------------------------------------------------------------
 * Get error code
 * ------------------------------------------------------------------------
 */
int nowdb_result_errcode(nowdb_result_t res) {
	if (res->status == ACK) return NOWDB_OK;
	return (int)res->err;
}

/* ------------------------------------------------------------------------
 * Error code is EOF
 * ------------------------------------------------------------------------
 */
int nowdb_result_eof(nowdb_result_t res) {
	if (res->status == ACK) return 0;
	return (res->err == NOWDB_ERR_EOF);
}

/* ------------------------------------------------------------------------
 * Get details of error message
 * ------------------------------------------------------------------------
 */
const char *nowdb_result_details(nowdb_result_t res) {
	if (res->status == ACK) return "OK";
	return res->buf;
}

/* ------------------------------------------------------------------------
 * Get Report details
 * ------------------------------------------------------------------------
 */
void nowdb_result_report(nowdb_result_t res,
                         uint64_t *affected, 
                         uint64_t *errors, 
                         uint64_t *runtime) {
	if (res == NULL) return;
	if (res->rtype != NOWDB_REPORT) return;
	if (affected != NULL) {
		*affected = res->affected;
	}
	if (errors != NULL) {
		*errors = res->errors;
	}
	if (runtime != NULL) {
		*runtime = res->runtime;
	}
}

/* ------------------------------------------------------------------------
 * Issue an SQL statement
 * ------------------------------------------------------------------------
 */
int execStatement(nowdb_con_t     con,
                  char     *statement,
                  nowdb_result_t *res,
                  char           zero) {
	int x;
	size_t sz;

	sz = strnlen(statement, 4097);
	if (sz > 4096) return NOWDB_ERR_INVALID;

	x = sendbytes(con, statement, sz);
	if (x != NOWDB_OK) return x;

	*res = mkResult(con);
	if (*res == NULL) return NOWDB_ERR_NOMEM;

	x = readResult(con, *res);
	if (x != NOWDB_OK) {
		nowdb_result_destroy(*res);
		*res = NULL; return x;
	}
	if (zero) return NOWDB_OK;

	(*res)->mybuf = malloc(BUFSIZE+4);
	if ((*res)->mybuf == NULL) {
		nowdb_result_destroy(*res); *res = NULL;
		return NOWDB_ERR_NOMEM;
	}
	memcpy((*res)->mybuf, (*res)->buf, (*res)->sz+1);
	(*res)->buf = (*res)->mybuf;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Issue an SQL statement (zerocopy result)
 * ------------------------------------------------------------------------
 */
int nowdb_exec_statementZC(nowdb_con_t     con,
                           char     *statement,
                           nowdb_result_t *res) 
{
	if (con == NULL) return NOWDB_ERR_INVALID;
	return execStatement(con, statement, res, 1);
}

/* ------------------------------------------------------------------------
 * Issue an SQL statement
 * ------------------------------------------------------------------------
 */
int nowdb_exec_statement(nowdb_con_t     con,
                         char     *statement,
                         nowdb_result_t *res) 
{
	if (con == NULL) return NOWDB_ERR_INVALID;
	return execStatement(con, statement, res, 0);
}

/* ------------------------------------------------------------------------
 * Handle rows
 * ------------------------------------------------------------------------
 */
#define ROW(x) \
	((struct nowdb_result_t*)x)

/* ------------------------------------------------------------------------
 * End of string
 * ------------------------------------------------------------------------
 */
static inline int findEndOfStr(char *buf, int sz, int idx) {
	return nowdb_row_findEndOfStr(buf, sz, idx);
}

/* ------------------------------------------------------------------------
 * End of row
 * ------------------------------------------------------------------------
 */
static inline int findEORow(nowdb_row_t p, int idx) {
	return nowdb_row_findEOR(ROW(p)->buf, ROW(p)->sz, idx);
}

/* ------------------------------------------------------------------------
 * Find last complete row
 * ------------------------------------------------------------------------
 */
static inline int findLastRow(char *buf, int sz) {
	return nowdb_row_findLastRow(buf, sz);
}

/* ------------------------------------------------------------------------
 * Get next row
 * ------------------------------------------------------------------------
 */
int nowdb_row_next(nowdb_row_t p) {
	int i,j;

	/* search start of next */
	i = findEORow(p, ROW(p)->off);
	if (i < 0) return NOWDB_ERR_EOF;

	/* make sure the row is complete */
	j = findEORow(p, i);
	if (j<0) return NOWDB_ERR_EOF;

	ROW(p)->off = i;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Rewind to first row
 * ------------------------------------------------------------------------
 */
void nowdb_row_rewind(nowdb_row_t p) {
	ROW(p)->off = 0;
}

/* ------------------------------------------------------------------------
 * Get nth field in current row
 * ------------------------------------------------------------------------
 */
void *nowdb_row_field(nowdb_row_t p, int field, int *type) {
	int i;
	int f=0;

	for(i=ROW(p)->off; i<ROW(p)->sz && ROW(p)->buf[i] != EOROW;)  {
		if (f==field) {
			*type = (int)ROW(p)->buf[i]; i++;
			return (ROW(p)->buf+i);
		}
		if (ROW(p)->buf[i] == NOWDB_TEXT) {
			i = findEndOfStr(ROW(p)->buf,
			                 ROW(p)->sz,i);
		} else {
			i+=9;
		}
		f++;
	}
	return NULL;
}

/* ------------------------------------------------------------------------
 * Write the row to file 'stream'
 * ------------------------------------------------------------------------
 */
int nowdb_row_write(FILE *stream, nowdb_row_t row) {
	char *buf;
	char tmp[32];
	uint32_t i=0;
	char t;
	char x=0;
	int err;
	int sz;

	if (row == NULL) return NOWDB_ERR_INVALID;

	buf = ROW(row)->buf;

	sz = findLastRow(buf, ROW(row)->sz);
	if (sz < 0) return NOWDB_ERR_PROTO;
	if (sz == 0) return NOWDB_OK;

	while(i<sz) {
		t = buf[i]; i++;
		if (t == EOROW) {
			fprintf(stream, "\n");
			fflush(stream);
			x=0;
			continue;
		} else if (x) {
			fprintf(stream, ";");
		}
		switch((int)t) {
		case NOWDB_TEXT:
			fprintf(stream, "%s", buf+i);
			i+=strlen(buf+i)+1; break;

		case NOWDB_DATE: 
		case NOWDB_TIME: 
			err = nowdb_time_show(*(nowdb_time_t*)(buf+i),
		                                    NOWDB_TIME_FORMAT,
		                                              tmp, 32);
			if (err != NOWDB_OK) return err;
			fprintf(stream, "%s", tmp); i+=8; break;

		case NOWDB_INT: 
			fprintf(stream, "%ld", *(int64_t*)(buf+i));
			i+=8; break;

		case NOWDB_UINT: 
			fprintf(stream, "%lu", *(uint64_t*)(buf+i));
			i+=8; break;

		case NOWDB_FLOAT: 
			fprintf(stream, "%.4f", *(double*)(buf+i));
			i+=8; break;

		case NOWDB_BOOL: 
			if (*(char*)(buf+i) == 0) {
				fprintf(stream, "false");
			} else {
				fprintf(stream, "true");
			}
			i++; break;

		case NOWDB_NOTHING:
			i+=8; break;

		default:
			return NOWDB_ERR_PROTO;

		}
		x = 1;
	}
	fflush(stream);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Cursor
 * ------------------------------------------------------------------------
 */
#define CUR(x) \
	((nowdb_result_t)x)

/* ------------------------------------------------------------------------
 * Open cursor from given result
 * ------------------------------------------------------------------------
 */
int nowdb_cursor_open(nowdb_result_t  res,
                      nowdb_cursor_t *cur) 
{
	if (res->rtype != NOWDB_CURSOR) return NOWDB_ERR_INVALID;
	if (res->mybuf == NULL) return NOWDB_ERR_CURZC;
	*cur = (nowdb_cursor_t)res;
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Close cursor
 * ------------------------------------------------------------------------
 */
int nowdb_cursor_close(nowdb_cursor_t cur) {
	int x;
	char *sql;
	size_t sz;
	nowdb_result_t res;

	sql = malloc(32);
	if (sql == NULL) return NOWDB_ERR_NOMEM;

	sprintf(sql, "close %lu;", CUR(cur)->cur);

	sz = strlen(sql);
	x = sendbytes(CUR(cur)->con, sql, sz); free(sql);
	if (x != NOWDB_OK) return x;

	res = mkResult(CUR(cur)->con);
	if (res == NULL) return NOWDB_ERR_NOMEM;

	x = readResult(CUR(cur)->con, res);
	if (x != NOWDB_OK) {
		nowdb_result_destroy(res);
		return x;
	}
	if (res->status != NOWDB_OK) {
		fprintf(stderr, "CLOSE %d/%d: %s\n",
		  res->status, res->err, nowdb_result_details(res));
		nowdb_result_destroy(res);
		return NOWDB_ERR_CURCL;
	}
	nowdb_result_destroy(res);
	nowdb_result_destroy(CUR(cur));
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Rescue bytes of incomplete row at end of cursor
 * ------------------------------------------------------------------------
 */
static inline int leftover(nowdb_cursor_t cur) {
	int l=0;
	char *buf = CUR(cur)->mybuf;

	CUR(cur)->lo = 0;
	if (CUR(cur)->sz == 0) return NOWDB_OK;

	l = findLastRow(buf, CUR(cur)->sz);
	if (l < 0) return NOWDB_ERR_PROTO;
	if (l >= ROW(cur)->sz) return NOWDB_OK;

	CUR(cur)->lo = CUR(cur)->sz - l;
	if (CUR(cur)->lo >= MAXROW) {
		fprintf(stderr, "leftover too big: %d\n",
			CUR(cur)->lo);
		return NOWDB_ERR_PROTO;
	}

	/*
	fprintf(stderr, "have leftover: %d of %d size: %d, %d/%d/%d/%d/%d\n",
	      l, CUR(cur)->sz, CUR(cur)->lo,
	      (int)buf[l-2],
	      (int)buf[l-1], 
	      (int)buf[l], 
	      (int)buf[l+1], 
	      (int)buf[l+2]);
	*/

	memcpy(buf, buf+l, CUR(cur)->lo);

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Fetch next bunch of rows from cursor
 * ------------------------------------------------------------------------
 */
int nowdb_cursor_fetch(nowdb_cursor_t  cur) {
	int x;
	size_t sz;
	char *sql;

	x = leftover(cur);
	if (x != NOWDB_OK) {
		fprintf(stderr, "leftover: %d\n", x);
		return x;
	}

	sql = malloc(32);
	if (sql == NULL) return NOWDB_ERR_NOMEM;

	sprintf(sql, "fetch %lu;", CUR(cur)->cur);

	sz = strlen(sql);
	x = sendbytes(CUR(cur)->con, sql, sz); free(sql);
	if (x != NOWDB_OK) return x;

	x = readResult(CUR(cur)->con, CUR(cur));
	if (x != NOWDB_OK) {
		fprintf(stderr, "read result: %d\n", x);
		return x;
	}
	if (CUR(cur)->mybuf != NULL) {
		memcpy(CUR(cur)->mybuf+CUR(cur)->lo,
		       CUR(cur)->con->buf,
		       CUR(cur)->sz);
		CUR(cur)->sz += CUR(cur)->lo;
		CUR(cur)->buf = CUR(cur)->mybuf;
	}

	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Check eof 
 * ------------------------------------------------------------------------
 */
int nowdb_cursor_eof(nowdb_cursor_t cur) {
	return nowdb_result_eof(CUR(cur));
}

/* ------------------------------------------------------------------------
 * Check for error
 * ------------------------------------------------------------------------
 */
int nowdb_cursor_ok(nowdb_cursor_t cur) {
	return (CUR(cur)->status == NOWDB_OK);
}

/* ------------------------------------------------------------------------
 * Get error code from cursor
 * ------------------------------------------------------------------------
 */
int nowdb_cursor_errcode(nowdb_cursor_t cur) {
	return nowdb_result_errcode(CUR(cur));
}

/* ------------------------------------------------------------------------
 * Get error details from cursor
 * ------------------------------------------------------------------------
 */
const char *nowdb_cursor_details(nowdb_cursor_t cur) {
	return nowdb_result_details(CUR(cur));
}

/* ------------------------------------------------------------------------
 * Get row from cursor
 * ------------------------------------------------------------------------
 */
nowdb_row_t nowdb_cursor_row(nowdb_cursor_t cur) {
	if (cur == NULL) return NULL;
	CUR(cur)->off = 0;
	return ((nowdb_row_t)cur);
}

/* ------------------------------------------------------------------------
 * Get row from cursor
 * ------------------------------------------------------------------------
 */
nowdb_row_t nowdb_row_copy(nowdb_row_t row) {
	nowdb_row_t cp;

	if (row == NULL) return NULL;

	cp = (nowdb_row_t)mkResult(ROW(row)->con);
	if (cp == NULL) return NULL;

	ROW(cp)->rtype = NOWDB_ROW;
	ROW(cp)->status = ROW(row)->status;
	ROW(cp)->err = ROW(row)->err;
	ROW(cp)->cur = ROW(row)->cur;
	ROW(cp)->off = ROW(row)->off;
	ROW(cp)->sz = ROW(row)->sz;
	ROW(cp)->buf = ROW(row)->buf;
	ROW(cp)->lo  = 0;
	ROW(cp)->mybuf = NULL;

	return cp;
}

/* ------------------------------------------------------------------------
 * Get cursor id
 * ------------------------------------------------------------------------
 */
uint64_t nowdb_cursor_id(nowdb_cursor_t cur) {
	if (cur == NULL) return 0xffffffffffffffff;
	return CUR(cur)->cur;
}

/* ------------------------------------------------------------------------
 * Wrappers
 * ------------------------------------------------------------------------
 */
void *nowdb_time_fromString(const char *buf,
                            const char *frm,
                            nowdb_time_t *t);

void *nowdb_time_toString(nowdb_time_t  t,
                          const char *frm,
                                char *buf,
                               size_t max);

void *nowdb_time_now(nowdb_time_t *t);

void nowdb_time_fromSystem(const struct timespec *tp,
                         nowdb_time_t *t);

void *nowdb_time_toSystem(nowdb_time_t t,
                          struct timespec *tp);

int nowdb_err_code(void *err);
void nowdb_err_release(void *err);
void nowdb_err_print(void *err);
const char *nowdb_err_desc(int errcode);

/* ------------------------------------------------------------------------
 * Get time from string
 * ------------------------------------------------------------------------
 */
int nowdb_time_parse(const char *buf,
                     const char *frm,
                     nowdb_time_t *t) {

	void *err;
	int rc;

	err = nowdb_time_fromString(buf, frm, t);
	if (err != NULL) {
		rc = nowdb_err_code(err);
		nowdb_err_release(err);
		return rc;
	}
	return NOWDB_OK;
}
	
/* ------------------------------------------------------------------------
 * Write time to string
 * ------------------------------------------------------------------------
 */
int nowdb_time_show(nowdb_time_t  t,
                    const char *frm,
                          char *buf,
                         size_t max) {
	void *err;
	int rc;

	err = nowdb_time_toString(t, frm, buf, max);
	if (err != NULL) {
		rc = nowdb_err_code(err);
		nowdb_err_release(err);
		return rc;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Get system time as nowdb time
 * ------------------------------------------------------------------------
 */
nowdb_time_t nowdb_time_get() {
	nowdb_time_t t;
	void *err;
	err = nowdb_time_now(&t);
	if (err != NULL) return NOWDB_TIME_DAWN;
	return t;
}

/* ------------------------------------------------------------------------
 * Unix time to nowdb time
 * ------------------------------------------------------------------------
 */
nowdb_time_t nowdb_time_fromUnix(const struct timespec *tp) {
	nowdb_time_t t;
	nowdb_time_fromSystem(tp, &t);
	return t;
}

/* ------------------------------------------------------------------------
 * Nowdb time to Unix time
 * ------------------------------------------------------------------------
 */
int nowdb_time_toUnix(nowdb_time_t t,
                      struct timespec *tp) {
	void *err;
	int rc;
	err = nowdb_time_toSystem(t, tp);
	if (err != NULL) {
		rc = nowdb_err_code(err);
		nowdb_err_release(err);
		return rc;
	}
	return 0;
}

/* ------------------------------------------------------------------------
 * Describe error
 * ------------------------------------------------------------------------
 */
const char *nowdb_err_explain(int err) {
	if (err == 0) return "OK";
	if (err > 0) return nowdb_err_desc(err);
	switch(err) {
	case NOWDB_ERR_NOMEM:   return "client out of memory";
	case NOWDB_ERR_NOCON:   return "cannot connect";
	case NOWDB_ERR_NOSOCK:  return "cannot create socket";
	case NOWDB_ERR_ADDR:    return "cannot initialise address";
	case NOWDB_ERR_NORES:   return "no result";
	case NOWDB_ERR_INVALID: return "invalid input";
	case NOWDB_ERR_NOREAD:  return "cannot read from socket";
	case NOWDB_ERR_NOWRITE: return "cannot write to socket";
	case NOWDB_ERR_NOOPEN:  return "cannot open";
	case NOWDB_ERR_NOCLOSE: return "cannot close socket";
	case NOWDB_ERR_NOUSE:   return "cannot use that db";
	case NOWDB_ERR_PROTO:   return "protocol error";
	case NOWDB_ERR_TOOBIG:  return "requested resource too big";
	case NOWDB_ERR_OSERR:   return "OS error in client; check errno!";
	case NOWDB_ERR_FORMAT:  return "cannot connect";
	case NOWDB_ERR_CURZC:   return "cursor with zero copy requested";
	case NOWDB_ERR_CURCL:   return "cannot close cursor";
	default: return "unknown client error";
	}
}

