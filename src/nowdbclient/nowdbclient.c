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

#define BUFSIZE 0x10000
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
	char *host;  /* host address                */
	char *user;  /* connected user              */
	char *pw;    /* pw (do we need to store it? */
	char *buf;   /* temporary buffer            */
	int   sock;  /* socket                      */
	int   flags; /* session properties          */
	short port;  /* the port                    */
};

/* ------------------------------------------------------------------------
 * Multi-purpose result
 * ------------------------------------------------------------------------
 */
struct nowdb_result_t {
	uint64_t cur;     /* cursor id (if result is cursor)         */
	nowdb_con_t con;  /* connection from where it came           */
	char *buf;        /* pointer to the content buffer           */
	char *mybuf;      /* my own private buffer                   */
	int rtype;        /* result type (status, row, cursor, etc.) */
	int status;       /* status: ok or nok                       */
	int sz;           /* content size                            */
	int off;          /* offset into a row                       */
	int lo;           /* size of leftover (in case of rows)      */
	short err;        /* error code                              */
};

/* ------------------------------------------------------------------------
 * Destroy the connection
 * ------------------------------------------------------------------------
 */
static void destroyCon(struct nowdb_con_t *con) {
	if (con == NULL) return;
	if (con->host != NULL) {
		free(con->host); con->host = NULL;
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
static int getAddress(struct nowdb_con_t *con) {
	// use getaddrinfo to get addr infos
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Connect
 * ------------------------------------------------------------------------
 */
static int tryConnect(struct nowdb_con_t *con) {
	struct sockaddr_in adr;

	if ((con->sock = socket(AF_INET, SOCK_STREAM, IPPROTO_IP)) < 0) {
		perror("cannot create socket");
		return NOWDB_ERR_NOSOCK;
	}

	adr.sin_family = AF_INET;
	adr.sin_port   = htons(con->port);

	if (inet_aton(con->host, &adr.sin_addr) == 0) {
		perror("cannot set address");
		return NOWDB_ERR_ADDR;
	}
	if (connect(con->sock, (struct sockaddr*)&adr, sizeof(adr)) != 0) {
		perror("cannot connect");
		return NOWDB_ERR_NOCON;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Just send the buffer
 * ------------------------------------------------------------------------
 */
static inline int sendbuf(struct nowdb_con_t *con, int sz) {
	if (write(con->sock, con->buf, sz) != sz) {
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
 * Read type and status flags
 * ------------------------------------------------------------------------
 */
static inline int readStatus(struct nowdb_con_t *con) {
	if (read(con->sock, con->buf, 2) != 2) {
		perror("cannot read socket");
		return NOWDB_ERR_NOREAD;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Read content type
 * ------------------------------------------------------------------------
 */
static inline int readSize(struct nowdb_con_t *con, int *sz) {
	if (read(con->sock, sz, 4) != 4) {
		perror("cannot read socket");
		return NOWDB_ERR_NOREAD;
	}
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
		if (read(con->sock, &res->err, 2) != 2) {
			perror("cannot read socket");
			return NOWDB_ERR_NOREAD;
		}

		// we provide no further information on EOF
		if (res->err == NOWDB_ERR_EOF) {
			res->buf[0] = 0; return 0;
		}
	}

	// in case of cursor, read cursor id
	if (res->rtype == NOWDB_CURSOR) {
		if (read(con->sock, &res->cur, 8) != 8) {
			perror("cannot read cursor id");
			return NOWDB_ERR_NOREAD;
		}
	}

	// read size
	x = readSize(con, &res->sz);
	if (x != NOWDB_OK) return x;

	// read the content
	if (read(con->sock, con->buf, res->sz) != res->sz) {
		perror("cannot read socket");
		return NOWDB_ERR_NOREAD;
	}

	// we handle status reports and reports as strings
	if (res->rtype == NOWDB_STATUS ||
	    res->rtype == NOWDB_REPORT) res->buf[res->sz] = 0;

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
                  char *host, short port,
                  char *user, char *pw,
                  int flags) 
{
	int  err;
	size_t s;

	if (host == NULL) return NOWDB_ERR_INVALID;
	s = strnlen(host, 4097);
	if (s > 4096) return NOWDB_ERR_INVALID;

	*con = calloc(1,sizeof(struct nowdb_con_t));
	if (*con == NULL) return NOWDB_ERR_NOMEM;

	(*con)->sock = -1;
	(*con)->port = port;
	(*con)->flags= flags;

	(*con)->host = malloc(s+1);
	if ((*con)->host == NULL) {
		free(*con); *con = NULL;
		return NOWDB_ERR_NOMEM;
	}
	strcpy((*con)->host, host);

	(*con)->buf = malloc(BUFSIZE+4);
	if ((*con)->buf == NULL) {
		destroyCon(*con); free(*con); *con = NULL;
		return NOWDB_ERR_NOMEM;
	}

	err = getAddress(*con);
	if (err != NOWDB_OK) {
		destroyCon(*con); free(*con); *con = NULL;
		return err;
	}

	err = tryConnect(*con);
	if (err != NOWDB_OK) {
		destroyCon(*con); free(*con); *con = NULL;
		return err;
	}

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
	memcpy((*res)->mybuf, (*res)->buf, (*res)->sz);
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
 * Get next row
 * ------------------------------------------------------------------------
 */
const char *nowdb_row_next(nowdb_row_t p) {
	int i,j;

	/* search start of next */
	for(i=ROW(p)->off; i<ROW(p)->sz; i++) {
		if (ROW(p)->buf[i] == EOROW) break;
	}
	if (i == ROW(p)->sz) {
		ROW(p)->off = ROW(p)->sz;
		return NULL;
	}
	i++;

	/* make sure the row is complete */
	for(j=i; j<ROW(p)->sz; j++) {
		if (ROW(p)->buf[j] == EOROW) break;
	}
	if (j == ROW(p)->sz) return NULL;
	ROW(p)->off = i;
	return ROW(p)->buf+i;
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
			*type = (int)ROW(p)->buf[i];
			return ROW(p)->buf+i;
		}
		if (ROW(p)->buf[i] == NOWDB_TEXT) {
			while(i<ROW(p)->sz && ROW(p)->buf[i] != 0) i++;
			i++;
		} else {
			i+=8;
		}
		f++;
	}
	return NULL;
}

/* ------------------------------------------------------------------------
 * Write the row to tile 'stream'
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
	int cnt=0;

	if (row == NULL) return NOWDB_ERR_INVALID;

	buf = ROW(row)->buf;

	// find last row
	for(sz=ROW(row)->sz-1; sz>0; sz--) {
		if (buf[sz] == EOROW) break;
	}
	if (sz == 0) return NOWDB_OK;
	sz++;
	while(i<sz) {
		t = buf[i]; i++;
		if (t == EOROW) {
			cnt++;
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
			err = nowdb_time_toString(*(nowdb_time_t*)(buf+i),
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

		case NOWDB_NOTHING:
			i+=8; break;

		default: return NOWDB_ERR_PROTO;

		}
		x = 1;
	}
	// fprintf(stream, "\n"); fflush(stream);
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
		return NOWDB_ERR_NOCLOSE;
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
	int i;
	char *buf = CUR(cur)->mybuf;

	CUR(cur)->lo = 0;
	if (CUR(cur)->sz == 0) return NOWDB_OK;
	
	for(i=ROW(cur)->sz-1; i>0; i--) {
		if (buf[i] == EOROW) break;
	}
	if (i == ROW(cur)->sz-1) return NOWDB_OK;
	i++;

	CUR(cur)->lo = CUR(cur)->sz - i;
	if (CUR(cur)->lo >= MAXROW) {
		fprintf(stderr, "leftover too big: %d\n",
			CUR(cur)->lo);
		return NOWDB_ERR_PROTO;
	}

	/*
	fprintf(stderr, "have leftover: %d of %d size: %d, %d\n",
	                   i, CUR(cur)->sz, CUR(cur)->lo, buf[i]);
	*/

	memcpy(buf, buf+i, CUR(cur)->lo);
	
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
	return (nowdb_row_t)cur;
}

/* ------------------------------------------------------------------------
 * Unit
 * ------------------------------------------------------------------------
 */
static int64_t persec = 1000000000l;

/* ------------------------------------------------------------------------
 * The epoch
 * ------------------------------------------------------------------------
 */
static int64_t nowdb_epoch = 0;

#define NANOPERSEC 1000000000

/* ------------------------------------------------------------------------
 * Important constants
 * ------------------------------------------------------------------------
 */
static int64_t max_unix_sec = sizeof(time_t) == 4?INT_MAX:LONG_MAX;
static int64_t min_unix_sec = sizeof(time_t) == 4?INT_MIN:LONG_MIN;

/* ------------------------------------------------------------------------
 * Helper to treat negative values uniformly, in particular
 * if the nanoseconds are negative,
 * the seconds are decremented by 1
 * and nanoseconds are set to one second + nanoseconds, i.e.
 * second - abs(nanosecond).
 * ------------------------------------------------------------------------
 */
static inline void normalize(struct timespec *tm) {
	if (tm->tv_nsec < 0) {
		tm->tv_sec -= 1;
		tm->tv_nsec = NANOPERSEC + tm->tv_nsec;
	}
}

/* ------------------------------------------------------------------------
 * Convert UNIX system time to nowdb time (helper)
 * ------------------------------------------------------------------------
 */
static inline void fromSystem(const struct timespec *tm,
                                    nowdb_time_t  *time) {
	struct timespec tmp;
	memcpy(&tmp, tm, sizeof(struct timespec));
	normalize(&tmp);
	*time = tmp.tv_sec;
	*time *= persec;
	*time += tmp.tv_nsec/(NANOPERSEC/persec);
	*time += nowdb_epoch;
}

/* ------------------------------------------------------------------------
 * Get time from string
 * ------------------------------------------------------------------------
 */
int nowdb_time_fromString(const char *buf,
                          const char *frm,
                          nowdb_time_t *t) {
	struct tm tm;
	char  *nsecs, *hlp;
	struct timespec tv;

	memset(&tm, 0, sizeof(struct tm));
	memset(&tv, 0, sizeof(struct timespec));

	nsecs = strptime(buf, frm, &tm);
	if (nsecs == NULL || (*nsecs != '.' && *nsecs != 0)) {
		return NOWDB_ERR_FORMAT;
	}

	tv.tv_sec = timegm(&tm);

	if (nsecs != NULL && *nsecs != 0) {
		nsecs++;

		tv.tv_nsec = strtol(nsecs, &hlp, 10);
		if (hlp == NULL || *hlp != 0) {
			return NOWDB_ERR_FORMAT;
		}
		switch(strlen(nsecs)) {
		case 1: tv.tv_nsec *= 100000000; break;
		case 2: tv.tv_nsec *= 10000000; break;
		case 3: tv.tv_nsec *= 1000000; break;
		case 4: tv.tv_nsec *= 100000; break;
		case 5: tv.tv_nsec *= 10000; break;
		case 6: tv.tv_nsec *= 1000; break;
		case 7: tv.tv_nsec *= 100; break;
		case 8: tv.tv_nsec *= 10; break;
		}
	}
	fromSystem(&tv, t);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Convert nowdb time to UNIX system time (helper)
 * ------------------------------------------------------------------------
 */
static inline int toSystem(nowdb_time_t       time,
                           struct timespec     *tm) {
	nowdb_time_t t = time - nowdb_epoch;
	nowdb_time_t tmp = t / persec;
	if (tmp > max_unix_sec || tmp < min_unix_sec) {
		return NOWDB_ERR_TOOBIG;
	}
	tm->tv_sec = (time_t)tmp;
	tm->tv_nsec = (long)(t - tmp*persec) * (NANOPERSEC/persec);
	normalize(tm);
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Write time to string
 * ------------------------------------------------------------------------
 */
int nowdb_time_toString(nowdb_time_t  t,
                        const char *frm,
                              char *buf,
                            size_t max) {
	struct tm tm;
	struct timespec tv;
	size_t sz;
	int err;

	memset(&tv, 0, sizeof(struct timespec));

	err = toSystem(t, &tv);
	if (err != NOWDB_OK) return err;

	if (gmtime_r(&tv.tv_sec, &tm) == NULL) {
		return NOWDB_ERR_OSERR;
	}
	sz = strftime(buf, max, frm, &tm);
	if (sz == 0) {
		return NOWDB_ERR_TOOBIG;
	}
	if (tv.tv_nsec > 0 && sz+11<max) {
		sprintf(buf+sz, ".%09ld", tv.tv_nsec);
	} else if (tv.tv_nsec > 0) {
		return NOWDB_ERR_TOOBIG;
	}
	return NOWDB_OK;
}
