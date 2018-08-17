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

struct nowdb_con_t {
	char *host;
	char *user;
	char *pw;
	char *buf;
	struct addrinfo *addr;
	int   sock;
	int   flags;
	short port;
};

struct nowdb_result_t {
	uint64_t cur;
	nowdb_con_t con;
	int rtype;
	short err;
	int sz;
	char *buf;
	int off;
	int status;
};

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

static int getAddress(struct nowdb_con_t *con) {
	// use getaddrinfo to get addr infos
	return NOWDB_OK;
}

static int tryConnect(struct nowdb_con_t *con) {
	struct sockaddr_in adr;

	if ((con->sock = socket(AF_INET, SOCK_STREAM, IPPROTO_IP)) < 0) {
		perror("cannot create socket");
		return NOWDB_ERR_NOCON;
	}

	adr.sin_family = AF_INET;
	adr.sin_port   = htons(con->port);

	if (inet_aton(con->host, &adr.sin_addr) == 0) {
		perror("cannot set address");
		return NOWDB_ERR_NOCON;
	}
	if (connect(con->sock, (struct sockaddr*)&adr, sizeof(adr)) != 0) {
		perror("cannot connect");
		return NOWDB_ERR_NOCON;
	}
	return NOWDB_OK;
}

static inline int sendbytes(struct nowdb_con_t *con, char *bytes, int sz) {
	memcpy(con->buf, &sz, 4);
	memcpy(con->buf+4, bytes, sz);
	if (write(con->sock, con->buf, sz+4) != sz+4) {
		perror("cannot write to socket");
		return NOWDB_ERR_NOWRITE;
	}
	return NOWDB_OK;
}

static inline int sendbuf(struct nowdb_con_t *con, int sz) {
	if (write(con->sock, con->buf, sz) != sz) {
		perror("cannot write to socket");
		return NOWDB_ERR_NOWRITE;
	}
	return NOWDB_OK;
}

static inline int readbyte(struct nowdb_con_t *con, char *byte) {
	if (read(con->sock, con->buf, 1) != 1) {
		perror("cannot read socket");
		return NOWDB_ERR_NOREAD;
	}
	*byte = con->buf[0];
	return NOWDB_OK;
}

static inline int readStatus(struct nowdb_con_t *con, int *res) {
	if (read(con->sock, con->buf, 2) != 2) {
		perror("cannot read socket");
		return NOWDB_ERR_NOREAD;
	}
	if (con->buf[0] != NOWDB_STATUS) return NOWDB_ERR_PROTO;
	*res = (int)con->buf[1];
	return NOWDB_OK;
}

static inline int readSize(struct nowdb_con_t *con, int *sz) {
	if (read(con->sock, sz, 4) != 4) {
		perror("cannot read socket");
		return NOWDB_ERR_NOREAD;
	}
	if (*sz > BUFSIZE-2) {
		fprintf(stderr, "SIZE: %d\n", *sz);
		return NOWDB_ERR_TOOBIG;
	}
	return NOWDB_OK;
}

static inline int readResult(struct nowdb_con_t    *con,
                             struct nowdb_result_t *res) {
	int x;

	res->buf = con->buf;

	if (read(con->sock, con->buf, 2) != 2) {
		perror("cannot read socket");
		return NOWDB_ERR_NOREAD;
	}

	res->rtype = (int)con->buf[0];

	// fprintf(stderr, "type: %x\n", res->rtype);

	res->status = -1;
	if (con->buf[1] == ACK) {
		res->status = 0;
		if (res->rtype == NOWDB_STATUS) return NOWDB_OK;

	} else if (res->rtype == NOWDB_STATUS) {
		if (con->buf[1] != NOK) return NOWDB_ERR_PROTO;
		if (read(con->sock, &res->err, 2) != 2) {
			perror("cannot read socket");
			return NOWDB_ERR_NOREAD;
		}
		if (res->err == NOWDB_ERR_EOF) {
			res->buf[0] = 0; return 0;
		}
	}
	if (res->rtype == NOWDB_CURSOR) {
		if (read(con->sock, &res->cur, 8) != 8) {
			perror("cannot read cursor id");
			return NOWDB_ERR_NOREAD;
		}
	}

	x = readSize(con, &res->sz);
	if (x != NOWDB_OK) return x;

	if (read(con->sock, con->buf, res->sz) != res->sz) {
		perror("cannot read socket");
		return NOWDB_ERR_NOREAD;
	}
	if (res->rtype == NOWDB_STATUS ||
	    res->rtype == NOWDB_REPORT) res->buf[res->sz] = 0;
	return NOWDB_OK;
}

static inline int readbytes(struct nowdb_con_t *con, size_t *sz) {
	/*
	int typ;

	if (read(con->sock, con->buf, 1) != 1) {
		perror("cannot read socket");
		return NOWDB_ERR_NOWRITE;
	}
	typ = (int)con->buf[0];

	// switch(typ) read stuff
	*/

	return NOWDB_OK;
}

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

	(*con)->buf = malloc(BUFSIZE);
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

int nowdb_connection_close(nowdb_con_t con) {
	// int x;

	/* DISCONNECT statement
	x = sendbyte(con, DISC);
	if (x != NOWDB_OK) return x;
	*/

	if (close(con->sock) != 0) {
		perror("cannot close socket");
		return NOWDB_ERR_NOCLOSE;
	}
	destroyCon(con); free(con);
	return NOWDB_OK;
}

void nowdb_connection_destroy(nowdb_con_t con) {
	destroyCon(con); free(con);
}

/* ------------------------------------------------------------------------
 * Possible results
 * ------------------------------------------------------------------------
 */

static nowdb_result_t mkResult(nowdb_con_t con) {
	nowdb_result_t r = calloc(1, sizeof(struct nowdb_result_t));
	if (r == NULL) return NULL;
	r->con = con;
	return r;
}

void nowdb_result_destroy(nowdb_result_t res) {
	free(res);
}

int nowdb_result_type(nowdb_result_t res) {
	return res->rtype;
}

int nowdb_result_status(nowdb_result_t res) {
	return res->status;
}

short nowdb_result_errcode(nowdb_result_t res) {
	if (res->status == ACK) return NOWDB_OK;
	return res->err;
}

int nowdb_result_eof(nowdb_result_t res) {
	if (res->status == ACK) return 0;
	return (res->err == NOWDB_ERR_EOF);
}

const char *nowdb_result_details(nowdb_result_t res) {
	if (res->status == ACK) return "OK";
	return res->buf;
}

nowdb_report_t nowdb_result_report(nowdb_result_t res) {
	return (nowdb_report_t)res;
}

#define ROW(x) \
	((struct nowdb_result_t*)x)

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

void nowdb_row_rewind(nowdb_row_t p) {
	ROW(p)->off = 0;
}

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

int nowdb_row_write(FILE *stream, nowdb_row_t row) {
	char *buf;
	char tmp[32];
	uint32_t i=0;
	char t;
	char x=0;
	int err;
	int sz;

	buf = ROW(row)->buf;

	// find last row
	for(sz=ROW(row)->sz; sz>0; sz--) {
		if (buf[sz] == EOROW) break;
	}
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

		default: return NOWDB_ERR_PROTO;

		}
		x = 1;
	}
	fprintf(stream, "\n"); fflush(stream);
	return NOWDB_OK;
}

nowdb_cursor_t nowdb_result_cursor(nowdb_result_t res) {
	return NULL;
}

/* ------------------------------------------------------------------------
 * Single SQL statement
 * ------------------------------------------------------------------------
 */
int nowdb_exec_statement(nowdb_con_t     con,
                         char     *statement,
                         nowdb_result_t *res) {
	int x;
	size_t sz;

	if (con == NULL) return NOWDB_ERR_INVALID;

	sz = strnlen(statement, 4097);
	if (sz > 4096) return NOWDB_ERR_INVALID;

	x = sendbytes(con, statement, sz);
	if (x != NOWDB_OK) return x;

	*res = mkResult(con);
	if (*res == NULL) return NOWDB_ERR_NOMEM;

	x = readResult(con, *res);
	if (x != NOWDB_OK) {
		free(*res); *res = NULL;
		return x;
	}
	return NOWDB_OK;
}

/* ------------------------------------------------------------------------
 * Cursor
 * ------------------------------------------------------------------------
 */
int nowdb_cursor_open(nowdb_result_t  res,
                      nowdb_cursor_t *cur) 
{
	if (res->rtype != NOWDB_CURSOR) return NOWDB_ERR_INVALID;
	*cur = (nowdb_cursor_t)res;
	return NOWDB_OK;
}

#define CUR(x) \
	((nowdb_result_t)x)

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
		free(res); return x;
	}
	if (res->status != NOWDB_OK) {
		fprintf(stderr, "CLOSE %d/%d: %s\n",
		  res->status, res->err, nowdb_result_details(res));
		free(res); return NOWDB_ERR_NOCLOSE;
	}
	free(res); free(cur);
	return NOWDB_OK;
}

int nowdb_cursor_fetch(nowdb_cursor_t  cur) {
	int x;
	size_t sz;
	char *sql;

	sql = malloc(32);
	if (sql == NULL) return NOWDB_ERR_NOMEM;

	sprintf(sql, "fetch %lu;", CUR(cur)->cur);

	sz = strlen(sql);
	x = sendbytes(CUR(cur)->con, sql, sz); free(sql);
	if (x != NOWDB_OK) return x;

	x = readResult(CUR(cur)->con, CUR(cur));
	if (x != NOWDB_OK) return x;

	return NOWDB_OK;
}

int nowdb_cursor_eof(nowdb_cursor_t cur) {
	return nowdb_result_eof(CUR(cur));
}

int nowdb_cursor_ok(nowdb_cursor_t cur) {
	return (CUR(cur)->status == NOWDB_OK);
}

short nowdb_cursor_errcode(nowdb_cursor_t cur) {
	return nowdb_result_errcode(CUR(cur));
}

const char *nowdb_cursor_details(nowdb_cursor_t cur) {
	return nowdb_result_details(CUR(cur));
}

nowdb_row_t nowdb_cursor_row(nowdb_cursor_t cur) {
	return (nowdb_row_t)cur;
}

/* ------------------------------------------------------------------------
 * Misc
 * ------------------------------------------------------------------------
 */
int nowdb_use(nowdb_con_t con, char *db, nowdb_result_t *res) {
	int x;
	size_t sz;

	if (con == NULL) return NOWDB_ERR_INVALID;

	sz = strnlen(db, 4097);
	if (sz > 4096) return NOWDB_ERR_INVALID;

	memcpy(con->buf+4, "USE", 3); con->buf[7] = ' ';
	memcpy(con->buf+8, db, sz);
	memcpy(con->buf+8+sz, ";", 1);
	sz+=5;
	memcpy(con->buf, &sz, 4);

	x = sendbuf(con, sz+4);
	if (x != NOWDB_OK) return x;

	*res = mkResult(con);
	if (*res == NULL) return NOWDB_ERR_NOMEM;

	x = readResult(con, *res);
	if (x != NOWDB_OK) {
		free(*res); *res = NULL;
		return x;
	}
	return NOWDB_OK;
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
