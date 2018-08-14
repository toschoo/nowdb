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
	int rtype;
	int sz;
	char *buf;
	int off;
	int status;
};

struct nowdb_cursor_t {
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

static inline int sendstr(struct nowdb_con_t *con, char *str) {
	size_t sz = strlen(str);
	memcpy(con->buf, str, sz);
	if (write(con->sock, con->buf, sz) != sz) {
		perror("cannot write to socket");
		return NOWDB_ERR_NOWRITE;
	}
	return NOWDB_OK;
} 

static inline int sendbyte(struct nowdb_con_t *con, char byte) {
	memcpy(con->buf, &byte, 1);
	if (write(con->sock, con->buf, 1) != 1) {
		perror("cannot write to socket");
		return NOWDB_ERR_NOWRITE;
	}
	return NOWDB_OK;
} 

static inline int sendbytes(struct nowdb_con_t *con, char *bytes, int sz) {
	memcpy(con->buf, bytes, sz);
	if (write(con->sock, con->buf, sz) != sz) {
		perror("cannot write to socket");
		return NOWDB_ERR_NOWRITE;
	}
	return NOWDB_OK;
}

static inline int sendbuf(struct nowdb_con_t *con, int sz) {
	if (write(con->sock, &sz, 4) != 4) {
		perror("cannot write to socket");
		return NOWDB_ERR_NOWRITE;
	}
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
	if (*sz > BUFSIZE-2) return NOWDB_ERR_TOOBIG;
	return NOWDB_OK;
}

static inline int readResult(struct nowdb_con_t    *con,
                             struct nowdb_result_t *res) {
	int x;

	if (read(con->sock, con->buf, 1) != 1) {
		perror("cannot read socket");
		return NOWDB_ERR_NOREAD;
	}

	res->rtype = (int)con->buf[0];

	switch(con->buf[0]) {
	case NOWDB_STATUS:
		if (read(con->sock, con->buf, 1) != 1) {
			perror("cannot read socket");
			return NOWDB_ERR_NOREAD;
		}
		res->buf = con->buf;
		res->status = -1;
		if (con->buf[0] == ACK) {
			res->status = 0;
			return NOWDB_OK;
		}
		if (con->buf[0] != NOK) return NOWDB_ERR_PROTO;

	case NOWDB_REPORT:
	case NOWDB_ROW:
		x = readSize(con, &res->sz);
		if (x != NOWDB_OK) return x;
		res->buf = con->buf;
		if (read(con->sock, con->buf, res->sz) != res->sz) {
			perror("cannot read socket");
			return NOWDB_ERR_NOREAD;
		}
		if (res->rtype == NOWDB_REPORT) res->buf[res->sz] = 0;
		return NOWDB_OK;
		
	case NOWDB_CURSOR:
		// read cursor id

	default: return NOWDB_ERR_PROTO;   
	}
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

static nowdb_result_t mkResult() {
	return calloc(1, sizeof(struct nowdb_result_t));
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

const char *nowdb_result_details(nowdb_result_t res) {
	if (res->status == ACK) return "OK";
	return res->buf;
}

nowdb_report_t nowdb_result_report(nowdb_result_t res) {
	return (nowdb_report_t)res;
}

nowdb_row_t nowdb_result_row(nowdb_result_t res) {
	return (nowdb_row_t)res;
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
                         nowdb_result_t *res);

/* ------------------------------------------------------------------------
 * Cursor
 * ------------------------------------------------------------------------
 */
int nowdb_cursor_open(nowdb_con_t     con,
                      char     *statement,
                      nowdb_cursor_t *cur);

int nowdb_cursor_close(nowdb_cursor_t cur);

int nowdb_cursor_fetch(nowdb_cursor_t cur,
                       nowdb_row_t   *row);

int nowdb_cursor_fetchBulk(nowdb_cursor_t  cur,
                           uint32_t  requested,
                           uint32_t   received,
                           nowdb_row_t   *row);

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

	memcpy(con->buf, "USE", 3); con->buf[3] = ' ';
	memcpy(con->buf+4, db, sz);
	memcpy(con->buf+4+sz, ";", 1);

	x = sendbuf(con, sz+5);
	if (x != NOWDB_OK) return x;

	*res = mkResult();
	if (*res == NULL) return NOWDB_ERR_NOMEM;

	x = readResult(con, *res);
	if (x != NOWDB_OK) {
		free(*res); *res = NULL;
		return x;
	}
	return NOWDB_OK;
}

