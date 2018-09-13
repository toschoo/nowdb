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

#include <common/bench.h>

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>

/* -----------------------------------------------------------------------
 * Version Info
 * -----------------------------------------------------------------------
 */
#define VERSION 0
#define MAJOR 0
#define MINOR 0
#define BUILD 1

/* -----------------------------------------------------------------------
 * Global variables
 * -----------------------------------------------------------------------
 */
char *global_serv = "55505";
char *global_node = "127.0.0.1";
char *global_db = NULL;
char *global_query = NULL;
char global_feedback = 1;
char global_timing = 0;

/* -----------------------------------------------------------------------
 * get a little help for my friends
 * -----------------------------------------------------------------------
 */
void helptxt(char *progname) {
	fprintf(stderr, "%s [options]\n", progname);
	fprintf(stderr, "all options are in the format -opt value\n");
	fprintf(stderr, "-p: port or service, default: 55505\n");
	fprintf(stderr, "-s: node name or address, default: 127.0.0.1\n");
	fprintf(stderr, "-d: database, default: NULL\n");
	fprintf(stderr, "-Q: query string, default: NULL\n");
	fprintf(stderr, "-t: timing\n");
	fprintf(stderr, "-q: quiet\n");
	fprintf(stderr, "-V: version\n");
	fprintf(stderr, "-?: \n");
	fprintf(stderr, "-h: help\n");
}

/* -----------------------------------------------------------------------
 * Print version
 * -----------------------------------------------------------------------
 */
void printVersion() {
	fprintf(stdout, "%d.%d.%d.%d\n", VERSION, MAJOR, MINOR, BUILD);
}

/* -----------------------------------------------------------------------
 * Get Options
 * -----------------------------------------------------------------------
 */
int getOpts(int argc, char **argv) {
	char *opts = "p:s:d:Q:tqVh?";
	char c;

	while((c = getopt(argc, argv, opts)) != -1) {
		switch(c) {
		case 'p':
			global_serv = optarg;
			if (global_serv[0] == '-') {
				fprintf(stderr,
				"invalid argument for service: %s\n",
				global_serv);
				return -1;
			}
			break;
		
		case 's': 
			global_node = optarg;
			if (global_node[0] == '-') {
				fprintf(stderr,
				"invalid argument for server address: %s\n",
				global_node);
				return -1;
			}
			break;

		case 'd':
			global_db = optarg;
			if (global_db[0] == '-') {
				fprintf(stderr,
				"invalid argument for database: %s\n",
				global_db);
				return -1;
			}
			break;

		case 'Q':
			global_query = optarg;
			if (global_query[0] == '-') {
				fprintf(stderr,
				"invalid argument for query: %s\n",
				global_query);
				return -1;
			}
			break;

		case 't': global_timing=1; break;
		case 'q': global_feedback=0; break;
		case 'V':
			printVersion(); return 1;
		case 'h':
		case '?':
			helptxt(argv[0]); return 1;
		default:
			fprintf(stderr, "unknown option: %c\n", c);
			return -1;
		}
	}
	return 0;
}

/* -----------------------------------------------------------------------
 * process status result
 * -----------------------------------------------------------------------
 */
int processStatus(nowdb_result_t res) {
	if (global_feedback)
		fprintf(stderr, "OK\n");
	nowdb_result_destroy(res);
	return 0;
}

/* -----------------------------------------------------------------------
 * process report
 * -----------------------------------------------------------------------
 */
int processReport(nowdb_result_t res) {
	uint64_t a,e,r;
	nowdb_result_report(res, &a, &e, &r);
	fprintf(stderr, "%lu rows affected\n", a); 
	fprintf(stderr, "%lu errors\n", e); 
	fprintf(stderr, "%luus running time\n", r); 
	nowdb_result_destroy(res);
	return 0;
}

/* -----------------------------------------------------------------------
 * process cursor
 * -----------------------------------------------------------------------
 */
int processCursor(nowdb_con_t con, nowdb_result_t res) {
	nowdb_cursor_t cur;
	int err;

	err = nowdb_cursor_open(res, &cur);
	if (err != NOWDB_OK) {
		fprintf(stderr, "NOT A CURSOR\n");
		nowdb_result_destroy(res);
		return -1;
	}
	while (nowdb_cursor_ok(cur)) {
		err = nowdb_row_write(stdout, nowdb_cursor_row(cur));
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot write row: %d\n", err);
			nowdb_result_destroy(res);
			return -1;
		}
		err = nowdb_cursor_fetch(cur);
		if (err != NOWDB_OK) {
			fprintf(stderr, "cannot fetch: %d\n", err);
			nowdb_result_destroy(res);
			return -1;
		}
	}
	if (!nowdb_cursor_eof(cur)) {
		fprintf(stderr, "ERROR %d: %s\n",
		        nowdb_cursor_errcode(cur),
		        nowdb_cursor_details(cur));
		nowdb_result_destroy(res);
		return -1;
	}
	err = nowdb_cursor_close(cur);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot close cursor: %d\n", err);
		nowdb_result_destroy(res);
		return -1;
	}
	fflush(stdout);
	return 0;
}

/* -----------------------------------------------------------------------
 * handle query
 * -----------------------------------------------------------------------
 */
int handleQuery(nowdb_con_t con, char *buf, int n) {
	struct timespec t1, t2;
	nowdb_result_t res;
	int err;
	int rc;

	if (n == 0) return 0;

	if (global_timing) timestamp(&t1);

	buf[n] = 0;
	if (global_feedback) 
		fprintf(stderr, "executing \"%s\"\n", buf);
	err = nowdb_exec_statement(con, buf, &res);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot execute statement: %d\n", err); 
		return -1;
	}
	if (nowdb_result_status(res) != 0) {
		fprintf(stderr, "ERROR %hd: %s\n",
		        nowdb_result_errcode(res),
		       nowdb_result_details(res));
		nowdb_result_destroy(res);
		return -1;
	}
	switch(nowdb_result_type(res)) {
	case NOWDB_RESULT_STATUS:
		rc = processStatus(res); break;
		
	case NOWDB_RESULT_REPORT:
		rc = processReport(res); break;

	case NOWDB_RESULT_CURSOR:
		rc = processCursor(con, res); break;

	default:
		fprintf(stderr, "unexpected result\n");
		nowdb_result_destroy(res); return -1;
	}
	if (rc != 0) return -1;

	if (global_timing) {
		timestamp(&t2);
		fprintf(stderr, "processing time: %ldus\n",
		                      minus(&t2, &t1)/1000);
	}
	return 0;
}

/* -----------------------------------------------------------------------
 * set database
 * -----------------------------------------------------------------------
 */
int setDB(nowdb_con_t con) {
	int s;
	char *q;

	if (global_db == NULL) return -1;
	s = strnlen(global_db, 4097);
	if (s > 4096) {
		fprintf(stderr, "database name too long");
		return -1;
	}
	q = malloc(s+5);
	if (q == NULL) {
		fprintf(stderr, "out-of-memory\n");
		return -1;
	}
	sprintf(q, "use %s", global_db);
	if (handleQuery(con, q, s+4) != 0) {
		fprintf(stderr, "cannot use database %s\n", global_db);
		free(q); return -1;
	}
	free(q);
	return 0;
}

/* -----------------------------------------------------------------------
 * handle query parameter
 * -----------------------------------------------------------------------
 */
int handleGlobalQuery(nowdb_con_t con) {
	int s;

	if (global_query == NULL) return -1;
	s = strnlen(global_query, 4097);
	if (s > 4096) {
		fprintf(stderr, "query too long");
		return -1;
	}
	return handleQuery(con, global_query, s);
}

/* -----------------------------------------------------------------------
 * filter whitspace before and after query
 * -----------------------------------------------------------------------
 */
int whitespace(char *buf, int n, int x) {
	n++;
	while(n<x) {
		if(buf[n] != ' '  &&
		   buf[n] != '\t' && 
		   buf[n] != '\n') break;
		n++;
	}
	return n;
}

/* -----------------------------------------------------------------------
 * find end of sql statement
 * -----------------------------------------------------------------------
 */
int findEnd(char *buf, int x) {
	char on=0;
	int i;
	for(i=0;i<x;i++) {
		if (buf[i] == '\'') {
			on=on?0:1; continue;
		}
		if (on) continue;
		if (buf[i] == ';') break;
	}
	if (on) return -1;
	if (i==x) return -1;
	return i;
}

/* -----------------------------------------------------------------------
 * check commented line
 * -----------------------------------------------------------------------
 */
char comment(char *buf, int n) {
	if (buf[0] == '-' && buf[1] == '-') return 1;
	return 0;
}

/* -----------------------------------------------------------------------
 * check for empty query
 * -----------------------------------------------------------------------
 */
char empty(char *buf, int n) {
	if (n < 1) return 1;
	return 0;
}

/* -----------------------------------------------------------------------
 * temporary buffer
 * -----------------------------------------------------------------------
 */
#define BUFSIZE 8192
char buf[BUFSIZE];

/* -----------------------------------------------------------------------
 * handle queries in file
 * -----------------------------------------------------------------------
 */
int handleFile(nowdb_con_t con, FILE *ifile) {
	char eof=0;
	int x;
	int n=0;
	int r=0;
	int k=0;
	int rc = 0;

	for(;;) {
		// get more from file
		if (!eof) {
			x = fread(buf+r, 1, BUFSIZE-r, ifile);
			if (feof(ifile)) eof=1;
			if (x <= 0) {
				if (eof) {
					if (r==0) break;
				} else {
					perror("cannot read");
					rc = -1; break;
				}
			}
			x+=r;
		}
		// find end of next query
		// buf[x] = 0;
		n = findEnd(buf, x);
		if (n < 0) {
			fprintf(stderr, "string too long\n");
			rc = -1; break;
		}

		// remove whitespace
		k = whitespace(buf, -1, n);

		// if not empty and not a comment:
		// handle this query
		if (!empty(buf+k, n-k) &&
                    !comment(buf+k, n-k)) {
			if (handleQuery(con, buf+k, n-k) != 0) {
				rc = -1; break;
			}
		}

		// remove whitespace
		n = whitespace(buf, n, x);

		// prepare buffer for next query
		r = x-n;
		if (r <= 0) {
			if (eof) break;
			continue;
		}
		memmove(buf, buf+n, r); x-=n;
	}
	return rc;
}

/* -----------------------------------------------------------------------
 * main
 * -----------------------------------------------------------------------
 */
int main(int argc, char **argv) {
	int rc = EXIT_SUCCESS;
	FILE *ifile=NULL;
	int err;
	int x;
	int flags=0;
	nowdb_con_t con;

	x = getOpts(argc, argv);
	if (x > 0) return EXIT_SUCCESS;
	if (x < 0) return EXIT_FAILURE;
	if (global_query == NULL) ifile = stdin;

	err = nowdb_connect(&con, global_node, global_serv, NULL, NULL, flags);
	if (err != 0) {
		fprintf(stderr, "cannot get connection: %d\n", err);
		return EXIT_FAILURE;
	}
	if (global_db != NULL) {
		if (setDB(con) != 0) {
			fprintf(stderr, "cannot set DB\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
	if (global_query != NULL) {
		if (handleGlobalQuery(con) != 0) {
			fprintf(stderr, "cannot handle query\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	} else {
		if (handleFile(con, ifile) != 0) {
			fprintf(stderr, "cannot handle file\n");
			rc = EXIT_FAILURE; goto cleanup;
		}
	}
cleanup:
	err = nowdb_connection_close(con);
	if (err != 0) {
		fprintf(stderr, "cannot get connection: %d\n", err);
		nowdb_connection_destroy(con);
		return EXIT_FAILURE;
	}
	return rc;
}
