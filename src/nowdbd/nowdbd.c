/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * NoWDB Daemon (a.k.a. "server")
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/error.h>
#include <nowdb/ifc/nowdb.h>

#include <common/cmd.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>

/* -----------------------------------------------------------------------
 * get a little help for my friends
 * -----------------------------------------------------------------------
 */
void helptxt(char *progname) {
	fprintf(stderr, "%s <path-to-base> [options]\n", progname);
	fprintf(stderr, "all options are in the format -opt value\n");
	fprintf(stderr, "port: port to bin (default: 55505)\n");
}

uint64_t global_port;

/* -----------------------------------------------------------------------
 * get options
 * -----------------------------------------------------------------------
 */
int parsecmd(int argc, char **argv) {
	int err = 0;

	global_port = ts_algo_args_findUint(
	            argc, argv, 2, "port", 55505, &err);
	if (err != 0) {
		fprintf(stderr, "command line error: %d\n", err);
		return -1;
	}
	return 0;
}

/* -----------------------------------------------------------------------
 * signal stop to listener
 * -----------------------------------------------------------------------
 */
static volatile sig_atomic_t global_stop;

/* -----------------------------------------------------------------------
 * stop handler
 * -----------------------------------------------------------------------
 */
void stophandler(int sig) {
	global_stop = 1;
}

/* -----------------------------------------------------------------------
 * ignore handler: does nothing but does not ignore the signal
 *                 before it could interrupt the process (like IGN)
 * -----------------------------------------------------------------------
 */
void ignhandler(int sig) {}

/* -----------------------------------------------------------------------
 * server object
 * -----------------------------------------------------------------------
 */
typedef struct {
	nowdb_t         *lib;  /* the library object          */
	nowdb_err_t      err;  /* error occurred              */
	nowdb_task_t  master;  /* threadid of the main thread */
	short           port;  /* port we are running on      */
	uint64_t ses_started;  /* number of started session   */
	uint64_t   ses_ended;  /* number of stopped session   */
	// addresses
	// options...
} srv_t;

/* -----------------------------------------------------------------------
 * init server object
 * -----------------------------------------------------------------------
 */
void initServer(srv_t *srv, nowdb_t *lib, short port) {
	srv->lib = lib;
	srv->master = nowdb_task_myself();
	srv->err = NOWDB_OK;
	srv->port = port;
	srv->ses_started = 0;
	srv->ses_ended = 0;
}

char *OBJECT = "nowdbd";

/* -----------------------------------------------------------------------
 * Set error with OS errcode passed in as parameter
 * -----------------------------------------------------------------------
 */
#define SETXRR(c,x,s) \
	srv->err = nowdb_err_get(c,x,OBJECT,s);

/* -----------------------------------------------------------------------
 * Set error with TRUE/FALSE indicator
 * -----------------------------------------------------------------------
 */
#define SETERR(c,t,s) \
	srv->err = nowdb_err_get(c,t,OBJECT,s);

/* -----------------------------------------------------------------------
 * handle connection
 * -----------------------------------------------------------------------
 */
void handleConnection(srv_t *srv, int con, struct sockaddr_in adr) {
	nowdb_err_t err;
	nowdb_session_t *ses;

	err = nowdb_getSession(srv->lib, &ses, srv->master, con, con, con);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot get session\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
	}

	if (srv->ses_started == 0x7fffffffffffffff) {
		srv->ses_started = 1;
	} else {
		srv->ses_started++;
	}

	err = nowdb_session_run(ses);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot run session\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		srv->ses_started--;
	}
}

/* -----------------------------------------------------------------------
 * listener
 * -----------------------------------------------------------------------
 */
void *serve(void *arg) {
	srv_t *srv = arg;
	int sock, con;
	struct sockaddr_in badr, aadr;
	socklen_t len=0;
	int on = 1;
	sigset_t s;
	int x;

	sigemptyset(&s);
	sigaddset(&s, SIGUSR2);

	memset(&badr, 0, sizeof(struct sockaddr_in));
	memset(&aadr, 0, sizeof(struct sockaddr_in));

	if ((sock = socket(AF_INET, SOCK_STREAM, IPPROTO_IP)) < 0) {
		SETERR(nowdb_err_socket, TRUE, "creating socket");
		return NULL;
	}
	if (setsockopt(sock,SOL_SOCKET,SO_REUSEADDR,&on,sizeof(int)) != 0) {
		SETERR(nowdb_err_socket, TRUE, "setting option reuseaddr");
		close(sock);
		return NULL;
	}

	badr.sin_family = AF_INET;
	badr.sin_port   = htons(srv->port); /* should be parameter */
	badr.sin_addr.s_addr = INADDR_ANY;  /* should be parameter */

	if (bind(sock, (struct sockaddr*)&badr, sizeof(badr)) != 0) {
		SETERR(nowdb_err_bind, TRUE, NULL);
		close(sock);
		return NULL;
	}
	if (listen(sock, 1024) != 0) {
		SETERR(nowdb_err_listen, TRUE, NULL);
		close(sock);
		return NULL;
	}
	for(;;) {
		x = pthread_sigmask(SIG_UNBLOCK, &s, NULL);
		if (x != 0) {
			SETXRR(nowdb_err_sigset, x, "unblock");
			break;
		}
		con = accept(sock, (struct sockaddr*)&aadr, &len);
		if (con < 0) {
			perror("cannot accept");
			if (global_stop) break;
			continue;
		}
		fprintf(stderr, "received something\n");
		x = pthread_sigmask(SIG_BLOCK, &s, NULL);
		if (x != 0) {
			SETXRR(nowdb_err_sigset, x, "block");
			break;
		}
		if (global_stop) break;
		handleConnection(srv, con, aadr);
	}
	close(sock);
	return NULL;
}

/* -----------------------------------------------------------------------
 * signal and stop listener
 * -----------------------------------------------------------------------
 */
nowdb_err_t stopListener(srv_t *srv, nowdb_task_t listener) {
	nowdb_err_t err;
	int x;

	if (srv->err == NOWDB_OK) {
		x = pthread_kill(listener, SIGUSR2);
		if (x != 0) {
			return nowdb_err_getRC(nowdb_err_signal, x, OBJECT,
			                     "sending SIGUSR2 to listener");
		}
	}
	err = nowdb_task_join(listener);
	if (err != NOWDB_OK) return err;

	return NOWDB_OK;
}

/* -----------------------------------------------------------------------
 * the server process
 * -----------------------------------------------------------------------
 */
int runServer(int argc, char **argv) {
	struct sigaction sact;
	nowdb_task_t listener;
	nowdb_err_t err;
	nowdb_t *lib;
	int rc = EXIT_SUCCESS;
	srv_t srv;
	char *path;
	sigset_t s;
	int sig, x;

	if (argc < 2) {
		helptxt(argv[0]);
		return EXIT_FAILURE;
	}
	path = argv[1];
	if (strnlen(path, 4097) > 4096) {
		fprintf(stderr, "path too long (max: 4096)\n");
		return EXIT_FAILURE;
	}
	if (path[0] == '-') {
		fprintf(stderr, "invalid path\n");
		helptxt(argv[0]);
		return EXIT_FAILURE;
	}
	if (parsecmd(argc, argv) != 0) {
		helptxt(argv[0]);
		return EXIT_FAILURE;
	}
	err = nowdb_library_init(&lib, path, 64);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot initialise library\n");
		nowdb_err_print(err);
		nowdb_err_release(err);
		return EXIT_FAILURE;
	}
	initServer(&srv, lib, 55505);

	/* install stop handler */
	memset(&sact, 0, sizeof(struct sigaction));
	sact.sa_handler = stophandler;
	sact.sa_flags = 0;
	sigemptyset(&sact.sa_mask);
	sigaddset(&sact.sa_mask, SIGUSR2);

	if (sigaction(SIGUSR2, &sact, NULL) != 0) {
		perror("cannot set signal handler for SIGUSR2");
		nowdb_library_close(lib);
		return EXIT_FAILURE;
	}

	/* install ignore handler (interrupts a single session) */
	sact.sa_handler = ignhandler;
	sigemptyset(&sact.sa_mask);
	sigaddset(&sact.sa_mask, SIGUSR1);

	if (sigaction(SIGUSR1, &sact, NULL) != 0) {
		perror("cannot set signal handler for SIGUSR1");
		nowdb_library_close(lib);
		return EXIT_FAILURE;
	}

	/* install IGN handler for SIGPIPE */
	memset(&sact, 0, sizeof(struct sigaction));
	sact.sa_handler = SIG_IGN;
	sigemptyset(&sact.sa_mask);

	if (sigaction(SIGPIPE, &sact, NULL) != 0) {
		perror("cannot set signal handler for SIGPIPE");
		nowdb_library_close(lib);
		return EXIT_FAILURE;
	}

	/* signals we actively waiting for */
	sigemptyset(&s);
	sigaddset(&s, SIGUSR1);
	sigaddset(&s, SIGABRT);
	sigaddset(&s, SIGINT);

	/* start listener */
	err = nowdb_task_create(&listener, &serve, &srv);
	if (err != NOWDB_OK) {
		nowdb_err_print(err);
		nowdb_err_release(err);
		nowdb_library_close(lib);
		return EXIT_FAILURE;
	}

	/* make a nice welcome banner and an option to suppress it */
	fprintf(stderr, "server running\n");

	for(;;) {
		if (srv.err != NOWDB_OK) break;
		x = sigwait(&s, &sig);
		if (x != 0) {
			err = nowdb_err_getRC(nowdb_err_sigwait,
			                       x, "main", NULL);
			fprintf(stderr, "cannot wait for signal\n");
			nowdb_err_print(err);
			nowdb_err_release(err);
			rc = EXIT_FAILURE;
			break;
		}
		switch(sig) {

		// session has ended
		case SIGUSR1:
			if (srv.ses_ended == 0x7fffffffffffffff) {
				srv.ses_ended = 1;
			} else {
				srv.ses_ended++; 
			}
			break;

		// user wants us to terminate
		case SIGABRT:
		case SIGINT:
			global_stop = 1; 
			break;

		// unknown signal
		default:
			fprintf(stderr, "unknown signal: %d\n", sig);
		}
		if (global_stop) break;
	}

	// stop listener
	err = stopListener(&srv, listener);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot stop listener");
		nowdb_err_print(err);
		nowdb_err_release(err);
		rc = EXIT_FAILURE;
	}
	// report listener error
	if (srv.err != NOWDB_OK) {
		fprintf(stderr, "error in listener:");
		nowdb_err_print(err);
		nowdb_err_release(err);
		srv.err = NOWDB_OK;
		rc = EXIT_FAILURE;
	}
	// shutdown library
	err = nowdb_library_shutdown(lib);
	if (err != NOWDB_OK) {
		fprintf(stderr, "cannot shutdown library");
		nowdb_err_print(err);
		nowdb_err_release(err);
		rc = EXIT_FAILURE;
	}
	// and finally close it
	nowdb_library_close(lib);

	// make a nice farewell banner
	// and an option to suppress it
	return rc;
}

/* -----------------------------------------------------------------------
 * this could well be elsewhere
 * -----------------------------------------------------------------------
 */
int main(int argc, char **argv) {
	return runServer(argc, argv);
}

