/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * NoWDB Daemon (a.k.a. "server")
 * ========================================================================
 */
#include <nowdb/types/types.h>
#include <nowdb/types/version.h>
#include <nowdb/types/error.h>
#include <nowdb/types/time.h>
#include <nowdb/ifc/nowdb.h>

#include <common/cmd.h>

#include <nowdb/mem/t2tmap.h>

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
#include <netdb.h>

/* -----------------------------------------------------------------------
 * Global variables
 * -----------------------------------------------------------------------
 */
char *global_serv = "55505";
char *global_node = NULL;
char *global_path = "./";
char *global_db = NULL;
char global_feedback = 1;
char global_timing = 0;
char global_banner = 1;
char global_python = 0;
char global_lua = 0;
int global_cpool = 128;

/* -----------------------------------------------------------------------
 * produce some output
 * -----------------------------------------------------------------------
 */
#define LOG(e,m) \
	if (global_feedback || e) \
		fprintf(stderr, "%s\n", m);

/* -----------------------------------------------------------------------
 * different kinds of messages
 * -----------------------------------------------------------------------
 */
#define LOGERR(m) LOG(1,m);
#define LOGMSG(m) LOG(0,m);

/* -----------------------------------------------------------------------
 * HELP HELP HELP
 * -----------------------------------------------------------------------
 */
void helptxt(char *progname) {
	fprintf(stderr, "%s [options]\n", progname);
	fprintf(stderr, "all options are in the format -opt value\n");
	fprintf(stderr, "-b: base path (default: ./)\n");
	fprintf(stderr, "-c: number of connections (0: infinite)\n");
	fprintf(stderr, "-l: enable server-side lua\n");
	fprintf(stderr, "-p: port or service (default: 55505)\n");
	fprintf(stderr, "-s: bind domain or address (default: any)\n");
	fprintf(stderr, "-t: timing\n");
	fprintf(stderr, "-q: quiet\n");
	fprintf(stderr, "-n: no banner\n");
	fprintf(stderr, "-y: enable server-side python\n");
	fprintf(stderr, "-V: version\n");
	fprintf(stderr, "-?: \n");
	fprintf(stderr, "-h: help\n");
}

/* -----------------------------------------------------------------------
 * Print version
 * -----------------------------------------------------------------------
 */
void printVersion() {
	fprintf(stdout, "%d.%d.%d.%d\n", nowdb_version(),
	                                 nowdb_major(),
	                                 nowdb_minor(),
	                                 nowdb_build());
}

/* -----------------------------------------------------------------------
 * Get Options
 * -----------------------------------------------------------------------
 */
int getOpts(int argc, char **argv) {
	char *opts = "b:c:p:s:tqlyVh?";
	char c;
	char *tmp, *hlp;

	while((c = getopt(argc, argv, opts)) != -1) {
		switch(c) {
		case 'b':
			global_path = optarg;
			if (global_path[0] == '-') {
				fprintf(stderr,
				"invalid argument for path: %s\n",
				global_path);
				return -1;
			}
			break;

		case 'c':
			tmp = optarg;
			if (tmp[0] == '-') {
				fprintf(stderr,
				"invalid value for connections: %s\n",
				tmp);
				return -1;
			}
			global_cpool = (int)strtoul(tmp, &hlp, 10);
			if (hlp == NULL || *hlp != 0) {
				fprintf(stderr,
				"invalid value for connections: %s\n",
				tmp);
				return -1;
			}
			break;

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

		case 'l': global_lua=1; break;
		case 'y':

#ifdef _NOWDB_WITH_PYTHON
			global_python=1;
#endif
			break;

		case 't': global_timing=1; break;
		case 'q': global_feedback=0; break;
		case 'n': global_banner=0; break;
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
 * split path at ':'
 * -----------------------------------------------------------------------
 */
static inline int add2map(nowdb_t2tmap_t *pmap, char *path) {
	nowdb_err_t err;
	size_t s = strlen(path);
	int i;
	char *db;
	char *p;

	for(i=0;i<s;i++) {
		if(path[i] == ':') break;
	}
	if (i == s) return 0;

	path[i] = 0;
	db = strdup(path);
	if (db == NULL) return -1;

	p = strdup(path+i+1);
	if (p == NULL) {
		free(db); return -1;
	}

	err = nowdb_t2tmap_add(pmap, db, p);
	if (err != NOWDB_OK) {
		nowdb_err_release(err);
		free(db); free(p);
		return -1;
	}
	return 0;
}

/* -----------------------------------------------------------------------
 * Build db/path map
 * -----------------------------------------------------------------------
 */
int buildpathmap(char *path, nowdb_t2tmap_t **pmap) {
	nowdb_err_t err;
	char *wrk = strdup(path);
	char *ptr = NULL;
	char *tmp;
	int  rc=0;

	err = nowdb_t2tmap_new(pmap);
	if (err != NOWDB_OK) {
		nowdb_err_release(err);
		return -1;
	}
	for(tmp = strtok_r(wrk, ";", &ptr); tmp!=NULL;
	    tmp = strtok_r(NULL, ";", &ptr)) {
		if (add2map(*pmap, tmp) != 0) {
			rc = -1; break;
		}
	}
	if (wrk != NULL) free(wrk);
	if (*pmap != NULL && rc != 0) {
		nowdb_t2tmap_destroy(*pmap); free(*pmap);
	}
	return rc;
}


/* -----------------------------------------------------------------------
 * Environment
 * -----------------------------------------------------------------------
 */
int getNowEnv(nowdb_t2tmap_t **pmap) {
	char *nluap = getenv(NOWDB_LUA_PATH);
	if (nluap == NULL) return 0;

	if (buildpathmap(nluap, pmap) != 0) return -1;

	return 0;
}

/* -----------------------------------------------------------------------
 * banner
 * -----------------------------------------------------------------------
 */
void banner(nowdb_t *lib, char *path) {
char tstr[32];
nowdb_time_t tm;
int rc;

if (!global_banner) return;

rc = nowdb_time_now(&tm);
if (rc != 0) {
	fprintf(stderr, "bad start: we have no banner :-(\n");
	fprintf(stderr, "error code %d on get timestamp\n", rc);
	return;
}

tm /= 1000000;
tm *= 1000000;

rc = nowdb_time_toString(tm, NOWDB_TIME_FORMAT, tstr, 32);
if (rc != 0) {
	fprintf(stderr, "bad start: we have no banner :-(\n");
	fprintf(stderr, "error code %d on timestamp to string\n", rc);
	return;
}

// fprintf(stdout, "\n");

fprintf(stdout, "+---------------------------------------------------------------+ \n");
fprintf(stdout, " \n");
fprintf(stdout, "  UTC %s\n", tstr);
fprintf(stdout, " \n");
fprintf(stdout, "  The server is ready\n");
fprintf(stdout, " \n");
fprintf(stdout, "    - with lua support ");
if (lib->luaEnabled) {
	fprintf(stdout, "enabled\n");
} else {
	fprintf(stdout, "disabled\n");
}
#ifdef _NOWDB_WITH_PYTHON
fprintf(stdout, "    - with python support ");
if (lib->pyEnabled) {
	fprintf(stdout, "enabled\n");
} else {
	fprintf(stdout, "disabled\n");
}
fprintf(stdout, " \n");
#endif
fprintf(stdout, " \n");
fprintf(stdout, " \n");
fprintf(stdout, "+---------------------------------------------------------------+\n");
fprintf(stdout, "  nnnn   nnnn          nnnn    nnnnnn       nnnnnn       nnnnnn\n");
fprintf(stdout, "    wi  i   wi       i      i    iw           iw           er  \n");
fprintf(stdout, "    wi i     wi     n        n    iw         e  wi        e    \n");
fprintf(stdout, "    wii      wi    wi        iw    iw       e    wi      e     \n");
fprintf(stdout, "    wi       wi    wi        iw     iw     e      wi    e      \n");
fprintf(stdout, "    wi       wi     n        n       iw   e        wi  e       \n");
fprintf(stdout, "    wi       wi      i      i         iw e          wie        \n");
fprintf(stdout, "   nnnn     nnnn       nnnn             n            n         \n");
fprintf(stdout, "+---------------------------------------------------------------+\n\n\n");
fflush(stdout);
}

uint64_t global_port;

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
	nowdb_t          *lib;  // the library object          
	nowdb_err_t       err;  // error occurred              
	nowdb_task_t   master;  // threadid of the main thread 
	char            *serv;  // service (usually a port)    
	uint64_t  ses_started;  // number of started session  
	uint64_t    ses_ended;  // number of stopped session 
	// addresses
	// options...
} srv_t;

/* -----------------------------------------------------------------------
 * init server object
 * -----------------------------------------------------------------------
 */
void initServer(srv_t *srv, nowdb_t *lib) {
	srv->lib = lib;
	srv->master = nowdb_task_myself();
	srv->err = NOWDB_OK;
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
		close(con);
		return;
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
 * in case of error stop everything
 * -----------------------------------------------------------------------
 */
#define STOPMASTER() \
	x = pthread_kill(srv->master, SIGINT); \
	if (x != 0) { \
		fprintf(stderr, "CANNOT SIGNAL MASTER: %d\n", x); \
	} \

/* -----------------------------------------------------------------------
 * listener
 * -----------------------------------------------------------------------
 */
void *serve(void *arg) {
	struct addrinfo hints;
	struct addrinfo *as=NULL, *runner;
	struct sockaddr_in aadr;
	srv_t *srv = arg;
	int sock, con;
	socklen_t len=0;
	int on = 1;
	sigset_t s;
	int x;

	sigemptyset(&s);
	sigaddset(&s, SIGUSR2);

	memset(&hints, 0, sizeof(struct addrinfo));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	x = getaddrinfo(global_node, global_serv, &hints, &as);
	if (x != 0) {
		/* return code needs to be diversified */
		SETXRR(nowdb_err_addr, x, "getaddrinfo");
		STOPMASTER();
		return NULL;
	}
	for(runner=as; runner!=NULL; runner=runner->ai_next) {
		if ((sock = socket(runner->ai_family,
		                   runner->ai_socktype,
		                   runner->ai_protocol)) == -1) 
		{
			perror("cannot create socket");
			continue;
		}
		if (setsockopt(sock,SOL_SOCKET,
		                  SO_REUSEADDR,
		               &on,sizeof(int)) != 0)
		{
			SETERR(nowdb_err_socket, TRUE,
			   "setting option reuseaddr");
			close(sock);
			STOPMASTER();
			return NULL;
		}
		if (bind(sock, runner->ai_addr, runner->ai_addrlen) == 0) {
			break;
		}
		close(sock);
	}
	if (as != NULL) freeaddrinfo(as);
	if (runner == NULL) {
		SETERR(nowdb_err_addr, FALSE, "stopping listener");
		STOPMASTER();
		return NULL;
	}
	if (listen(sock, 1024) != 0) {
		SETERR(nowdb_err_listen, TRUE, NULL);
		close(sock);
		STOPMASTER();
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
		LOGMSG("ACCEPTED");
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
	nowdb_t2tmap_t  *pmap=NULL;
	struct sigaction sact;
	nowdb_task_t listener;
	nowdb_err_t err;
	nowdb_t *lib;
	int rc = EXIT_SUCCESS;
	srv_t srv;
	sigset_t s,b;
	int sig, x;
	uint64_t flags = 0;

	x = getOpts(argc, argv);
	if (x > 0) return EXIT_SUCCESS;
	if (x < 0) return EXIT_FAILURE;

	if (global_lua)
		flags |= NOWDB_ENABLE_LUA;

	if (global_python)
		flags |= NOWDB_ENABLE_PYTHON;

	// environment variables
	if (getNowEnv(&pmap) != 0) {
		LOGERR("cannot get environment");
		return EXIT_FAILURE;
	}

	err = nowdb_library_init(&lib, global_path,
	                         pmap, global_feedback,
	                               global_cpool,
                                              flags);
	if (err != NOWDB_OK) {
		LOGERR("cannot initialise library");
		nowdb_err_print(err);
		nowdb_err_release(err);
		if (pmap != NULL) {
			nowdb_t2tmap_destroy(pmap); free(pmap);
		}
		return EXIT_FAILURE;
	}

	srand(time(NULL) ^ (uint64_t)&printf);
	initServer(&srv, lib);

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

	/* signals we block */
	sigemptyset(&b);
	sigaddset(&b, SIGABRT);
	sigaddset(&b, SIGINT);
	sigaddset(&b, SIGTERM);
	x = pthread_sigmask(SIG_BLOCK, &b, NULL);
	if (x != 0) {
		fprintf(stderr, "cannot set signal mask\n");
		nowdb_library_close(lib);
		return EXIT_FAILURE;
	}

	/* signals we actively waiting for */
	sigemptyset(&s);
	sigaddset(&s, SIGUSR1);
	sigaddset(&s, SIGABRT);
	sigaddset(&s, SIGTERM);
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
	// fprintf(stderr, "server running\n");
	banner(lib, global_path);
	fprintf(stderr, "connections: %d\n", global_cpool);

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
		case SIGTERM:
		case SIGINT:
			global_stop = 1; 
			break;

		// unknown signal
		default:
			fprintf(stderr, "unknown signal: %d\n", sig);
		}
		if (global_stop) break;
		if (rc) break;
	}

	// stop listener
	fprintf(stderr, "stop listener\n");
	err = stopListener(&srv, listener);
	if (err != NOWDB_OK) {
		LOGERR("cannot stop listener: ");
		nowdb_err_print(err);
		nowdb_err_release(err);
		rc = EXIT_FAILURE;
	}
	// report listener error
	if (srv.err != NOWDB_OK) {
		LOGERR("error in listener: ");
		nowdb_err_print(srv.err);
		nowdb_err_release(srv.err);
		srv.err = NOWDB_OK;
		rc = EXIT_FAILURE;
	}
	// shutdown library
	fprintf(stderr, "shutdown\n");
	err = nowdb_library_shutdown(lib);
	if (err != NOWDB_OK) {
		LOGERR("cannot shutdown library: ");
		nowdb_err_print(err);
		nowdb_err_release(err);
		rc = EXIT_FAILURE;
	}
	// and finally close it
	fprintf(stderr, "close lib\n");
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

