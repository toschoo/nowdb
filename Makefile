# =========================================================================
# Makefile for NoWDB
# (c) Tobias Schoofs, 2018
# =========================================================================
CC = @gcc
CXX = @g++
AR = @ar
LEM = @nowlemon -Tlemon/lempar.c
LEX = @flex

LNKMSG = @printf "linking   $@\n"
CMPMSG = @printf "compiling $@\n"
LEXMSG = @printf "lexer $@\n"
LEMONMSG = @printf "die goldenen zitronen $@\n"

FLGMSG = @printf "CFLAGS: $(CFLAGS)\nLDFLAGS: $(LDFLAGS)\n"

INSMSG = @printf ". setenv.sh"

CFLAGS = -O3 -g -Wall -std=c99 -fPIC \
         -D_GNU_SOURCE \
         -D_POSIX_C_SOURCE=200809L \
         -D_NOWDB_WITH_PYTHON

LDFLAGS = -L./lib

INC = -I./include -I./test -I./src -I./ -I/usr/include/lua5.3
LIB = lib
CLIENTLIB = nowclientlib 

SRC = src/nowdb
SRD = src/nowdbd
SRL = src/nowdbclient
PYC = pynow
LUA = lua
SQL = $(SRC)/sql
HDR = include/nowdb
TST = test
COM = common
BENCH= bench
SMK = test/smoke
STRESS = test/stress
CMK = test/client
BIN = bin
LOG = log
TOOLS = tools
RSC = rsc
OUTLIB = lib
DOC=doc
MAN=doc/manual
LIBPY = python2.7
LIBLUA = lua5.3
libs = -lm -ldl -lpthread -ltsalgo -lbeet -lzstd -lcsv -l$(LIBPY) -l$(LIBLUA)
clibs = -lm -lpthread -ltsalgo -lcsv

OBJ = $(SRC)/types/types.o    \
      $(SRC)/types/lib.o      \
      $(SRC)/types/errman.o   \
      $(SRC)/types/error.o    \
      $(SRC)/types/time.o     \
      $(SRC)/io/dir.o         \
      $(SRC)/io/file.o        \
      $(SRC)/task/lock.o      \
      $(SRC)/task/task.o      \
      $(SRC)/task/queue.o     \
      $(SRC)/task/worker.o    \
      $(SRC)/sort/sort.o      \
      $(SRC)/mem/lru.o        \
      $(SRC)/mem/ptlru.o      \
      $(SRC)/mem/pklru.o      \
      $(SRC)/mem/pplru.o      \
      $(SRC)/mem/plru8r.o     \
      $(SRC)/mem/blist.o      \
      $(SRC)/mem/t2tmap.o     \
      $(SRC)/store/store.o    \
      $(SRC)/store/storage.o  \
      $(SRC)/store/comp.o     \
      $(SRC)/store/indexer.o  \
      $(SRC)/store/storewrk.o \
      $(SRC)/scope/context.o  \
      $(SRC)/scope/scope.o    \
      $(SRC)/scope/loader.o   \
      $(SRC)/scope/procman.o  \
      $(SRC)/scope/ipc.o      \
      $(SRC)/scope/dml.o      \
      $(SRC)/index/index.o    \
      $(SRC)/index/compare.o  \
      $(SRC)/index/man.o      \
      $(SRC)/reader/reader.o  \
      $(SRC)/model/model.o    \
      $(SRC)/text/text.o      \
      $(SRC)/fun/fun.o        \
      $(SRC)/fun/group.o      \
      $(SRC)/fun/expr.o       \
      $(SRC)/sql/ast.o        \
      $(SRC)/sql/lex.o        \
      $(SRC)/sql/nowdbsql.o   \
      $(SRC)/sql/state.o      \
      $(SRC)/sql/parser.o     \
      $(SRC)/qplan/plan.o     \
      $(SRC)/query/stmt.o     \
      $(SRC)/query/row.o      \
      $(SRC)/query/rowutl.o   \
      $(SRC)/query/cursor.o   \
      $(SRC)/ifc/proc.o       \
      $(SRC)/ifc/nowproc.o    \
      $(SRC)/ifc/luaproc.o    \
      $(SRC)/ifc/nowdb.o

DEP = $(SRC)/types/version.h  \
      $(SRC)/types/types.h    \
      $(SRC)/types/errman.h   \
      $(HDR)/errcode.h        \
      $(SRC)/types/error.h    \
      $(SRC)/types/time.h     \
      $(SRC)/io/dir.h         \
      $(SRC)/io/file.h        \
      $(SRC)/task/lock.h      \
      $(SRC)/task/task.h      \
      $(SRC)/task/queue.h     \
      $(SRC)/task/worker.h    \
      $(SRC)/sort/sort.h      \
      $(SRC)/mem/lru.h        \
      $(SRC)/mem/ptlru.h      \
      $(SRC)/mem/pklru.h      \
      $(SRC)/mem/pplru.h      \
      $(SRC)/mem/plru8r.h     \
      $(SRC)/mem/blist.h      \
      $(SRC)/mem/t2tmap.h     \
      $(SRC)/store/store.h    \
      $(SRC)/store/storage.h  \
      $(SRC)/store/comp.h     \
      $(SRC)/store/indexer.h  \
      $(SRC)/store/storewrk.h \
      $(SRC)/scope/context.h  \
      $(SRC)/scope/scope.h    \
      $(SRC)/scope/loader.h   \
      $(SRC)/scope/procman.h  \
      $(SRC)/scope/dml.h      \
      $(SRC)/scope/ipc.h      \
      $(SRC)/index/index.h    \
      $(SRC)/index/man.h      \
      $(SRC)/reader/reader.h  \
      $(SRC)/model/types.h    \
      $(SRC)/model/model.h    \
      $(SRC)/text/text.h      \
      $(SRC)/fun/expr.h       \
      $(SRC)/fun/fun.h        \
      $(SRC)/fun/group.h      \
      $(SRC)/qplan/plan.h     \
      $(SRC)/query/rowutl.h   \
      $(SRC)/query/row.h      \
      $(SRC)/query/stmt.h     \
      $(SRC)/query/cursor.h   \
      $(SRC)/sql/ast.h        \
      $(SRC)/sql/lex.h        \
      $(SRC)/sql/nowdbsql.h   \
      $(SRC)/sql/state.h      \
      $(SRC)/sql/parser.h     \
      $(SRC)/ifc/proc.h       \
      $(SRC)/ifc/luaproc.h    \
      $(SRC)/ifc/nowdb.h

CLIENTDEP = $(HDR)/errcode.h  \
            $(HDR)/nowclient.h

IFC = include/nowdb/nowdb.h \
      include/nowdb/nowproc.h

default:	lib 

#all:	default tools tests bench server client
all:	default tools tests client lua server 

install:	lib server client tools
		cp lib/*.so /usr/local/lib
		cp bin/nowdbd /usr/local/bin
		cp bin/nowclient /usr/local/bin
		cp -r pynow /usr/local/
		cp -r lua /usr/local/
		cp -r include/nowdb /usr/local/include/

client_install:	client bin/nowclient
		cp lib/libnowdbclient.so /usr/local/lib
		cp bin/nowclient /usr/local/bin
		cp -r pynow /usr/local/
		cp -r include/nowdb /usr/local/include/

server:	$(BIN)/nowdbd lua

client:	$(BIN)/nowclient

tools:	bin/randomfile    \
	bin/readfile      \
	bin/catalog       \
	bin/waitscope     \
	bin/sqltool       \
	bin/plantool

#	bin/writecsv      \
#	bin/scopetool

bench: bin/readplainbench    \
       bin/writestorebench   \
       bin/writecontextbench \
       bin/readerbench       \
       bin/qstress           \
       bin/parserbench

smoke:	$(SMK)/errsmoke                \
	$(SMK)/timesmoke               \
	$(SMK)/pathsmoke               \
	$(SMK)/tasksmoke               \
	$(SMK)/queuesmoke              \
	$(SMK)/workersmoke             \
	$(SMK)/filesmoke               \
	$(SMK)/storesmoke              \
	$(SMK)/insertstoresmoke        \
	$(SMK)/insertvertexstoresmoke  \
	$(SMK)/insertandsortstoresmoke \
	$(SMK)/insertandsortvertexsmoke \
	$(SMK)/readersmoke             \
	$(SMK)/exprsmoke               \
	$(SMK)/filtersmoke             \
	$(SMK)/funsmoke                \
	$(SMK)/rowsmoke                \
	$(SMK)/pmansmoke               \
	$(SMK)/scopesmoke              \
	$(SMK)/imansmoke               \
	$(SMK)/indexsmoke              \
	$(SMK)/indexersmoke            \
	$(SMK)/modelsmoke              \
	$(SMK)/textsmoke               \
	$(SMK)/sortsmoke               \
	$(SMK)/msortsmoke              \
	$(SMK)/scopesmoke2             \
	$(SMK)/mergesmoke

clientsmoke:	$(CMK)/clientsmoke

stress:	$(STRESS)/deepscope

tests: smoke stress clientsmoke

debug:	CFLAGS += -g
debug:	default
debug:	tools
debug:	tests
debug:	bench

flags:
	$(FLGMSG)

.SUFFIXES: .c .o

.c.o:	$(DEP)
	$(CMPMSG)
	$(CC) $(CFLAGS) $(INC) -c -o $@ $<

.cpp.o:	$(DEP)
	$(CMPMSG)
	$(CXX) $(CFLAGS) $(INC) -c -o $@ $<

# Library
lib:	lib/libnowdb.so

lib/libnowdb.so:	$(OBJ) $(DEP)
			$(LNKMSG)
			$(CC) -shared \
			      -o $(OUTLIB)/libnowdb.so \
			         $(OBJ) $(libs)

# Client Library 
nowclientlib:		lib/libnowdbclient.so

lib/libnowdbclient.so:	$(SRL)/nowdbclient.o $(CLIENTDEP) \
                        $(SRC)/types/types.o \
                        $(SRC)/types/errman.o \
                        $(SRC)/types/error.o \
                        $(SRC)/types/time.o \
                        $(SRC)/query/rowutl.o
			$(LNKMSG)
			$(CC) -shared \
			      -o $(OUTLIB)/libnowdbclient.so \
                        	 $(SRC)/types/types.o \
                        	 $(SRC)/types/errman.o \
                        	 $(SRC)/types/error.o \
                        	 $(SRC)/types/time.o \
                        	 $(SRC)/query/rowutl.o \
			         $(SRL)/nowdbclient.o $(clibs)

rsclua:	rsc/lua/nowdb.lua rsc/lua/db.lua rsc/lua/hw.lua

rsc/lua/nowdb.lua:	lua/nowdb.lua
			cp lua/nowdb.lua rsc/lua/

rsc/lua/hw.lua:	lua/hw.lua
		cp lua/hw.lua rsc/lua/

rsc/lua/db.lua:	test/server/db.lua
		cp test/server/db.lua rsc/lua/

# Lua Client library
lua:	nowluaifc rsclua

nowluaifc:	$(LUA)/libnowluaifc.so

$(LUA)/libnowluaifc.so:	nowclientlib \
			$(LUA)/libluaifc.o
			$(LNKMSG)
			$(CC) -shared \
		      	-o $(LUA)/libnowluaifc.so \
		           $(LUA)/libluaifc.o -lnowdbclient -llua5.3

$(LUA)/libluaifc.o:	$(LUA)/libluaifc.c
			$(CMPMSG)
			$(CC) $(CFLAGS) $(INC) -c -o $@ $<

# Lemon
lemon/lemon.o:	lemon/lemon.c
		$(CMPMSG)
		$(CC) $(CFLAGS) $(INC) -c -o $@ $<

lemon/nowlemon:	lemon/lemon.o
		$(LNKMSG)
		$(CC) -o lemon/nowlemon lemon/lemon.o

/usr/local/bin/nowlemon:	lemon/nowlemon
				sudo cp lemon/nowlemon /usr/local/bin/

lemon:		/usr/local/bin/nowlemon

# sql parser stuff
$(SQL)/nowdbsql.o:	$(SQL)/nowdbsql.c $(DEP)
			$(CMPMSG)
			$(CC) $(CFLAGS) -DNDEBUG $(INC) -c -o $@ $<

$(SQL)/nowdbsql.c:	$(SQL)/nowdbsql.y
			$(LEMONMSG)
			$(LEM) $(SQL)/nowdbsql.y

$(SQL)/lex.o:	$(SQL)/lex.c $(SQL)/lex.h
		$(CMPMSG)
		$(CC) $(CFLAGS) -DYY_NO_UNPUT \
		                -DYY_NO_INPUT \
		$(INC) -c -o $@ $<

$(SQL)/lex.c:	$(SQL)/nowdbsql.l $(SQL)/nowdbsql.c
		$(LEXMSG)
		$(LEX)  --header-file=$(SQL)/lex.h \
			--outfile=$(SQL)/lex.c \
			$(SQL)/nowdbsql.l

# Smoke Tests
compileme:	$(SMK)/compileme

$(SMK)/compileme:	$(LIB) $(DEP) $(SMK)/compileme.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $(BIN)/compileme   \
					    $(OBJ)             \
					    $(SMK)/compileme.o \
			                    $(libs) -lnowdb

$(SMK)/errsmoke:	$(LIB) $(DEP) $(SMK)/errsmoke.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o \
			                 $(libs) -lnowdb

$(SMK)/timesmoke:	$(LIB) $(DEP) $(SMK)/timesmoke.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o \
			                 $(libs) -lnowdb

$(SMK)/pathsmoke:	$(LIB) $(DEP) $(SMK)/pathsmoke.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o \
			                 $(libs) -lnowdb

$(SMK)/tasksmoke:	$(LIB) $(DEP) $(SMK)/tasksmoke.o $(COM)/bench.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o     \
			                 $(COM)/bench.o \
			                 $(libs) -lnowdb

$(SMK)/queuesmoke:	$(LIB) $(DEP) $(SMK)/queuesmoke.o \
			$(COM)/bench.o $(COM)/math.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o     \
			                 $(COM)/bench.o \
			                 $(COM)/math.o  \
			                 $(libs) -lnowdb

$(SMK)/workersmoke:	$(LIB) $(DEP) $(SMK)/workersmoke.o \
			$(COM)/bench.o $(COM)/math.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o     \
			                 $(COM)/bench.o \
			                 $(COM)/math.o  \
			                 $(libs) -lnowdb

$(SMK)/filesmoke:	$(LIB) $(DEP) $(SMK)/filesmoke.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o \
			                 $(libs) -lnowdb

$(SMK)/storesmoke:	$(LIB) $(DEP) $(SMK)/storesmoke.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o \
			                 $(libs) -lnowdb

$(SMK)/insertstoresmoke:	$(LIB) $(DEP) $(SMK)/insertstoresmoke.o \
			        $(COM)/stores.o \
			        $(COM)/bench.o
				$(LNKMSG)
				$(CC) $(LDFLAGS) -o $@ $@.o      \
			                         $(COM)/stores.o \
			                         $(COM)/bench.o  \
				                 $(libs) -lnowdb

$(SMK)/insertvertexstoresmoke:	$(LIB) $(DEP) $(SMK)/insertvertexstoresmoke.o \
			        $(COM)/stores.o \
			        $(COM)/bench.o
				$(LNKMSG)
				$(CC) $(LDFLAGS) -o $@ $@.o      \
			                         $(COM)/stores.o \
			                         $(COM)/bench.o  \
				                 $(libs) -lnowdb

$(SMK)/readersmoke:	$(LIB) $(DEP) $(SMK)/readersmoke.o \
			$(COM)/stores.o \
			$(COM)/bench.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o      \
			                 $(COM)/stores.o \
			                 $(COM)/bench.o  \
				         $(libs) -lnowdb

$(SMK)/filtersmoke:	$(LIB) $(DEP) $(SMK)/filtersmoke.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o \
			                 $(libs) -lnowdb

$(SMK)/exprsmoke:	$(LIB) $(DEP) $(SMK)/exprsmoke.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o \
			                 $(libs) -lnowdb

$(SMK)/funsmoke:	$(LIB) $(DEP) $(SMK)/funsmoke.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o \
			                 $(libs) -lnowdb

$(SMK)/rowsmoke:	$(LIB) $(DEP) $(SMK)/rowsmoke.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o \
			                 $(libs) -lnowdb

$(SMK)/vrowsmoke:	$(LIB) $(DEP) $(SMK)/vrowsmoke.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o \
			                 $(libs) -lnowdb

$(SMK)/pmansmoke:	$(LIB) $(DEP) $(SMK)/pmansmoke.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o \
			                 $(libs) -lnowdb

$(SMK)/insertandsortstoresmoke:	$(LIB) $(DEP) $(SMK)/insertandsortstoresmoke.o \
			        $(COM)/stores.o \
			        $(COM)/bench.o
				$(LNKMSG)
				$(CC) $(LDFLAGS) -o $@ $@.o      \
			                         $(COM)/stores.o \
			                         $(COM)/bench.o  \
				                 $(libs) -lnowdb

$(SMK)/insertandsortvertexsmoke:	$(LIB) $(DEP) $(SMK)/insertandsortvertexsmoke.o \
			        	$(COM)/stores.o \
			   	     $(COM)/bench.o
					$(LNKMSG)
					$(CC) $(LDFLAGS) -o $@ $@.o      \
			                         $(COM)/stores.o \
			                         $(COM)/bench.o  \
				                 $(libs) -lnowdb

$(SMK)/scopesmoke:	$(LIB) $(DEP) $(SMK)/scopesmoke.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o \
			                 $(libs) -lnowdb

$(SMK)/scopesmoke2:	$(LIB) $(DEP) $(SMK)/scopesmoke2.o \
			$(COM)/scopes.o \
			$(COM)/bench.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o \
			                 $(COM)/scopes.o \
			                 $(COM)/bench.o  \
			                 $(libs) -lnowdb

$(SMK)/imansmoke: 	$(LIB) $(DEP) $(SMK)/imansmoke.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o \
			                 $(libs) -lnowdb

$(SMK)/indexsmoke: 	$(LIB) $(DEP) $(SMK)/indexsmoke.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o \
			                 $(libs) -lnowdb

$(SMK)/indexersmoke: 	$(LIB) $(DEP) $(SMK)/indexersmoke.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o \
			                 $(libs) -lnowdb

$(SMK)/modelsmoke: 	$(LIB) $(DEP) $(SMK)/modelsmoke.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o \
			                 $(libs) -lnowdb

$(SMK)/textsmoke: 	$(LIB) $(DEP) $(SMK)/textsmoke.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o \
			                 $(libs) -lnowdb

$(SMK)/sortsmoke: 	$(LIB) $(DEP) $(SMK)/sortsmoke.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o \
			                 $(libs) -lnowdb

$(SMK)/msortsmoke: 	$(LIB) $(DEP) $(SMK)/msortsmoke.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o \
			                 $(libs) -lnowdb


$(SMK)/mergesmoke:	$(LIB) $(DEP) $(SMK)/mergesmoke.o \
			$(COM)/scopes.o \
			$(COM)/db.o \
			$(COM)/bench.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o \
			                 $(COM)/scopes.o \
			                 $(COM)/db.o     \
			                 $(COM)/bench.o  \
			                 $(libs) -lnowdb
# STRESSTESTS
$(STRESS)/deepscope:	$(LIB) $(DEP) $(STRESS)/deepscope.o \
			              $(COM)/bench.o      \
			              $(COM)/cmd.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $(STRESS)/deepscope.o \
			              	       $(COM)/bench.o      \
			              	       $(COM)/cmd.o        \
			                 $(libs) -lnowdb
		
# BENCHMARKS
$(BIN)/readplainbench:	$(LIB) $(DEP) $(BENCH)/readplainbench.o \
			              $(COM)/progress.o         \
			              $(COM)/bench.o            \
			              $(COM)/cmd.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $(BENCH)/readplainbench.o \
			                       $(COM)/progress.o         \
			                       $(COM)/bench.o            \
			                       $(COM)/cmd.o              \
			                 $(libs) -lnowdb

$(BIN)/writestorebench:	$(LIB) $(DEP) $(BENCH)/writestorebench.o \
			              $(COM)/progress.o          \
			              $(COM)/bench.o             \
			              $(COM)/stores.o            \
			              $(COM)/cmd.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $(BENCH)/writestorebench.o \
			                       $(COM)/progress.o          \
			                       $(COM)/bench.o             \
			              	       $(COM)/stores.o            \
			                       $(COM)/cmd.o               \
			                 $(libs) -lnowdb

$(BIN)/readerbench:	$(LIB) $(DEP) $(BENCH)/readerbench.o \
			              $(COM)/progress.o          \
			              $(COM)/bench.o             \
			              $(COM)/cmd.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $(BENCH)/readerbench.o \
			                       $(COM)/progress.o          \
			                       $(COM)/bench.o             \
			                       $(COM)/cmd.o               \
			                 $(libs) -lnowdb

$(BIN)/writecontextbench:	$(LIB) $(DEP) $(BENCH)/writecontextbench.o \
			                      $(COM)/progress.o            \
			                      $(COM)/bench.o               \
			                      $(COM)/cmd.o
				$(LNKMSG)
				$(CC) $(LDFLAGS) -o $@ $(BENCH)/writecontextbench.o \
			                               $(COM)/progress.o            \
			              	               $(COM)/bench.o               \
			                               $(COM)/cmd.o                 \
			                 $(libs) -lnowdb

$(BIN)/qstress:		$(LIB) $(DEP) $(BENCH)/qstress.o \
			              $(COM)/cmd.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $(BENCH)/qstress.o \
			                       $(COM)/cmd.o       \
			                 $(libs) -lnowdb

$(BIN)/parserbench:	$(SQL)/lex.o $(SQL)/nowdbsql.o \
			$(SRC)/sql/ast.o $(SQL)/state.o $(SQL)/parser.o \
			$(BENCH)/parserbench.o $(COM)/bench.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $(BIN)/parserbench  \
			         $(SQL)/lex.o $(SQL)/nowdbsql.o \
			         $(SRC)/sql/ast.o $(SQL)/state.o \
			         $(SQL)/parser.o $(COM)/bench.o \
			         $(BENCH)/parserbench.o $(libs) -lnowdb
		
# Tools
$(BIN)/randomfile:	$(LIB) $(DEP) $(TOOLS)/randomfile.o \
			              $(COM)/cmd.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $(TOOLS)/randomfile.o \
			                 $(COM)/cmd.o                \
			                 $(libs) -lnowdb

$(BIN)/readfile:	$(LIB) $(DEP) $(TOOLS)/readfile.o \
			              $(COM)/cmd.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $(TOOLS)/readfile.o \
			                 $(COM)/cmd.o                \
			                 $(libs) -lnowdb

$(BIN)/catalog:		$(LIB) $(DEP) $(TOOLS)/catalog.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $(TOOLS)/catalog.o \
			                 $(libs) -lnowdb

$(BIN)/keepstoreopen:	$(LIB) $(DEP) $(TOOLS)/keepstoreopen.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $(TOOLS)/keepstoreopen.o \
			                 $(libs) -lnowdb

$(BIN)/waitstore:	$(LIB) $(DEP) $(TOOLS)/waitstore.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $(TOOLS)/waitstore.o \
			                 $(libs) -lnowdb

$(BIN)/waitscope:	$(LIB) $(DEP) $(TOOLS)/waitscope.o $(COM)/cmd.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $(TOOLS)/waitscope.o \
			                 $(COM)/cmd.o \
			                 $(libs) -lnowdb

$(BIN)/writecsv:	$(LIB) $(DEP) $(TOOLS)/writecsv.o \
			              $(COM)/bench.o      \
			              $(COM)/cmd.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $(TOOLS)/writecsv.o \
			              	       $(COM)/bench.o      \
			              	       $(COM)/cmd.o        \
			                       $(libs) -lnowdb

$(BIN)/sqltool:		$(LIB) $(DEP) $(TOOLS)/sqltool.o \
			              $(COM)/bench.o      \
			              $(COM)/cmd.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $(TOOLS)/sqltool.o \
			              	       $(COM)/bench.o     \
			              	       $(COM)/cmd.o       \
			                 $(libs) -lnowdb

$(BIN)/plantool:	$(LIB) $(DEP) $(TOOLS)/plantool.o \
			              $(COM)/bench.o      \
			              $(COM)/cmd.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $(TOOLS)/plantool.o \
			              	       $(COM)/bench.o     \
			              	       $(COM)/cmd.o       \
			                 $(libs) -lnowdb

$(BIN)/scopetool:	$(LIB) $(DEP) $(TOOLS)/scopetool.o \
			              $(COM)/bench.o      \
			              $(COM)/cmd.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $(TOOLS)/scopetool.o \
			              	       $(COM)/bench.o      \
			              	       $(COM)/cmd.o        \
			                 $(libs) -lnowdb

$(BIN)/nowdbd:		$(IFC) $(LIB) $(SRD)/nowdbd.o \
			              $(COM)/cmd.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $(SRD)/nowdbd.o \
			              	       $(COM)/cmd.o      \
			                 $(libs) -lnowdb

$(BIN)/nowclient:	$(CLIENTDEP) $(CLIENTLIB) \
			$(TOOLS)/nowclient.o \
			$(COM)/cmd.o \
			$(COM)/bench.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $(TOOLS)/nowclient.o \
			              	       $(COM)/cmd.o      \
			              	       $(COM)/bench.o      \
			                 $(clibs) -lnowdbclient

$(CMK)/clientsmoke:	$(CLIENTDEP) $(CLIENTLIB) \
			$(CMK)/clientsmoke.o \
			$(COM)/bench.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $(CMK)/clientsmoke.o \
			              	       $(COM)/bench.o      \
			                 $(clibs) -lnowdbclient


$(CMK)/clientsmoke2:	$(CLIENTDEP) $(CLIENTLIB) \
			$(CMK)/clientsmoke2.o \
			$(COM)/bench.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $(CMK)/clientsmoke2.o \
			              	       $(COM)/bench.o      \
			                 $(clibs) -lnowdbclient

# Clean up
clean:
	rm -f $(SRC)/*/*.o
	rm -f $(SRD)/*.o
	rm -f $(SRL)/*.o
	rm -f $(PYC)/*.pyc
	rm -rf $(PYC)/__pycache__
	rm -f $(TST)/*/*.o
	rm -f $(TST)/*/*.pyc
	rm -rf $(TST)/*/__pycache__
	rm -f $(COM)/*.o
	rm -f $(BENCH)/*.o
	rm -f $(TOOLS)/*.o
	rm -f $(SQL)/lex.c
	rm -f $(SQL)/lex.h
	rm -f $(SQL)/nowdbsql.h
	rm -f $(SQL)/nowdbsql.c
	rm -f $(SQL)/nowdbsql.out
	rm -f todo/bin/*.pyc
	rm -rf todo/bin/__pycache__
	rm -f lemon/*.o
	rm -f lemon/nowlemon
	rm -f $(OUTLIB)/libnowdb.so
	rm -f $(OUTLIB)/libnowdbclient.so
	rm -f $(OUTLIB)/libnowluaifc.so
	rm -f $(LUA)/libnowluaifc.so
	rm -f $(LUA)/*.o
	rm -f $(LOG)/*.log
	rm -f $(RSC)/*.db
	rm -f $(RSC)/*.dbz
	rm -f $(RSC)/*.bin
	rm -f $(RSC)/*.binz
	rm -f $(RSC)/*.csv
	rm -f $(RSC)/*.csv.zip
	rm -f $(RSC)/*.sql
	rm -f $(RSC)/*.err
	rm -rf $(RSC)/test
	rm -rf $(RSC)/teststore
	rm -rf $(RSC)/test?
	rm -rf $(RSC)/test??
	rm -rf $(RSC)/test???
	rm -rf $(RSC)/store?
	rm -rf $(RSC)/store??
	rm -rf $(RSC)/testscope
	rm -rf $(RSC)/scope?
	rm -rf $(RSC)/scope??
	rm -rf $(RSC)/scope???
	rm -rf $(RSC)/client???
	rm -rf $(RSC)/iman??
	rm -rf $(RSC)/pman??
	rm -rf $(RSC)/idx??
	rm -rf $(RSC)/ctx??
	rm -rf $(RSC)/db??
	rm -rf $(RSC)/db???
	rm -rf $(RSC)/idxdb??
	rm -rf $(RSC)/idxdb???
	rm -rf $(RSC)/vertex??
	rm -rf $(RSC)/model??
	rm -rf $(RSC)/text??
	rm -f  $(RSC)/lua/*.lua
	rm -f $(SMK)/errsmoke
	rm -f $(SMK)/timesmoke
	rm -f $(SMK)/pathsmoke
	rm -f $(SMK)/tasksmoke
	rm -f $(SMK)/queuesmoke
	rm -f $(SMK)/workersmoke
	rm -f $(SMK)/filesmoke
	rm -f $(SMK)/storesmoke
	rm -f $(SMK)/insertstoresmoke
	rm -f $(SMK)/insertvertexstoresmoke
	rm -f $(SMK)/insertstorevertexsmoke
	rm -f $(SMK)/readersmoke
	rm -f $(SMK)/exprsmoke
	rm -f $(SMK)/filtersmoke
	rm -f $(SMK)/funsmoke
	rm -f $(SMK)/rowsmoke
	rm -f $(SMK)/vrowsmoke
	rm -f $(SMK)/pmansmoke
	rm -f $(SMK)/insertandsortstoresmoke
	rm -f $(SMK)/insertandsortvertexsmoke
	rm -f $(SMK)/scopesmoke
	rm -f $(SMK)/scopesmoke2
	rm -f $(SMK)/imansmoke
	rm -f $(SMK)/indexsmoke
	rm -f $(SMK)/indexersmoke
	rm -f $(SMK)/modelsmoke
	rm -f $(SMK)/textsmoke
	rm -f $(SMK)/mergesmoke
	rm -f $(SMK)/sortsmoke
	rm -f $(SMK)/msortsmoke
	rm -f $(CMK)/clientsmoke
	rm -f $(CMK)/clientsmoke2
	rm -f $(STRESS)/deepscope
	rm -f $(BIN)/compileme
	rm -f $(BIN)/readfile
	rm -f $(BIN)/randomfile
	rm -f $(BIN)/readplainbench
	rm -f $(BIN)/writestorebench
	rm -f $(BIN)/writecontextbench
	rm -f $(BIN)/readerbench
	rm -f $(BIN)/parserbench
	rm -f $(BIN)/keepstoreopen
	rm -f $(BIN)/waitstore
	rm -f $(BIN)/waitscope
	rm -f $(BIN)/writecsv
	rm -f $(BIN)/sqltool
	rm -f $(BIN)/plantool
	rm -f $(BIN)/scopetool
	rm -f $(BIN)/scopetool2
	rm -f $(BIN)/qstress
	rm -f $(BIN)/catalog
	rm -f $(BIN)/nowdbd
	rm -f $(BIN)/nowclient
	rm -f $(DOC)/*/*.aux
	rm -f $(DOC)/*/*.log
	rm -f $(DOC)/*/*.out
	rm -f $(DOC)/*/*.toc
	rm -f $(DOC)/*/*.nav
	rm -f $(DOC)/*/*.snm
	rm -f $(DOC)/*/*.vrb

