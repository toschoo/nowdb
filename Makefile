# =========================================================================
# Makefile for NOWDB
# (c) Tobias Schoofs, 2018
# =========================================================================
CC = @gcc
CXX = @g++
AR = @ar

LNKMSG = @printf "linking   $@\n"
CMPMSG = @printf "compiling $@\n"

FLGMSG = @printf "CFLAGS: $(CFLAGS)\nLDFLAGS: $(LDFLAGS)\n"

INSMSG = @printf ". setenv.sh"

CFLAGS = -O3 -Wall -std=c99 -fPIC -D_GNU_SOURCE -D_POSIX_C_SOURCE=200809L
LDFLAGS = -L./lib

INC = -I./include -I./test -I./src -I./
LIB = lib

SRC = src/nowdb
HDR = include/nowdb
TST = test
COM = common
BENCH= bench
SMK = test/smoke
BIN = bin
LOG = log
TOOLS = tools
RSC = rsc
OUTLIB = lib
libs = -lm -lpthread -ltsalgo -lzstd

OBJ = $(SRC)/types/types.o    \
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
      $(SRC)/store/store.o    \
      $(SRC)/store/comp.o     \
      $(SRC)/store/storewrk.o \
      $(SRC)/scope/context.o  \
      $(SRC)/scope/scope.o    \
      $(SRC)/reader/reader.o

DEP = $(SRC)/types/types.h    \
      $(SRC)/types/errman.h   \
      $(SRC)/types/error.h    \
      $(SRC)/types/time.h     \
      $(SRC)/io/dir.h         \
      $(SRC)/io/file.h        \
      $(SRC)/task/lock.h      \
      $(SRC)/task/task.h      \
      $(SRC)/task/queue.h     \
      $(SRC)/task/worker.h    \
      $(SRC)/sort/sort.h      \
      $(SRC)/store/store.h    \
      $(SRC)/store/comp.h     \
      $(SRC)/store/storewrk.h \
      $(SRC)/scope/context.h  \
      $(SRC)/scope/scope.h    \
      $(SRC)/reader/reader.h

default:	lib 

all:	default tools tests bench

tools:	bin/randomfile    \
	bin/readfile      \
	bin/catalog       \
	bin/keepstoreopen \
	bin/waitstore     \

bench: bin/readplainbench    \
       bin/writestorebench   \
       bin/writecontextbench \
       bin/readerbench       \
       bin/qstress

smoke:	$(SMK)/errsmoke                \
	$(SMK)/timesmoke               \
	$(SMK)/pathsmoke               \
	$(SMK)/tasksmoke               \
	$(SMK)/queuesmoke              \
	$(SMK)/workersmoke             \
	$(SMK)/filesmoke               \
	$(SMK)/storesmoke              \
	$(SMK)/insertstoresmoke        \
	$(SMK)/insertandsortstoresmoke \
	$(SMK)/readersmoke             \
	$(SMK)/scopesmoke

tests: smoke

debug:	CFLAGS += -g
debug:	default
debug:	tools
debug:	tests
debug:	bench

flags:
	$(FLGMSG)

.SUFFIXES: .c .o

.c.o:
	$(CMPMSG)
	$(CC) $(CFLAGS) $(INC) -c -o $@ $<

.cpp.o:
	$(CMPMSG)
	$(CXX) $(CFLAGS) $(INC) -c -o $@ $<

# Tests and demos

# Tools

# Library
lib:	lib/libnowdb.so

lib/libnowdb.so:	$(OBJ) $(DEP)
			$(LNKMSG)
			$(CC) -shared \
			      -o $(OUTLIB)/libnowdb.so \
			         $(OBJ) $(libs)
			
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

$(SMK)/readersmoke:	$(LIB) $(DEP) $(SMK)/readersmoke.o \
			$(COM)/stores.o \
			$(COM)/bench.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $@.o      \
			                 $(COM)/stores.o \
			                 $(COM)/bench.o  \
				         $(libs) -lnowdb


$(SMK)/insertandsortstoresmoke:	$(LIB) $(DEP) $(SMK)/insertandsortstoresmoke.o \
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
# Clean up
clean:
	rm -f $(SRC)/*/*.o
	rm -f $(TST)/*/*.o
	rm -f $(COM)/*.o
	rm -f $(BENCH)/*.o
	rm -f $(TOOLS)/*.o
	rm -f $(OUTLIB)/libnowdb.so
	rm -f $(LOG)/*.log
	rm -f $(RSC)/*.db
	rm -f $(RSC)/*.dbz
	rm -f $(RSC)/*.bin
	rm -f $(RSC)/*.binz
	rm -rf $(RSC)/teststore
	rm -rf $(RSC)/store?
	rm -rf $(RSC)/store??
	rm -rf $(RSC)/testscope
	rm -rf $(RSC)/scope?
	rm -rf $(RSC)/scope??
	rm -f $(SMK)/errsmoke
	rm -f $(SMK)/timesmoke
	rm -f $(SMK)/pathsmoke
	rm -f $(SMK)/tasksmoke
	rm -f $(SMK)/queuesmoke
	rm -f $(SMK)/workersmoke
	rm -f $(SMK)/filesmoke
	rm -f $(SMK)/storesmoke
	rm -f $(SMK)/insertstoresmoke
	rm -f $(SMK)/readersmoke
	rm -f $(SMK)/insertandsortstoresmoke
	rm -f $(SMK)/scopesmoke
	rm -f $(BIN)/compileme
	rm -f $(BIN)/readfile
	rm -f $(BIN)/randomfile
	rm -f $(BIN)/readplainbench
	rm -f $(BIN)/writestorebench
	rm -f $(BIN)/writecontextbench
	rm -f $(BIN)/readerbench
	rm -f $(BIN)/keepstoreopen
	rm -f $(BIN)/waitstore
	rm -f $(BIN)/qstress
	rm -f $(BIN)/catalog

