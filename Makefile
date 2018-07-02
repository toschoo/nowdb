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

CFLAGS = -O3 -g -Wall -std=c99 -fPIC -D_GNU_SOURCE -D_POSIX_C_SOURCE=200809L
LDFLAGS = -L./lib

INC = -I./include -I./test -I./src -I./
LIB = lib

SRC = src/nowdb
SQL = $(SRC)/sql
HDR = include/nowdb
TST = test
COM = common
BENCH= bench
SMK = test/smoke
STRESS = test/stress
BIN = bin
LOG = log
TOOLS = tools
RSC = rsc
OUTLIB = lib
libs = -lm -ldl -lpthread -ltsalgo -lbeet -lzstd -lcsv

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
      $(SRC)/store/indexer.o  \
      $(SRC)/store/storewrk.o \
      $(SRC)/scope/context.o  \
      $(SRC)/scope/scope.o    \
      $(SRC)/scope/loader.o   \
      $(SRC)/index/index.o    \
      $(SRC)/index/compare.o  \
      $(SRC)/index/man.o      \
      $(SRC)/reader/reader.o  \
      $(SRC)/reader/filter.o  \
      $(SRC)/model/model.o    \
      $(SRC)/query/ast.o      \
      $(SRC)/query/stmt.o     \
      $(SRC)/query/plan.o     \
      $(SRC)/query/cursor.o   \
      $(SRC)/sql/lex.o        \
      $(SRC)/sql/nowdbsql.o   \
      $(SRC)/sql/state.o      \
      $(SRC)/sql/parser.o

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
      $(SRC)/store/indexer.h  \
      $(SRC)/store/storewrk.h \
      $(SRC)/scope/context.h  \
      $(SRC)/scope/scope.h    \
      $(SRC)/scope/loader.h   \
      $(SRC)/index/index.h    \
      $(SRC)/index/man.h      \
      $(SRC)/reader/reader.h  \
      $(SRC)/reader/filter.h  \
      $(SRC)/model/types.h    \
      $(SRC)/model/model.h    \
      $(SRC)/query/ast.h      \
      $(SRC)/query/stmt.h     \
      $(SRC)/query/plan.h     \
      $(SRC)/query/cursor.h   \
      $(SRC)/sql/lex.h        \
      $(SRC)/sql/nowdbsql.h   \
      $(SRC)/sql/state.h      \
      $(SRC)/sql/parser.h

default:	lib 

all:	default tools tests bench

tools:	bin/randomfile    \
	bin/readfile      \
	bin/catalog       \
	bin/keepstoreopen \
	bin/waitstore     \
	bin/waitscope     \
	bin/writecsv      \
	bin/scopetool

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
	$(SMK)/insertandsortstoresmoke \
	$(SMK)/readersmoke             \
	$(SMK)/filtersmoke             \
	$(SMK)/scopesmoke              \
	$(SMK)/imansmoke               \
	$(SMK)/indexsmoke              \
	$(SMK)/indexersmoke            \
	$(SMK)/modelsmoke              \
	$(SMK)/sqlsmoke

stress:	$(STRESS)/deepscope

tests: smoke stress

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

$(SMK)/sqlsmoke.o:	$(SMK)/sqlsmoke.c
			$(CMPMSG)
			$(CC) $(CFLAGS) $(INC) -c -o $@ $<

$(SMK)/sqlsmoke:	$(LIB) $(DEP) $(SMK)/sqlsmoke.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $(SMK)/sqlsmoke \
			         $(SMK)/sqlsmoke.o $(libs) -lnowdb
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
			$(SRC)/query/ast.o $(SQL)/state.o $(SQL)/parser.o \
			$(BENCH)/parserbench.o $(COM)/bench.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $(BIN)/parserbench  \
			         $(SQL)/lex.o $(SQL)/nowdbsql.o \
			         $(SRC)/query/ast.o $(SQL)/state.o \
			         $(SQL)/parser.o $(COM)/bench.o \
			         $(BENCH)/parserbench.o -lnowdb
		
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

$(BIN)/scopetool:	$(LIB) $(DEP) $(TOOLS)/scopetool.o \
			              $(COM)/bench.o      \
			              $(COM)/cmd.o
			$(LNKMSG)
			$(CC) $(LDFLAGS) -o $@ $(TOOLS)/scopetool.o \
			              	       $(COM)/bench.o      \
			              	       $(COM)/cmd.o        \
			                 $(libs) -lnowdb
# Clean up
clean:
	rm -f $(SRC)/*/*.o
	rm -f $(TST)/*/*.o
	rm -f $(COM)/*.o
	rm -f $(BENCH)/*.o
	rm -f $(TOOLS)/*.o
	rm -f $(SQL)/lex.c
	rm -f $(SQL)/lex.h
	rm -f $(SQL)/nowdbsql.h
	rm -f $(SQL)/nowdbsql.c
	rm -f $(SQL)/nowdbsql.out
	rm -f lemon/*.o
	rm -f lemon/nowlemon
	rm -f $(OUTLIB)/libnowdb.so
	rm -f $(LOG)/*.log
	rm -f $(RSC)/*.db
	rm -f $(RSC)/*.dbz
	rm -f $(RSC)/*.bin
	rm -f $(RSC)/*.binz
	rm -f $(RSC)/*.csv
	rm -f $(RSC)/*.csv.zip
	rm -f $(RSC)/*.sql
	rm -rf $(RSC)/test
	rm -rf $(RSC)/teststore
	rm -rf $(RSC)/store?
	rm -rf $(RSC)/store??
	rm -rf $(RSC)/testscope
	rm -rf $(RSC)/scope?
	rm -rf $(RSC)/scope??
	rm -rf $(RSC)/iman??
	rm -rf $(RSC)/idx??
	rm -rf $(RSC)/ctx??
	rm -rf $(RSC)/vertex??
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
	rm -f $(SMK)/filtersmoke
	rm -f $(SMK)/insertandsortstoresmoke
	rm -f $(SMK)/scopesmoke
	rm -f $(SMK)/imansmoke
	rm -f $(SMK)/indexsmoke
	rm -f $(SMK)/indexersmoke
	rm -f $(SMK)/modelsmoke
	rm -f $(SMK)/sqlsmoke
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
	rm -f $(BIN)/scopetool
	rm -f $(BIN)/qstress
	rm -f $(BIN)/catalog

