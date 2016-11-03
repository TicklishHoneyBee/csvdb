AM_CPPFLAGS =
AM_CFLAGS = -std=gnu99
AM_LDFLAGS =

bin_PROGRAMS = csvdb
## libtool library
lib_LTLIBRARIES = libcsvdb.la

## vars
topinc = $(top_srcdir)/inc

## beef
csvdb_SOURCES =
csvdb_CPPFLAGS =
csvdb_LDADD =
csvdb_SOURCES += client/main.c
if !USE_READLINE
csvdb_SOURCES += client/history.c
csvdb_SOURCES += client/tty.c
endif
csvdb_SOURCES += client/rdln.c
csvdb_CPPFLAGS += -I$(topinc)
csvdb_LDADD += libcsvdb.la
if USE_READLINE
csvdb_LDADD += $(READLINE_LIBS)
endif

libcsvdb_la_SOURCES =
libcsvdb_la_CPPFLAGS =
libcsvdb_la_SOURCES += lib/lib.c
libcsvdb_la_SOURCES += lib/nvp.c
libcsvdb_la_SOURCES += lib/result.c
libcsvdb_la_SOURCES += lib/sql.c
libcsvdb_la_SOURCES += lib/table.c
libcsvdb_la_SOURCES += lib/row.c
libcsvdb_la_SOURCES += lib/column.c
libcsvdb_la_CPPFLAGS += -I$(topinc)

## headers for library
include_HEADERS = $(topinc)/csvdb.h

## we don't install client headers, so just list them here
EXTRA_csvdb_SOURCES =
EXTRA_csvdb_SOURCES += $(topinc)/csvdb-client.h
