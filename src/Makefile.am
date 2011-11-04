AM_CPPFLAGS = -D_GNU_SOURCE -D_POSIX_C_SOURCE 200809L
AM_LDFLAGS =

bin_PROGRAMS = csvdb
## dynamic lib later
noinst_LIBRARIES = libcsvdb.a

csvdb_SOURCES =
csvdb_CPPFLAGS =
csvdb_LDADD =
csvdb_SOURCES += client/main.c
csvdb_SOURCES += client/history.c
csvdb_SOURCES += client/tty.c
csvdb_CPPFLAGS += -I$(top_srcdir)/inc
csvdb_LDADD += libcsvdb.a

libcsvdb_a_SOURCES =
libcsvdb_a_CPPFLAGS =
libcsvdb_a_SOURCES += lib/lib.c
libcsvdb_a_SOURCES += lib/nvp.c
libcsvdb_a_SOURCES += lib/result.c
libcsvdb_a_SOURCES += lib/sql.c
libcsvdb_a_SOURCES += lib/table.c
libcsvdb_a_CPPFLAGS += -I$(top_srcdir)/inc
