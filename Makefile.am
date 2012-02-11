ACLOCAL_AMFLAGS = -I m4 

SUBDIRS = po src

man1_MANS =
man1_MANS += man/csvdb.1

EXTRA_DIST = config.rpath m4/ChangeLog 
EXTRA_DIST += $(man1_MANS)
EXTRA_DIST += doc/csvdb-doc.html
EXTRA_DIST += doc/queries
EXTRA_DIST += test-unnamed.csv
EXTRA_DIST += test.csv
EXTRA_DIST += LICENSE
EXTRA_DIST += CHANGES

fresh: clean all
