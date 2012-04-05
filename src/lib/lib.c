/************************************************************************
* lib.c
* Copyright (C) Lisa Milne 2011-2012 <lisa@ltmnet.com>
*
* This program is free software: you can redistribute it and/or modify it
* under the terms of the GNU General Public License as published by the
* Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful, but
* WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
* See the GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License along
* with this program.  If not, see <http://www.gnu.org/licenses/>
************************************************************************/

#include "csvdb.h"
#if defined HAVE_CONFIG_H
# include "config.h"
#endif	/* HAVE_CONFIG_H */
#if defined USE_GETTEXT
/* gnu gettext */
# include <locale.h>
# include "gettext.h"
#define _(string) gettext(string)
#else
#define _(__s) __s
#endif	/* USE_GETTEXT */

unsigned int csvdb_settings = CSVDB_SET_VOID;
static char* keywords[] = {
	"SELECT",
	"ALL",
	"DISTINCT",
	"DISTINCTROW",
	"FROM",
	"WHERE",
	"LIMIT",
	"OFFSET",
	"GROUP",
	"HAVING",
	"ORDER",
	"INTO",
	NULL
};

int csvdb_init()
{
#if defined USE_GETTEXT
	setlocale(LC_ALL, "");
	bindtextdomain(PACKAGE, LOCALEDIR);
	textdomain(PACKAGE);
#endif	/* USE_GETTEXT */
	return 0;
}

int is_keyword(char* w)
{
	int i;

	for (i=0; keywords[i]; i++) {
		if (!strcasecmp(w,keywords[i]))
			return 1;
	}

	return 0;
}

int is_numeric(char* w)
{
	int i;

	for (i=0; w[i]; i++) {
		if (!isdigit(w[i]))
			return 0;
	}

	return 1;
}

int remove_wildcard(char* buff, char* str)
{
	int i;
	int o = 0;
	for (i=0; str[i]; i++) {
		if (str[i] == '%') {
			if (str[i+1] == '%') {
				i++;
			}else{
				continue;
			}
		}
		buff[o++] = str[i];
	}

	buff[o] = 0;
	return o;
}

static char* s_csvdb_errors[] = {
	"CSVDB_ERROR_NONE",
	"CSVDB_ERROR_INTERNAL",
	"CSVDB_ERROR_SYNTAX",
	"CSVDB_ERROR_TABLEREF",
	"CSVDB_ERROR_COLUMNREF",
	"CSVDB_ERROR_FILEREF",
	"CSVDB_ERROR_FILEEXISTS",
	"CSVDB_ERROR_UNSUPPORTED",
	"CSVDB_ERROR_OUTOFRANGE",
	"CSVDB_ERROR_BADRESULT",
	"CSVDB_ERROR_SUBQUERY"
};

int csvdb_print_result(result_t *res)
{
	row_t *row;
	column_ref_t *col;
	column_ref_t *c;
	table_ref_t *tbl;
	nvp_t *cd;
	nvp_t *t;
	row_t *r;
	int l;
	char buff[1024];
	float tm;
	char* v;
	int rc;
	if (!res) {
		printf(_("invalid result set\n"));
		return -1;
	}
	tm = (float)res->time/1000.0;
	r = res->result;
	rc = row_count(r);

	if (res->error) {
		t = res->error;
		while (t) {
			switch (t->num) {
			case CSVDB_ERROR_NONE:
				break;
			case CSVDB_ERROR_INTERNAL:
				printf(_("An internal error occured\n"));
				break;
			case CSVDB_ERROR_SYNTAX:
				printf(_("Syntax Error near '%s'\n"),t->value);
				break;
			case CSVDB_ERROR_TABLEREF:
				printf(_("Invalid Table Reference '%s'\n"),t->value);
				break;
			case CSVDB_ERROR_COLUMNREF:
				printf(_("Invalid Column Reference '%s'\n"),t->value);
				break;
			case CSVDB_ERROR_FILEREF:
				printf(_("Invalid File Reference '%s'\n"),t->value);
				break;
			case CSVDB_ERROR_FILEEXISTS:
				printf(_("File '%s' can not be written as it already exists\n"),t->value);
				break;
			case CSVDB_ERROR_UNSUPPORTED:
				printf(_("Unsupported SQL '%s'\n"),t->value);
				break;
			case CSVDB_ERROR_OUTOFRANGE:
				printf(_("Reference is Out Of Range '%s'\n"),t->value);
				break;
			case CSVDB_ERROR_BADRESULT:
				printf(_("Bad Result Set '%s'\n"),t->value);
				break;
			case CSVDB_ERROR_SUBQUERY:
				printf(_("Error in SubQuery: %s\n"),t->value);
				break;
			default:
				printf(_("Unknown error (%d): %s\n"),t->num,t->value);
			}
			t = t->next;
		}
	}

	if (!rc) {
		if (res->ar) {
			printf(_("-----\n%d rows affected in %.3f seconds\n\n"),res->ar,tm);
			return res->ar;
		}
		printf(_("0 rows returned\n"));
		return 0;
	}

	if (!res->error) {
		printf(_("formatting results...\n"));
		col = res->cols;
		while (col) {
			if (col->alias[0]) {
				col->num = strlen(col->alias);
			}else{
				col->num = strlen(col->name);
				if (res->table && res->table->next) {
					tbl = table_resolve(col->table->name->value,res);
					if (tbl->alias && tbl->alias[0]) {
						col->num += strlen(tbl->alias)+1;
					}else if (tbl->t->name->next) {
						col->num += strlen(tbl->t->name->next->value)+1;
					}else{
						col->num += strlen(tbl->t->name->value)+1;
					}
				}
			}
			col = col->next;
		}
		row = r;
		while (row) {
			cd = row->data;
			c = res->cols;
			while (cd && c) {
				l = strlen(cd->value);
				if (l > c->num)
					c->num = l;
				cd = cd->next;
				c = c->next;
			}
			row = row->next;
		}
		col = res->cols;
		l = 0;
		while (col) {
			l += col->num+3;
			col = col->next;
		}
		l += 2;
		v = alloca(sizeof(char)*(l));
		v[0] = '+';
		l = 1;
		col = res->cols;
		while (col) {
			memset(v+l,'-',col->num+2);
			l += col->num+2;
			v[l] = '+';
			l++;
			col = col->next;
		}
		v[l] = '\0';

		puts(v);
		col = res->cols;
		while (col) {
			if (col->alias[0]) {
				printf("| %*s ",col->num,col->alias);
			}else{
				if (res->table && res->table->next) {
					tbl = table_resolve(col->table->name->value,res);
					if (tbl->alias && tbl->alias[0]) {
						sprintf(buff,"%s.%s",tbl->alias,col->name);
						printf("| %*s ",col->num,buff);
					}else if (tbl->t->name->next) {
						sprintf(buff,"%s.%s",tbl->t->name->next->value,col->name);
						printf("| %*s ",col->num,buff);
					}else{
						sprintf(buff,"%s.%s",tbl->t->name->value,col->name);
						printf("| %*s ",col->num,buff);
					}
				}else{
						printf("| %*s ",col->num,col->name);
				}
			}
			col = col->next;
		}
		printf("|\n");
		puts(v);

		row = r;
		while (row) {
			cd = row->data;
			c = res->cols;
			while (cd && c) {
				printf("| %*s ",c->num,cd->value);
				cd = cd->next;
				c = c->next;
			}
			printf("|\n");
			row = row->next;
		}
		puts(v);
	}

	printf(_("%d rows returned in %.3f seconds\n\n"),rc,tm);

	return rc;
}

void error(result_t *r, int err, char* ref)
{
	nvp_t *n;
	if (!r)
		return;

	if (csvdb_settings&CSVDB_SET_DEBUG)
		printf("DEBUG: %s error near '%s'",s_csvdb_errors[err],ref);

	n = nvp_add(&r->error,NULL,ref);
	n->num = err;
}
