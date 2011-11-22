/************************************************************************
* lib.c
* Copyright (C) Lisa Milne 2011 <lisa@ltmnet.com>
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

int csvdb_print_result(result_t *res)
{
	row_t *row;
	column_ref_t *col;
	column_ref_t *c;
	nvp_t *cd;
	nvp_t *t;
	row_t *r;
	int l;
	float tm;
	char* v;
	char* s = "";
	int rc;
	if (!res) {
		printf("invalid result set\n");
		return -1;
	}
	tm = (float)res->time/1000.0;
	r = res->result;
	rc = row_count(r);
	if (rc != 1)
		s = "s";

	if (res->error) {
		t = res->error;
		while (t) {
			printf("%d: %s",t->num,t->value);
			t = t->next;
		}
	}

	if (!rc) {
		if (res->ar) {
			s = "";
			if (res->ar != 1)
				s = "s";
			printf("-----\n%d row%s affected in %.3f seconds\n\n",res->ar,s,tm);
			return res->ar;
		}
		printf("0 row%s returned\n",s);
		return 0;
	}

	if (!res->error) {
		printf("formatting results...\n");
		col = res->cols;
		while (col) {
			if (col->alias[0]) {
				col->num = strlen(col->alias);
			}else{
				col->num = strlen(col->name);
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
		l++;
		v = alloca(sizeof(char)*(l+1));
		memset(v,'-',l);
		v[l] = '\0';

		puts(v);
		col = res->cols;
		while (col) {
			if (col->alias[0]) {
				printf("| %*s ",col->num,col->alias);
			}else{
				printf("| %*s ",col->num,col->name);
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

	printf("%d row%s returned in %.3f seconds\n\n",rc,s,tm);

	return rc;
}

void error(result_t *r, int err, char* str, ...)
{
	va_list ap;
	char buff[1024];
	nvp_t *n;
	if (!r)
		return;

	va_start(ap, str);
	vsnprintf(buff, 1024, str, ap);
	va_end(ap);

	/* TODO: should be able to do something like `SET DEBUG ON' to make
	 * this show without needing to recompile */
	puts(buff);

	n = nvp_add(&r->error,NULL,buff);
	n->num = err;
}
