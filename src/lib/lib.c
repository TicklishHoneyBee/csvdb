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

int get_column_id(char* buff, result_t *r, char* str)
{
	char *c;
	char* nve;
	char* nv;
	int k;
	c = strchr(str,'(');

	nv = c+1;
	nve = strchr(nv,')');
	if (nve)
		*nve = 0;
	k = atoi(nv);
	if (k < 0 || !nvp_grabi(r->table->columns,k-1)) {
		printf("'COLUMN(%d)' is out of range in %s\r\n",k,r->table->name->value);
		return 1;
	}
	sprintf(buff,"%d",k);
	if (nve)
		*nve = ')';
	return 0;
}

int csvdb_print_result(result_t *res)
{
	row_t *row;
	nvp_t *col;
	nvp_t *t;
	row_t *r;
	float tm;
	char* s = "";
	int rc;
	if (!res) {
		printf("invalid result set\r\n");
		return -1;
	}
	r = res->result;
	rc = row_count(r);
	if (rc != 1)
		s = "s";

	if (!rc) {
		if (res->ar) {
			s = "";
			if (res->ar != 1)
				s = "s";
			tm = (float)res->time/1000.0;
			printf("-----\r\n%d row%s affected in %.3f seconds\r\n\r\n",res->ar,s,tm);
			return res->ar;
		}
		printf("0 row%s returned\r\n",s);
		return 0;
	}
	col = res->cols;
	while (col) {
		t = nvp_search(col->child,"AS");
		if (!t || !t->next) {
			printf(" %s",col->value);
		}else{
			printf(" %s",t->next->value);
		}
		col = col->next;
	}
	printf("\r\n");

	row = r;
	while (row) {
		col = row->data;
		while (col) {
			if (col->prev) {
				printf(" | %s",col->value);
			}else{
				printf("%s",col->value);
			}
			col = col->next;
		}
		printf("\r\n");
		row = row->next;
	}
	tm = (float)res->time/1000.0;
	printf("-----\r\n%d row%s returned in %.3f seconds\r\n\r\n",rc,s,tm);
	return rc;
}
