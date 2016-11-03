/************************************************************************
* table.c
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

static struct table_settings_s {
	char sep;
	char enc;
	char esc;
} table_settings = {
	',',
	'"',
	'\\'
};

table_t *tables = NULL;

table_t *table_add()
{
	table_t *t = malloc(sizeof(table_t));
	t->name = NULL;
	t->columns = NULL;
	t->rows = NULL;
	t->key = 0;
	t->next = NULL;
	t->prev = NULL;

	if (tables) {
		table_t *p = tables;
		while (p->next) {
			p = p->next;
		}
		p->next = t;
		t->prev = p;
	}else{
		tables = t;
	}

	return t;
}

int table_next_key(table_t *t)
{
	if (!t)
		return 0;
	return t->key++;
}

table_t *table_load_apache(char* file)
{
	FILE *f;
	char c = ' ';
	char* p;
	char fbuff[2048];
	char buff[1024];
	int key;
	int b;
	int l;
	int tm;
	size_t r;
	int cc = 0;
	int ccc = 0;
	int fl;
	int fp;
	nvp_t *n;
	nvp_t **row = NULL;
	row_t **att;
	row_t *rw;
	table_t *t = table_find(file);

	if (t)
		return t;

	f = fopen(file,"r");
	if (!f)
		return NULL;

	t = table_add();
	t->name = nvp_create(NULL,file);
	key = table_next_key(t);

	b = 0;
	l = 0;
	tm = 0;
	r = 1;
	fl = 0;
	fp = 0;
	att = &t->rows;
	while (r > 0) {
		if (fp == fl) {
			r = fread(fbuff,1,2048,f);
			fl = r;
			fp = 0;
		}
		if (r < 1)
			goto column_end;
		c = fbuff[fp++];
		if (tm) {
			if (c == ']') {
				if (b && buff[b-1] == '\\') {
					b--;
				}else{
					tm = 0;
					if (b < 1023)
						buff[b++] = c;
					goto column_end;
				}
			}else if (c == '\n') {
				if (!b || buff[b-1] != '\r')
					buff[b++] = '\r';
			}
		}else if (c == '"') {
			continue;
		}else if (!b && c == '[') {
			tm = 1;
		}else if (!b && c != '\n' && isspace(c)) {
			continue;
		}else if (c == ' ' || c == '\n') {
column_end:
			buff[b] = 0;
			if (!b && ccc != cc) {
				b = 0;
				continue;
			}
			if (!l) {
				nvp_add(&t->columns,NULL,buff);
				cc++;
			}
			if (!ccc) {
				if (!b && fp == fl) {
					break;
				}
				rw = row_add(att,key);
				att = &rw;
				row = &rw->data;
			}

			b = 0;
			nvp_add(row,NULL,buff);
			ccc++;

			if (c == '\n') {
				l++;
				ccc = 0;
				key = table_next_key(t);
			}
			continue;
		}
		if (b < 1023)
			buff[b++] = c;
	}

	t->rc = l;

	if (!t->columns) {
		table_free(file);
		t = NULL;
	}else{
		nvp_t *c;
		n = t->columns;
		t->columns = NULL;
		c = n;
		b = 1;
		l = 0;
		while (c) {
			if (c->value[0] == '[') {
				nvp_add(&t->columns,NULL,"datetime");
			}else if (is_numeric(c->value)) {
				l = strlen(c->value);
				if (l == 3) {
					nvp_add(&t->columns,NULL,"response");
				}else{
					nvp_add(&t->columns,NULL,"length");
				}
			}else if (!strcasecmp(c->value,"GET") || !strcasecmp(c->value,"POST") || !strcasecmp(c->value,"HEAD") || !strcasecmp(c->value,"PUT")) {
				nvp_add(&t->columns,NULL,"request");
				l = 1;
			}else if (l) {
				nvp_add(&t->columns,NULL,"url");
				l = 0;
			}else if (strstr(c->value,"HTTP/1")) {
				nvp_add(&t->columns,NULL,"protocol");
			}else if ((p = strchr(c->value,'.')) && (p = strchr(p,'.')) && (p = strchr(p,'.'))) {
				nvp_add(&t->columns,NULL,"IPaddress");
			}else{
				sprintf(buff,"unknown%d",b++);
				nvp_add(&t->columns,NULL,buff);
			}
			c = c->next;
		}
		nvp_free_all(n);
	}

	fclose(f);

	return t;
}

table_t *table_load_csv(char* file, nvp_t *cols, nvp_t *opts)
{
	static char *envi = (char*)-1;
	FILE *f;
	char c = ' ';
	char fbuff[2048];
	char buff[1024];
	char sep = table_settings.sep;
	char enc = table_settings.enc;
	char esc = table_settings.esc;
	int key;
	int b;
	int l;
	int s;
	int se;
	int fl;
	int fp;
	size_t r;
	int cc = 0;
	int ccc = 0;
	nvp_t *n;
	nvp_t **row = NULL;
	row_t **att;
	row_t *rw;
	table_t *t = table_find(file);

	if (t)
		return t;

	f = fopen(file,"r");
	if (!f)
		return NULL;

	if (envi == (char*)-1 && (envi = getenv("DBDELIMITER")) != NULL) {
		table_settings.sep = envi[0];
		sep = envi[0];
	}

	if (opts) {
		n = opts;
		while (n) {
			if (!strcmp(n->name,"sep")) {
				sep = n->value[0];
			}else if (!strcmp(n->name,"enc")) {
				enc = n->value[0];
			}else if (!strcmp(n->name,"esc")) {
				esc = n->value[0];
			}
			n = n->next;
		}
	}

	t = table_add();
	t->name = nvp_create(NULL,file);
	key = table_next_key(t);

	if (cols) {
		n = cols;
		while (n) {
			nvp_add(&t->columns,n->name,n->value);
			cc++;
			n = n->next;
		}
	}

	b = 0;
	l = 0;
	s = 0;
	se = 0;
	r = 1;
	fl = 0;
	fp = 0;
	att = &t->rows;
	while (r > 0) {
		if (fp == fl) {
			r = fread(fbuff,1,2048,f);
			fl = r;
			fp = 0;
		}
		if (r < 1)
			goto column_end;
		c = fbuff[fp++];
		if (s) {
			if (c == enc) {
				if (b && buff[b-1] == esc) {
					b--;
				}else{
					s = 0;
					se = 1;
					goto column_end;
				}
			}
			if (c == '\n') {
				if (!b || buff[b-1] != '\r')
					buff[b++] = '\r';
			}
		}else if (!b && c == enc) {
			s = 1;
			continue;
		}else if ((!b || (!cols && !l)) && isspace(c) && c != '\n') {
			continue;
		}else if (!cols && !l && c == '-') {
			continue;
		}else if (c == sep || c == '\n') {
			if (se) {
				b = 0;
				se = 0;
				if (c == '\n')
					goto row_end;
				continue;
			}
column_end:
			buff[b] = 0;
			if (!cols && !l) {
				nvp_add(&t->columns,NULL,buff);
				cc++;
			}else{
				if (!ccc) {
					if (!b && fp == fl)
						break;
					rw = row_add(att,key);
					att = &rw;
					row = &rw->data;
				}
				nvp_add(row,NULL,buff);
				ccc++;
			}
row_end:
			if (c == '\n') {
				l++;
				ccc = 0;
				key = table_next_key(t);
			}
			b = 0;
			continue;
		}
		if (b < 1023)
			buff[b++] = c;
	}

	t->rc = l;

	fclose(f);

	return t;
}

table_t *table_find(char* name)
{
	nvp_t *n;
	table_t *t = tables;
	char* nm;

	if (name[0] == '`' && (nm = strchr(name+1,'`'))) {
		char* tmp = nm;
		*tmp = 0;
		nm = alloca(strlen(name));
		strcpy(nm,name+1);
		name = nm;
		*tmp = '`';
	}

	while (t) {
		n = t->name;
		while (n) {
			if (!strcmp(n->value,name))
				goto table_find_end;
			n = n->next;
		}
		t = t->next;
	}

table_find_end:
	return t;
}

void table_free(char* name)
{
	table_t *t = table_find(name);
	if (!t)
		return;

	if (t->prev) {
		t->prev->next = t->next;
		if (t->next)
			t->next->prev = t->prev;
	}else{
		tables = t->next;
		if (tables)
			tables->prev = NULL;
	}

	row_free_all(t->rows);
	nvp_free_all(t->columns);
	nvp_free_all(t->name);
	free(t);
}

int table_write(table_t *t, char* of, nvp_t *opts)
{
	FILE *f;
	row_t *r;
	nvp_t *v;
	char* q;
	char* p;
	char sep = table_settings.sep;
	char enc = table_settings.enc;
	char esc = table_settings.esc;
	char escenc[3];
	if (!t)
		return -1;
	if (!of)
		of = t->name->value;

	if (!strcmp(of,"-")) {
		f = stdout;
	}else{
		f = fopen(of,"w");
	}
	if (!f)
		return -2;

	if (opts) {
		v = opts;
		while (v) {
			if (!strcmp(v->name,"sep")) {
				sep = v->value[0];
			}else if (!strcmp(v->name,"enc")) {
				enc = v->value[0];
			}else if (!strcmp(v->name,"esc")) {
				esc = v->value[0];
			}
			v = v->next;
		}
	}

	sprintf(escenc,"%c%c",esc,enc);

	v = t->columns;
	while (v) {
		if (v->prev)
			fputc(sep,f);
		fputs(v->value,f);
		v = v->next;
	}
	fputs("\n",f);

	r = t->rows;
	while (r) {
		v = r->data;
		while (v) {
			if (v->prev)
				fputc(sep,f);
			if (strchr(v->value,sep) || strchr(v->value,'\n') || strchr(v->value,' ') || strchr(v->value,enc)) {
				p = v->value;
				fputc(enc,f);
				while ((q = strchr(p,enc))) {
					*q = 0;
					fputs(p,f);
					fputs(escenc,f);
					*q = enc;
					p = q+1;
				}
				fputs(p,f);
				fputc(enc,f);
			}else{
				fputs(v->value,f);
			}
			v = v->next;
		}
		fputs("\n",f);
		r = r->next;
	}

	fclose(f);

	return 0;
}

/* resolve a string to a table in a query */
table_ref_t *table_resolve(char* str, result_t *r)
{
	char* tbl;
	char* rdup;
	char* tmp;
	table_ref_t *t = NULL;
	int l;
	if (!str || !r) {
		error(r,CSVDB_ERROR_TABLEREF,str);
		return NULL;
	}

	l = strlen(str);
	rdup = alloca(l+1);
	strcpy(rdup,str);

	tbl = rdup;

	if (!tbl || !tbl[0]) {
		error(r,CSVDB_ERROR_TABLEREF,str);
		return NULL;
	}

	if (tbl[0] == '`' && (tmp = strchr(tbl+1,'`')) && !tmp[1]) {
		tbl++;
		*tmp = 0;
	}

	/* this is resolving a table somewhere other than a table selection */
	if (r->table) {
		t = r->table;
		if (!strcasecmp(tbl,"FILE")) {
			return t;
		}else{
			while (t) {
				if (!strcmp(tbl,t->alias) || nvp_search(t->t->name,tbl))
					return t;
				t = t->next;
			}
		}
	/* this is resolving a table in a table selection clause */
	}else{
		table_t *tab = table_find(tbl);
		if (!tab) {
			if (!strcasecmp(tbl,"FILE")) {
				tab = tables;
			}else{
				tab = table_load_csv(tbl,NULL,NULL);
			}
		}

		if (tab) {
			t = malloc(sizeof(table_ref_t));
			t->t = tab;
			t->next = NULL;
			t->alias = "";
			return t;
		}
	}

	error(r,CSVDB_ERROR_TABLEREF,str);;

	return NULL;
}

void table_free_refs(table_ref_t *t)
{
	table_ref_t *n;
	if (!t)
		return;

	if (t->alias && t->alias[0])
		free(t->alias);

	while ((n = t->next)) {
		t->next = n->next;
		if (n->alias && n->alias[0])
			free(n->alias);
		free(n);
	}

	free(t);
}
