/************************************************************************
* column.c
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

column_ref_t *column_create(char* name, char* alias, table_t *table)
{
	column_ref_t *ret = malloc(sizeof(column_ref_t));
	ret->name = strdup(name);
	ret->alias = "";
	if (alias)
		ret->alias = strdup(alias);
	ret->t_index = 0;
	ret->r_index = 0;
	ret->keywords = NULL;
	ret->table = table;
	ret->next = NULL;

	return ret;
}

column_ref_t *column_add(column_ref_t **stack, char* name, char* alias, table_t *table)
{
	column_ref_t *ret = column_create(name,alias,table);

	if (*stack) {
		int i = 0;
		column_ref_t *s = *stack;
		while (s->next) {
			i++;
			s = s->next;
		}
		s->next = ret;
	}else{
		*stack = ret;
	}

	return ret;
}

column_ref_t *column_push(column_ref_t **stack, column_ref_t *col)
{
	if (*stack) {
		int i = 0;
		column_ref_t *s = *stack;
		while (s->next) {
			i++;
			s = s->next;
		}
		s->next = col;
	}else{
		*stack = col;
	}

	return col;
}

void column_free(column_ref_t *c)
{
	if (c->name)
		free(c->name);
	if (c->alias && c->alias[0])
		free(c->alias);

	nvp_free_all(c->keywords);

	free(c);
}

void column_free_all(column_ref_t *c)
{
	column_ref_t *n;
	while (c) {
		n = c->next;
		column_free(c);
		c = n;
	}
}

/* temporary column reference */
column_ref_t *cref = NULL;

/* resolve a string to a column in a table in a query */
column_ref_t *column_resolve(char* str, result_t *r)
{
	char* tbl = NULL;
	char* col;
	char* rdup;
	char* tmp;
	char* c;
	nvp_t *n;
	int t_i = 0;
	int r_i;
	table_ref_t *t = NULL;
	int l;
	column_ref_t *ret = NULL;
	if (!str || !r) {
		error(r,CSVDB_ERROR_COLUMNREF,str);
		return NULL;
	}

	l = strlen(str);
	rdup = alloca(l+1);
	strcpy(rdup,str);

	if ((col = strchr(rdup,'.'))) {
		*col = 0;
		col++;
		tbl = rdup;
	}else{
		col = rdup;
	}

	if (!col || !col[0]) {
		error(r,CSVDB_ERROR_COLUMNREF,str);
		return NULL;
	}

	if (tbl) {
		if (tbl[0] == '`' && (tmp = strchr(tbl+1,'`')) && !tmp[1]) {
			tbl++;
			*tmp = 0;
		}
		t = r->table;
		t_i = 0;
		while (t) {
			if (!strcmp(tbl,t->alias) || nvp_search(t->t->name,tbl))
				break;
			t_i++;
			t = t->next;
		}
		if (!t) {
			error(r,CSVDB_ERROR_TABLEREF,str);
			return NULL;
		}
	}

	if (col[0] == '`' && (tmp = strchr(col+1,'`')) && !tmp[1]) {
		col++;
		*tmp = 0;
	}

	ret = r->cols;
	while (ret) {
		if ((!tbl && !strcmp(col,ret->alias)) || ((!tbl || ret->table == t->t) && !strcmp(col,ret->name)))
			return ret;
		ret = ret->next;
	}

	if ((tmp = strchr(col,'('))) {
		*tmp = 0;
		if (!strcasecmp(col,"COLUMN")) {
			*tmp = '(';
			c = strchr(tmp+1,')');
			if (c) {
				*c = 0;
				l = atoi(tmp+1);
				*c = ')';
				l--;
				if (t) {
					n = nvp_grabi(t->t->columns,l);
					if (!n) {
						char bf[50];
						sprintf(bf,"COLUMN(%d)",l);
						error(r,CSVDB_ERROR_OUTOFRANGE,bf);
						return NULL;
					}
					return column_resolve(n->value,r);
				}else if (!r->table->next) {
					n = nvp_grabi(r->table->t->columns,l);
					if (!n) {
						char bf[50];
						sprintf(bf,"COLUMN(%d)",l);
						error(r,CSVDB_ERROR_OUTOFRANGE,bf);
						return NULL;
					}
					return column_resolve(n->value,r);
				}
			}
		}else if (!strcasecmp(col,"COUNT")) {
			*tmp = '(';
			return NULL;
		}
		*tmp = '(';
	}

	/* this reference doesn't exist in the result, but may be another
	 * column from a table referenced in the result */

	if (t) {
		n = t->t->columns;
		r_i = 0;
		while (n) {
			if (!strcmp(col,n->value)) {
				if (!cref) {
					cref = malloc(sizeof(column_ref_t));
					cref->name = malloc(1024);
					cref->alias = "";
					cref->t_index = 0;
					cref->r_index = 0;
					cref->keywords = NULL;
					cref->table = NULL;
					cref->next = NULL;
				}
				strcpy(cref->name,n->value);
				cref->t_index = t_i;
				cref->r_index = r_i;
				cref->table = t->t;
				return cref;
			}
			r_i++;
			n = n->next;
		}
	}else{
		t = r->table;
		t_i = 0;
		while (t) {
			r_i = 0;
			n = t->t->columns;
			while (n) {
				if (!strcmp(col,n->value)) {
					if (!cref) {
						cref = malloc(sizeof(column_ref_t));
						cref->name = malloc(1024);
						cref->alias = "";
						cref->t_index = 0;
						cref->r_index = 0;
						cref->keywords = NULL;
						cref->table = NULL;
						cref->next = NULL;
					}
					strcpy(cref->name,n->value);
					cref->t_index = t_i;
					cref->r_index = r_i;
					cref->table = t->t;
					return cref;
				}
				r_i++;
				n = n->next;
			}
			t_i++;
			t = t->next;
		}
	}

	error(r,CSVDB_ERROR_COLUMNREF,str);

	return NULL;
}

/* find a column in a table referenced in a result */
column_ref_t *column_find(char* str, result_t *r)
{
	char* tbl = NULL;
	char* col;
	char* rdup;
	char* tmp;
	char* c;
	nvp_t *n;
	int t_i = 0;
	int r_i = 0;
	table_ref_t *t = NULL;
	table_t *tab = NULL;
	int l;
	column_ref_t *ret = NULL;
	column_ref_t *cl;
	if (!str || !r) {
		error(r,CSVDB_ERROR_COLUMNREF,str);
		return NULL;
	}

	l = strlen(str);
	rdup = alloca(l+1);
	strcpy(rdup,str);

	if ((col = strchr(rdup,'.'))) {
		*col = 0;
		col++;
		tbl = rdup;
	}else{
		col = rdup;
	}

	if (!col || !col[0]) {
		error(r,CSVDB_ERROR_COLUMNREF,str);
		return NULL;
	}

	if (tbl) {
		if (tbl[0] == '`' && (tmp = strchr(tbl+1,'`')) && !tmp[1]) {
			tbl++;
			*tmp = 0;
		}
		t = r->table;
		if (strcasecmp(tbl,"FILE")) {
			while (t) {
				if (!strcmp(tbl,t->alias) || nvp_search(t->t->name,tbl))
					break;
					t_i++;
				t = t->next;
			}
		}
		if (!t) {
			error(r,CSVDB_ERROR_TABLEREF,str);
			return NULL;
		}
	}

	if (col[0] == '`' && (tmp = strchr(col+1,'`')) && !tmp[1]) {
		col++;
		*tmp = 0;
	}

	if (!strcmp(col,"*")) {
		if (t) {
			tab = t->t;
			n = tab->columns;
			while (n) {
				cl = column_add(&ret,n->value,NULL,tab);
				cl->r_index = r_i++;
				cl->t_index = t_i;
				n = n->next;
			}
		}else{
			t = r->table;
			t_i = 0;
			while (t) {
				n = t->t->columns;
				r_i = 0;
				while (n) {
					cl = column_add(&ret,n->value,NULL,t->t);
					cl->r_index = r_i++;
					cl->t_index = t_i;
					n = n->next;
				}
				t_i++;
				t = t->next;
			}
		}
		return ret;
	}

	if (!t) {
		t = r->table;
		t_i = 0;
		while (t) {
			if (nvp_searchi(t->t->columns,col) > -1) {
				tab = t->t;
				break;
			}
			t_i++;
			t = t->next;
		}
	}else{
		t_i = 0;
		tab = t->t;
	}

	if (tab) {
		l = nvp_searchi(tab->columns,col);

		if (l < 0) {
			if ((tmp = strchr(col,'('))) {
				*tmp = 0;
				if (!strcasecmp(col,"COLUMN")) {
					*tmp = '(';
					c = strchr(tmp+1,')');
					if (c) {
						*c = 0;
						l = atoi(tmp+1);
						*c = ')';
						l--;
						if (t) {
							n = nvp_grabi(t->t->columns,l);
							if (!n) {
								char bf[50];
								sprintf(bf,"COLUMN(%d)",l);
								error(r,CSVDB_ERROR_OUTOFRANGE,bf);
								return NULL;
							}
							return column_find(n->value,r);
						}else if (!r->table->next) {
							n = nvp_grabi(r->table->t->columns,l);
							if (!n) {
								char bf[50];
								sprintf(bf,"COLUMN(%d)",l);
								error(r,CSVDB_ERROR_OUTOFRANGE,bf);
								return NULL;
							}
							return column_find(n->value,r);
						}
					}
				}
				*tmp = '(';
			}
		}

		if (l > -1) {
			n = nvp_grabi(tab->columns,l);
			ret = column_create(n->value,NULL,tab);
			ret->t_index = t_i;
			ret->r_index = l;
			return ret;
		}
	}

	error(r,CSVDB_ERROR_COLUMNREF,str);

	return NULL;
}

/* fetch the actual data refered to by a column reference in a row */
nvp_t *column_fetch_data(row_t *row, column_ref_t *col)
{
	nvp_t *r;
	row_t *rw = row->t_data;
	int i = 0;
	if (!rw)
		rw = row;

	if (col->t_index > -1) {
		while (col->t_index != i && rw) {
			i++;
			rw = rw->next;
		}
	}else{
		rw = row;
	}

	if (!rw)
		return NULL;

	r = rw->data;
	i = 0;
	while (col->r_index != i && r) {
		i++;
		r = r->next;
	}

	if (r && !r->value)
		return NULL;

	return r;
}
