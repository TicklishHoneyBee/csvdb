/************************************************************************
* result.c
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

void result_where(result_t *r)
{
	int j;
	int k;
	int c = 0;
	int hl = 0;
	int lim = atoi(r->limit->next->value);
	int off = atoi(r->limit->value);
	int tl = off+lim;
	nvp_t *l;
	nvp_t *q;
	row_t *row = r->table->rows;
	row_t *rw;
	if (lim > -1) {
		hl = 1;
		if (r->group) {
			hl = 0;
		}else if (r->order) {
			hl = 0;
		}else if (strncasecmp(r->q,"SELECT ",7)) {
			hl = 0;
		}else if (r->count) {
			hl = 0;
		}else{
			l = r->cols;
			while (l) {
				if (nvp_search(l->child,"DISTINCT")) {
					hl = 0;
					break;
				}
				l = l->next;
			}
		}
	}
	while (row) {
		j = 0;
		q = r->where;
		while (q) {
			k = nvp_searchi(r->table->columns,q->name);
			l = nvp_grabi(row->data,k);
			if (l) {
				if (strchr(q->value,'%') && q->child) {
					if (!strcasestr(l->value,q->child->value))
						j = 1;
				}else{
					if (!strcasestr(l->value,q->value))
						j = 1;
				}
			}else if (strcmp(q->value,"NULL")) {
				j = 1;
			}
			q = q->next;
		}
		if (!j) {
			c++;
			if (hl && c < off) {
				row = row->next;
				continue;
			}
			rw = row_add(&r->result,row->key);
			rw->data = row->data;
		}
		if (hl && c == tl)
			break;
		row = row->next;
	}
}

void result_distinct(result_t *r)
{
	int j;
	nvp_t *l;
	nvp_t *u;
	row_t *q;
	row_t *tr;
	row_t *trs;
	nvp_t *n = r->cols;
	while (n) {
		if (nvp_search(n->child,"DISTINCT")) {
			u = NULL;
			trs = NULL;
			if (is_numeric(n->value)) {
				j = atoi(n->value);
				j--;
			}else{
				j = nvp_searchi(r->table->columns,n->value);
			}
			q = r->result;
			while (q) {
				l = nvp_grabi(q->data,j);
				if (l && !nvp_search(u,l->value)) {
					nvp_add(&u,NULL,l->value);
					tr = row_add(&trs,q->key);
					tr->data = q->data;
				}
				q = q->next;
			}
			row_free_keys(r->result);
			r->result = trs;
		}
		n = n->next;
	}
}

void result_group(result_t *r)
{
	int j;
	int k;
	char buff[1024];
	char sbuff[64];
	nvp_t *l;
	nvp_t *u;
	row_t *q;
	nvp_t *t;
	row_t *tr;
	nvp_t **b;
	row_t *trs;
	nvp_t *n = r->group;
	while (n) {
		trs = NULL;
		k = nvp_searchi(r->table->columns,n->value);
		u = NULL;
		q = r->result;
		while (q) {
			t = nvp_grabi(q->data,k);
			if (t) {
				sprintf(buff,"%s",t->value);
			}else{
				strcpy(buff,"NULL");
			}
			l = r->count;
			while (l) {
				b = &l->child;
				if (l->child && !strcasecmp(l->child->value,"AS"))
					b = &l->child->next->next;
				t = nvp_search_name(*b,n->value);
				if (t) {
					j = atoi(t->value);
					j++;
					sprintf(sbuff,"%d",j);
					nvp_set(t,n->value,sbuff);
				}else{
					t = nvp_add(b,n->value,"1");
					t->num = q->key;
				}
				l = l->next;
			}
			if (!nvp_search(u,buff)) {
				tr = row_add(&trs,q->key);
				tr->data = q->data;
				nvp_add(&u,NULL,buff);
			}
			q = q->next;
		}
		row_free_keys(r->result);
		r->result = trs;
		n = n->next;
	}
}

void result_order(result_t *r)
{
	int k;
	char buff[1024];
	row_t *l;
	row_t *q;
	nvp_t *t;
	row_t *tr;
	row_t *trs;
	nvp_t *n = r->order;
	while (n) {
		trs = NULL;
		k = nvp_searchi(r->table->columns,n->value);
		if (nvp_search(n->child,"INT")) {
			if (!nvp_search(n->child,"DESC")) {
				q = r->result;
				while (q) {
					t = nvp_grabi(q->data,k);
					if (t) {
						sprintf(buff,"%s",t->value);
					}else{
						strcpy(buff,"0");
					}
					if ((l = row_search_higher_int(trs,k,buff))) {
						tr = row_create(q->key);
						tr->data = q->data;
						if (l->prev) {
							tr->prev = l->prev;
							tr->next = l;
							l->prev = tr;
							tr->prev->next = tr;
						}else{
							tr->next = trs;
							if (trs)
								trs->prev = tr;
							trs = tr;
						}
					}else{
						l = row_add(&trs,q->key);
					}
					q = q->next;
				}
			}else{
				q = r->result;
				while (q) {
					t = nvp_grabi(q->data,k);
					if (t) {
						sprintf(buff,"%s",t->value);
					}else{
						strcpy(buff,"0");
					}
					if ((l = row_search_lower_int(trs,k,buff))) {
						tr = row_create(q->key);
						tr->data = q->data;
						if (l->prev) {
							tr->prev = l->prev;
							tr->next = l;
							l->prev = tr;
							tr->prev->next = tr;
						}else{
							tr->next = trs;
							if (trs)
								trs->prev = tr;
							trs = tr;
						}
					}else{
						l = row_add(&trs,q->key);
					}
					q = q->next;
				}
			}
		}else{
			if (!nvp_search(n->child,"DESC")) {
				q = r->result;
				while (q) {
					t = nvp_grabi(q->data,k);
					if (t) {
						sprintf(buff,"%s",t->value);
					}else{
						buff[0] = 0;
					}
					if ((l = row_search_higher_string(trs,k,buff))) {
						tr = row_create(q->key);
						tr->data = q->data;
						if (l->prev) {
							tr->prev = l->prev;
							tr->next = l;
							l->prev = tr;
							tr->prev->next = tr;
						}else{
							tr->next = trs;
							if (trs)
								trs->prev = tr;
							trs = tr;
						}
					}else{
						l = row_add(&trs,q->key);
					}
					q = q->next;
				}
			}else{
				q = r->result;
				while (q) {
					t = nvp_grabi(q->data,k);
					if (t) {
						sprintf(buff,"%s",t->value);
					}else{
						buff[0] = 0;
					}
					if ((l = row_search_lower_string(trs,k,buff))) {
						tr = row_create(q->key);
						tr->data = q->data;
						if (l->prev) {
							tr->prev = l->prev;
							tr->next = l;
							l->prev = tr;
							tr->prev->next = tr;
						}else{
							tr->next = trs;
							if (trs)
								trs->prev = tr;
							trs = tr;
						}
					}else{
						l = row_add(&trs,q->key);
					}
					q = q->next;
				}
			}
		}
		row_free_keys(r->result);
		r->result = trs;
		n = n->next;
	}
}

void result_cols(result_t *r)
{
	int k;
	nvp_t *q;
	nvp_t *u;
	nvp_t *t;
	row_t *n = r->result;
	if (!r->cols && r->count)
		return;
	while (n) {
		u = NULL;
		q = r->cols;
		while (q) {
			if (is_numeric(q->value)) {
				k = atoi(q->value);
				k--;
			}else{
				k = nvp_searchi(r->table->columns,q->value);
			}
			t = nvp_grabi(n->data,k);
			if (t) {
				nvp_add(&u,NULL,t->value);
			}else if (k < 0 && r->count && (t = nvp_last(n->data))) {
				nvp_add(&u,NULL,t->value);
			}else{
				nvp_add(&u,NULL,"NULL");
			}
			q = q->next;
		}
		n->data = u;
		n = n->next;
	}
}

void result_count(result_t *r)
{
	int j;
	int k;
	char buff[1024];
	nvp_t *l;
	nvp_t *q;
	row_t *t;
	nvp_t *u;
	nvp_t *b;
	if (r->count && strlen(r->count->value)) {
		j = row_count(r->result);
		if (!r->group) {
			sprintf(buff,"%d",j);
			if (r->count->child && !strcasecmp(r->count->child->value,"AS")) {
				r->cols = nvp_create(NULL,r->count->child->next->value);
			}else{
				r->cols = nvp_create(NULL,r->count->value);
			}
			q = r->count->next;
			while (q) {
				if (q->child && !strcasecmp(q->child->value,"AS")) {
					nvp_add(&r->cols->child,NULL,q->child->next->value);
				}else{
					nvp_add(&r->cols->child,NULL,q->value);
				}
				q = q->next;
			}
			row_free_all(r->result);
			r->result = row_create(0);
			r->result->data = nvp_create(NULL,buff);
		}else{
			q = r->count;
			while (q) {
				t = r->result;
				b = q->child;
				if (b && !strcasecmp(b->value,"AS"))
					b = b->next->next;
				if (!b) {
					q = q->next;
					continue;
				}
				while (t) {
					u = b;
					while (u) {
						if (t->key == u->num) {
							if (is_numeric(u->name)) {
								k = atoi(u->name);
							}else{
								k = nvp_searchi(r->table->columns,u->name);
							}
							l = nvp_grabi(t->data,k);
							if (!l) {
								nvp_add(&t->data,NULL,u->value);
								break;
							}else{
								nvp_add(&t->data,NULL,u->value);
								break;
							}
						}
						u = u->next;
					}
					t = t->next;
				}
				if (q->child && !strcasecmp(q->child->value,"AS")) {
					nvp_add(&r->cols,NULL,q->child->next->value);
				}else{
					nvp_add(&r->cols,NULL,q->value);
				}
				q = q->next;
			}
		}
	}
}

void result_limit(result_t *r)
{
	int j = 0;
	int lim1;
	int lim2;
	row_t *trs = NULL;
	row_t *n = r->result;
	lim1 = atoi(r->limit->value);
	lim2 = atoi(r->limit->next->value);
	while (n) {
		if (j && j == lim1) {
			trs = n;
			if (n->prev) {
				n->prev->next = NULL;
				n->prev = NULL;
				row_free_keys(r->result);
			}
			r->result = trs;
		}
		j++;
		if (lim2 > -1 && j >= lim1+lim2) {
			row_free_keys(n->next);
			n->next = NULL;
			break;
		}
		n = n->next;
	}
}

void result_free(result_t *r)
{
	if (!r)
		return;
	if (r->q)
		free(r->q);
	nvp_free_all(r->cols);
	nvp_free_all(r->keywords);
	nvp_free_all(r->where);
	nvp_free_all(r->group);
	nvp_free_all(r->order);
	nvp_free_all(r->limit);
	nvp_free_all(r->count);
	row_free_all(r->result);

	free(r);
}

table_t *result_to_table(result_t *r, char* name)
{
	table_t *t;
	row_t *n;
	nvp_t *c;
	row_t *cr;
	int key;

	if (!r || !name)
		return NULL;

	t = table_add();

	nvp_add(&t->name,NULL,name);

	c = r->cols;
	while (c) {
		nvp_add(&t->columns,NULL,c->value);
		c = c->next;
	}

	n = r->result;
	while (n) {
		key = table_next_key(t);

		cr = row_add(&t->rows,key);
		if (cr) {
			c = n->data;
			while (c) {
				nvp_add(&cr->data,NULL,c->value);
				c = c->next;
			}
		}
		n = n->next;
	}

	return t;
}
