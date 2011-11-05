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
	int lim = atoi(r->limit->next->value);
	int off = 0;
	int tl = off+lim;
	nvp_t *l;
	nvp_t *q;
	nvp_t *n = r->table->rows;
	if (lim > -1) {
		if (r->group) {
			lim = -1;
		}else if (r->order) {
			lim = -1;
		}else if (strncasecmp(r->q,"SELECT ",7)) {
			lim = -1;
		}else if (r->count) {
			lim = -1;
		}else{
			l = r->cols;
			while (l) {
				if (nvp_search(l->child,"DISTINCT")) {
					lim = -1;
					break;
				}
				l = l->next;
			}
		}
	}
	while (n) {
		j = 0;
		q = r->where;
		while (q) {
			k = nvp_searchi(r->table->columns,q->name);
			if (!k) {
				l = n;
			}else{
				l = nvp_grabi(n->child,k-1);
			}
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
			nvp_add(&r->result,n->name,n->value);
			l = nvp_last(r->result);
			if (l) {
				nvp_t **v = &l->child;
				q = n->child;
				while (q) {
					nvp_add(v,q->name,q->value);
					q = q->next;
				}
			}
		}
		if (lim > -1 && c == tl)
			break;
		n = n->next;
	}
}

void result_distinct(result_t *r)
{
	int j;
	nvp_t *l;
	nvp_t *u;
	nvp_t *q;
	nvp_t *t;
	nvp_t *trs;
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
				if (!j) {
					l = q;
				}else{
					l = nvp_grabi(q->child,j-1);
				}
				if (l && !nvp_search(u,l->value)) {
					nvp_add(&u,NULL,l->value);
					nvp_add(&trs,q->name,q->value);
					l = nvp_last(trs);
					if (l) {
						nvp_t **v = &l->child;
						t = q->child;
						while (t) {
							nvp_add(v,t->name,t->value);
							t = t->next;
						}
					}
				}
				q = q->next;
			}
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
	char buff2[1024];
	char sbuff[64];
	nvp_t *l;
	nvp_t *u;
	nvp_t *q;
	nvp_t *t;
	nvp_t **b;
	nvp_t *trs;
	nvp_t *n = r->group;
	while (n) {
		trs = NULL;
		k = nvp_searchi(r->table->columns,n->value);
		u = NULL;
		q = r->result;
		while (q) {
			if (k) {
				t = nvp_grabi(q->child,k-1);
			}else{
				t = q;
			}
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
				sprintf(buff2,"%s-%s",n->value,buff);
				t = nvp_search_name(*b,buff2);
				if (t) {
					j = atoi(t->value);
					j++;
					sprintf(sbuff,"%d",j);
					nvp_set(t,buff2,sbuff);
				}else{
					nvp_add(b,buff2,"1");
				}
				l = l->next;
			}
			if (!nvp_search(u,buff)) {
				if (trs) {
					l = nvp_last(trs);
					l->next = nvp_insert_all(NULL,q);
					l->next->prev = l;
				}else{
					trs = nvp_insert_all(NULL,q);
				}
				nvp_add(&u,NULL,buff);
			}
			q = q->next;
		}
		nvp_free_all(r->result);
		r->result = trs;
		n = n->next;
	}
}

void result_order(result_t *r)
{
	int k;
	char buff[1024];
	nvp_t *l;
	nvp_t *q;
	nvp_t *t;
	nvp_t *trs;
	nvp_t *n = r->order;
	while (n) {
		trs = NULL;
		k = nvp_searchi(r->table->columns,n->value);
		if (nvp_search(n->child,"INT")) {
			if (!nvp_search(n->child,"DESC")) {
				q = r->result;
				while (q) {
					if (k) {
						t = nvp_grabi(q->child,k-1);
					}else{
						t = q;
					}
					if (t) {
						sprintf(buff,"%s",t->value);
					}else{
						strcpy(buff,"0");
					}
					if ((l = nvp_search_higher_int(trs,k,buff))) {
						t = nvp_insert_all(l->prev,q);
						if (!l->prev) {
							t->next = trs;
							trs->prev = t;
							trs = t;
						}
					}else{
						t = nvp_last(trs);
						l = nvp_insert_all(NULL,q);
						if (t) {
							t->next = l;
							t->next->prev = t;
						}else{
							trs = l;
						}
					}
					q = q->next;
				}
			}else{
				q = r->result;
				while (q) {
					if (k) {
						t = nvp_grabi(q->child,k-1);
					}else{
						t = q;
					}
					if (t) {
						sprintf(buff,"%s",t->value);
					}else{
						strcpy(buff,"0");
					}
					if ((l = nvp_search_lower_int(trs,k,buff))) {
						t = nvp_insert_all(l->prev,q);
						if (!l->prev) {
							t->next = trs;
							trs->prev = t;
							trs = t;
						}
					}else{
						t = nvp_last(trs);
						l = nvp_insert_all(NULL,q);
						if (t) {
							t->next = l;
							t->next->prev = t;
						}else{
							trs = l;
						}
					}
					q = q->next;
				}
			}
		}else{
			if (!nvp_search(n->child,"DESC")) {
				q = r->result;
				while (q) {
					if (k) {
						t = nvp_grabi(q->child,k-1);
					}else{
						t = q;
					}
					if (t) {
						sprintf(buff,"%s",t->value);
					}else{
						buff[0] = 0;
					}
					if ((l = nvp_search_higher_string(trs,k,buff))) {
						t = nvp_insert_all(l->prev,q);
						if (!l->prev) {
							t->next = trs;
							trs->prev = t;
							trs = t;
						}
					}else{
						t = nvp_last(trs);
						l = nvp_insert_all(NULL,q);
						if (t) {
							t->next = l;
							t->next->prev = t;
						}else{
							trs = l;
						}
					}
					q = q->next;
				}
			}else{
				q = r->result;
				while (q) {
					if (k) {
						t = nvp_grabi(q->child,k-1);
					}else{
						t = q;
					}
					if (t) {
						sprintf(buff,"%s",t->value);
					}else{
						buff[0] = 0;
					}
					if ((l = nvp_search_lower_string(trs,k,buff))) {
						t = nvp_insert_all(l->prev,q);
						if (!l->prev) {
							t->next = trs;
							trs->prev = t;
							trs = t;
						}
					}else{
						t = nvp_last(trs);
						l = nvp_insert_all(NULL,q);
						if (t) {
							t->next = l;
							t->next->prev = t;
						}else{
							trs = l;
						}
					}
					q = q->next;
				}
			}
		}
		nvp_free_all(r->result);
		r->result = trs;
		n = n->next;
	}
}

void result_cols(result_t *r)
{
	int k;
	nvp_t *l;
	nvp_t *q;
	nvp_t *u;
	nvp_t *t;
	nvp_t *trs = NULL;
	nvp_t *n = r->result;
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
			if (!k) {
				t = n;
			}else{
				t = nvp_grabi(n->child,k-1);
			}
			if (t) {
				if (u) {
					nvp_add(&u->child,NULL,t->value);
				}else{
					u = nvp_create(NULL,t->value);
				}
			}else{
				if (u) {
					nvp_add(&u->child,n->name,"NULL");
				}else{
					u = nvp_create(NULL,"NULL");
				}
			}
			q = q->next;
		}
		l = nvp_last(trs);
		if (l) {
			l->next = u;
		}else{
			trs = u;
		}
		n = n->next;
	}
	nvp_free_all(r->result);
	r->result = trs;
}

void result_count(result_t *r)
{
	int j;
	int k;
	char buff[1024];
	char* c;
	char* c1;
	nvp_t *l;
	nvp_t *q;
	nvp_t *t;
	nvp_t *u;
	j = nvp_count(r->result);
	if (r->count && strlen(r->count->value)) {
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
			r->result = nvp_create(NULL,buff);
		}else{
			q = r->count;
			while (q) {
				u = q->child;
				if (u && !strcasecmp(u->value,"AS"))
					u = u->next->next;
				/* <group_by_column>-<group_column_value> */
				while (u) {
					c = strchr(u->name,'-');
					if (!c) {
						printf("internal error on group by counts '%s'\n",u->name);
						nvp_free_all(r->result);
						r->result = NULL;
						return;
					}
					*c = 0;
					c1 = c+1;
					if (is_numeric(u->name)) {
						k = atoi(u->name);
					}else{
						k = nvp_searchi(r->cols,u->name);
					}
					t = r->result;
					while (t) {
						if (!k) {
							l = t;
						}else{
							l = nvp_grabi(t->child,k-1);
						}
						if (!l) {
							if (!strcmp(c1,"NULL")) {
								nvp_add(&t->child,NULL,u->value);
								break;
							}
						}else{
							if (!strcmp(l->value,c1)) {
								nvp_add(&t->child,NULL,u->value);
								break;
							}
						}
						t = t->next;
					}
					u = u->next;
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
	nvp_t *l;
	nvp_t *u;
	nvp_t *trs = NULL;
	nvp_t *n = r->result;
	lim1 = atoi(r->limit->value);
	lim2 = atoi(r->limit->next->value);
	while (n) {
		if (j >= lim1) {
			u = nvp_insert_all(NULL,n);
			l = nvp_last(trs);
			if (l) {
				l->next = u;
				u->prev = l;
			}else{
				trs = u;
			}
		}
		j++;
		if (lim2 > -1 && j >= lim1+lim2)
			break;
		n = n->next;
	}
	nvp_free_all(r->result);
	r->result = trs;
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
	nvp_free_all(r->result);

	free(r);
}

table_t *result_to_table(result_t *r, char* name)
{
	table_t *t;
	nvp_t *n;
	nvp_t *c;
	nvp_t *cr;
	int key;
	char kbuff[64];

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
		sprintf(kbuff,"%d",key);
		nvp_add(&t->rows,kbuff,n->value);
		cr = nvp_last(t->rows);
		if (cr) {
			c = n->child;
			while (c) {
				nvp_add(&cr->child,kbuff,c->value);
				c = c->next;
			}
		}
		n = n->next;
	}

	return t;
}
