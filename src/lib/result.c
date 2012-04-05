/************************************************************************
* result.c
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

#if defined HAVE_CONFIG_H
# include "config.h"
#endif	/* HAVE_CONFIG_H */

#include "csvdb.h"

#ifndef HAVE_STRCASESTR
char* strcasestr(const char* haystack, const char* needle)
{
	char* h = alloca(strlen(haystack)+1);
	char* n = alloca(strlen(needle)+1);
	char* t;
	int i;
	for (i=0; haystack[i]; i++) {
		if (isupper(haystack[i])) {
			h[i] = tolower(haystack[i]);
			continue;
		}
		h[i] = haystack[i];
	}
	h[i] = 0;
	for (i=0; needle[i]; i++) {
		if (isupper(needle[i])) {
			n[i] = tolower(needle[i]);
			continue;
		}
		n[i] = needle[i];
	}
	n[i] = 0;

	t = strstr(h,n);
	if (!t)
		return NULL;

	return (char*)(haystack+(t-h));
}
#endif /* !HAVE_STRCASESTR */

/* compare q->value using q->num against row value l->value in result r */
static int where_compare(result_t *r, row_t *row, nvp_t *q, nvp_t *l)
{
	int v;
	int k;
	int s = 0;
	int c = q->num;
	result_t *sub;
	nvp_t *n;
	column_ref_t *col;
	row_t *rw;
	if (q->num&CMP_STRING) {
		c -= CMP_STRING;
		s = 1;
	}
	switch (c) {
	case CMP_EQUALS:
		if (s) {
			if (l) {
				if (strcasecmp(l->value,q->value))
					return 0;
			}else if (strcmp(q->value,"NULL")) {
				return 0;
			}
		}else{
			col = column_find(q->value,r);
			if (!col)
				return 0;
			n = column_fetch_data(row,col);
			column_free(col);
			if (n) {
				if (!l)
					return 0;
				if (strcasecmp(n->value,l->value))
					return 0;
			}else if (l) {
				return 0;
			}
		}
		break;
	case CMP_LIKE:
		if (s) {
			if (l) {
				if (!strcasestr(l->value,q->value))
					return 0;
			}else if (strcmp(q->value,"NULL")) {
				return 0;
			}
		}else{
			col = column_find(q->value,r);
			if (!col)
				return 0;
			n = column_fetch_data(row,col);
			column_free(col);
			if (n) {
				if (!l)
					return 0;
				if (!strcasestr(n->value,l->value))
					return 0;
			}else if (l) {
				return 0;
			}
		}
		break;
	case CMP_LESS:
		k = atoi(q->value);
		if (l) {
			if (!is_numeric(l->value)) {
				col = column_find(l->value,r);
				if (!col)
					return 0;
				n = column_fetch_data(row,col);
				column_free(col);
				if (n) {
					v = atoi(n->value);
				}else{
					v = 0;
				}
			}else{
				v = atoi(l->value);
			}

		}else{
			v = 0;
		}
		if (v >= k)
			return 0;
		break;
	case CMP_LESSOREQ:
		k = atoi(q->value);
		if (l) {
			if (!is_numeric(l->value)) {
				col = column_find(l->value,r);
				if (!col)
					return 0;
				n = column_fetch_data(row,col);
				column_free(col);
				if (n) {
					v = atoi(n->value);
				}else{
					v = 0;
				}
			}else{
				v = atoi(l->value);
			}

		}else{
			v = 0;
		}
		if (v > k)
			return 0;
		break;
	case CMP_GREATER:
		k = atoi(q->value);
		if (l) {
			if (!is_numeric(l->value)) {
				col = column_find(l->value,r);
				if (!col)
					return 0;
				n = column_fetch_data(row,col);
				column_free(col);
				if (n) {
					v = atoi(n->value);
				}else{
					v = 0;
				}
			}else{
				v = atoi(l->value);
			}
		}else{
			v = 0;
		}
		if (v <= k)
			return 0;
		break;
	case CMP_GREATEROREQ:
		k = atoi(q->value);
		if (l) {
			if (!is_numeric(l->value)) {
				col = column_find(l->value,r);
				if (!col)
					return 0;
				n = column_fetch_data(row,col);
				column_free(col);
				if (n) {
					v = atoi(n->value);
				}else{
					v = 0;
				}
			}else{
				v = atoi(l->value);
			}

		}else{
			v = 0;
		}
		if (v < k)
			return 0;
		break;
	case CMP_IN:
		sub = (result_t*)q->value;
		if (!sub->cols || sub->cols->next) {
			error(r,CSVDB_ERROR_SUBQUERY,sub->q);
			return 0;
		}
		rw = sub->result;
		while (rw) {
			if (!strcasecmp(l->value,rw->data->value))
				return 1;
			rw = rw->next;
		}
		return 0;
		break;
	case CMP_NOTEQUALS:
		if (s) {
			if (l) {
				if (!strcasecmp(l->value,q->value))
					return 0;
			}else if (!strcmp(q->value,"NULL")) {
				return 0;
			}
		}else{
			col = column_find(q->value,r);
			if (!col)
				return 0;
			n = column_fetch_data(row,col);
			column_free(col);
			if (n) {
				if (!l)
					return 1;
				if (strcasecmp(n->value,l->value))
					return 0;
			}else if (!l) {
				return 0;
			}
		}
		break;
	case CMP_NOTLIKE:
		if (s) {
			if (l) {
				if (strcasestr(l->value,q->value))
					return 0;
			}else if (!strcmp(q->value,"NULL")) {
				return 0;
			}
		}else{
			col = column_find(q->value,r);
			if (!col)
				return 0;
			n = column_fetch_data(row,col);
			column_free(col);
			if (n) {
				if (!l)
					return 1;
				if (strcasestr(n->value,l->value))
					return 0;
			}else if (l) {
				return 0;
			}
		}
		break;
	case CMP_NOTIN:
		sub = (result_t*)q->value;
		if (!sub->cols || sub->cols->next) {
			error(r,CSVDB_ERROR_SUBQUERY,sub->q);
			return 0;
		}
		rw = sub->result;
		while (rw) {
			if (!strcasecmp(l->value,rw->data->value))
				return 0;
			rw = rw->next;
		}
		break;
	default:
		error(r,CSVDB_ERROR_SYNTAX,q->name);
		row_free_keys(r->result);
		return 0;
	}

	return 1;
}

void result_where(result_t *r)
{
	int j;
	int c = 0;
	int cr;
	int hl = 0;
	int lim = atoi(r->limit->next->value);
	int off = atoi(r->limit->value);
	int tl = off+lim;
	nvp_t *l;
	nvp_t *q;
	nvp_t *t;
	row_t *row;
	row_t *rw;
	column_ref_t *cl;
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
			cl = r->cols;
			while (cl) {
				if (nvp_search(cl->keywords,"DISTINCT")) {
					hl = 0;
					break;
				}
				l = l->next;
			}
		}
	}
	if (r->join) {
		row = r->join;
	}else{
		row = r->table->t->rows;
	}

	while (row) {
		j = 0;
		q = r->where;
		while (q) {
			cl = column_resolve(q->name,r);
			l = column_fetch_data(row,cl);
			cr = where_compare(r,row,q,l);
			if (r->error) {
				row_free_keys(r->result);
				r->result = NULL;
				return;
			}
			if (!cr) {
				t = q->child;
				while (t) {
					cl = column_resolve(t->name,r);
					l = column_fetch_data(row,cl);
					cr = where_compare(r,row,t,l);
					if (r->error) {
						row_free_keys(r->result);
						r->result = NULL;
						return;
					}
					if (cr)
						break;
					t = t->next;
				}
			}
			if (!cr) {
				j = 1;
				break;
			}
			q = q->next;
		}
		if (!j) {
			c++;
			if (hl && c <= off) {
				row = row->next;
				continue;
			}
			if (!r->join) {
				rw = row_add(&r->result,row->key);
				rw->t_data = row_create(row->key);
				rw->t_data->data = row->data;
			}else{
				rw = row_add(&r->result,row->key);
				rw->t_data = row->t_data;
				rw->data = row->data;
				row->t_data = NULL;
			}
		}
		if (hl && c == tl)
			break;
		row = row->next;
	}
	if (hl) {
		nvp_set(r->limit,NULL,"0");
		nvp_set(r->limit->next,NULL,"-1");
	}
	if (r->join) {
		row_free_keys(r->join);
		r->join = NULL;
	}
}

void result_having(result_t *r)
{
	int j;
	int c = 0;
	int cr;
	nvp_t *l;
	nvp_t *q;
	nvp_t *t;
	row_t *ors = r->result;
	row_t *row = r->result;
	row_t *rw;
	column_ref_t *cl;
	r->result = NULL;
	while (row) {
		j = 0;
		q = r->having;
		while (q) {
			cl = column_resolve(q->name,r);
			l = column_fetch_data(row,cl);
			cr = where_compare(r,row,q,l);
			if (!cr) {
				t = q->child;
				while (t) {
					cl = column_resolve(t->name,r);
					l = column_fetch_data(row,cl);
					cr = where_compare(r,row,t,l);
					if (cr)
						break;
					t = t->next;
				}
			}
			if (!cr) {
				j = 1;
				break;
			}
			q = q->next;
		}
		if (!j) {
			c++;
			rw = row_add(&r->result,row->key);
			rw->t_data = row->t_data;
			rw->data = row->data;
			row->t_data = NULL;
		}
		row = row->next;
	}
	row_free_keys(ors);
}

void result_distinct(result_t *r)
{
	nvp_t *l;
	nvp_t *u;
	row_t *q;
	row_t *tr;
	row_t *trs;
	column_ref_t *n = r->cols;
	while (n) {
		if (nvp_search(n->keywords,"DISTINCT")) {
			u = NULL;
			trs = NULL;
			q = r->result;
			while (q) {
				l = column_fetch_data(q,n);
				if (l && !nvp_search(u,l->value)) {
					nvp_add(&u,NULL,l->value);
					tr = row_add(&trs,q->key);
					tr->data = q->data;
					tr->t_data = q->t_data;
					q->t_data = NULL;
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
	column_ref_t *col;
	while (n) {
		trs = NULL;
		col = column_resolve(n->value,r);
		if (!col)
			break;
		u = NULL;
		q = r->result;
		while (q) {
			t = column_fetch_data(q,col);
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
				t = nvp_search_name(*b,buff);
				if (t) {
					j = atoi(t->value);
					j++;
					sprintf(sbuff,"%d",j);
					nvp_set(t,buff,sbuff);
				}else{
					t = nvp_add(b,buff,"1");
					t->num = q->key;
				}
				l = l->next;
			}
			if (!nvp_search(u,buff)) {
				tr = row_add(&trs,q->key);
				tr->data = q->data;
				tr->t_data = q->t_data;
				q->t_data = NULL;
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
	char buff[1024];
	row_t *l;
	row_t *q;
	nvp_t *t;
	row_t *tr;
	row_t *trs;
	nvp_t *n = r->order;
	column_ref_t *col;
	while (n) {
		trs = NULL;
		col = column_resolve(n->value,r);
		if (nvp_search(n->child,"INT")) {
			if (!nvp_search(n->child,"DESC")) {
				q = r->result;
				while (q) {
					t = column_fetch_data(q,col);
					if (t) {
						sprintf(buff,"%s",t->value);
					}else{
						strcpy(buff,"0");
					}
					if ((l = row_search_higher_int_col(trs,col,buff))) {
						tr = row_create(q->key);
						tr->data = q->data;
						tr->t_data = q->t_data;
						q->t_data = NULL;
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
						l->data = q->data;
						l->t_data = q->t_data;
						q->t_data = NULL;
					}
					q = q->next;
				}
			}else{
				q = r->result;
				while (q) {
					t = column_fetch_data(q,col);
					if (t) {
						sprintf(buff,"%s",t->value);
					}else{
						strcpy(buff,"0");
					}
					if ((l = row_search_lower_int_col(trs,col,buff))) {
						tr = row_create(q->key);
						tr->data = q->data;
						tr->t_data = q->t_data;
						q->t_data = NULL;
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
						l->data = q->data;
						l->t_data = q->t_data;
						q->t_data = NULL;
					}
					q = q->next;
				}
			}
		}else{
			if (!nvp_search(n->child,"DESC")) {
				q = r->result;
				while (q) {
					t = column_fetch_data(q,col);
					if (t) {
						sprintf(buff,"%s",t->value);
					}else{
						buff[0] = 0;
					}
					if ((l = row_search_higher_string_col(trs,col,buff))) {
						tr = row_create(q->key);
						tr->data = q->data;
						tr->t_data = q->t_data;
						q->t_data = NULL;
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
						l->data = q->data;
						l->t_data = q->t_data;
						q->t_data = NULL;
					}
					q = q->next;
				}
			}else{
				q = r->result;
				while (q) {
					t = column_fetch_data(q,col);
					if (t) {
						sprintf(buff,"%s",t->value);
					}else{
						buff[0] = 0;
					}
					if ((l = row_search_lower_string_col(trs,col,buff))) {
						tr = row_create(q->key);
						tr->data = q->data;
						tr->t_data = q->t_data;
						q->t_data = NULL;
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
						l->data = q->data;
						l->t_data = q->t_data;
						q->t_data = NULL;
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
	column_ref_t *q;
	nvp_t *u;
	nvp_t *t;
	row_t *n = r->result;
	int i;
	if (!r->cols && r->count)
		return;
	while (n) {
		u = NULL;
		q = r->cols;
		while (q) {
			t = column_fetch_data(n,q);
			if (t) {
				nvp_add(&u,NULL,t->value);
			}else{
				nvp_add(&u,NULL,"NULL");
			}
			q = q->next;
		}
		nvp_push(&u,n->data);
		n->data = u;
		row_free_keys(n->t_data);
		n->t_data = NULL;
		n = n->next;
	}
	q = r->cols;
	i = 0;
	while (q) {
		q->r_index = i++;
		q = q->next;
	}
}

void result_count(result_t *r)
{
	int j;
	char buff[1024];
	nvp_t *q;
	row_t *t;
	nvp_t *u;
	nvp_t *b;
	int i = 0;
	column_ref_t *c;
	if (r->count && strlen(r->count->value)) {
		j = row_count(r->result);
		if (!r->group) {
			sprintf(buff,"%d",j);
			column_free_all(r->cols);
			if (r->count->child && !strcasecmp(r->count->child->value,"AS")) {
				r->cols = column_create(r->count->value,r->count->child->next->value,NULL);
			}else{
				r->cols = column_create(r->count->value,NULL,NULL);
			}

			r->cols->t_index = -1;
			r->cols->r_index = i++;

			q = r->count->next;
			while (q) {
				if (q->child && !strcasecmp(q->child->value,"AS")) {
					c = column_add(&r->cols,q->value,q->child->next->value,NULL);
				}else{
					c = column_add(&r->cols,q->value,NULL,NULL);
				}
				r->cols->t_index = -1;
				r->cols->r_index = i++;
				q = q->next;
			}
			row_free_keys(r->result);
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
							nvp_add(&t->data,NULL,u->value);
							break;
						}
						u = u->next;
					}
					t = t->next;
				}
				if (q->child && !strcasecmp(q->child->value,"AS")) {
					c = column_add(&r->cols,q->value,q->child->next->value,NULL);
				}else{
					c = column_add(&r->cols,q->value,NULL,NULL);
				}
				c->t_index = -1;
				c->r_index = i++;
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
		if (lim2 > -1 && j == lim1+lim2) {
			row_free_keys(n->next);
			n->next = NULL;
			break;
		}
		n = n->next;
	}
}

void csvdb_free_result(result_t *r)
{
	result_t *s;
	nvp_t *w;
	nvp_t *wo;
	if (!r)
		return;
	if (r->q)
		free(r->q);

	column_free_all(r->cols);
	nvp_free_all(r->keywords);
	/* IN means the value is a result set, so we need to NULL them out to
	 * prevent double free's occuring */
	w = r->where;
	while (w) {
		if (w->num == CMP_IN || w->num == CMP_NOTIN)
			w->value = NULL;
		wo = w->child;
		while (wo) {
			if (wo->num == CMP_IN || wo->num == CMP_NOTIN)
				wo->value = NULL;
			wo = wo->next;
		}
		w = w->next;
	}
	nvp_free_all(r->where);
	nvp_free_all(r->group);
	nvp_free_all(r->order);
	nvp_free_all(r->limit);
	nvp_free_all(r->count);
	row_free_all(r->result);
	table_free_refs(r->table);

	while ((s = r->sub)) {
		r->sub = s->next;
		csvdb_free_result(s);
	}

	free(r);
}

void result_free(result_t *r) __attribute__ ((weak, alias ("csvdb_free_result")));

table_t *result_to_table(result_t *r, char* name)
{
	table_t *t;
	row_t *n;
	nvp_t *c;
	column_ref_t *col;
	row_t *cr;
	int key;

	if (!r || !name)
		return NULL;

	t = table_add();

	nvp_add(&t->name,NULL,name);

	col = r->cols;
	while (col) {
		if (col->alias[0]) {
			nvp_add(&t->columns,NULL,col->alias);
		}else{
			nvp_add(&t->columns,NULL,col->name);
		}
		col = col->next;
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
