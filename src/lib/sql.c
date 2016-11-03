/************************************************************************
* sql.c
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

unsigned int ticks()
{
	unsigned int ticks;
	struct timespec now;
	clock_gettime(CLOCK_REALTIME,&now);
	ticks = (now.tv_sec*1000)+(now.tv_nsec/1000000);
	return ticks;
}

/* split a query into keywords */
nvp_t *sql_split(char* q)
{
	nvp_t *r = NULL;
	char c;
	char* n;
	char buff[1024];
	int b;
	int l;
	int s;
	int i;
	int ws = 0;

	b = 0;
	l = strlen(q)+1;
	s = 0;
	for (i=0; i<l; i++) {
		c = q[i];
		if (!q[i])
			goto column_end;
		if (c == '\r')
			continue;
		if (s) {
			if (c == '\'') {
				if (b &&  buff[b-1] == '\\') {
					b--;
				}else{
					s = 0;
					ws = 1;
					goto column_end;
				}
			}
		}else if (!b && c == '\'') {
			s = 1;
			continue;
		}else if (!b && isspace(c)) {
			continue;
		}else if (c == '\n') {
			continue;
		}else if (c == ',' || c == ' ' || c == '(' || c == ')') {
column_end:
			buff[b] = 0;
			/* splitting on parenthesis, but don't destroy functions */
			if ((c == '(' || c == ')') && b > 4 && (!strncasecmp(buff,"COUNT",5) || (b > 5 && !strncasecmp(buff,"COLUMN",6))))
				goto push_char;
			if (!b) {
				if (c == ',' || c == '(' || c == ')') {
					sprintf(buff,"%c",c);
				}else{
					continue;
				}
			}
			if (ws) {
				n = "'";
				ws = 0;
			}else{
				n = NULL;
			}
			nvp_add(&r,n,buff);
			if (b) {
				if (c == ',' || c == '(' || c == ')') {
					sprintf(buff,"%c",c);
					nvp_add(&r,NULL,buff);
				}
			}
			b = 0;
			continue;
		}
push_char:
		if (b < 1023)
			buff[b++] = c;
	}

	return r;
}

/* JOIN processing */
table_ref_t *sql_join(result_t *r, nvp_t *pt, nvp_t **end, table_ref_t *tbl)
{
	nvp_t *n;
	table_ref_t *ret = tbl;
	table_ref_t *tab;

	n = pt->next;
	/* for now, we'll just ignore these */
	if (!strcasecmp(n->value,"LEFT")) {
		n = n->next;
	}else if (!strcasecmp(n->value,"RIGHT")) {
		n = n->next;
	}else if (!strcasecmp(n->value,"INNER")) {
		n = n->next;
	}else if (!strcasecmp(n->value,"OUTER")) {
		n = n->next;
	}else if (!strcasecmp(n->value,"NATURAL")) {
		n = n->next;
	}else if (!strcasecmp(n->value,"CROSS")) {
		n = n->next;
	}
	if (strcasecmp(n->value,"JOIN") && strcasecmp(n->value,",")) {
		error(r,CSVDB_ERROR_SYNTAX,n->value);
		table_free_refs(ret);
		return NULL;
	}

	do {
		n = n->next;

		tab = table_resolve(n->value,r);
		if (!tab) {
			table_free_refs(ret);
			return NULL;
		}

		tbl->next = tab;

		n = n->next;

		if (n && !strcasecmp(n->value,"AS")) {
			if (!n->next) {
				error(r,CSVDB_ERROR_SYNTAX,r->q);
				table_free_refs(ret);
				return NULL;
			}
			n = n->next;
			tab->alias = strdup(n->value);
			n = n->next;
		}

		tbl = tab;
	} while (n && !strcmp(n->value,","));

	/* TODO: ON support */
	if (n && !strcasecmp(n->value,"ON")) {
		error(r,CSVDB_ERROR_UNSUPPORTED,r->q);
		table_free_refs(ret);
		return NULL;
	}else{
		row_t *l;
		row_t *b;
		row_t *rw;
		table_ref_t *t = ret;
		int c = 1;
		int tc = 0;
		int ctc = 0;
		int i;
		while (t) {
			t = t->next;
			tc++;
		}
		t = ret;
		while (t) {
			rw = t->t->rows;
			l = r->join;
			while (rw) {
				if (!l) {
					l = row_add(&r->join,c);
					for (i=0; i<tc; i++) {
						row_add(&l->t_data,c);
					}
					c++;
				}
				b = l->t_data;
				for (i=0; i<ctc; i++) {
					b = b->next;
				}
				if (!b) {
					error(r,CSVDB_ERROR_INTERNAL,"internal error in compiling join data\n");
					table_free_refs(ret);
					return NULL;
				}
				b->data = rw->data;
				l = l->next;
				rw = rw->next;
			}
			ctc++;
			t = t->next;
		}

	}

	*end = n;

	return ret;
}

/* get the string "SELECT x FROM y ..." from a sub query starting at node start,
 * and return the next node after the end of the query in end */
char* sql_subquery_getstring(result_t *or, nvp_t *start, nvp_t **end)
{
	char* ret;
	char* p;
	nvp_t *n = start;
	int bc = 1;
	int is;
	int len = strlen(or->q);
	ret = malloc(sizeof(char)*len);
	if (!ret)
		return NULL;

	ret[0] = 0;

	while (bc && n) {
		if (n->name && !strcmp(n->name,"'")) {
			is = 1;
		}else{
			is = 0;
		}
		if (n == start) {
			if (!strcasecmp(start->value,"(SELECT")) {
				strcpy(ret,"SELECT ");
			}else if (strcmp(start->value,"(")) {
				free(ret);
				error(or,CSVDB_ERROR_SUBQUERY,start->value);
				return NULL;
			}
			n = n->next;
			continue;
		}
		if (strchr(n->value,'('))
			bc++;
		if (strchr(n->value,')')) {
			bc--;
			if (!bc) {
				if (n->value[1]) {
					p = strchr(n->value,')');
					*p = 0;
					strcat(ret," ");
					if (is) {
						strcat(ret,"'");
						strcat(ret,n->value);
						strcat(ret,"'");
					}else{
						strcat(ret,n->value);
					}
					*p = ')';
				}
				n = n->next;
				break;
			}
		}
		strcat(ret," ");
		if (is) {
			strcat(ret,"'");
			strcat(ret,n->value);
			strcat(ret,"'");
		}else{
			strcat(ret,n->value);
		}
		n = n->next;
	}

	*end = n;

	if (bc) {
		free(ret);
		error(or,CSVDB_ERROR_SUBQUERY,start->value);
		return NULL;
	}

	return ret;
}

/* DESCRIBE table */
void sql_describe(result_t *r)
{
	nvp_t *n;
	row_t *row;
	column_add(&r->cols,"name",NULL,NULL);
	if (!r->keywords->next) {
		error(r,CSVDB_ERROR_SYNTAX,r->keywords->value);
		return;
	}else{
		r->table = table_resolve(r->keywords->next->value,r);
	}

	if (!r->table) {
		error(r,CSVDB_ERROR_TABLEREF,r->keywords->next->value);
		return;
	}

	n = r->table->t->columns;
	while (n) {
		row = row_add(&r->result,0);
		nvp_add(&row->data,NULL,n->value);
		n = n->next;
	}
}

/* SHOW COLUMNS FROM table */
void sql_show_columns(result_t *r)
{
	row_t *row;
	nvp_t *n = r->keywords->next;
	column_add(&r->cols,"name",NULL,NULL);
	if (!n->next || strcasecmp(n->next->value,"FROM")) {
		error(r,CSVDB_ERROR_SYNTAX,n->value);
		return;
	}
	n = n->next;
	if (!n->next) {
		error(r,CSVDB_ERROR_SYNTAX,n->value);
		return;
	}
	n = n->next;
	r->table = table_resolve(n->value,r);

	if (!r->table) {
		error(r,CSVDB_ERROR_TABLEREF,n->value);
		return;
	}

	n = r->table->t->columns;
	while (n) {
		row = row_add(&r->result,0);
		nvp_add(&row->data,NULL,n->value);
		n = n->next;
	}
}

/* SHOW TABLES */
void sql_show_tables(result_t *r)
{
	char buff[1024];
	int i;
	nvp_t *l;
	row_t *row;
	char* cv = NULL;
	if (nvp_count(r->keywords) < 2) {
		l = nvp_last(r->keywords);
		error(r,CSVDB_ERROR_SYNTAX,l->value);
		return;
	}
	l = r->keywords->next;

	if (!strcasecmp(l->value,"TABLES")) {
		table_t *t;
		column_add(&r->cols,"name",NULL,NULL);
		column_add(&r->cols,"aliases",NULL,NULL);
		if (l->next && !strcasecmp(l->next->value,"LIKE")) {
			l = l->next;
			if (!l->next) {
				error(r,CSVDB_ERROR_SYNTAX,l->value);
				return;
			}
			l = l->next;
			remove_wildcard(buff,l->value);
			cv = strdup(buff);
		}
		t = tables;
		while (t) {
			if (cv) {
				if (!strcasestr(t->name->value,cv)) {
					l = t->name->next;
					while (l) {
						if (strcasestr(l->value,cv))
							goto table_name_found;
						l = l->next;
					}
					t = t->next;
					continue;
				}
			}
table_name_found:
			row = row_add(&r->result,0);
			nvp_add(&row->data,NULL,t->name->value);
			l = t->name->next;
			buff[0] = 0;
			i = 0;
			while (l) {
				if (i)
					strcat(buff,",");
				strcat(buff,l->value);
				l = l->next;
				i++;
			}
			nvp_add(&row->data,NULL,buff);
			t = t->next;
		}
	}else{
		error(r,CSVDB_ERROR_SYNTAX,l->value);
		return;
	}
}

/* SHOW SETTINGS */
void sql_show_settings(result_t *r)
{
	row_t *rw;
	column_add(&r->cols,"setting",NULL,NULL);
	column_add(&r->cols,"state",NULL,NULL);

	rw = row_add(&r->result,1);
	nvp_add(&rw->data,NULL,"DEBUG");
	if (csvdb_settings&CSVDB_SET_DEBUG) {
		nvp_add(&rw->data,NULL,"ON");
	}else{
		nvp_add(&rw->data,NULL,"OFF");
	}

	rw = row_add(&r->result,1);
	nvp_add(&rw->data,NULL,"PERMANENT");
	if (csvdb_settings&CSVDB_SET_PERMANENT) {
		nvp_add(&rw->data,NULL,"ON");
	}else{
		nvp_add(&rw->data,NULL,"OFF");
	}
}

/* SHOW ... */
void sql_show(result_t *r)
{
	nvp_t *l;
	if (nvp_count(r->keywords) < 2) {
		l = nvp_last(r->keywords);
		error(r,CSVDB_ERROR_SYNTAX,l->value);
		return;
	}
	l = r->keywords->next;

	if (!strcasecmp(l->value,"TABLES")) {
		sql_show_tables(r);
	}else if (!strcasecmp(l->value,"COLUMNS")) {
		sql_show_columns(r);
	}else if (!strcasecmp(l->value,"SETTINGS")) {
		sql_show_settings(r);
	}else{
		error(r,CSVDB_ERROR_SYNTAX,l->value);
		return;
	}
}

/* SELECT columns FROM table */
void sql_select(result_t *r)
{
	char buff[1024];
	int k;
	int lim1;
	int wor = 0;
	char* c;
	char* c1;
	char* c2;
	result_t *sub;
	result_t *s;
	column_ref_t *col = NULL;
	table_ref_t *tbl;
	nvp_t *l = NULL;
	nvp_t *t;
	nvp_t *u;
	nvp_t *output_opts = NULL;
	nvp_t *of = NULL;
	nvp_t *n = r->keywords;
	nvp_t *q = nvp_search(r->keywords,"FROM");
	nvp_t *cc = q;
	if (!q) {
		error(r,CSVDB_ERROR_TABLEREF,r->q);
		return;
	}
	if (!q->next) {
		error(r,CSVDB_ERROR_SYNTAX,q->value);
		return;
	}

	q = q->next;

	tbl = table_resolve(q->value,r);
	if (!tbl)
		return;

	if (q->next && q->next->next && !strcasecmp(q->next->value,"AS")) {
		q = q->next->next;
		tbl->alias = strdup(q->value);
	}

	/* support multiple tables using implicit or explicit JOIN's */
	k = nvp_searchi(q,"JOIN");
	if (k < 0 && q->next && !strcmp(q->next->value,","))
		k = nvp_searchi(q,",");
	lim1 = nvp_searchi(q,"(");
	if (lim1 < 0)
		lim1 = nvp_searchi(q,"(SELECT");

	if (k > -1 && ((lim1 < 0) || k < lim1)) {
		if (!(tbl = sql_join(r,q,&cc,tbl)))
			return;
	}

	r->table = tbl;

	if (!r->table)
		return;

	nvp_add(&r->limit,NULL,"0");
	nvp_add(&r->limit,NULL,"-1");

	q = NULL;

	lim1 = 0;
	while (n) {
		if (!strcasecmp(n->value,"FROM"))
			break;
		if (!strcmp(n->value,",")) {
			n = n->next;
			continue;
		}
		if (is_keyword(n->value)) {
			nvp_add(&q,NULL,n->value);
			n = n->next;
			continue;
		}
		if (!strcasecmp(n->value,"AS")) {
			if (!col || !n->next) {
				error(r,CSVDB_ERROR_SYNTAX,r->q);
				return;
			}
			n = n->next;
			if (lim1) {
				t = nvp_last(r->count);
				if (!t) {
					error(r,CSVDB_ERROR_INTERNAL,n->value);
					return;
				}
				t->child = nvp_create(NULL,"AS");
				nvp_add(&t->child,NULL,n->value);
			}else{
				col->alias = strdup(n->value);
			}
			if (n)
				n = n->next;
			continue;
		}
		if ((c = strchr(n->value,'('))) {
			*c = 0;
			if (!strcasecmp(n->value,"COUNT")) {
				*c = '(';
				t = nvp_add(&r->count,NULL,n->value);
				if (t) {
					*c = 0;
					c1 = c+1;
					c2 = strchr(c1,')');
					if (!c2) {
						*c = '(';
						error(r,CSVDB_ERROR_SYNTAX,n->value);
						return;
					}
					if (*(c2+1) == ')')
						c2++;
					*c2 = 0;
					col = column_find(c1,r);
					*c = '(';
					*c2 = ')';
					if (!col)
						return;
					col->keywords = q;
					q = NULL;
				}
				lim1 = 1;
				n = n->next;
				continue;
			}
			*c = '(';
		}
		col = column_find(n->value,r);
		if (!col)
			return;
		col->keywords = q;
		q = NULL;
		column_push(&r->cols,col);
		lim1 = 0;
		n = n->next;
	}

	n = cc;

	while (n) {
		if (is_keyword(n->value)) {
			if (!strcasecmp(n->value,"LIMIT")) {
				l = n;
				n = n->next;
				if (!n) {
					error(r,CSVDB_ERROR_SYNTAX,l->value);
					return;
				}
				nvp_set(r->limit->next,NULL,n->value);
				n = n->next;
				if (n && n->next) {
					if (!strcmp(n->value,",")) {
						n = n->next;
						nvp_set(r->limit,NULL,r->limit->next->value);
						nvp_set(r->limit->next,NULL,n->value);
						n = n->next;
					}else if (!strcasecmp(n->value,"OFFSET")) {
						n = n->next;
						nvp_set(r->limit,NULL,n->value);
						n = n->next;
					}
				}
				continue;
			}else if (!strcasecmp(n->value,"WHERE")) {
				n = n->next;
				while (n) {
					if (!strcasecmp(n->value,"AND")) {
						n = n->next;
						wor = 0;
						continue;
					}else if (!strcasecmp(n->value,"OR")) {
						n = n->next;
						wor = 1;
						continue;
					}else if (is_keyword(n->value)) {
						break;
					}
					col = column_find(n->value,r);
					if (!col)
						return;
					n = n->next;
					if (!n) {
						error(r,CSVDB_ERROR_SYNTAX,col->name);
						return;
					}else if (!n->next) {
						error(r,CSVDB_ERROR_SYNTAX,n->value);
						return;
					}else if (!strcmp(n->value,"=")) {
						k = CMP_EQUALS;
					}else if (!strcmp(n->value,"!=")) {
						k = CMP_NOTEQUALS;
					}else if (!strcasecmp(n->value,"IS")) {
						if (!strcasecmp(n->next->value,"NOT") && n->next->next && !is_keyword(n->next->next->value)) {
							k = CMP_NOTEQUALS;
							n = n->next;
						}else{
							k = CMP_EQUALS;
						}
					}else if (!strcasecmp(n->value,"LIKE")) {
						k = CMP_LIKE;
					}else if (!strcasecmp(n->value,">")) {
						k = CMP_GREATER;
					}else if (!strcasecmp(n->value,">=")) {
						k = CMP_GREATEROREQ;
					}else if (!strcasecmp(n->value,"<")) {
						k = CMP_LESS;
					}else if (!strcasecmp(n->value,"<=")) {
						k = CMP_LESSOREQ;
					}else if (!strcasecmp(n->value,"IN")) {
						k = CMP_IN;
						if (!strcmp(n->next->value,"(") || !strcasecmp(n->next->value,"(SELECT")) {
							c = sql_subquery_getstring(r,n->next,&u);
							if (!c)
								return;
							sub = csvdb_query(c);
							if (!sub) {
								error(r,CSVDB_ERROR_SUBQUERY,c);
								free(c);
								return;
							}else if (sub->error) {
								r->error = sub->error;
								sub->error = NULL;
								csvdb_free_result(sub);
								free(c);
								return;
							}
							free(c);
							if (r->sub) {
								s = r->sub;
								while (s->next) {
									s = s->next;
								}
								s->next = sub;
							}else{
								r->sub = sub;
							}
							if (wor) {
								q = nvp_add(&l->child,col->name,NULL);
								q->num = CMP_IN;
								q->value = (char*)sub;
							}else{
								l = nvp_add(&r->where,col->name,NULL);
								l->num = CMP_IN;
								l->value = (char*)sub;
							}
							n = u;
							continue;
						}
					}else if (!strcasecmp(n->value,"NOT")) {
						if (n->next->next) {
							if (!strcasecmp(n->next->value,"LIKE")) {
								k = CMP_NOTLIKE;
								n = n->next;
							}else if (!strcasecmp(n->next->value,"IN")) {
								k = CMP_NOTIN;
								n = n->next;
								if (!strcmp(n->next->value,"(") || !strcasecmp(n->next->value,"(SELECT")) {
									result_t *sub;
									c = sql_subquery_getstring(r,n->next,&u);
									if (!c)
										return;
									sub = csvdb_query(c);
									if (!sub) {
										error(r,CSVDB_ERROR_SUBQUERY,c);
										free(c);
										return;
									}else if (sub->error) {
										r->error = sub->error;
										sub->error = NULL;
										csvdb_free_result(sub);
										free(c);
										return;
									}
									free(c);
									if (r->sub) {
										s = r->sub;
										while (s->next) {
											s = s->next;
										}
										s->next = sub;
									}else{
										r->sub = sub;
									}
									if (wor) {
										q = nvp_add(&l->child,col->name,NULL);
										q->num = CMP_NOTIN;
										q->value = (char*)sub;
									}else{
										l = nvp_add(&r->where,col->name,NULL);
										l->num = CMP_NOTIN;
										l->value = (char*)sub;
									}
									n = u;
									continue;
								}
							}else{
								k = CMP_NOTEQUALS;
							}
						}else{
							k = CMP_NOTEQUALS;
						}
					}else{
						error(r,CSVDB_ERROR_SYNTAX,col->name);
						return;
					}
					n = n->next;
					if (wor) {
						q = nvp_add(&l->child,col->name,n->value);
						q->num = k;
						if (strchr(n->value,'%')) {
							remove_wildcard(buff,n->value);
							nvp_set(q,col->name,buff);
						}
						if (is_numeric(n->value) || (n->name && !strcmp(n->name,"'")) || !strcmp(n->value,"NULL"))
							q->num |= CMP_STRING;
					}else{
						l = nvp_add(&r->where,col->name,n->value);
						l->num = k;
						if (strchr(n->value,'%')) {
							remove_wildcard(buff,n->value);
							nvp_set(l,col->name,buff);
						}
						if (is_numeric(n->value) || (n->name && !strcmp(n->name,"'")) || !strcmp(n->value,"NULL"))
							l->num |= CMP_STRING;
					}
					column_free(col);
					n = n->next;
				}
				continue;
			}else if (!strcasecmp(n->value,"GROUP") && n->next && !strcasecmp(n->next->value,"BY")) {
				q = n->next->next;
				if (!q) {
					error(r,CSVDB_ERROR_SYNTAX,n->next->value);
					return;
				}
				while (q) {
					if (!strcmp(q->value,",")) {
						q = q->next;
						continue;
					}
					if (is_keyword(q->value))
						break;
					col = column_find(q->value,r);
					if (!col)
						return;
					nvp_add(&r->group,NULL,col->name);
					column_free(col);
					q = q->next;
				}
				n = q;
				continue;
			}else if (!strcasecmp(n->value,"ORDER") && n->next && !strcasecmp(n->next->value,"BY")) {
				q = n->next->next;
				if (!q) {
					error(r,CSVDB_ERROR_SYNTAX,n->next->value);
					return;
				}
				while (q) {
					if (!strcmp(q->value,",")) {
						q = q->next;
						continue;
					}
					col = column_find(q->value,r);
					if (!col)
						return;
					t = nvp_add(&r->order,NULL,col->name);
					column_free(col);
					q = q->next;
					while (q) {
						if (!strcasecmp(q->value,"AS")) {
							q = q->next;
							if (!q) {
								error(r,CSVDB_ERROR_SYNTAX,r->q);
								return;
							}
							if (!strcasecmp(q->value,"INT")) {
								nvp_add(&t->child,NULL,q->value);
							}else if (strcasecmp(q->value,"STRING")) {
								error(r,CSVDB_ERROR_SYNTAX,q->value);
								return;
							}
						}else if (!strcasecmp(q->value,"DESC")) {
							nvp_add(&t->child,NULL,q->value);
						}else if (strcasecmp(q->value,"ASC")) {
							break;
						}
						q = q->next;
					}
				}
				n = q;
				continue;
			}else if (!strcasecmp(n->value,"HAVING")) {
				if (!r->group) {
					error(r,CSVDB_ERROR_SYNTAX,r->q);
					return;
				}
				wor = 0;
				n = n->next;
				while (n) {
					if (!strcasecmp(n->value,"AND")) {
						n = n->next;
						wor = 0;
						continue;
					}else if (!strcasecmp(n->value,"OR")) {
						n = n->next;
						wor = 1;
						continue;
					}
					col = column_resolve(n->value,r);
					if (!col)
						return;
					n = n->next;
					if (!n) {
						error(r,CSVDB_ERROR_SYNTAX,col->name);
						return;
					}else if (!n->next) {
						error(r,CSVDB_ERROR_SYNTAX,n->value);
						return;
					}else if (!strcmp(n->value,"=")) {
						k = CMP_EQUALS;
					}else if (!strcmp(n->value,"!=")) {
						k = CMP_NOTEQUALS;
					}else if (!strcasecmp(n->value,"IS")) {
						if (!strcasecmp(n->next->value,"NOT") && n->next->next && !is_keyword(n->next->next->value)) {
							k = CMP_NOTEQUALS;
							n = n->next;
						}else{
							k = CMP_EQUALS;
						}
					}else if (!strcasecmp(n->value,"LIKE")) {
						k = CMP_LIKE;
					}else if (!strcasecmp(n->value,">")) {
						k = CMP_GREATER;
					}else if (!strcasecmp(n->value,">=")) {
						k = CMP_GREATEROREQ;
					}else if (!strcasecmp(n->value,"<")) {
						k = CMP_LESS;
					}else if (!strcasecmp(n->value,"<=")) {
						k = CMP_LESSOREQ;
					}else if (!strcasecmp(n->value,"IN")) {
						k = CMP_IN;
					}else if (!strcasecmp(n->value,"NOT")) {
						if (n->next->next) {
							if (!strcasecmp(n->next->value,"LIKE")) {
								k = CMP_NOTLIKE;
								n = n->next;
							}else if (!strcasecmp(n->next->value,"IN")) {
								k = CMP_NOTIN;
								n = n->next;
							}else{
								k = CMP_NOTEQUALS;
							}
						}else{
							k = CMP_NOTEQUALS;
						}
					}else{
						error(r,CSVDB_ERROR_SYNTAX,col->name);
						return;
					}
					n = n->next;
					if (wor) {
						q = nvp_add(&l->child,col->name,n->value);
						q->num = k;
						if (strchr(n->value,'%')) {
							remove_wildcard(buff,n->value);
							nvp_set(q,col->name,buff);
						}
						if (is_numeric(n->value) || (n->name && !strcmp(n->name,"'")) || !strcmp(n->value,"NULL"))
							q->num |= CMP_STRING;
					}else{
						l = nvp_add(&r->having,col->name,n->value);
						l->num = k;
						if (strchr(n->value,'%')) {
							remove_wildcard(buff,n->value);
							nvp_set(l,col->name,buff);
						}
						if (is_numeric(n->value) || (n->name && !strcmp(n->name,"'")) || !strcmp(n->value,"NULL"))
							l->num |= CMP_STRING;
					}
					n = n->next;
				}
				continue;
			}else if (!strcasecmp(n->value,"INTO") && n->next && n->next->next && !strcasecmp(n->next->value,"OUTFILE")) {
				n = n->next->next;
				of = n;
				l = n->next;
				if (l && !strcasecmp(l->value,"FIELDS")) {
					l = l->next;
					if (
						!l || strcasecmp(l->value,"TERMINATED")
						|| !l->next || strcasecmp(l->next->value,"BY")
						|| !l->next->next
					) {
						error(r,CSVDB_ERROR_SYNTAX,l->value);
						return;
					}
					l = l->next->next;
					if (strcasecmp(l->value,"DEFAULT")) {
						output_opts = nvp_create("sep",l->value);
					}
					l = l->next;
					if (l && l->next && l->next->next) {
						if (!strcasecmp(l->value,"OPTIONALLY")) {
							l = l->next;
							if (!l->next->next) {
								error(r,CSVDB_ERROR_SYNTAX,l->value);
								return;
							}
						}
						if (
							!strcasecmp(l->value,"ENCLOSED")
							&& !strcasecmp(l->next->value,"BY")
						) {
							l = l->next->next;
							if (strcasecmp(l->value,"DEFAULT")) {
								nvp_add(&output_opts,"enc",l->value);
							}
							l = l->next;
						}
						if (l && l->next && l->next->next) {
							if (
								!strcasecmp(l->value,"ESCAPED")
								&& !strcasecmp(l->next->value,"BY")
							) {
								l = l->next->next;
								if (strcasecmp(l->value,"DEFAULT")) {
									nvp_add(&output_opts,"esc",l->value);
								}
								l = l->next;
							}
						}
					}
				}
				n = l;
				continue;
			}else if (strcasecmp(n->value,"FROM") && strcasecmp(n->prev->value,"FROM")) {
				error(r,CSVDB_ERROR_SYNTAX,n->value);
				return;
			}
		}else if (strcasecmp(n->value,"FROM") && strcasecmp(n->prev->value,"FROM")) {
			error(r,CSVDB_ERROR_SUBQUERY,n->value);
			return;
		}
		n = n->next;
	}

	result_where(r);
	result_distinct(r);
	if (r->group)
		result_group(r);
	if (r->count)
		result_count(r);
	if (r->order)
		result_order(r);
	if (r->group && r->having)
		result_having(r);
	if (atoi(r->limit->value) > 0 || atoi(r->limit->next->value) > -1)
		result_limit(r);
	if (!(r->count && !r->group))
		result_cols(r);

	if (of) {
		table_t *t;
		FILE *f;
		if (strcmp(of->value,"-") && (f = fopen(of->value,"r"))) {
			fclose(f);
			error(r,CSVDB_ERROR_FILEEXISTS,of->value);
		}else{
			t = result_to_table(r,of->value);
			table_write(t,of->value,output_opts);
		}
		row_free_all(r->result);
		r->result = NULL;
	}
}

/* LOAD DATA INFILE file [FIELDS [TERMINATED BY 'char'] [[OPTIONALLY] ENCLOSED BY 'char'] [ESCAPED BY 'char']] */
void sql_load(result_t *r)
{
	int i;
	int j;
	int ap = 0;
	int un = 0;
	char buff[128];
	table_t *t;
	nvp_t *o = NULL;
	nvp_t *f;
	nvp_t *l;
	nvp_t *u;
	nvp_t *of = NULL;
	char* n = NULL;
	nvp_t *cols = NULL;
	nvp_t *ocols = NULL;
	if (nvp_count(r->keywords) < 4) {
		l = nvp_last(r->keywords);
		error(r,CSVDB_ERROR_SYNTAX,l->value);
		return;
	}

	l = r->keywords->next;
	if (strcasecmp(l->value,"DATA")) {
		error(r,CSVDB_ERROR_SYNTAX,l->value);
		return;
	}

	l = l->next;
	if (strcmp(l->value,"INFILE")) {
		error(r,CSVDB_ERROR_SYNTAX,l->value);
		return;
	}

	l = l->next;
	if (!strcmp(l->value,"WITHOUT")) {
		l = l->next;
		if (strcmp(l->value,"NAMES")) {
			error(r,CSVDB_ERROR_SYNTAX,l->value);
			return;
		}
		un = 1;
		l = l->next;
		f = l;
		l = l->next;
		t = table_load_csv(f->value,NULL,NULL);
		if (!t) {
			error(r,CSVDB_ERROR_TABLEREF,f->value);
			return;
		}
		i = nvp_count(t->columns);
		table_free(f->value);
		for (j=0; j<i; j++) {
			sprintf(buff,"column%d",j+1);
			nvp_add(&cols,NULL,buff);
		}
	}else if (!strcmp(l->value,"APACHE")) {
		ap = 1;
		l = l->next;
		if (!l->next) {
			error(r,CSVDB_ERROR_SYNTAX,l->value);
			return;
		}
		f = l;
		l = l->next;
	}else{
		f = l;
		l = l->next;
	}

	if (l && !strcasecmp(l->value,"AS")) {
		if (!l->next) {
			error(r,CSVDB_ERROR_SYNTAX,l->value);
			return;
		}
		l = l->next;
		n = l->value;
		l = l->next;
	}

	if (l && !strcasecmp(l->value,"INTO")) {
		if (!l->next) {
			error(r,CSVDB_ERROR_SYNTAX,l->value);
			return;
		}
		l = l->next;
		if (!l->next || strcasecmp(l->value,"TABLE")) {
			error(r,CSVDB_ERROR_SYNTAX,l->value);
			return;
		}
		l = l->next;
		of = l;
		l = l->next;
	}

	if (l && l->value[0] == '(') {
		int go = 1;
		char* c = NULL;
		u = l->next;
		sprintf(buff,"%s",l->value+1);
		if ((c = strchr(buff,')')))
			*c = 0;
		nvp_add(&ocols,NULL,buff);
		if (c) {
			*c = ')';
		}else{
			while (go) {
				if (!u) {
					error(r,CSVDB_ERROR_SYNTAX,l->value);
					return;
				}
				sprintf(buff,"%s",u->value);
				if ((c = strchr(buff,')')))
					*c = 0;
				nvp_add(&ocols,NULL,buff);
				if (c) {
					*c = ')';
					go = 0;
				}
				u = u->next;
			}
		}
		l = u;
	}

	if (l && !strcasecmp(l->value,"FIELDS")) {
		l = l->next;
		if (
			!l || strcasecmp(l->value,"TERMINATED")
			|| !l->next || strcasecmp(l->next->value,"BY")
			|| !l->next->next
		) {
			error(r,CSVDB_ERROR_SYNTAX,l->value);
			return;
		}
		l = l->next->next;
		if (strcasecmp(l->value,"DEFAULT")) {
			o = nvp_create("sep",l->value);
		}
		l = l->next;
		if (l && l->next && l->next->next) {
			if (!strcasecmp(l->value,"OPTIONALLY")) {
				l = l->next;
				if (!l->next->next) {
					error(r,CSVDB_ERROR_SYNTAX,l->value);
					return;
				}
			}
			if (
				!strcasecmp(l->value,"ENCLOSED")
				&& !strcasecmp(l->next->value,"BY")
			) {
				l = l->next->next;
				if (strcasecmp(l->value,"DEFAULT")) {
					nvp_add(&o,"enc",l->value);
				}
				l = l->next;
			}
			if (l && l->next && l->next->next) {
				if (
					!strcasecmp(l->value,"ESCAPED")
					&& !strcasecmp(l->next->value,"BY")
				) {
					l = l->next->next;
					if (strcasecmp(l->value,"DEFAULT")) {
						nvp_add(&o,"esc",l->value);
					}
					l = l->next;
				}
			}
		}
	}

	if (un && ocols && !of) {
		nvp_free_all(cols);
		cols = ocols;
		ocols = NULL;
	}

	if (ap) {
		t = table_load_apache(f->value);
	}else{
		t = table_load_csv(f->value,cols,o);
	}
	if (!t) {
		error(r,CSVDB_ERROR_TABLEREF,f->value);
		return;
	}

	if (n)
		nvp_add(&t->name,NULL,n);

	nvp_free_all(cols);

	if (of) {
		i = table_write(t,of->value,o);
		if (i) {
			char bf[20];
			sprintf(bf,"%d",i);
			error(r,CSVDB_ERROR_INTERNAL,bf);
		}
		if (strcmp(of->value,"-")) {
			table_load_csv(of->value,NULL,o);
		}
	}
}

/* DROP TABLE table */
void sql_drop(result_t *r)
{
	int p = 0;
	int tp = 0;
	int i = 0;
	table_t *t;
	nvp_t *l;
	nvp_t *f = NULL;
	if (nvp_count(r->keywords) < 3) {
		l = nvp_last(r->keywords);
		error(r,CSVDB_ERROR_SYNTAX,l->value);
		return;
	}

	l = r->keywords->next;
	if (!strcmp(l->value,"TEMPORARY")) {
		tp = 1;
		l = l->next;
	}else if (!strcmp(l->value,"PERMANENT")) {
		p = 1;
		l = l->next;
	}

	if (strcasecmp(l->value,"TABLE")) {
		error(r,CSVDB_ERROR_SYNTAX,l->value);
		return;
	}

	l = l->next;
	f = l;
	l = l->next;

	if (l) {
		if (strcmp(l->value,"IF")) {
			error(r,CSVDB_ERROR_SYNTAX,l->value);
			return;
		}
		l = l->next;
		if (strcmp(l->value,"EXISTS")) {
			error(r,CSVDB_ERROR_SYNTAX,l->value);
			return;
		}
		l = l->next;
		if (l) {
			error(r,CSVDB_ERROR_SYNTAX,l->value);
			return;
		}
		i = 1;
	}

	t = table_find(f->value);
	if (!t) {
		if (!i)
			error(r,CSVDB_ERROR_TABLEREF,f->value);
		return;
	}

	if (p || (!tp && (csvdb_settings&CSVDB_SET_PERMANENT))) {
		errno = 0;
		if (unlink(t->name->value) < 0) {
			char bf[20];
			sprintf(bf,"%d",errno);
			error(r,CSVDB_ERROR_FILEREF,bf);
		}
	}

	table_free(f->value);
}

/* ALIAS TABLE table AS alias */
void sql_alias(result_t *r)
{
	nvp_t *l;
	nvp_t *n;
	table_t *t;
	if (nvp_count(r->keywords) < 5) {
		l = nvp_last(r->keywords);
		error(r,CSVDB_ERROR_SYNTAX,l->value);
		return;
	}
	l = r->keywords->next;
	if (strcasecmp(l->value,"TABLE")) {
		error(r,CSVDB_ERROR_SYNTAX,l->value);
		return;
	}
	l = l->next;
	n = l;
	l = l->next;
	if (strcasecmp(l->value,"AS")) {
		error(r,CSVDB_ERROR_SYNTAX,l->value);
		return;
	}
	l = l->next;
	t = table_find(n->value);
	if (!t) {
		error(r,CSVDB_ERROR_TABLEREF,n->value);
		return;
	}
	while (l) {
		nvp_add(&t->name,NULL,l->value);
		l = l->next;
		if (l) {
			if (strcmp(l->value,",")) {
				error(r,CSVDB_ERROR_SYNTAX,l->value);
				return;
			}
			l = l->next;
		}
	}
}

/* CREATE [TEMPORARY] TABLE [IF NOT EXISTS] tbl_name (create_definition,...) */
void sql_create(result_t *r)
{
	nvp_t *l;
	nvp_t *n;
	table_t *t;
	int bc = 0;
	if (nvp_count(r->keywords) < 4) {
		l = nvp_last(r->keywords);
		error(r,CSVDB_ERROR_SYNTAX,l->value);
		return;
	}
	l = r->keywords->next;
	if (!strcasecmp(l->value,"TEMPORARY"))
		l = l->next;

	if (strcasecmp(l->value,"TABLE")) {
		error(r,CSVDB_ERROR_SYNTAX,l->value);
		return;
	}
	l = l->next;
	if (!strcasecmp(l->value,"IF")) {
		if (!l->next || strcasecmp(l->next->value,"NOT")) {
			error(r,CSVDB_ERROR_SYNTAX,l->value);
			return;
		}
		l = l->next;
		if (!l->next || strcasecmp(l->next->value,"EXISTS")) {
			error(r,CSVDB_ERROR_SYNTAX,l->value);
			return;
		}
		l = l->next;
		if (!l->next) {
			error(r,CSVDB_ERROR_SYNTAX,l->value);
			return;
		}
		l = l->next;
	}
	n = l;
	if (!l->next) {
		error(r,CSVDB_ERROR_SYNTAX,l->value);
		return;
	}
	l = l->next;
	if (strcmp(l->value,"(")) {
		error(r,CSVDB_ERROR_SYNTAX,l->value);
		return;
	}else{
		if (!l->next) {
			error(r,CSVDB_ERROR_SYNTAX,l->value);
			return;
		}
		l = l->next;
	}

	t = table_add();
	if (!t) {
		error(r,CSVDB_ERROR_INTERNAL,n->value);
		return;
	}
	nvp_add(&t->name,NULL,n->value);

	while (l) {
		nvp_add(&t->columns,NULL,l->value);
		while (l) {
			if (!strcmp(l->value,","))
				break;
			if (!strcmp(l->value,"(")) {
				l = l->next;
				bc++;
				continue;
			}
			if (!strcmp(l->value,")")) {
				if (bc) {
					l = l->next;
					bc--;
					continue;
				}
				break;
			}
			l = l->next;
		}
		l = l->next;
	}
	if (csvdb_settings&CSVDB_SET_PERMANENT) {
		table_write(r->table->t,NULL,NULL);
	}
}

/* INSERT [IGNORE] INTO tbl_name [(col_name,...)] {VALUES | VALUE} ({expr | DEFAULT},...),(...),... */
void sql_insert(result_t *r)
{
	nvp_t *l;
	nvp_t *n;
	nvp_t *c;
	row_t *row;
	row_t *t;
	column_ref_t *col;
	char buff[1024];
	int key;
	if (nvp_count(r->keywords) < 7) {
		l = r->keywords;
		error(r,CSVDB_ERROR_SYNTAX,l->value);
		return;
	}
	l = r->keywords->next;
	if (!strcasecmp(l->value,"IGNORE")) {
		l = l->next;
	}
	if (strcasecmp(l->value,"INTO")) {
		error(r,CSVDB_ERROR_SYNTAX,l->value);
		return;
	}
	l = l->next;
	n = l;
	r->table = table_resolve(n->value,r);
	if (!r->table) {
		error(r,CSVDB_ERROR_TABLEREF,n->value);
		return;
	}
	l = l->next;
	if (!strcmp(l->value,"(")) {
		l = l->next;
		while (l) {
			col = column_find(l->value,r);
			column_push(&r->cols,col);
			while (l) {
				if (!strcmp(l->value,","))
					break;
				if (!strcmp(l->value,")")) {
					l = l->next;
					goto end_cols;
				}
				l = l->next;
			}
			l = l->next;
		}
	}else{
		r->cols = column_find("*",r);
	}
end_cols:
	if (!l) {
		error(r,CSVDB_ERROR_SYNTAX,n->value);
		return;
	}
	if (!l) {
		error(r,CSVDB_ERROR_SYNTAX,n->value);
		return;
	}else if (strcasecmp(l->value,"VALUES") && strcasecmp(l->value,"VALUE")) {
		error(r,CSVDB_ERROR_SYNTAX,l->value);
		return;
	}
	if (!l->next || strcmp(l->next->value,"(")) {
		error(r,CSVDB_ERROR_SYNTAX,l->value);
		return;
	}
	l = l->next->next;
	r->ar = 0;
	while (l) {
		row = row_add(&r->result,0);
		if (!row) {
			error(r,CSVDB_ERROR_INTERNAL,n->value);
			return;
		}
		nvp_add(&row->data,NULL,l->value);
		l = l->next;
		while (l) {
			if (!strcmp(l->value,",")) {
				l = l->next;
				continue;
			}
			if (!strcmp(l->value,")")) {
				l = l->next;
				if (l && l->next && !strcmp(l->value,",") && !strcmp(l->next->value,"(")) {
					l = l->next;
					break;
				}
				goto end_rows;
			}
			nvp_add(&row->data,NULL,l->value);
			l = l->next;
		}
		if (l)
			l = l->next;
	}
end_rows:

	t = r->result;
	while (t) {
		key = table_next_key(r->table->t);

		row = row_add(&r->table->t->rows,key);
		c = r->table->t->columns;
		while (c) {
			col = column_resolve(c->value,r);
			if (col) {
				n = column_fetch_data(t,col);
				if (n) {
					strcpy(buff,n->value);
				}else{
					buff[0] = 0;
				}
			}else{
				buff[0]= 0;
			}

			nvp_add(&row->data,NULL,buff);
			c = c->next;
		}
		r->ar++;
		t = t->next;
	}

	row_free_all(r->result);
	r->result = NULL;
	if (csvdb_settings&CSVDB_SET_PERMANENT) {
		table_write(r->table->t,NULL,NULL);
	}
}

/* UPDATE [IGNORE] table_reference SET col_name1=expr1 [, col_name2=expr2] ... [WHERE where_condition] [ORDER BY ...] [LIMIT row_count] */
void sql_update(result_t *r)
{
	nvp_t *l;
	nvp_t *n;
	nvp_t *t;
	nvp_t *q;
	row_t *row;
	row_t *rw;
	column_ref_t *col;
	char* b;
	char* c;
	char* v;
	char* qu;
	int k;
	int wor = 0;
	char buff[1024];
	if (nvp_count(r->keywords) < 4) {
		l = nvp_last(r->keywords);
		error(r,CSVDB_ERROR_SYNTAX,l->value);
		return;
	}

	nvp_add(&r->limit,NULL,"0");
	nvp_add(&r->limit,NULL,"-1");

	l = r->keywords->next;
	if (!strcasecmp(l->value,"IGNORE")) {
		l = l->next;
	}
	n = l;
	r->table = table_resolve(n->value,r);
	if (!r->table) {
		error(r,CSVDB_ERROR_TABLEREF,n->value);
		return;
	}
	l = l->next;
	if (!l->next || strcasecmp(l->value,"SET")) {
		error(r,CSVDB_ERROR_SYNTAX,l->value);
		return;
	}
	l = l->next;

	while (l) {
		if (!strcmp(l->value,",")) {
			l = l->next;
			continue;
		}
		if (is_keyword(l->value))
			break;

		c = l->value;
		if ((b = strchr(c,'='))) {
			*b = 0;
			v = b+1;
		}
		if (b && *v) {
			while (*v && *v == ' ') {
				v++;
			}
			if (v[0] == '\'')
				v++;
			buff[0] = 0;
			while (v) {
				qu = strchr(v,'\'');
				if (!qu) {
					strcat(buff,v);
					break;
				}
				*qu = 0;
				strcat(buff,v);
				*qu = '\'';
				if (*(qu-1) == '\\') {
					strcat(buff,"'");
				}
				v = qu+1;
			}
		}else{
			l = l->next;
			if (!l) {
				error(r,CSVDB_ERROR_SYNTAX,c);
				return;
			}
			if (!b) {
				if (!strcmp(l->value,"=")) {
					l = l->next;
					if (!l) {
						error(r,CSVDB_ERROR_SYNTAX,c);
						return;
					}
					strcpy(buff,l->value);
				}else if (l->value[0] == '=') {
					v = l->value+1;
					while (*v && *v == ' ') {
						v++;
					}
					if (v[0] == '\'')
						v++;
					buff[0] = 0;
					while (v) {
						qu = strchr(v,'\'');
						if (!qu) {
							strcat(buff,v);
							break;
						}
						*qu = 0;
						strcat(buff,v);
						*qu = '\'';
						if (*(qu-1) == '\\') {
							strcat(buff,"'");
						}
						v = qu+1;
					}
				}
			}
		}
		if (is_keyword(c))
			break;
		col = column_find(c,r);
		if (!col)
			return;
		nvp_add(&col->keywords,NULL,buff);
		column_push(&r->cols,col);
		if (l)
			l = l->next;
	}

	n = l;
	while (n) {
		if (is_keyword(n->value)) {
			if (!strcasecmp(n->value,"LIMIT")) {
				l = n;
				n = n->next;
				if (!n) {
					error(r,CSVDB_ERROR_SYNTAX,l->value);
					return;
				}
				nvp_set(r->limit->next,NULL,n->value);
				n = n->next;
				if (n && n->next) {
					if (!strcmp(n->value,",")) {
						n = n->next;
						nvp_set(r->limit,NULL,r->limit->next->value);
						nvp_set(r->limit->next,NULL,n->value);
						n = n->next;
					}else if (!strcasecmp(n->value,"OFFSET")) {
						n = n->next;
						nvp_set(r->limit,NULL,n->value);
						n = n->next;
					}
				}
				continue;
			}else if (!strcasecmp(n->value,"WHERE")) {
				n = n->next;
				while (n) {
					if (!strcasecmp(n->value,"AND")) {
						n = n->next;
						wor = 0;
						continue;
					}else if (!strcasecmp(n->value,"OR")) {
						n = n->next;
						wor = 1;
						continue;
					}
					col = column_find(n->value,r);
					if (!col)
						return;
					n = n->next;
					if (!n) {
						error(r,CSVDB_ERROR_SYNTAX,col->name);
						return;
					}else if (!n->next) {
						error(r,CSVDB_ERROR_SYNTAX,n->value);
						return;
					}else if (!strcmp(n->value,"=")) {
						k = CMP_EQUALS;
					}else if (!strcmp(n->value,"!=")) {
						k = CMP_NOTEQUALS;
					}else if (!strcasecmp(n->value,"IS")) {
						if (!strcasecmp(n->next->value,"NOT") && n->next->next && !is_keyword(n->next->next->value)) {
							k = CMP_NOTEQUALS;
							n = n->next;
						}else{
							k = CMP_EQUALS;
						}
					}else if (!strcasecmp(n->value,"LIKE")) {
						k = CMP_LIKE;
					}else if (!strcasecmp(n->value,">")) {
						k = CMP_GREATER;
					}else if (!strcasecmp(n->value,">=")) {
						k = CMP_GREATEROREQ;
					}else if (!strcasecmp(n->value,"<")) {
						k = CMP_LESS;
					}else if (!strcasecmp(n->value,"<=")) {
						k = CMP_LESSOREQ;
					}else if (!strcasecmp(n->value,"IN")) {
						k = CMP_IN;
					}else if (!strcasecmp(n->value,"NOT")) {
						if (n->next->next) {
							if (!strcasecmp(n->next->value,"LIKE")) {
								k = CMP_NOTLIKE;
								n = n->next;
							}else if (!strcasecmp(n->next->value,"IN")) {
								k = CMP_NOTIN;
								n = n->next;
							}else{
								k = CMP_NOTEQUALS;
							}
						}else{
							k = CMP_NOTEQUALS;
						}
					}else{
						error(r,CSVDB_ERROR_SYNTAX,col->name);
						return;
					}
					n = n->next;
					if (wor) {
						q = nvp_add(&l->child,col->name,n->value);
						q->num = k;
						if (strchr(n->value,'%')) {
							remove_wildcard(buff,n->value);
							nvp_set(q,col->name,buff);
						}
						if (is_numeric(n->value) || (n->name && !strcmp(n->name,"'")) || !strcmp(n->value,"NULL"))
							q->num |= CMP_STRING;
					}else{
						l = nvp_add(&r->where,col->name,n->value);
						l->num = k;
						if (strchr(n->value,'%')) {
							remove_wildcard(buff,n->value);
							nvp_set(l,col->name,buff);
						}
						if (is_numeric(n->value) || (n->name && !strcmp(n->name,"'")) || !strcmp(n->value,"NULL"))
							l->num |= CMP_STRING;
					}
					column_free(col);
					n = n->next;
				}
				continue;
			}else if (!strcasecmp(n->value,"ORDER") && n->next && !strcasecmp(n->next->value,"BY")) {
				q = n->next->next;
				if (!q) {
					error(r,CSVDB_ERROR_SYNTAX,n->next->value);
					return;
				}
				while (q) {
					if (!strcmp(q->value,",")) {
						q = q->next;
						continue;
					}
					col = column_find(n->value,r);
					if (!col)
						return;
					t = nvp_add(&r->order,NULL,col->name);
					column_free(col);
					q = q->next;
					while (q) {
						if (!strcasecmp(q->value,"AS")) {
							q = q->next;
							if (!q) {
								error(r,CSVDB_ERROR_SYNTAX,r->q);
								return;
							}
							if (!strcasecmp(q->value,"INT")) {
								nvp_add(&t->child,NULL,q->value);
							}else if (strcasecmp(q->value,"STRING")) {
								error(r,CSVDB_ERROR_SYNTAX,q->value);
								return;
							}
						}else if (!strcasecmp(q->value,"DESC")) {
							nvp_add(&t->child,NULL,q->value);
						}else if (strcasecmp(q->value,"ASC")) {
							break;
						}
						q = q->next;
					}
				}
				n = q;
				continue;
			}else if (strcasecmp(n->value,"FROM") && strcasecmp(n->prev->value,"FROM")) {
				error(r,CSVDB_ERROR_SYNTAX,n->value);
				return;
			}
		}else if (strcasecmp(n->value,"FROM") && strcasecmp(n->prev->value,"FROM")) {
			error(r,CSVDB_ERROR_SYNTAX,n->value);
			return;
		}
		n = n->next;
	}

	result_where(r);
	if (r->order)
		result_order(r);
	if (atoi(r->limit->value) > 0 || atoi(r->limit->next->value) > -1)
		result_limit(r);
	result_cols(r);

	rw = r->result;
	r->ar = 0;
	while (rw) {
		r->ar++;
		col = r->cols;
		row = row_search(r->table->t->rows,rw->key);
		if (!row) {
			error(r,CSVDB_ERROR_INTERNAL,r->table->t->name->value);
			goto end_update;
		}
		while (col) {
			l = column_fetch_data(row,col);
			if (!l) {
				error(r,CSVDB_ERROR_COLUMNREF,col->name);
				goto end_update;
			}
			nvp_set(l,NULL,col->keywords->value);
			col = col->next;
		}
		rw = rw->next;
	}
end_update:
	row_free_keys(r->result);
	r->result = NULL;
	if (csvdb_settings&CSVDB_SET_PERMANENT) {
		table_write(r->table->t,NULL,NULL);
	}
}

/* DELETE [IGNORE] FROM tbl_name [WHERE where_condition] [ORDER BY ...] [LIMIT row_count] */
void sql_delete(result_t *r)
{
	nvp_t *l;
	nvp_t *n;
	nvp_t *t;
	nvp_t *q;
	row_t *row;
	row_t *rw;
	column_ref_t *col;
	int wor = 0;
	int k;
	char buff[1024];
	int ign = 0;
	if (nvp_count(r->keywords) < 4) {
		l = nvp_last(r->keywords);
		error(r,CSVDB_ERROR_SYNTAX,l->value);
		return;
	}

	nvp_add(&r->limit,NULL,"0");
	nvp_add(&r->limit,NULL,"-1");

	l = r->keywords->next;
	if (!strcasecmp(l->value,"IGNORE")) {
		ign = 1;
		l = l->next;
	}

	if (strcasecmp(l->value,"FROM")) {
		error(r,CSVDB_ERROR_SYNTAX,l->value);
		return;
	}
	l = l->next;
	n = l;
	r->table = table_resolve(n->value,r);
	if (!r->table) {
		error(r,CSVDB_ERROR_TABLEREF,n->value);
		return;
	}
	l = l->next;

	n = l;
	while (n) {
		if (is_keyword(n->value)) {
			if (!strcasecmp(n->value,"LIMIT")) {
				l = n;
				n = n->next;
				if (!n) {
					error(r,CSVDB_ERROR_SYNTAX,l->value);
					return;
				}
				nvp_set(r->limit->next,NULL,n->value);
				n = n->next;
				if (n && n->next) {
					if (!strcmp(n->value,",")) {
						n = n->next;
						nvp_set(r->limit,NULL,r->limit->next->value);
						nvp_set(r->limit->next,NULL,n->value);
						n = n->next;
					}else if (!strcasecmp(n->value,"OFFSET")) {
						n = n->next;
						nvp_set(r->limit,NULL,n->value);
						n = n->next;
					}
				}
				continue;
			}else if (!strcasecmp(n->value,"WHERE")) {
				n = n->next;
				while (n) {
					if (!strcasecmp(n->value,"AND")) {
						n = n->next;
						wor = 0;
						continue;
					}else if (!strcasecmp(n->value,"OR")) {
						n = n->next;
						wor = 1;
						continue;
					}else if (is_keyword(n->value)) {
						break;
					}
					col = column_find(n->value,r);
					if (!col)
						return;
					n = n->next;
					if (!n) {
						error(r,CSVDB_ERROR_SYNTAX,col->name);
						return;
					}else if (!n->next) {
						error(r,CSVDB_ERROR_SYNTAX,n->value);
						return;
					}else if (!strcmp(n->value,"=")) {
						k = CMP_EQUALS;
					}else if (!strcmp(n->value,"!=")) {
						k = CMP_NOTEQUALS;
					}else if (!strcasecmp(n->value,"IS")) {
						if (!strcasecmp(n->next->value,"NOT") && n->next->next && !is_keyword(n->next->next->value)) {
							k = CMP_NOTEQUALS;
							n = n->next;
						}else{
							k = CMP_EQUALS;
						}
					}else if (!strcasecmp(n->value,"LIKE")) {
						k = CMP_LIKE;
					}else if (!strcasecmp(n->value,">")) {
						k = CMP_GREATER;
					}else if (!strcasecmp(n->value,">=")) {
						k = CMP_GREATEROREQ;
					}else if (!strcasecmp(n->value,"<")) {
						k = CMP_LESS;
					}else if (!strcasecmp(n->value,"<=")) {
						k = CMP_LESSOREQ;
					}else if (!strcasecmp(n->value,"IN")) {
						k = CMP_IN;
					}else if (!strcasecmp(n->value,"NOT")) {
						if (n->next->next) {
							if (!strcasecmp(n->next->value,"LIKE")) {
								k = CMP_NOTLIKE;
								n = n->next;
							}else if (!strcasecmp(n->next->value,"IN")) {
								k = CMP_NOTIN;
								n = n->next;
							}else{
								k = CMP_NOTEQUALS;
							}
						}else{
							k = CMP_NOTEQUALS;
						}
					}else{
						error(r,CSVDB_ERROR_SYNTAX,col->name);
						return;
					}
					n = n->next;
					if (wor) {
						q = nvp_add(&l->child,col->name,n->value);
						q->num = k;
						if (strchr(n->value,'%')) {
							remove_wildcard(buff,n->value);
							nvp_set(q,col->name,buff);
						}
						if (is_numeric(n->value) || (n->name && !strcmp(n->name,"'")) || !strcmp(n->value,"NULL"))
							q->num |= CMP_STRING;
					}else{
						l = nvp_add(&r->where,col->name,n->value);
						l->num = k;
						if (strchr(n->value,'%')) {
							remove_wildcard(buff,n->value);
							nvp_set(l,col->name,buff);
						}
						if (is_numeric(n->value) || (n->name && !strcmp(n->name,"'")) || !strcmp(n->value,"NULL"))
							l->num |= CMP_STRING;
					}
					column_free(col);
					n = n->next;
				}
				continue;
			}else if (!strcasecmp(n->value,"ORDER") && n->next && !strcasecmp(n->next->value,"BY")) {
				q = n->next->next;
				if (!q) {
					error(r,CSVDB_ERROR_SYNTAX,n->next->value);
					return;
				}
				while (q) {
					if (!strcmp(q->value,",")) {
						q = q->next;
						continue;
					}
					col = column_find(n->value,r);
					if (!col)
						return;

					t = nvp_add(&r->order,NULL,col->name);
					column_free(col);
					q = q->next;
					while (q) {
						if (!strcasecmp(q->value,"AS")) {
							q = q->next;
							if (!q) {
								error(r,CSVDB_ERROR_SYNTAX,r->q);
								return;
							}
							if (!strcasecmp(q->value,"INT")) {
								nvp_add(&t->child,NULL,q->value);
							}else if (strcasecmp(q->value,"STRING")) {
								error(r,CSVDB_ERROR_SYNTAX,q->value);
								return;
							}
						}else if (!strcasecmp(q->value,"DESC")) {
							nvp_add(&t->child,NULL,q->value);
						}else if (strcasecmp(q->value,"ASC")) {
							break;
						}
						q = q->next;
					}
				}
				n = q;
				continue;
			}else if (strcasecmp(n->value,"FROM") && strcasecmp(n->prev->value,"FROM")) {
				error(r,CSVDB_ERROR_SYNTAX,n->value);
				return;
			}
		}else if (strcasecmp(n->value,"FROM") && strcasecmp(n->prev->value,"FROM")) {
			error(r,CSVDB_ERROR_SYNTAX,n->value);
			return;
		}
		n = n->next;
	}

	result_where(r);
	if (r->order)
		result_order(r);
	if (atoi(r->limit->value) > 0 || atoi(r->limit->next->value) > -1)
		result_limit(r);


	rw = r->result;
	r->ar = 0;
	while (rw) {
		row = row_search(r->table->t->rows,rw->key);
		if (!row) {
			if (ign) {
				rw = rw->next;
				continue;
			}
			error(r,CSVDB_ERROR_INTERNAL,r->table->t->name->value);
			goto end_delete;
		}
		r->ar++;
		if (row->prev) {
			row->prev->next = row->next;
		}else{
			r->table->t->rows = row->next;
		}
		if (row->next)
			row->next->prev = row->prev;
		row->next = NULL;
		row_free_all(row);
		rw = rw->next;
	}
end_delete:
	/* keys, so we don't destroy the table! */
	row_free_keys(r->result);
	r->result = NULL;
	if (csvdb_settings&CSVDB_SET_PERMANENT) {
		table_write(r->table->t,NULL,NULL);
	}
}

static nvp_t *find_default(nvp_t *n)
{
	int d = nvp_searchi(n,"DEFAULT");
	int c = nvp_searchi(n,",");
	if (d < 0)
		return NULL;

	if (c > -1 && c < d)
		return NULL;

	return nvp_grabi(n,d+1);
}

void sql_alter(result_t *r)
{
	nvp_t *col;
	nvp_t *t;
	nvp_t *q;
	nvp_t *d;
	row_t *rw;
	int i;
	nvp_t *n = r->keywords->next;
	int ign = 0;

	if (!n) {
		error(r,CSVDB_ERROR_SYNTAX,r->keywords->value);
		return;
	}

	if (!strcasecmp(n->value,"IGNORE")) {
		ign = 1;
		n = n->next;
	}
	if (strcasecmp(n->value,"TABLE") || !n->next) {
		error(r,CSVDB_ERROR_SYNTAX,n->value);
		return;
	}
	n = n->next;
	r->table = table_resolve(n->value,r);
	if (!r->table)
		return;

	if (!n->next) {
		error(r,CSVDB_ERROR_SYNTAX,n->value);
		return;
	}

	do {
		if (!n || !n->next) {
			error(r,CSVDB_ERROR_SYNTAX,n->value);
			return;
		}
		n = n->next;
		if (!n->next) {
			error(r,CSVDB_ERROR_SYNTAX,n->value);
			return;
		}
		if (!strcasecmp(n->value,"ADD")) {
			n = n->next;
			if (n->next && !strcasecmp(n->value,"COLUMN")) {
				n = n->next;
				if (nvp_search(r->table->t->columns,n->value)) {
					if (ign) {
						n = nvp_search(n,",");
						continue;
					}else{
						error(r,CSVDB_ERROR_COLUMNREF,n->value);
						return;
					}
				}
				col = nvp_create(NULL,n->value);
				n = n->next;
				if (n) {
					if (!strcasecmp(n->value,"FIRST")) {
						col->next = r->table->t->columns;
						r->table->t->columns = col;
						if (col->next)
							col->next->prev = col;
						n = n->next;
						d = find_default(n);
						/* TODO: reorder data */
						rw = r->table->t->rows;
						while (rw) {
							if (d) {
								t = nvp_create(NULL,d->value);
							}else{
								t = nvp_create(NULL,NULL);
							}
							t->next = rw->data;
							rw->data = t;
							if (t->next)
								t->next->prev = t;
							rw = rw->next;
						}
					}else if (!strcasecmp(n->value,"AFTER")) {
						if (!n->next) {
							error(r,CSVDB_ERROR_COLUMNREF,n->value);
							return;
						}
						n = n->next;
						t = nvp_search(r->table->t->columns,n->value);
						if (!t) {
							error(r,CSVDB_ERROR_COLUMNREF,n->value);
							return;
						}
						i = nvp_searchi(r->table->t->columns,n->value);
						col->next = t->next;
						t->next = col;
						col->prev = t;
						if (col->next)
							col->next->prev = col;
						n = n->next;
						d = find_default(n);
						/* TODO: reorder data */
						rw = r->table->t->rows;
						while (rw) {
							t = nvp_grabi(rw->data,i);
							if (d) {
								q = nvp_create(NULL,d->value);
							}else{
								q = nvp_create(NULL,NULL);
							}
							q->next = t->next;
							t->next = q;
							q->prev = t;
							if (t->next)
								t->next->prev = t;
							rw = rw->next;
						}
					}else{
						goto ins_last_col;
					}
				}else{
ins_last_col:
					nvp_push(&r->table->t->columns,col);
					d = find_default(n);
					/* reorder data */
					if (d) {
						rw = r->table->t->rows;
						i = nvp_count(r->table->t->columns);
						i--;
						while (rw) {
							while (nvp_count(rw->data) < i) {
								nvp_add(&rw->data,NULL,NULL);
							}
							nvp_add(&rw->data,NULL,d->value);
							rw = rw->next;
						}
					}
				}
			}else{
				error(r,CSVDB_ERROR_SYNTAX,n->value);
				return;
			}
			n = nvp_search(n,",");
		}else if (!strcasecmp(n->value,"ALTER")) {
			error(r,CSVDB_ERROR_UNSUPPORTED,r->q);
			return;
			n = n->next;
		}else if (!strcasecmp(n->value,"CHANGE")) {
			if (!n->next) {
				error(r,CSVDB_ERROR_SYNTAX,n->value);
				return;
			}
			n = n->next;
			if (!strcasecmp(n->value,"COLUMN")) {
				if (!n->next) {
					error(r,CSVDB_ERROR_SYNTAX,n->value);
					return;
				}
				n = n->next;
			}
			if (!n->next) {
				error(r,CSVDB_ERROR_SYNTAX,n->value);
				return;
			}
			col = nvp_search(r->table->t->columns,n->value);
			if (!col) {
				if (ign && nvp_search(r->table->t->columns,n->next->value)) {
					n = nvp_search(n,",");
					continue;
				}else{
					error(r,CSVDB_ERROR_COLUMNREF,n->value);
					return;
				}
			}
			n = n->next;
			nvp_set(col,NULL,n->value);
			n = nvp_search(n,",");
		}else if (!strcasecmp(n->value,"MODIFY")) {
			error(r,CSVDB_ERROR_UNSUPPORTED,r->q);
			return;
			n = n->next;
		}else if (!strcasecmp(n->value,"DROP")) {
			if (!n->next) {
				error(r,CSVDB_ERROR_SYNTAX,n->value);
				return;
			}
			n = n->next;
			if (!strcasecmp(n->value,"COLUMN")) {
				if (!n->next) {
					error(r,CSVDB_ERROR_SYNTAX,n->value);
					return;
				}
				n = n->next;
			}
			i = nvp_searchi(r->table->t->columns,n->value);
			if (i < 0) {
				if (ign) {
					n = nvp_search(n,",");
					continue;
				}else{
					error(r,CSVDB_ERROR_COLUMNREF,n->value);
					return;
				}
			}
			col = nvp_grabi(r->table->t->columns,i);
			if (col->prev) {
				col->prev->next = col->next;
				if (col->next)
					col->next->prev = col->prev;
				nvp_free(col);
				rw = r->table->t->rows;
				while (rw) {
					col = nvp_grabi(rw->data,i);
					if (col) {
						col->prev->next = col->next;
						if (col->next)
							col->next->prev = col->prev;
						nvp_free(col);
					}
					rw = rw->next;
				}
			}else{
				r->table->t->columns = col->next;
				if (col->next)
					col->next->prev = NULL;
				nvp_free(col);
				rw = r->table->t->rows;
				while (rw) {
					col = nvp_grabi(rw->data,i);
					if (col) {
						rw->data = col->next;
						if (col->next)
							col->next->prev = col->prev;
						nvp_free(col);
					}
					rw = rw->next;
				}
			}
		}else if (!strcasecmp(n->value,"RENAME")) {
			if (!n->next) {
				error(r,CSVDB_ERROR_SYNTAX,n->value);
				return;
			}
			/* if the new name ends in '.csv' we're renaming the file,
			 * otherwise, we're just assigning a new alias */
			n = n->next;
			i = strlen(n->value);
			if (!strcmp(n->value+(i-4),".csv")) {
				nvp_set(r->table->t->name,NULL,n->value);
				if (table_write(r->table->t,NULL,NULL)) {
					error(r,CSVDB_ERROR_FILEREF,n->value);
					return;
				}
			}else{
				nvp_add(&r->table->t->name,NULL,n->value);
			}
		}else{
			error(r,CSVDB_ERROR_SYNTAX,n->value);
			return;
		}
	} while (n && !strcmp(n->value,","));
}

void sql_set(result_t *r)
{
	nvp_t *n = r->keywords->next;
	if (!n) {
		error(r,CSVDB_ERROR_SYNTAX,r->keywords->value);
		return;
	}

	if (!strcasecmp(n->value,"DEBUG")) {
		if (!n->next || !strcasecmp(n->next->value,"ON")) {
			csvdb_settings |= CSVDB_SET_DEBUG;
		}else if (n->next && !strcasecmp(n->next->value,"OFF")) {
			csvdb_settings &= ~CSVDB_SET_DEBUG;
		}else{
			error(r,CSVDB_ERROR_SYNTAX,n->value);
			return;
		}
	}else if (!strcasecmp(n->value,"PERMANENT")) {
		if (!n->next || !strcasecmp(n->next->value,"ON")) {
			csvdb_settings |= CSVDB_SET_PERMANENT;
		}else if (n->next && !strcasecmp(n->next->value,"OFF")) {
			csvdb_settings &= ~CSVDB_SET_PERMANENT;
		}else{
			error(r,CSVDB_ERROR_SYNTAX,n->value);
			return;
		}
	}
}

result_t *csvdb_query(char* q)
{
	result_t *r;
	if (!q)
		return NULL;

	r = malloc(sizeof(result_t));
	r->time = ticks();
	r->ar = 0;
	r->q = strdup(q);
	r->keywords = sql_split(q);
	r->table = NULL;
	r->cols = NULL;
	r->where = NULL;
	r->group = NULL;
	r->having = NULL;
	r->order = NULL;
	r->limit = NULL;
	r->count = NULL;
	r->result = NULL;
	r->error = NULL;
	r->next = NULL;
	r->sub = NULL;
	r->join = NULL;

	if (!r->keywords) {
		error(r,CSVDB_ERROR_SYNTAX,r->q);
		return r;
	}

	if (!strcasecmp(r->keywords->value,"DESCRIBE") || !strcasecmp(r->keywords->value,"DESC")) {
		sql_describe(r);
	}else if (!strcasecmp(r->keywords->value,"SELECT")) {
		sql_select(r);
	}else if (!strcasecmp(r->keywords->value,"LOAD")) {
		sql_load(r);
	}else if (!strcasecmp(r->keywords->value,"SHOW")) {
		sql_show(r);
	}else if (!strcasecmp(r->keywords->value,"ALIAS")) {
		sql_alias(r);
	}else if (!strcasecmp(r->keywords->value,"DROP")) {
		sql_drop(r);
	}else if (!strcasecmp(r->keywords->value,"INSERT")) {
		sql_insert(r);
	}else if (!strcasecmp(r->keywords->value,"UPDATE")) {
		sql_update(r);
	}else if (!strcasecmp(r->keywords->value,"DELETE")) {
		sql_delete(r);
	}else if (!strcasecmp(r->keywords->value,"CREATE")) {
		sql_create(r);
	}else if (!strcasecmp(r->keywords->value,"ALTER")) {
		sql_alter(r);
	}else if (!strcasecmp(r->keywords->value,"SET")) {
		sql_set(r);
	}else{
		error(r,CSVDB_ERROR_SYNTAX,r->keywords->value);
	}

	r->time = (ticks()-r->time);

	return r;
}
