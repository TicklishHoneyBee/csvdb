/************************************************************************
* sql.c
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

unsigned int ticks()
{
	unsigned int ticks;
	struct timeval now;
	gettimeofday(&now, NULL);
	ticks = (now.tv_sec*1000)+(now.tv_usec/1000);
	return ticks;
}

/* split a query into keywords */
nvp_t *sql_split(char* q, int bs)
{
	nvp_t *r = NULL;
	char c;
	char buff[1024];
	int b;
	int l;
	int s;
	int se;
	int i;

	b = 0;
	l = strlen(q)+1;
	s = 0;
	se = 0;
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
					se = 1;
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
		}else if (c == ',' || c == ' ' || (bs && (c == '(' || c == ')'))) {
column_end:
			buff[b] = 0;
			if (!b) {
				if (c == ',' || (bs && (c == '(' || c == ')'))) {
					sprintf(buff,"%c",c);
				}else{
					continue;
				}
			}
			nvp_add(&r,NULL,buff);
			if (b) {
				if (c == ',' || (bs && (c == '(' || c == ')'))) {
					sprintf(buff,"%c",c);
					nvp_add(&r,NULL,buff);
				}
			}
			b = 0;
			continue;
		}
		if (b < 1023)
			buff[b++] = c;
	}

	return r;
}

/* DESCRIBE table */
void sql_describe(result_t *r)
{
	nvp_t *n;
	nvp_add(&r->cols,NULL,"name");
	if (!r->keywords->next) {
		printf("syntax error near '%s': \"%s\"\r\n",r->keywords->value,r->q);
		return;
	}else if (!strcasecmp(r->keywords->next->value,"FILE")) {
		r->table = tables;
	}else{
		r->table = table_load_csv(r->keywords->next->value,NULL);
	}

	if (!r->table) {
		printf("invalid table or file referred to in '%s'\r\n",r->keywords->next->value);
		return;
	}

	n = r->table->columns;
	while (n) {
		nvp_add(&r->result,NULL,n->value);
		n = n->next;
	}
}

/* SHOW COLUMNS FROM table */
void sql_show_columns(result_t *r)
{
	nvp_t *n = r->keywords->next;
	nvp_add(&r->cols,NULL,"name");
	if (!n->next || strcasecmp(n->next->value,"FROM")) {
		printf("syntax error near '%s': \"%s\"\r\n",n->value,r->q);
		return;
	}
	n = n->next;
	if (!n->next) {
		printf("syntax error near '%s': \"%s\"\r\n",n->value,r->q);
		return;
	}
	n = n->next;
	if (!strcasecmp(n->value,"FILE")) {
		r->table = tables;
	}else{
		r->table = table_load_csv(n->value,NULL);
	}

	if (!r->table) {
		printf("invalid table or file referred to in '%s'\r\n",n->value);
		return;
	}

	n = r->table->columns;
	while (n) {
		nvp_add(&r->result,NULL,n->value);
		n = n->next;
	}
}

/* SHOW TABLES */
void sql_show_tables(result_t *r)
{
	char buff[1024];
	int i;
	nvp_t *l;
	char* cv = NULL;
	if (nvp_count(r->keywords) < 2) {
		l = nvp_last(r->keywords);
		printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
		return;
	}
	l = r->keywords->next;

	if (!strcasecmp(l->value,"TABLES")) {
		table_t *t;
		r->cols = nvp_create(NULL,"name");
		nvp_add(&r->cols,NULL,"aliases");
		if (l->next && !strcasecmp(l->next->value,"LIKE")) {
			l = l->next;
			if (!l->next) {
				printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
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
			nvp_add(&r->result,NULL,t->name->value);
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
			l = nvp_last(r->result);
			nvp_add(&l->child,NULL,buff);
			t = t->next;
		}
	}else{
		printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
		return;
	}
}

/* SHOW ... */
void sql_show(result_t *r)
{
	nvp_t *l;
	if (nvp_count(r->keywords) < 2) {
		l = nvp_last(r->keywords);
		printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
		return;
	}
	l = r->keywords->next;

	if (!strcasecmp(l->value,"TABLES")) {
		sql_show_tables(r);
	}else if (!strcasecmp(l->value,"COLUMNS")) {
		sql_show_columns(r);
	}else{
		printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
		return;
	}
}

/* SELECT columns FROM table */
void sql_select(result_t *r)
{
	char buff[1024];
	int k;
	int lim1;
	char* c;
	char* c1;
	char* c2;
	nvp_t *l;
	nvp_t *t;
	nvp_t *u;
	nvp_t *of = NULL;
	nvp_t *n = r->keywords;
	nvp_t *q = nvp_search(r->keywords,"FROM");
	if (!q) {
		printf("no table or file specified: \"%s\"\r\n",r->q);
		return;
	}
	if (!q->next) {
		printf("syntax error near '%s': \"%s\"\r\n",q->value,r->q);
		return;
	}

	q = q->next;

	if (!strcasecmp(q->value,"FILE")) {
		r->table = tables;
	}else{
		r->table = table_load_csv(q->value,NULL);
	}
	if (!r->table) {
		printf("invalid table or file referred to in '%s'\r\n",q->value);
		return;
	}

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
			l = nvp_last(r->cols);
			n = n->next;
			if (n && !is_keyword(n->value)) {
				if (!lim1) {
					nvp_add(&l->child,NULL,"AS");
					nvp_add(&l->child,NULL,n->value);
				}else if (r->count) {
					t = nvp_last(r->count);
					nvp_add(&t->child,NULL,"AS");
					nvp_add(&t->child,NULL,n->value);
				}
			}
			if (n)
				n = n->next;
			continue;
		}
		if ((c = strchr(n->value,'('))) {
			*c = 0;
			if (!strcasecmp(n->value,"COUNT")) {
				*c = '(';
				nvp_add(&r->count,NULL,n->value);
				t = nvp_last(r->count);
				if (t) {
					*c = 0;
					c1 = c+1;
					c2 = strchr(c1,')');
					if (!c2) {
						*c = '(';
						printf("syntax error near '%s': \"%s\"\r\n",n->value,r->q);
						return;
					}
					if (*(c2+1) == ')')
						c2++;
					*c2 = 0;
					if (is_numeric(c1)) {
						k = atoi(c1);
						k--;
						q = nvp_grabi(r->table->columns,k);
					}else{
						q = nvp_search(r->table->columns,c1);
					}
					if (!q) {
						if (!strcmp(c1,"*")) {
							q = r->table->columns;
						}else{
							char* c;
							if ((c = strchr(c1,'('))) {
								*c = 0;
								if (!strcasecmp(c1,"COLUMN")) {
									*c = '(';
									if (get_column_id(buff,r,c1))
										return;
									k = atoi(buff);
									k--;
									q = nvp_grabi(r->table->columns,k);
								}
								*c = '(';
							}
							u = r->cols;
							while (u) {
								if (u->child && (l = nvp_search(u->child,"AS")) && l->next && !strcmp(c1,l->next->value)) {
									q = nvp_search(r->table->columns,u->value);
									break;
								}
								u = u->next;
							}
						}
						if (!q) {
							printf("unknown column '%s' in COUNT for table %s\r\n",c1,r->table->name->value);
							return;
						}
					}
					strcpy(buff,t->value);
					nvp_set(t,q->value,buff);
					*c = '(';
					*c2 = ')';
				}
				lim1 = 1;
				n = n->next;
				continue;
			}else if (!strcasecmp(n->value,"COLUMN")) {
				*c = '(';
				if (get_column_id(buff,r,n->value))
					return;
				nvp_add(&r->cols,NULL,buff);
				lim1 = 0;
				n = n->next;
				continue;
			}
			*c = '(';
		}
		if (!strcmp(n->value,"*")) {
			l = r->table->columns;
			lim1 = 0;
			while (l) {
				nvp_add(&r->cols,NULL,l->value);
				l = l->next;
			}
			n = n->next;
			continue;
		}
		if (!nvp_search(r->table->columns,n->value)) {
			printf("unknown column '%s' in %s\r\n",n->value,r->table->name->value);
			return;
		}
		nvp_add(&r->cols,NULL,n->value);
		lim1 = 0;
		if (q) {
			l = nvp_last(r->cols);
			if (l)
				l->child = q;
			q = NULL;
		}
		n = n->next;
	}

	while (n) {
		if (is_keyword(n->value)) {
			if (!strcasecmp(n->value,"LIMIT")) {
				l = n;
				n = n->next;
				if (!n) {
					printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
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
					if (!strcasecmp(n->value,"AND") || !strcasecmp(n->value,"OR"))
						continue;
					t = nvp_search(r->table->columns,n->value);
					if (!t) {
						if (is_keyword(n->value))
							break;
						if ((c = strchr(n->value,'('))) {
							*c = 0;
							if (!strcasecmp(n->value,"COLUMN")) {
								*c = '(';
								if (get_column_id(buff,r,n->value))
									return;
								k = atoi(buff);
								k--;
								t = nvp_grabi(r->table->columns,k);
							}
							*c = '(';
						}
						q = r->cols;
						while (q) {
							if (q->child && (l = nvp_search(q->child,"AS")) && l->next && !strcmp(n->value,l->next->value)) {
								t = nvp_search(r->table->columns,q->value);
								break;
							}
							q = q->next;
						}
						if (!t) {
							printf("unknown column '%s' in WHERE clause for table %s\r\n",n->value,r->table->name->value);
							return;
						}
					}
					n = n->next;
					if (!n) {
						printf("syntax error near '%s': \"%s\"\r\n",t->value,r->q);
						return;
					}else if (!n->next) {
						printf("syntax error near '%s': \"%s\"\r\n",n->value,r->q);
						return;
					}else if (strcmp(n->value,"=") && strcasecmp(n->value,"LIKE") && strcasecmp(n->value,"IS")) {
						printf("syntax error near '%s': \"%s\"\r\n",t->value,r->q);
						return;
					}
					n = n->next;
					nvp_add(&r->where,t->value,n->value);
					if (strchr(n->value,'%')) {
						l = nvp_last(r->where);
						remove_wildcard(buff,n->value);
						nvp_add(&l->child,NULL,buff);
					}
					n = n->next;
				}
				continue;
			}else if (!strcasecmp(n->value,"GROUP") && n->next && !strcasecmp(n->next->value,"BY")) {
				q = n->next->next;
				if (!q) {
					printf("syntax error near '%s': \"%s\"\r\n",n->next->value,r->q);
					return;
				}
				while (q) {
					if (!strcmp(q->value,",")) {
						q = q->next;
						continue;
					}
					t = nvp_search(r->table->columns,q->value);
					if (!t) {
						if (is_keyword(q->value))
							break;
						if ((c = strchr(q->value,'('))) {
							*c = 0;
							if (!strcasecmp(q->value,"COLUMN")) {
								*c = '(';
								if (get_column_id(buff,r,q->value))
									return;
								k = atoi(buff);
								k--;
								t = nvp_grabi(r->table->columns,k);
							}
							*c = '(';
						}
						u = r->cols;
						while (u) {
							if (u->child && (l = nvp_search(u->child,"AS")) && l->next && !strcmp(q->value,l->next->value)) {
								t = nvp_search(r->table->columns,u->value);
								break;
							}
							u = u->next;
						}
						if (!t) {
							printf("unknown column '%s' in ORDER clause for table %s\r\n",n->value,r->table->name->value);
							return;
						}
					}
					nvp_add(&r->group,NULL,t->value);
					q = q->next;
				}
				n = q;
				continue;
			}else if (!strcasecmp(n->value,"ORDER") && n->next && !strcasecmp(n->next->value,"BY")) {
				q = n->next->next;
				if (!q) {
					printf("syntax error near '%s': \"%s\"\r\n",n->next->value,r->q);
					return;
				}
				while (q) {
					if (!strcmp(q->value,",")) {
						q = q->next;
						continue;
					}
					t = nvp_search(r->table->columns,q->value);
					if (!t) {
						if (is_keyword(q->value))
							break;
						if ((c = strchr(q->value,'('))) {
							*c = 0;
							if (!strcasecmp(q->value,"COLUMN")) {
								*c = '(';
								if (get_column_id(buff,r,q->value))
									return;
								k = atoi(buff);
								k--;
								t = nvp_grabi(r->table->columns,k);
							}
							*c = '(';
						}
						u = r->cols;
						while (u) {
							if (u->child && (l = nvp_search(u->child,"AS")) && l->next && !strcmp(q->value,l->next->value)) {
								t = nvp_search(r->table->columns,u->value);
								break;
							}
							u = u->next;
						}
						if (!t) {
							printf("unknown column '%s' in ORDER clause for table %s\r\n",n->value,r->table->name->value);
							return;
						}
					}
					nvp_add(&r->order,NULL,t->value);
					t = nvp_last(r->order);
					q = q->next;
					while (q) {
						if (!strcasecmp(q->value,"AS")) {
							q = q->next;
							if (!q) {
								printf("syntax error near 'AS': \"%s\"\r\n",r->q);
								return;
							}
							if (!strcasecmp(q->value,"INT")) {
								nvp_add(&t->child,NULL,q->value);
							}else if (strcasecmp(q->value,"STRING")) {
								printf("syntax error near unknown token '%s': \"%s\"\r\n",q->value,r->q);
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
			/* TODO: HAVING */
			}else if (!strcasecmp(n->value,"INTO") && n->next && n->next->next && !strcasecmp(n->next->value,"OUTFILE")) {
				n = n->next->next;
				of = n;
			}else if (strcasecmp(n->value,"FROM") && strcasecmp(n->prev->value,"FROM")) {
				printf("syntax error near '%s': \"%s\"\r\n",n->value,r->q);
				return;
			}
		}else if (strcasecmp(n->value,"FROM") && strcasecmp(n->prev->value,"FROM")) {
			printf("syntax error near '%s': \"%s\"\r\n",n->value,r->q);
			return;
		}
		n = n->next;
	}

	result_where(r);
	result_distinct(r);
	if (r->group)
		result_group(r);
	if (r->order)
		result_order(r);
	if (strncasecmp(r->q,"SELECT * FROM ",14))
		result_cols(r);
	if (r->count)
		result_count(r);
	if (atoi(r->limit->value) > 0 || atoi(r->limit->next->value) > -1)
		result_limit(r);

	if (of) {
		table_t *t;
		FILE *f;
		if (strcmp(of->value,"-") && (f = fopen(of->value,"r"))) {
			fclose(f);
			printf("file '%s' already exists\r\n",of->value);
		}else{
			t = result_to_table(r,of->value);
			table_write(t,of->value);
		}
		nvp_free_all(r->result);
		r->result = NULL;
	}
}

/* LOAD DATA INFILE file */
void sql_load(result_t *r)
{
	int i;
	int j;
	int ap = 0;
	int un = 0;
	char buff[128];
	table_t *t;
	nvp_t *f;
	nvp_t *l;
	nvp_t *u;
	nvp_t *of = NULL;
	char* n = NULL;
	nvp_t *cols = NULL;
	nvp_t *ocols = NULL;
	if (nvp_count(r->keywords) < 4) {
		l = nvp_last(r->keywords);
		printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
		return;
	}

	l = r->keywords->next;
	if (strcmp(l->value,"DATA")) {
		printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
		return;
	}

	l = l->next;
	if (strcmp(l->value,"INFILE")) {
		printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
		return;
	}

	l = l->next;
	if (!strcmp(l->value,"WITHOUT")) {
		l = l->next;
		if (strcmp(l->value,"NAMES")) {
			printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
			return;
		}
		un = 1;
		l = l->next;
		f = l;
		l = l->next;
		t = table_load_csv(f->value,NULL);
		if (!t) {
			printf("invalid table or file referred to in '%s'\r\n",f->value);
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
			printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
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
			printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
			return;
		}
		l = l->next;
		n = l->value;
		l = l->next;
	}

	if (l && !strcasecmp(l->value,"INTO")) {
		if (!l->next) {
			printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
			return;
		}
		l = l->next;
		if (!l->next || strcasecmp(l->value,"TABLE")) {
			printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
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
					printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
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

	if (un && ocols && !of) {
		nvp_free_all(cols);
		cols = ocols;
		ocols = NULL;
	}

	if (ap) {
		t = table_load_apache(f->value);
	}else{
		t = table_load_csv(f->value,cols);
	}
	if (!t) {
		printf("invalid table or file referred to in '%s'\r\n",f->value);
		return;
	}

	if (n)
		nvp_add(&t->name,NULL,n);

	nvp_free_all(cols);

	if (of) {
		i = table_write(t,of->value);
		if (strcmp(of->value,"-")) {
			table_load_csv(of->value,NULL);
		}
	}
}

/* DROP TABLE table */
void sql_drop(result_t *r)
{
	int p = 0;
	int i = 0;
	table_t *t;
	nvp_t *l;
	nvp_t *f = NULL;
	if (nvp_count(r->keywords) < 3) {
		l = nvp_last(r->keywords);
		printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
		return;
	}

	l = r->keywords->next;
	if (!strcmp(l->value,"TEMPORARY")) {
		l = l->next;
	}else if (!strcmp(l->value,"PERMANENT")) {
		p = 1;
		l = l->next;
	}

	if (strcasecmp(l->value,"TABLE")) {
		printf("tsyntax error near '%s': \"%s\"\r\n",l->value,r->q);
		return;
	}

	l = l->next;
	f = l;
	l = l->next;

	if (l) {
		if (strcmp(l->value,"IF")) {
			printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
			return;
		}
		l = l->next;
		if (strcmp(l->value,"EXISTS")) {
			printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
			return;
		}
		l = l->next;
		if (l) {
			printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
			return;
		}
		i = 1;
	}

	t = table_find(f->value);
	if (!t) {
		if (!i)
			printf("no such table '%s': \"%s\"\r\n",f->value,r->q);
		return;
	}

	if (p) {
		errno = 0;
		if (unlink(t->name->value) < 0) {
			printf("could not permanently drop table file (errno %d)\r\n",errno);
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
		printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
		return;
	}
	l = r->keywords->next;
	if (strcasecmp(l->value,"TABLE")) {
		printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
		return;
	}
	l = l->next;
	n = l;
	l = l->next;
	if (strcasecmp(l->value,"AS")) {
		printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
		return;
	}
	l = l->next;
	t = table_find(n->value);
	if (!t) {
		printf("invalid table or file referred to in '%s'\r\n",n->value);
		return;
	}
	while (l) {
		nvp_add(&t->name,NULL,l->value);
		l = l->next;
		if (l) {
			if (strcmp(l->value,",")) {
				printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
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
	char* b;
	int bc = 0;
	int ine = 0;
	nvp_free_all(r->keywords);
	r->keywords = NULL;
	b = strdup(r->q);
	r->keywords = sql_split(b,1);
	if (nvp_count(r->keywords) < 4) {
		l = nvp_last(r->keywords);
		printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
		return;
	}
	l = r->keywords->next;
	if (!strcasecmp(l->value,"TEMPORARY"))
		l = l->next;

	if (strcasecmp(l->value,"TABLE")) {
		printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
		return;
	}
	l = l->next;
	if (!strcasecmp(l->value,"IF")) {
		if (!l->next || strcasecmp(l->next->value,"NOT")) {
			printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
			return;
		}
		l = l->next;
		if (!l->next || strcasecmp(l->next->value,"EXISTS")) {
			printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
			return;
		}
		l = l->next;
		ine = 1;
		if (!l->next) {
			printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
			return;
		}
		l = l->next;
	}
	n = l;
	if (!l->next) {
		printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
		return;
	}
	l = l->next;
	if (strcmp(l->value,"(")) {
		printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
		return;
	}else{
		if (!l->next) {
			printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
			return;
		}
		l = l->next;
	}

	t = table_add();
	if (!t) {
		printf("internal error creating table %s\r\n",n->value);
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
}

/* INSERT [IGNORE] INTO tbl_name [(col_name,...)] {VALUES | VALUE} ({expr | DEFAULT},...),(...),... */
void sql_insert(result_t *r)
{
	nvp_t *l;
	nvp_t *n;
	nvp_t *c;
	nvp_t *row;
	char* b;
	int ig = 0;
	int k;
	char kbuff[20];
	char buff[1024];
	int key;
	nvp_free_all(r->keywords);
	r->keywords = NULL;
	b = strdup(r->q);
	r->keywords = sql_split(b,1);
	if (nvp_count(r->keywords) < 7) {
		l = nvp_last(r->keywords);
		printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
		return;
	}
	l = r->keywords->next;
	if (!strcasecmp(l->value,"IGNORE")) {
		ig = 1;
		l = l->next;
	}
	if (strcasecmp(l->value,"INTO")) {
		printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
		return;
	}
	l = l->next;
	n = l;
	r->table = table_load_csv(n->value,NULL);
	if (!r->table) {
		printf("invalid table or file referred to in '%s'\r\n",n->value);
		return;
	}
	l = l->next;
	if (!strcmp(l->value,"(")) {
		l = l->next;
		while (l) {
			nvp_add(&r->cols,NULL,l->value);
			while (l) {
				if (!strcmp(l->value,","))
					break;
				if (!strcmp(l->value,")"))
					goto end_cols;
				l = l->next;
			}
			l = l->next;
		}
	}
end_cols:
	if (!l) {
		printf("syntax error near '%s': \"%s\"\r\n",n->value,r->q);
		return;
	}
	l = l->next;
	if (!l) {
		printf("syntax error near '%s': \"%s\"\r\n",n->value,r->q);
		return;
	}else if (strcasecmp(l->value,"VALUES") && strcasecmp(l->value,"VALUE")) {
		printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
		return;
	}
	if (!l->next || strcmp(l->next->value,"(")) {
		printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
		return;
	}
	l = l->next->next;
	r->ar = 0;
	while (l) {
		nvp_add(&r->result,NULL,l->value);
		n = nvp_last(r->result);
		if (!n) {
			printf("internal error inserting values into table %s\r\n",n->value);
			return;
		}
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
			nvp_add(&n->child,NULL,l->value);
			l = l->next;
		}
		if (l)
			l = l->next;
	}
end_rows:

	n = r->result;
	while (n) {
		key = table_next_key(r->table);
		sprintf(kbuff,"%d",key);
		nvp_add(&r->table->rows,kbuff,NULL);
		row = nvp_last(r->table->rows);
		c = r->table->columns;
		while (c) {
			k = nvp_searchi(r->cols,c->value);
			if (k < 0) {
				buff[0]= 0;
			}else if (!k) {
				strcpy(buff,n->value);
			}else{
				l = nvp_grabi(n->child,k-1);
				if (!l) {
					buff[0]= 0;
				}else{
					strcpy(buff,l->value);
				}
			}
			if (c->prev) {
				nvp_add(&row->child,kbuff,buff);
			}else{
				nvp_set(row,kbuff,buff);
			}
			c = c->next;
		}
		r->ar++;
		n = n->next;
	}

	nvp_free_all(r->result);
	r->result = NULL;
}

/* UPDATE [IGNORE] table_reference SET col_name1=expr1 [, col_name2=expr2] ... [WHERE where_condition] [ORDER BY ...] [LIMIT row_count] */
void sql_update(result_t *r)
{
	nvp_t *l;
	nvp_t *n;
	nvp_t *t;
	nvp_t *row;
	char* b;
	char* c;
	char* v;
	char* q;
	int k;
	char buff[1024];
	char cbuff[1024];
	int ign = 0;
	if (nvp_count(r->keywords) < 4) {
		l = nvp_last(r->keywords);
		printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
		return;
	}

	nvp_add(&r->limit,NULL,"0");
	nvp_add(&r->limit,NULL,"-1");

	l = r->keywords->next;
	if (!strcasecmp(l->value,"IGNORE")) {
		ign = 1;
		l = l->next;
	}
	n = l;
	r->table = table_load_csv(n->value,NULL);
	if (!r->table) {
		printf("invalid table or file referred to in '%s'\r\n",n->value);
		return;
	}
	l = l->next;
	if (!l->next || strcasecmp(l->value,"SET")) {
		printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
		return;
	}
	l = l->next;

	while (l) {
		if (!strcmp(l->value,","))
			continue;
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
				q = strchr(v,'\'');
				if (!q) {
					strcat(buff,v);
					break;
				}
				*q = 0;
				strcat(buff,v);
				*q = '\'';
				if (*(q-1) == '\\') {
					strcat(buff,"'");
				}
				v = q+1;
			}
		}else{
			l = l->next;
			if (!l) {
				printf("syntax error near '%s': \"%s\"\r\n",c,r->q);
				return;
			}
			if (!b) {
				if (!strcmp(l->value,"=")) {
					l = l->next;
					if (!l) {
						printf("syntax error near '%s': \"%s\"\r\n",c,r->q);
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
						q = strchr(v,'\'');
						if (!q) {
							strcat(buff,v);
							break;
						}
						*q = 0;
						strcat(buff,v);
						*q = '\'';
						if (*(q-1) == '\\') {
							strcat(buff,"'");
						}
						v = q+1;
					}
				}
			}
		}
		t = nvp_search(r->table->columns,c);
		if (!t) {
			if (is_keyword(c))
				break;
			if ((b = strchr(c,'('))) {
				*b = 0;
				if (!strcasecmp(c,"COLUMN")) {
					*b = '(';
					if (get_column_id(cbuff,r,c))
						return;
					k = atoi(cbuff);
					k--;
					t = nvp_grabi(r->table->columns,k);
				}
				*b = '(';
			}
			if (!t) {
				printf("unknown column '%s' for table %s\r\n",l->value,r->table->name->value);
				return;
			}
		}
		nvp_add(&r->cols,buff,t->value);
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
					printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
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
					if (!strcasecmp(n->value,"AND") || !strcasecmp(n->value,"OR"))
						continue;
					t = nvp_search(r->table->columns,n->value);
					if (!t) {
						if (is_keyword(n->value))
							break;
						if ((c = strchr(n->value,'('))) {
							*c = 0;
							if (!strcasecmp(n->value,"COLUMN")) {
								*c = '(';
								if (get_column_id(buff,r,n->value))
									return;
								k = atoi(buff);
								k--;
								t = nvp_grabi(r->table->columns,k);
							}
							*c = '(';
						}
						if (!t) {
							printf("unknown column '%s' in WHERE clause for table %s\r\n",n->value,r->table->name->value);
							return;
						}
					}
					n = n->next;
					if (!n) {
						printf("syntax error near '%s': \"%s\"\r\n",t->value,r->q);
						return;
					}else if (!n->next) {
						printf("syntax error near '%s': \"%s\"\r\n",n->value,r->q);
						return;
					}else if (strcmp(n->value,"=") && strcasecmp(n->value,"LIKE") && strcasecmp(n->value,"IS")) {
						printf("syntax error near '%s': \"%s\"\r\n",t->value,r->q);
						return;
					}
					n = n->next;
					nvp_add(&r->where,t->value,n->value);
					if (strchr(n->value,'%')) {
						l = nvp_last(r->where);
						remove_wildcard(buff,n->value);
						nvp_add(&l->child,NULL,buff);
					}
					n = n->next;
				}
				continue;
			}else if (!strcasecmp(n->value,"ORDER") && n->next && !strcasecmp(n->next->value,"BY")) {
				row = n->next->next;
				if (!row) {
					printf("syntax error near '%s': \"%s\"\r\n",n->next->value,r->q);
					return;
				}
				while (row) {
					if (!strcmp(row->value,",")) {
						row = row->next;
						continue;
					}
					t = nvp_search(r->table->columns,row->value);
					if (!t) {
						if (is_keyword(row->value))
							break;
						if ((c = strchr(row->value,'('))) {
							*c = 0;
							if (!strcasecmp(row->value,"COLUMN")) {
								*c = '(';
								if (get_column_id(buff,r,row->value))
									return;
								k = atoi(buff);
								k--;
								t = nvp_grabi(r->table->columns,k);
							}
							*c = '(';
						}
						if (!t) {
							printf("unknown column '%s' in ORDER clause for table %s\r\n",n->value,r->table->name->value);
							return;
						}
					}
					nvp_add(&r->order,NULL,t->value);
					t = nvp_last(r->order);
					row = row->next;
					while (row) {
						if (!strcasecmp(row->value,"AS")) {
							row = row->next;
							if (!row) {
								printf("syntax error near 'AS': \"%s\"\r\n",r->q);
								return;
							}
							if (!strcasecmp(row->value,"INT")) {
								nvp_add(&t->child,NULL,row->value);
							}else if (strcasecmp(row->value,"STRING")) {
								printf("syntax error near unknown token '%s': \"%s\"\r\n",row->value,r->q);
								return;
							}
						}else if (!strcasecmp(row->value,"DESC")) {
							nvp_add(&t->child,NULL,row->value);
						}else if (strcasecmp(row->value,"ASC")) {
							break;
						}
						row = row->next;
					}
				}
				n = row;
				continue;
			}else if (strcasecmp(n->value,"FROM") && strcasecmp(n->prev->value,"FROM")) {
				printf("syntax error near '%s': \"%s\"\r\n",n->value,r->q);
				return;
			}
		}else if (strcasecmp(n->value,"FROM") && strcasecmp(n->prev->value,"FROM")) {
			printf("syntax error near '%s': \"%s\"\r\n",n->value,r->q);
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

	n = r->result;
	r->ar = 0;
	while (n) {
		r->ar++;
		t = r->cols;
		row = nvp_search_name(r->table->rows,n->name);
		if (!row) {
			printf("internal error in update for table %s\r\n",r->table->name->value);
			goto end_update;
		}
		while (t) {
			k = nvp_searchi(r->table->columns,t->value);
			if (!k) {
				l = row;
			}else{
				l = nvp_grabi(row->child,k-1);
			}
			if (!l) {
				printf("unknown column '%s' for table %s\r\n",t->value,r->table->name->value);
				goto end_update;
			}
			nvp_set(l,NULL,t->name);
			t = t->next;
		}
		n = n->next;
	}
end_update:
	nvp_free_all(r->result);
	r->result = NULL;
}

/* DELETE [IGNORE] FROM tbl_name [WHERE where_condition] [ORDER BY ...] [LIMIT row_count] */
void sql_delete(result_t *r)
{
	nvp_t *l;
	nvp_t *n;
	nvp_t *t;
	nvp_t *row;
	char* c;
	int k;
	char buff[1024];
	int ign = 0;
	if (nvp_count(r->keywords) < 4) {
		l = nvp_last(r->keywords);
		printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
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
		printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
		return;
	}
	l = l->next;
	n = l;
	r->table = table_load_csv(n->value,NULL);
	if (!r->table) {
		printf("invalid table or file referred to in '%s'\r\n",n->value);
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
					printf("syntax error near '%s': \"%s\"\r\n",l->value,r->q);
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
					if (!strcasecmp(n->value,"AND") || !strcasecmp(n->value,"OR"))
						continue;
					t = nvp_search(r->table->columns,n->value);
					if (!t) {
						if (is_keyword(n->value))
							break;
						if ((c = strchr(n->value,'('))) {
							*c = 0;
							if (!strcasecmp(n->value,"COLUMN")) {
								*c = '(';
								if (get_column_id(buff,r,n->value))
									return;
								k = atoi(buff);
								k--;
								t = nvp_grabi(r->table->columns,k);
							}
							*c = '(';
						}
						if (!t) {
							printf("unknown column '%s' in WHERE clause for table %s\r\n",n->value,r->table->name->value);
							return;
						}
					}
					n = n->next;
					if (!n) {
						printf("syntax error near '%s': \"%s\"\r\n",t->value,r->q);
						return;
					}else if (!n->next) {
						printf("syntax error near '%s': \"%s\"\r\n",n->value,r->q);
						return;
					}else if (strcmp(n->value,"=") && strcasecmp(n->value,"LIKE") && strcasecmp(n->value,"IS")) {
						printf("syntax error near '%s': \"%s\"\r\n",t->value,r->q);
						return;
					}
					n = n->next;
					nvp_add(&r->where,t->value,n->value);
					if (strchr(n->value,'%')) {
						l = nvp_last(r->where);
						remove_wildcard(buff,n->value);
						nvp_add(&l->child,NULL,buff);
					}
					n = n->next;
				}
				continue;
			}else if (!strcasecmp(n->value,"ORDER") && n->next && !strcasecmp(n->next->value,"BY")) {
				row = n->next->next;
				if (!row) {
					printf("syntax error near '%s': \"%s\"\r\n",n->next->value,r->q);
					return;
				}
				while (row) {
					if (!strcmp(row->value,",")) {
						row = row->next;
						continue;
					}
					t = nvp_search(r->table->columns,row->value);
					if (!t) {
						if (is_keyword(row->value))
							break;
						if ((c = strchr(row->value,'('))) {
							*c = 0;
							if (!strcasecmp(row->value,"COLUMN")) {
								*c = '(';
								if (get_column_id(buff,r,row->value))
									return;
								k = atoi(buff);
								k--;
								t = nvp_grabi(r->table->columns,k);
							}
							*c = '(';
						}
						if (!t) {
							printf("unknown column '%s' in ORDER clause for table %s\r\n",n->value,r->table->name->value);
							return;
						}
					}
					nvp_add(&r->order,NULL,t->value);
					t = nvp_last(r->order);
					row = row->next;
					while (row) {
						if (!strcasecmp(row->value,"AS")) {
							row = row->next;
							if (!row) {
								printf("syntax error near 'AS': \"%s\"\r\n",r->q);
								return;
							}
							if (!strcasecmp(row->value,"INT")) {
								nvp_add(&t->child,NULL,row->value);
							}else if (strcasecmp(row->value,"STRING")) {
								printf("syntax error near unknown token '%s': \"%s\"\r\n",row->value,r->q);
								return;
							}
						}else if (!strcasecmp(row->value,"DESC")) {
							nvp_add(&t->child,NULL,row->value);
						}else if (strcasecmp(row->value,"ASC")) {
							break;
						}
						row = row->next;
					}
				}
				n = row;
				continue;
			}else if (strcasecmp(n->value,"FROM") && strcasecmp(n->prev->value,"FROM")) {
				printf("syntax error near '%s': \"%s\"\r\n",n->value,r->q);
				return;
			}
		}else if (strcasecmp(n->value,"FROM") && strcasecmp(n->prev->value,"FROM")) {
			printf("syntax error near '%s': \"%s\"\r\n",n->value,r->q);
			return;
		}
		n = n->next;
	}

	result_where(r);
	if (r->order)
		result_order(r);
	if (atoi(r->limit->value) > 0 || atoi(r->limit->next->value) > -1)
		result_limit(r);


	n = r->result;
	r->ar = 0;
	while (n) {
		row = nvp_search_name(r->table->rows,n->name);
		if (!row) {
			if (ign) {
				n = n->next;
				continue;
			}
			printf("internal error in delete for table %s\r\n",r->table->name->value);
			goto end_delete;
		}
		r->ar++;
		if (row->prev) {
			row->prev->next = row->next;
		}else{
			r->table->rows = row->next;
		}
		if (row->next)
			row->next->prev = row->prev;
		nvp_free(row);
		n = n->next;
	}
end_delete:
	nvp_free_all(r->result);
	r->result = NULL;
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
	r->keywords = sql_split(q,0);
	r->table = NULL;
	r->cols = NULL;
	r->where = NULL;
	r->group = NULL;
	r->order = NULL;
	r->limit = NULL;
	r->count = NULL;
	r->result = NULL;

	if (!r->keywords) {
		printf("invalid query: \"%s\"\r\n",r->q);
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
	}else{
		printf("unknown keyword '%s'\r\n",r->keywords->value);
	}

	r->time = (ticks()-r->time);

	return r;
}
