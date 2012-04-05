/************************************************************************
* row.c
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

row_t *row_create(int key)
{
	row_t *r = malloc(sizeof(row_t));
	r->key = key;
	r->data = NULL;
	r->t_data = NULL;
	r->prev = NULL;
	r->next = NULL;

	return r;
}

row_t *row_add(row_t **stack, int key)
{
	row_t *r = row_create(key);
	if (!r)
		return NULL;

	if (*stack) {
		row_t *s = *stack;
		while (s->next) {
			s = s->next;
		}
		s->next = r;
		r->prev = s;
	}else{
		*stack = r;
	}

	return r;
}

void row_free(row_t *r)
{
	free(r);
}

void row_free_keys(row_t *r)
{
	row_t *n = r;
	while (n) {
		r = n;
		if (r->t_data)
			row_free_keys(r->t_data);
		n = n->next;
		free(r);
	}
}

void row_free_all(row_t *r)
{
	row_t *n = r;
	while (n) {
		r = n;
		n = n->next;
		if (r->t_data)
			row_free_keys(r->t_data);
		nvp_free_all(r->data);
		free(r);
	}
}

row_t *row_search(row_t *r, int key)
{
	while (r) {
		if (r->key == key)
			return r;
		r = r->next;
	}

	return NULL;
}

row_t *row_search_lower_int(row_t *stack, int i, char* value)
{
	int v = atoi(value);
	int cv;
	nvp_t *t;
	row_t *s = stack;

	while (s) {
		t = nvp_grabi(s->data,i);
		if (!t)
			return s;
		cv = atoi(t->value);
		if (cv < v)
			return s;
		s = s->next;
	}

	return NULL;
}

row_t *row_search_higher_int(row_t *stack, int i, char* value)
{
	int v = atoi(value);
	int cv;
	nvp_t *t;
	row_t *s = stack;

	while (s) {
		t = nvp_grabi(s->data,i);
		if (!t) {
			return s;
		}
		cv = atoi(t->value);
		if (cv > v)
			return s;
		s = s->next;
	}

	return NULL;
}

row_t *row_search_lower_string(row_t *stack, int i, char* value)
{
	nvp_t *t;
	row_t *s = stack;

	while (s) {
		t = nvp_grabi(s->data,i);
		if (!t)
			return s;
		if (strcmp(t->value,value) < 0)
			return s;
		s = s->next;
	}

	return NULL;
}

row_t *row_search_higher_string(row_t *stack, int i, char* value)
{
	nvp_t *t;
	row_t *s = stack;

	while (s) {
		t = nvp_grabi(s->data,i);
		if (!t)
			return s;
		if (strcmp(t->value,value) > 0)
			return s;
		s = s->next;
	}

	return NULL;
}

row_t *row_search_lower_int_col(row_t *stack, column_ref_t *col, char* value)
{
	int v = atoi(value);
	int cv;
	nvp_t *t;
	row_t *s = stack;

	while (s) {
		t = column_fetch_data(s,col);
		if (!t)
			return s;
		cv = atoi(t->value);
		if (cv < v)
			return s;
		s = s->next;
	}

	return NULL;
}

row_t *row_search_higher_int_col(row_t *stack, column_ref_t *col, char* value)
{
	int v = atoi(value);
	int cv;
	nvp_t *t;
	row_t *s = stack;

	while (s) {
		t = column_fetch_data(s,col);
		if (!t) {
			return s;
		}
		cv = atoi(t->value);
		if (cv > v)
			return s;
		s = s->next;
	}

	return NULL;
}

row_t *row_search_lower_string_col(row_t *stack, column_ref_t *col, char* value)
{
	nvp_t *t;
	row_t *s = stack;

	while (s) {
		t = column_fetch_data(s,col);
		if (!t)
			return s;
		if (strcmp(t->value,value) < 0)
			return s;
		s = s->next;
	}

	return NULL;
}

row_t *row_search_higher_string_col(row_t *stack, column_ref_t *col, char* value)
{
	nvp_t *t;
	row_t *s = stack;

	while (s) {
		t = column_fetch_data(s,col);
		if (!t)
			return s;
		if (strcmp(t->value,value) > 0)
			return s;
		s = s->next;
	}

	return NULL;
}

int row_count(row_t *stack)
{
	int r = 0;
	while (stack) {
		r++;
		stack = stack->next;
	}

	return r;
}
