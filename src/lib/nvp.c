/************************************************************************
* nvp.c
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

nvp_t *nvp_create(char* name, char* value)
{
	nvp_t *n = malloc(sizeof(nvp_t));
	n->name = NULL;
	n->value = NULL;
	n->prev = NULL;
	n->next = NULL;
	n->child = NULL;

	if (name)
		n->name = strdup(name);

	if (value)
		n->value = strdup(value);

	return n;
}

void nvp_free(nvp_t *n)
{
	nvp_t *v;

	if (!n)
		return;

	if (n->name)
		free(n->name);

	if (n->value)
		free(n->value);

	while ((v = n->child)) {
		n->child = n->child->next;
		nvp_free(v);
	}

	free(n);
}

void nvp_free_all(nvp_t *n)
{
	nvp_t *c;
	while (n) {
		c = n->next;
		nvp_free(n);
		n = c;
	}
}

nvp_t *nvp_add(nvp_t **stack, char* name, char* value)
{
	nvp_t *n = nvp_create(name,value);
	if (*stack) {
		nvp_t *s = *stack;
		while (s->next) {
			s = s->next;
		}
		s->next = n;
		n->prev = s;
	}else{
		*stack = n;
	}

	return n;
}

nvp_t *nvp_push(nvp_t **stack, nvp_t *n)
{
	if (!n)
		return NULL;
	if (*stack) {
		nvp_t *s = *stack;
		while (s->next) {
			s = s->next;
		}
		s->next = n;
		n->prev = s;
	}else{
		*stack = n;
	}

	return n;
}

int nvp_count(nvp_t *stack)
{
	int r = 0;
	while (stack) {
		r++;
		stack = stack->next;
	}

	return r;
}

nvp_t *nvp_search(nvp_t *stack, char* value)
{
	while (stack) {
		if (!strcasecmp(stack->value,value))
			return stack;
		stack = stack->next;
	}

	return NULL;
}

nvp_t *nvp_search_name(nvp_t *stack, char* value)
{
	while (stack) {
		if (!strcasecmp(stack->name,value))
			return stack;
		stack = stack->next;
	}

	return NULL;
}

int nvp_searchi(nvp_t *stack, char* value)
{
	int i = 0;
	while (stack) {
		if (!strcasecmp(stack->value,value))
			return i;
		stack = stack->next;
		i++;
	}

	return -1;
}

nvp_t *nvp_grabi(nvp_t *stack, int in)
{
	int i = 0;
	while (stack) {
		if (i == in)
			return stack;
		stack = stack->next;
		i++;
	}

	return NULL;
}

nvp_t *nvp_last(nvp_t *stack)
{
	if (!stack)
		return NULL;

	while (stack->next) {
		stack = stack->next;
	}
	return stack;
}

int nvp_set(nvp_t *n, char* name, char* value)
{
	if (!n)
		return 1;

	if (n->name)
		free(n->name);
	n->name = 0;
	if (name)
		n->name = strdup(name);

	if (n->value)
		free(n->value);
	n->value = 0;
	if (value)
		n->value = strdup(value);

	return 0;
}

nvp_t *nvp_insert_all(nvp_t *prev, nvp_t *ins)
{
	nvp_t *i = ins->child;
	nvp_t *n = nvp_create(ins->name,ins->value);

	while (i) {
		nvp_add(&n->child,i->name,i->value);
		i = i->next;
	}

	if (prev) {
		n->next = prev->next;
		prev->next = n;
		if (n->next) {
			n->next->prev = n;
		}
		n->prev = prev;
	}

	return n;
}

nvp_t *nvp_search_lower_int(nvp_t *stack, int i, char* value)
{
	int v = atoi(value);
	int cv;
	nvp_t *t;
	nvp_t *s = stack;

	while (s) {
		if (i) {
			t = nvp_grabi(s->child,i-1);
		}else{
			t = s;
		}
		if (!t)
			return nvp_last(stack);
		cv = atoi(t->value);
		if (cv < v)
			return s;
		s = s->next;
	}

	return NULL;
}

nvp_t *nvp_search_higher_int(nvp_t *stack, int i, char* value)
{
	int v = atoi(value);
	int cv;
	nvp_t *t;
	nvp_t *s = stack;

	while (s) {
		if (i) {
			t = nvp_grabi(s->child,i-1);
		}else{
			t = s;
		}
		if (!t) {
			return nvp_last(stack);
		}
		cv = atoi(t->value);
		if (cv > v)
			return s;
		s = s->next;
	}

	return NULL;
}

nvp_t *nvp_search_lower_string(nvp_t *stack, int i, char* value)
{
	nvp_t *t;
	nvp_t *s = stack;

	while (s) {
		if (i) {
			t = nvp_grabi(s->child,i-1);
		}else{
			t = s;
		}
		if (!t)
			return nvp_last(s);
		if (strcmp(t->value,value) < 0)
			return s;
		s = s->next;
	}

	return NULL;
}

nvp_t *nvp_search_higher_string(nvp_t *stack, int i, char* value)
{
	nvp_t *t;
	nvp_t *s = stack;

	while (s) {
		if (i) {
			t = nvp_grabi(s->child,i-1);
		}else{
			t = s;
		}
		if (!t)
			return nvp_last(s);
		if (strcmp(t->value,value) > 0)
			return s;
		s = s->next;
	}

	return NULL;
}
