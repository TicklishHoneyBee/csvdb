/************************************************************************
* history.c
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

#include "csvdb-client.h"

static nvp_t *history = NULL;
static nvp_t *position = NULL;

void history_load()
{
	char buff[2048];
	FILE *f;
	char* h;
	char c;
	int r = 1;
	int o = 0;
	int is = 0;

	h = getenv("HOME");
	sprintf(buff,"%s/.csvdb_history",h);
	f = fopen(buff,"r");
	if (!f)
		return;

	while (r == 1) {
		r = fread(&c,1,1,f);
		if (r != 1)
			goto line_end;
		if (!isprint((int)c) && c != '\n')
			continue;
		if (c == '\'' && (!o || buff[o-1] != '\\'))
			is = !is;

		if (!is && c == '\n') {
line_end:
			buff[o] = 0;
			history_add(buff);
			o = 0;
			continue;
		}

		if (o < 2047)
			buff[o++] = c;
	}

	fclose(f);
}

void history_save()
{
	char buff[1024];
	FILE *f;
	char* h;
	nvp_t *n;

	h = getenv("HOME");
	sprintf(buff,"%s/.csvdb_history",h);
	f = fopen(buff,"w");
	if (!f)
		return;

	n = history;
	while (n) {
		fprintf(f,"%s\n",n->value);
		n = n->next;
	}
	fclose(f);
}

void history_add(char* str)
{
	if (!str || strlen(str) < 4)
		return;
	nvp_add(&history,NULL,str);
	position = nvp_last(history);
}

char* history_prev()
{
	nvp_t *p = position;
	if (p) {
		position = p->prev;
		return p->value;
	}
	return "";
}

char* history_next()
{
	if (!position || !position->next)
		return "";
	position = position->next;
	if (!position || !position->next)
		return "";
	position = position->next;
	return position->value;
}
