/************************************************************************
* main.c
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

#if defined HAVE_CONFIG_H
# include "config.h"
#endif	/* HAVE_CONFIG_H */
/* our stuff */
#include "csvdb-client.h"

#define countof(x)	(sizeof(x) / sizeof(*x))
#define charcnt(x)	(countof(x) - 1)

/* interactive handlers */
static const char banner[] = "\
csvDB shell\r\n\
Copyright (C) Lisa Milne 2011 <lisa@ltmnet.com>\r\n\
This program is free software: you can redistribute it and/or modify it\r\n\
under the terms of the GNU General Public License as published by the\r\n\
Free Software Foundation, either version 3 of the License, or\r\n\
(at your option) any later version.\r\n\
\r\n";
static const char discl[] = "\
This program is distributed in the hope that it will be useful, but\r\n\
WITHOUT ANY WARRANTY; without even the implied warranty of\r\n\
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.\r\n\
See the GNU General Public License for more details.\r\n";

#if !defined USE_READLINE
static void interact_orig(char *buff)
{
	result_t *r;
	char c;
	char d = ';';
	int o = 0;
	int is = 0;
	int eq = 0;
	char* b;
	char sbuff[64];
	int i;
	size_t ign;

	history_load();
	set_tty_raw();
	ign = write(STDOUT_FILENO,banner,sizeof(banner) - 1);
	ign = write(STDOUT_FILENO,discl,sizeof(discl) - 1);
	ign = write(STDOUT_FILENO,csvdb_prompt,strlen(csvdb_prompt));

	while ((c = getch()) != 0x04) {
		if (c == '\'' && (!o || buff[o-1] != '\\'))
			is = !is;
		if (c == 8) {
			sprintf(sbuff," %c",8);
			ign = write(STDOUT_FILENO,sbuff,2);
			if (o)
				o--;
			if (eq) {
				eq = 0;
			}else{
				if (buff[o] == '\'')
					is = !is;
			}
			continue;
		}else if (c == 0x03) {
			ign = write(STDOUT_FILENO,"\r\ncsvDB> ",9);
			eq = 0;
			is = 0;
			o = 0;
		}
		if (!is) {
			if (c > 64 && c < 69 && o > 1 && buff[o-2] == 27 && buff[o-1] == 91) {
				/* up */
				if (c == 65) {
					b = history_prev();
					/* undo the up arrow */
					ign = write(STDOUT_FILENO,"\r\n\r",3);
					o += 8;
					for (i=0; i<o; i++) {
						ign = write(STDOUT_FILENO," ",1);
					}
					ign = write(STDOUT_FILENO,"\rcsvDB> ",8);
					if (b[0]) {
						strcpy(buff,b);
						o = strlen(buff);
						ign = write(STDOUT_FILENO,buff,o);
						ign = write(STDOUT_FILENO,";",1);
						o++;
						eq = 1;
					}else{
						o = 0;
						eq = 0;
					}
					continue;
					/* down */
				}else if (c == 66) {
					b = history_next();
					/* undo the down arrow */
					sprintf(sbuff,"%c%c%c\r",27,91,65);
					ign = write(STDOUT_FILENO,sbuff,4);
					o += 8;
					for (i=0; i<o; i++) {
						ign = write(STDOUT_FILENO," ",1);
					}
					ign = write(STDOUT_FILENO,"\rcsvDB> ",8);
					if (b[0]) {
						strcpy(buff,b);
						o = strlen(buff);
						ign = write(STDOUT_FILENO,buff,o);
						ign = write(STDOUT_FILENO,";",1);
						o++;
						eq = 1;
					}else{
						o = 0;
						eq = 0;
					}
					continue;
					/* right */
				}else if (c == 67) {
					/* undo the right arrow */
					sprintf(sbuff,"%c%c%c",27,91,68);
					ign = write(STDOUT_FILENO,sbuff,3);
					/* left */
				}else if (c == 68) {
					/* undo the left arrow */
					sprintf(sbuff,"%c%c%c",27,91,67);
					ign = write(STDOUT_FILENO,sbuff,3);
				}
			}else if (c == d) {
				buff[o] = 0;
				eq = 1;
				continue;
			}else if (c == '\n' || c == '\r') {
				if (!o) {
					ign = write(STDOUT_FILENO,"\r\ncsvDB> ",9);
					continue;
				}
				if (eq) {
					if (!strcasecmp(buff,"QUIT"))
						break;
					ign = write(STDOUT_FILENO,"\r\n",2);
					if (o) {
						set_tty_cooked();
						r = csvdb_query(buff);
						csvdb_print_result(r);
						set_tty_raw();
						result_free(r);
						history_add(buff);
						o = 0;
					}
					eq = 0;
					ign = write(STDOUT_FILENO,"\r\ncsvDB> ",9);
					continue;
				}
				buff[o] = 0;
				if (!strcasecmp(buff,"QUIT"))
					break;
				if (!strncasecmp(buff,"DELIMITER",9)) {
					d = buff[10];
					ign = write(STDOUT_FILENO,"\r\ncsvDB> ",9);
					o = 0;
					continue;
				}
				c = ' ';
				ign = write(STDOUT_FILENO,"\n> ",3);
				if (!o)
					continue;
			}
		}
		if (o < 2047)
			buff[o++] = c;
	}
	set_tty_cooked();
	history_save();
	return;
}

#else  /* USE_READLINE */
static const char cmd_quit[] = "QUIT";
static const char cmd_delim[] = "DELIMITER";

static void interact_rdln(void)
{
	char d = ';';
	char *line;
	size_t i;

	csvdb_init_readline();

	/* print some banners, then go interactive */
	i = write(STDOUT_FILENO, banner, sizeof(banner) - 1);
	i = write(STDOUT_FILENO, discl, sizeof(discl) - 1);

	while ((line = csvdb_readline(d))) {
		/* check for special commands */
		if (!strncasecmp(line, cmd_quit, charcnt(cmd_quit))) {
			free(line);
			break;
		} else if (!strncasecmp(line, cmd_delim, charcnt(cmd_delim))) {
			/* next non whitespace char will be the new delim */
			static const char ws[] = " \t";
			size_t off = strcspn(line + charcnt(cmd_delim), ws);
			d = line[charcnt(cmd_delim) + off];
		} else {
			result_t *r;

			r = csvdb_query(line);
			csvdb_print_result(r);
			result_free(r);
		}
		free(line);
	}
	csvdb_deinit_readline();
	return;
}
#endif	/* !USE_READLINE */


int main(int argc, char** argv)
{
	result_t *r;
	table_t *tbl;
	nvp_t *q = NULL;
	nvp_t *f = NULL;
	nvp_t *a = NULL;
	nvp_t *l;
	char buff[2048];
	int i;
	int eq = 0;

	for (i=1; i<argc; i++) {
		if (!strcmp(argv[i],"-f")) {
			i++;
			if (i >= argc) {
				fprintf(stderr,"error: no file given for -f argument\n");
				return 0;
			}
			nvp_add(&f,NULL,argv[i]);
			eq = 0;
		}else if (!strcmp(argv[i],"-a")) {
			i++;
			if (i >= argc) {
				fprintf(stderr,"error: no file given for -a argument\n");
				return 0;
			}
			nvp_add(&a,NULL,argv[i]);
			eq = 1;
		}else if (!strcmp(argv[i],"-e")) {
			i++;
			if (i >= argc) {
				fprintf(stderr,"error: no query given for -e argument\n");
				return 0;
			}
			nvp_add(&q,NULL,argv[i]);
		}else if (!strcmp(argv[i],"-n")) {
			i++;
			if (i >= argc) {
				fprintf(stderr,"error: no value given for -n argument\n");
				return 0;
			}
			if (eq) {
				l = nvp_last(a);
			}else{
				l = nvp_last(f);
			}
			if (!l) {
				fprintf(stderr,"error: no file for -n argument\n");
				return 0;
			}
			if (!strcasecmp(argv[i],"FILE")) {
				fprintf(stderr,"error: cannot use keyword for table alias in -n argument\n");
				return 0;
			}
			strcpy(buff,l->value);
			nvp_set(l,argv[i],buff);
		}else if (!strcmp(argv[i],"-h")) {
			printf("yeah, I should probably write some help - try `man csvdb'\n");
			return 0;
		}
	}
	l = f;
	while (l) {
		tbl = table_load_csv(l->value,l->child);
		if (l->name) {
			nvp_add(&tbl->name,NULL,l->name);
		}
		l = l->next;
	}
	l = a;
	while (l) {
		tbl = table_load_apache(l->value);
		if (l->name) {
			nvp_add(&tbl->name,NULL,l->name);
		}
		l = l->next;
	}
	if (q) {
		l = q;
		while (l) {
			r = csvdb_query(l->value);
			csvdb_print_result(r);
			result_free(r);
			l =l->next;
		}
	}else {
#if !defined USE_READLINE
		interact_orig(buff);
#else  /* USE_READLINE */
		interact_rdln();
#endif	/* !USE_READLINE */
	}
	return 0;
}
