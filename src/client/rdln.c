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
#if defined USE_READLINE
/* gnu readline */
# include <stdlib.h>
# include <string.h>
# include <stdio.h>
# include <signal.h>
# include <readline/readline.h>
# include <readline/history.h>
#endif	/* USE_READLINE */
#include "csvdb-client.h"

#if !defined UNUSED
# define UNUSED(x)	__attribute__((unused)) x
#endif	/* !UNUSED */
#if !defined LIKELY
# define LIKELY(x)	(x)
#endif	/* !LIKELY */
#if !defined UNLIKELY
# define UNLIKELY(x)	(x)
#endif	/* !UNLIKELY */

char *csvdb_prompt = "csvDB> ";
char *csvdb_histfile = NULL;

/* this is pretty non-functional if USE_READLINE hasn't been set */
#if defined USE_READLINE
/* completers */
static char*
cmd_generator(const char *UNUSED(text), int UNUSED(state))
{
	/* if no names matched, then return NULL. */
	return NULL;
}

/**
 * Attempt to complete on the contents of TEXT.  START and END
 * bound the region of rl_line_buffer that contains the word to
 * complete.  TEXT is the word to complete.  We can use the entire
 * contents of rl_line_buffer in case we want to do some simple
 * parsing.  Return the array of matches, or NULL if there aren't any. */
static char**
csvdb_comp(const char *text, int start, int UNUSED(end))
{
	char **matches;

	matches = (char**)NULL;

	/* If this word is at the start of the line, then it is a command
	   to complete.  Otherwise it is the name of a file in the current
	   directory. */
	if (start == 0) {
		/* TODO */
		matches = rl_completion_matches(text, cmd_generator);
	}
	return matches;
}


static char*
get_histfile_name(const char *tmpl)
{
/* construct a canonical file name from template TMPL */
	char *h = getenv("HOME");
	size_t hlen = strlen(h);
	size_t tlen = strlen(tmpl);
	char *res;

	res = malloc(hlen + tlen + 2);
	memcpy(res, h, hlen);
	res[hlen] = '/';
	memcpy(res + hlen + 1, tmpl, tlen + 1);
	return res;
}

static void
__reset_rdln(int sig)
{
	rl_delete_text(0, rl_end);
	putc('\n', stdout);
	rl_on_new_line();
	rl_line_buffer[0] = '\0';
	rl_point = rl_end = 0;
	rl_done = 0;
	rl_forced_update_display();
	/* put ourselves on the signal stack again */
	signal(SIGINT, __reset_rdln);
	return;
}

void
csvdb_init_readline(void)
{
	static char wbc[] = "\t\n@$<>=;|&{( ";

	/* basic initialisation */
	rl_initialize();
	rl_readline_name = "csvdb";
	rl_attempted_completion_function = csvdb_comp;
	rl_basic_word_break_characters = wbc;

	/* signal handling */
	signal(SIGINT, __reset_rdln);
	rl_catch_signals = 0;

	/* load history */
	csvdb_histfile = get_histfile_name(CSVDB_HISTFILE);
	history_length = 0;
	using_history();
	if (read_history(csvdb_histfile) < 0) {
		return;
	}
	history_set_pos(history_length);
	return;
}

void
csvdb_deinit_readline(void)
{
	/* signal unhandling */
	signal(SIGINT, SIG_DFL);

	/* print newline */
	putc('\n', stdout);
	/* save the history file and free resources*/
	if (csvdb_histfile) {
		(void)write_history(csvdb_histfile);
		free(csvdb_histfile);
	}
	return;
}


/* line-reading beef */
char*
csvdb_readline(char delim)
{
	static char alt_prompt[] = " > ";
	const char *prompt = csvdb_prompt;
	char *stmt = NULL;
	size_t slen = 0UL;
	char *line;

	alt_prompt[0] = delim;
	while ((line = readline(prompt)) == NULL ||
	       strchr(line, delim) == NULL) {
		size_t llen = strlen(line);

		/* append to statement and prompt for next line */
		stmt = realloc(stmt, slen + llen + 1);
		memcpy(stmt + slen, line, llen + 1);
		slen += llen;
		prompt = alt_prompt;
	}

	if (stmt && !(stmt[0] == '\0' || stmt[0] == ' ')) {
		/* lest we lose our precious lines */
		add_history(stmt);
	}
	return stmt;
}
#endif	/* USE_READLINE */

/* rdln.c ends here */
