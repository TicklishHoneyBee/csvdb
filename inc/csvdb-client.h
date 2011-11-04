#ifndef _CSVDB_CLIENT_H
#define _CSVDB_CLIENT_H

#include "csvdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <termios.h>
#include <unistd.h>

#ifndef STDIN_FILENO
#define STDIN_FILENO 0
#endif
#ifndef STDOUT_FILENO
#define STDOUT_FILENO 1
#endif

/* defined in history.c */
void history_load(void);
void history_save(void);
void history_add(char* str);
char* history_prev(void);
char* history_next(void);

/* defined in tty.c */
int set_tty_raw(void);
int set_tty_cooked(void);
unsigned char getch(void);

/**
 * Global prompt string, cannot be customised at the moment.
 * defined in main.c */
extern char *csvdb_prompt;

/**
 * Global histfile string, cannot be customised at the moment.
 * defined in history.c */
extern char *csvdb_histfile;

/* default template */
#define CSVDB_HISTFILE	".csvdb_history"

/**
 * Initialise readline resources. */
extern void csvdb_init_readline(void);

/**
 * Deinitialise readline resources. */
extern void csvdb_deinit_readline(void);

/**
 * Read a line of input. */
extern char *csvdb_readline(void);

#endif
