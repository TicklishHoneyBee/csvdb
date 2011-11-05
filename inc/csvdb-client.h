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

#endif
