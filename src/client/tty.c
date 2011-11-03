/************************************************************************
* tty.c
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

static struct termios termattr;
static struct termios save_termattr_i;
static struct termios save_termattr_o;
static int ttysavefd = -1;
static enum {
	RESET,
	RAW
} ttystate = RESET;

int set_tty_raw(void)
{
	int i;

	i = tcgetattr(STDIN_FILENO, &termattr);
	if (i < 0) {
		return -1;
	}
	save_termattr_i = termattr;

	termattr.c_lflag &= ~(ECHO | ICANON | IEXTEN | ISIG);
	termattr.c_iflag &= ~(BRKINT | ICRNL | INPCK | ISTRIP | IXON);
	termattr.c_cflag &= ~(CSIZE | PARENB);
	termattr.c_cflag |= CS8;
	termattr.c_oflag &= ~(OPOST);

	termattr.c_cc[VMIN] = 1;
	termattr.c_cc[VTIME] = 0;

	i = tcsetattr(STDIN_FILENO, TCSANOW, &termattr);
	if (i < 0) {
		return -1;
	}

	i = tcgetattr(STDOUT_FILENO, &termattr);
	if (i < 0) {
		return -1;
	}
	save_termattr_o = termattr;

	termattr.c_lflag &= ~(ECHO | ICANON | IEXTEN | ISIG);
	termattr.c_iflag &= ~(BRKINT | ICRNL | INPCK | ISTRIP | IXON);
	termattr.c_cflag &= ~(CSIZE | PARENB);
	termattr.c_cflag |= CS8;
	termattr.c_oflag &= ~(OPOST);
	termattr.c_oflag |= ONLCR;

	termattr.c_cc[VMIN] = 1;
	termattr.c_cc[VTIME] = 0;
	i = tcsetattr(STDOUT_FILENO, TCSANOW, &termattr);
	if (i < 0) {
		return -1;
	}

	ttystate = RAW;
	ttysavefd = STDIN_FILENO;

	return 0;
}

int set_tty_cooked()
{
	int i;
	if (ttystate != RAW) {
		return 0;
	}
	i = tcsetattr(STDIN_FILENO, TCSAFLUSH, &save_termattr_i);
	if (i < 0) {
		return -1;
	}
	i = tcsetattr(STDOUT_FILENO, TCSAFLUSH, &save_termattr_i);
	if (i < 0) {
		return -1;
	}
	ttystate = RESET;
	return 0;
}

unsigned char getch(void)
{
	unsigned char ch = 0x03;
	size_t size;

	while (1) {
		usleep(20000);

		size = read(STDIN_FILENO, &ch, 1);
		if (size > 0) {
			printf("%c", ch);
			fflush(stdout);
			break;
		}
	}
	return ch;
}
