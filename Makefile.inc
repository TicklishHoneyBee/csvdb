CC ?= gcc
CD=cd

PWD ?= $(shell pwd)

TARGET=csvdb

CFLAGS ?= -Wall -g -Iinc/ -I. -ansi -pedantic
CLIBS ?= -g -L$(PWD) -l$(TARGET)

LCFLAGS ?= -Wall -g -Iinc/ -I. -ansi -pedantic -fPIC
LCLIBS ?=-shared -Wl,-soname,lib$(TARGET).so -g

PREFIX ?= /usr
BIN_DIR ?= $(PREFIX)/bin
LIB_DIR ?= $(PREFIX)/lib
MAN_DIR ?= $(PREFIX)/share/man/man1

SRC_DIR=src
BUILD_DIR=build

CLIENT_OBJS=main.o history.o tty.o
LIB_OBJS=lib.o nvp.o result.o sql.o table.o
