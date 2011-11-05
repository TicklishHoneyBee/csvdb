#ifndef _CSVDB_H
#define _CSVDB_H

#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <stdarg.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/time.h>
#include <errno.h>
#include <unistd.h>

typedef struct nvp_s {
	char* name;
	char* value;
	struct nvp_s *child;
	struct nvp_s *prev;
	struct nvp_s *next;
} nvp_t;

typedef struct table_s {
	int key;
	nvp_t *name;
	nvp_t *columns;
	nvp_t *rows;
	struct table_s *prev;
	struct table_s *next;
} table_t;

typedef struct result_s {
	int ar;
	char* q;
	table_t *table;
	nvp_t *cols;
	nvp_t *keywords;
	nvp_t *where;
	nvp_t *group;
	nvp_t *order;
	nvp_t *limit;
	nvp_t *count;
	nvp_t *result;
	unsigned int time;
} result_t;

/* defined in table.c */
extern table_t *tables;
table_t *table_add(void);
table_t *table_load_csv(char* file, nvp_t *cols);
table_t *table_load_apache(char* file);
int table_next_key(table_t *t);
table_t *table_find(char* name);
void table_free(char* name);
int table_write(table_t *t, char* of);

/* defined in nvp.c */
nvp_t *nvp_create(char* name, char* value);
void nvp_free(nvp_t *n);
void nvp_free_all(nvp_t *n);
nvp_t *nvp_add(nvp_t **stack, char* name, char* value);
int nvp_count(nvp_t *stack);
nvp_t *nvp_search(nvp_t *stack, char* value);
nvp_t *nvp_search_name(nvp_t *stack, char* value);
int nvp_searchi(nvp_t *stack, char* value);
nvp_t *nvp_grabi(nvp_t *stack, int in);
nvp_t *nvp_last(nvp_t *stack);
int nvp_set(nvp_t *n, char* name, char* value);
nvp_t *nvp_insert_all(nvp_t *prev, nvp_t *ins);
nvp_t *nvp_search_lower_int(nvp_t *stack, int i, char* value);
nvp_t *nvp_search_higher_int(nvp_t *stack, int i, char* value);
nvp_t *nvp_search_lower_string(nvp_t *stack, int i, char* value);
nvp_t *nvp_search_higher_string(nvp_t *stack, int i, char* value);

/* defined in sql.c */
result_t *csvdb_query(char* q);

/* defined in lib.c */
int is_keyword(char* w);
int is_numeric(char* w);
int remove_wildcard(char* buff, char* str);
int get_column_id(char* buff, result_t *r, char* str);
int csvdb_print_result(result_t *res);

/* defined in result.c */
void result_where(result_t *r);
void result_distinct(result_t *r);
void result_group(result_t *r);
void result_order(result_t *r);
void result_cols(result_t *r);
void result_count(result_t *r);
void result_limit(result_t *r);
void result_free(result_t *r);
table_t *result_to_table(result_t *r, char* name);

#endif
