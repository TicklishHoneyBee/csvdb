#ifndef _CSVDB_H
#define _CSVDB_H

#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <stdarg.h>
#include <stdlib.h>
#include <ctype.h>
#include <time.h>
#include <sys/time.h>
#include <errno.h>
#include <unistd.h>

typedef struct nvp_s {
	char* name;
	char* value;
	int num;
	struct nvp_s *child;
	struct nvp_s *prev;
	struct nvp_s *next;
} nvp_t;

typedef struct row_s {
	int key;
	nvp_t *data;
	struct row_s *t_data;
	struct row_s *prev;
	struct row_s *next;
} row_t;

typedef struct table_s {
	int key;
	int rc;
	nvp_t *name;
	nvp_t *columns;
	row_t *rows;
	struct table_s *prev;
	struct table_s *next;
} table_t;

typedef struct table_ref_s {
	char* alias;
	table_t *t;
	struct table_ref_s *next;
} table_ref_t;

typedef struct column_ref_s {
	char* name;
	char* alias;
	nvp_t *keywords;
	int num;
	/* index of the table in the results table references */
	int t_index;
	/* index of the column in a table row */
	int r_index;
	table_t *table;
	struct column_ref_s *next;
} column_ref_t;

typedef struct result_s {
	int ar;
	char* q;
	table_ref_t *table;
	column_ref_t *cols;
	nvp_t *keywords;
	nvp_t *where;
	nvp_t *group;
	nvp_t *having;
	nvp_t *order;
	nvp_t *limit;
	nvp_t *count;
	row_t *result;
	nvp_t *error;
	row_t *join;
	struct result_s *sub;
	struct result_s *next;
	unsigned int time;
} result_t;

/* used by WHERE */
#define CMP_EQUALS	0
#define CMP_LIKE	1
#define CMP_LESS	2
#define CMP_LESSOREQ	3
#define CMP_GREATER	4
#define CMP_GREATEROREQ	5
#define CMP_IN		6
#define CMP_NOTEQUALS	7
#define CMP_NOTLIKE	8
#define CMP_NOTIN	9

#define CMP_STRING	256

#define CSVDB_ERROR_NONE	0
#define CSVDB_ERROR_INTERNAL	1
#define CSVDB_ERROR_SYNTAX	2
#define CSVDB_ERROR_TABLEREF	3
#define CSVDB_ERROR_COLUMNREF	4
#define CSVDB_ERROR_FILEREF	5
#define CSVDB_ERROR_FILEEXISTS	6
#define CSVDB_ERROR_UNSUPPORTED	7
#define CSVDB_ERROR_OUTOFRANGE	8
#define CSVDB_ERROR_BADRESULT	9
#define CSVDB_ERROR_SUBQUERY	10

#define CSVDB_SET_VOID		0
#define CSVDB_SET_DEBUG		1
#define CSVDB_SET_PERMANENT	2
extern unsigned int csvdb_settings;

/* defined in table.c */
extern table_t *tables;
table_t *table_add(void);
table_t *table_load_csv(char* file, nvp_t *cols, nvp_t *opts);
table_t *table_load_apache(char* file);
int table_next_key(table_t *t);
table_t *table_find(char* name);
void table_free(char* name);
int table_write(table_t *t, char* of, nvp_t *opts);
table_ref_t *table_resolve(char* str, result_t *r);
void table_free_refs(table_ref_t *t);

/* defined in nvp.c */
nvp_t *nvp_create(char* name, char* value);
void nvp_free(nvp_t *n);
void nvp_free_all(nvp_t *n);
nvp_t *nvp_add(nvp_t **stack, char* name, char* value);
nvp_t *nvp_push(nvp_t **stack, nvp_t *n);
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
int csvdb_init(void);
int is_keyword(char* w);
int is_numeric(char* w);
int remove_wildcard(char* buff, char* str);
int csvdb_print_result(result_t *res);
void error(result_t *r, int err, char* ref);

/* defined in result.c */
void result_where(result_t *r);
void result_having(result_t *r);
void result_distinct(result_t *r);
void result_group(result_t *r);
void result_order(result_t *r);
void result_cols(result_t *r);
void result_count(result_t *r);
void result_limit(result_t *r);
void csvdb_free_result(result_t *r);
void result_free(result_t *r) __attribute__ ((deprecated ("use csvdb_free_result")));
table_t *result_to_table(result_t *r, char* name);

/* defined in row.c */
row_t *row_create(int key);
row_t *row_add(row_t **stack, int key);
void row_free(row_t *r);
void row_free_keys(row_t *r);
void row_free_all(row_t *r);
row_t *row_search(row_t *r, int key);
row_t *row_search_lower_int(row_t *stack, int i, char* value);
row_t *row_search_higher_int(row_t *stack, int i, char* value);
row_t *row_search_lower_string(row_t *stack, int i, char* value);
row_t *row_search_higher_string(row_t *stack, int i, char* value);
row_t *row_search_lower_int_col(row_t *stack, column_ref_t *col, char* value);
row_t *row_search_higher_int_col(row_t *stack, column_ref_t *col, char* value);
row_t *row_search_lower_string_col(row_t *stack, column_ref_t *col, char* value);
row_t *row_search_higher_string_col(row_t *stack, column_ref_t *col, char* value);
int row_count(row_t *stack);

/* defined in column.c */
column_ref_t *column_create(char* name, char* alias, table_t *table);
column_ref_t *column_add(column_ref_t **stack, char* name, char* alias, table_t *table);
column_ref_t *column_push(column_ref_t **stack, column_ref_t *col);
void column_free(column_ref_t *c);
void column_free_all(column_ref_t *c);
column_ref_t *column_resolve(char* str, result_t *r);
column_ref_t *column_find(char* str, result_t *r);
nvp_t *column_fetch_data(row_t *row, column_ref_t *col);

#endif
