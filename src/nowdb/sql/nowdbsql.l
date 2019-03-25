/* ========================================================================
 * (c) Tobias Schoofs, 2018
 * ========================================================================
 * SQL Lexer
 * ========================================================================
 * This parser definition file is intended for use with
 * the flex lexer (and similar)
 *
 * urgent TODO:
 * ------------
 * - multi-line comments...
 *   with the lexer alone, this appears to be complex.
 *   here's how to do it:
 *   http://westes.github.io/flex/manual/
 *   How-can-I-match-C_002dstyle-comments_003f.html
 * ========================================================================
 */

%{
#include <nowdb/sql/nowdbsql.h>
#include <nowdb/sql/state.h>
%}

%option reentrant

%%

"."			return NOWDB_SQL_DOT;
";"			return NOWDB_SQL_SEMICOLON;
"="			return NOWDB_SQL_EQ;
","			return NOWDB_SQL_COMMA;
"*"			return NOWDB_SQL_STAR;
"("			return NOWDB_SQL_LPAR;
")"			return NOWDB_SQL_RPAR;
"<"			return NOWDB_SQL_LT;
">"			return NOWDB_SQL_GT;

"<="			return NOWDB_SQL_LE;
">="			return NOWDB_SQL_GE;
"!="			return NOWDB_SQL_NE;
"<>"			return NOWDB_SQL_NE;

"+"			return NOWDB_SQL_PLUS;
"-"			return NOWDB_SQL_MINUS;
"/"			return NOWDB_SQL_DIV;
"%"			return NOWDB_SQL_PER;
"^"			return NOWDB_SQL_POW;

(?i:IS)			return NOWDB_SQL_IS;

(?i:CASE)		return NOWDB_SQL_CASE;
(?i:END)		return NOWDB_SQL_END;
(?i:WHEN)		return NOWDB_SQL_WHEN;
(?i:THEN)		return NOWDB_SQL_THEN;
(?i:ELSE)		return NOWDB_SQL_ELSE;

(?i:CREATE)		return NOWDB_SQL_CREATE;
(?i:DROP)		return NOWDB_SQL_DROP;
	/*
(?i:ALTER)		return NOWDB_SQL_ALTER;
	*/

(?i:SHOW)		return NOWDB_SQL_SHOW;
(?i:DESCRIBE)		return NOWDB_SQL_DESC;

(?i:LOAD)		return NOWDB_SQL_LOAD;
(?i:USE)		return NOWDB_SQL_USE;
(?i:FETCH)		return NOWDB_SQL_FETCH;
(?i:CLOSE)		return NOWDB_SQL_CLOSE;

(?i:SCOPE)		return NOWDB_SQL_SCOPE;
(?i:SCHEMA)		return NOWDB_SQL_SCOPE;
(?i:DATABASE)		return NOWDB_SQL_SCOPE;
(?i:STORAGE)		return NOWDB_SQL_STORAGE;
(?i:INDEX)		return NOWDB_SQL_INDEX;
(?i:VERTEX)		return NOWDB_SQL_VERTEX;
(?i:PROCEDURE)		return NOWDB_SQL_PROCEDURE;

(?i:EDGE)		return NOWDB_SQL_EDGE;
(?i:TYPE)		return NOWDB_SQL_TYPE;
(?i:TEXT)		return NOWDB_SQL_TEXT;
(?i:DATE)		return NOWDB_SQL_DATE;
(?i:TIME)		return NOWDB_SQL_TIME;
(?i:FLOAT)		return NOWDB_SQL_FLOAT;
(?i:INT)		return NOWDB_SQL_INT;
(?i:INTEGER)		return NOWDB_SQL_INT;
(?i:UINT)		return NOWDB_SQL_UINT;
(?i:UINTEGER)		return NOWDB_SQL_UINT;
(?i:BOOL)		return NOWDB_SQL_BOOL;
(?i:BOOLEAN)		return NOWDB_SQL_BOOL;
(?i:PRIMARY\ KEY)	return NOWDB_SQL_PK;
(?i:PK)			return NOWDB_SQL_PK;

(?i:ORIGIN)		return NOWDB_SQL_ORIGIN;
(?i:DESTINATION)	return NOWDB_SQL_DESTINATION;
(?i:DESTIN)		return NOWDB_SQL_DESTINATION;
(?i:DEST)		return NOWDB_SQL_DESTINATION;
(?i:TIMESTAMPED)	return NOWDB_SQL_STAMPED;
(?i:STAMPED)		return NOWDB_SQL_STAMPED;
(?i:TIMESTAMP)		return NOWDB_SQL_TIMESTAMP;
(?i:STAMP)		return NOWDB_SQL_TIMESTAMP;
(?i:TRUE)		return NOWDB_SQL_TRUE;
(?i:FALSE)		return NOWDB_SQL_FALSE;
(?i:NULL)		return NOWDB_SQL_NULL;

(?i:SELECT)		return NOWDB_SQL_SELECT;
(?i:INSERT)		return NOWDB_SQL_INSERT;
(?i:FROM)		return NOWDB_SQL_FROM;
(?i:WHERE)		return NOWDB_SQL_WHERE;
(?i:GROUP)		return NOWDB_SQL_GROUP;
(?i:ORDER)		return NOWDB_SQL_ORDER;
(?i:BY)			return NOWDB_SQL_BY;
(?i:AND)		return NOWDB_SQL_AND;
(?i:OR)			return NOWDB_SQL_OR;
(?i:NOT)		return NOWDB_SQL_NOT;
(?i:EXECUTE)		return NOWDB_SQL_EXECUTE;
(?i:EXEC)		return NOWDB_SQL_EXECUTE;
(?i:LANGUAGE)		return NOWDB_SQL_LANGUAGE;

(?i:INTO) 		return NOWDB_SQL_INTO;
(?i:SET)		return NOWDB_SQL_SET;
(?i:ON)			return NOWDB_SQL_ON;
(?i:IN)			return NOWDB_SQL_IN;
(?i:VALUES)		return NOWDB_SQL_VALUES;

(?i:IF)			return NOWDB_SQL_IF;
(?i:EXISTS)		return NOWDB_SQL_EXISTS;

(?i:AS)			return NOWDB_SQL_AS;

(?i:DISK)		return NOWDB_SQL_DISK;
(?i:HDD)		return NOWDB_SQL_HDD;
(?i:SSD)		return NOWDB_SQL_SSD;
(?i:RAID)		return NOWDB_SQL_RAID;

(?i:TINY)		return NOWDB_SQL_TINY;
(?i:SMALL)		return NOWDB_SQL_SMALL;
(?i:MEDIUM)		return NOWDB_SQL_MEDIUM;
(?i:BIG)		return NOWDB_SQL_BIG;
(?i:LARGE)		return NOWDB_SQL_LARGE;
(?i:HUGE)		return NOWDB_SQL_HUGE;

(?i:STRESS)		return NOWDB_SQL_STRESS;
(?i:MODERATE)		return NOWDB_SQL_MODERATE;
(?i:CONSTANT)		return NOWDB_SQL_CONSTANT;
(?i:INSANE)		return NOWDB_SQL_INSANE;

(?i:ALLOCSIZE)		return NOWDB_SQL_ALLOCSIZE;
(?i:LARGESIZE)		return NOWDB_SQL_LARGESIZE;
(?i:SIZING)		return NOWDB_SQL_SIZE;
(?i:SORTERS)		return NOWDB_SQL_SORTERS;
(?i:COMPRESSION)	return NOWDB_SQL_COMPRESSION;
(?i:ENCRYPTION)		return NOWDB_SQL_ENCRYPTION;

(?i:ERRORS)		return NOWDB_SQL_ERRORS;

(?i:IGNORE)		return NOWDB_SQL_IGNORE;
(?i:HEADER)		return NOWDB_SQL_HEADER;

[a-zA-Z][a-zA-Z0-9_]*  			return NOWDB_SQL_IDENTIFIER;
'[^']*'					return NOWDB_SQL_STRING;
(\+|\-)?((0|([1-9][0-9]*))\.[0-9]*)	return NOWDB_SQL_FLOAT;
(\+|\-)(0|([1-9][0-9]*))		return NOWDB_SQL_INTEGER;
0|([1-9][0-9]*)				return NOWDB_SQL_UINTEGER;
"--".*[\r\n]				return NOWDB_SQL_COMMENT;
[ \t\r\n]				;
.					return NOWDB_SQL_UNEXPECTED;

%%

int yywrap(void *yyscanner) {
	return 1;
}
