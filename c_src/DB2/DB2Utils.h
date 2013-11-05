/**
 * Copyright (c) 2009-2010 Beijing RYTong Information Technologies, Ltd.
 * All rights reserved.
 *
 * The contents of this file are subject to the Erlang Database Driver
 * Public License Version 1.0, (the "License"); you may not use this
 * file except in compliance with the License. You should have received
 * a copy of the Erlang Database Driver Public License along with this
 * software. If not, it can be retrieved via the world wide web at
 * http://www.rytong.com/.
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
 * the License for the specific language governing rights and limitations
 * under the License.
 */

#ifndef _DB2UTILS_H
#define _DB2UTILS_H

#include "../util/SysLogger.h"
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <sqlcli1.h>
#include <ei.h>
#include <erl_interface.h>

namespace rytong {

#define LOG_ERROR(message, format, args...) \
snprintf(message, sizeof(message), format, ## args); \
SysLogger::error("\n\rline  = %d\n\rfile  = %s\n\r%s\n\r", __LINE__, __FILE__, message);

#define SQL_ENV_SUCCESS(henv, cliRC) \
handleInfo(NULL, 0, SQL_HANDLE_ENV, henv, cliRC, __LINE__, __FILE__)

#define SQL_ENV_SUCCESS_WITH_RETURN(message, henv, cliRC) \
handleInfo(message, sizeof(message), SQL_HANDLE_ENV, henv, cliRC, __LINE__, __FILE__)

#define SQL_DBC_SUCCESS(hdbc, cliRC) \
handleInfo(NULL, 0, SQL_HANDLE_DBC, hdbc, cliRC, __LINE__, __FILE__)

#define SQL_DBC_SUCCESS_WITH_RETURN(message, hdbc, cliRC) \
handleInfo(message, sizeof(message), SQL_HANDLE_DBC, hdbc, cliRC, __LINE__, __FILE__)

#define SQL_STMT_SUCCESS(hstmt, cliRC) \
handleInfo(NULL, 0, SQL_HANDLE_STMT, hstmt, cliRC, __LINE__, __FILE__)

#define SQL_STMT_SUCCESS_WITH_RETURN(message, hstmt, cliRC) \
handleInfo(message, sizeof(message), SQL_HANDLE_STMT, hstmt, cliRC, __LINE__, __FILE__)

bool handleInfo(char*, int, SQLSMALLINT, SQLHANDLE, SQLRETURN, int, char*);

int calcSGByteLength(char*, int*);

int calcDBByteLength(char*, int*);

/* Decode data from erlang input buffer */
bool decodeBinary(char*, int*, char*, int, long*);
bool decodeString(char*, int*, char*, int, long*);
bool decodeInteger(char*, int*, long long*);
bool decodeDouble(char*, int*, double*);
bool decodeDBString (char*, int*, unsigned char*, int, long*);

}/* end of namespace rytong */
#endif  /* _DB2UTILS_H */

