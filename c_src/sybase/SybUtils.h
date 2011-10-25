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
 *
 *  @file SybUtils.h
 *  @brief Sybase utility functions, variables, defines.
 *  @author wang.meigong <wang.meigong@rytong.com>
 *  @version 1.0.0
 *  @date Wen May 4 15:06:54 CST 2011
 */

#ifndef _SYBUTILS_H
#define _SYBUTILS_H

#include "SysLogger.h"

#include <ctpublic.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include <ei.h>
#include <erl_interface.h>

namespace rytong {

/** Print ei_x_buff to stdio for debug. */
void ei_print(ei_x_buff* x);

/** The callback, use syslog to record client messages. */
CS_RETCODE clientmsg_cb(CS_CONTEXT *context, CS_CONNECTION *connection, CS_CLIENTMSG *errmsg);

/** The callback, use syslog to record server messages. */
CS_RETCODE servermsg_cb(CS_CONTEXT *context, CS_CONNECTION *connection, CS_SERVERMSG *srvmsg);

/** Use syslog to record error messages */
CS_VOID ex_error(const char *format, ...);

/** */
bool is_leap_year(int year);

/** Computes the total number of days starting from year 0*/
int date_to_days(int year, int month, int day);
}/* end of namespace rytong */
#endif  /* _SYBUTILS_H */

