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
 *  @file SybUtils.cpp
 *  @brief Sybase utility functions, variables, defines.
 *  @author wang.meigong <wang.meigong@rytong.com>
 *  @version 1.0.0
 *  @date Wen May 4 15:06:54 CST 2011
 */

#include "SybUtils.h"

namespace rytong {
inline int dy(int year);
inline int dm(int month, bool is_leap);

void ei_print(ei_x_buff* x)
{
    printf("<<");
    for (int i = 0; i < x->index; ++i) {
        printf("%d", (unsigned char)x->buff[i]);
        if (i < x->index - 1) {
            printf(",");
        }
    }
    printf(">>\n");
}

CS_RETCODE clientmsg_cb(CS_CONTEXT *context,
        CS_CONNECTION *connection, CS_CLIENTMSG *errmsg)
{
    SysLogger::error("Sybase Open Client Message:%s\n\r", errmsg->msgstring);

    return CS_SUCCEED;
}

CS_RETCODE servermsg_cb(CS_CONTEXT *context,
        CS_CONNECTION *connection, CS_SERVERMSG *srvmsg)
{
    SysLogger::error("Sybase Server Message:%s\n\r", srvmsg->text);
    
    return CS_SUCCEED;
}

CS_VOID ex_error(const char *format, ...)
{
    va_list ap;
    va_start(ap, format);
    SysLogger::error(format, ap);
    va_end(ap);
}

bool is_leap_year(int year)
{
    return  ((year % 4 ==0) && (year % 400 != 0))
            || (year % 400 == 0);
}

int date_to_days(int year, int month, int day)
{
    bool is_leap;
    int days;

    if (year < 0 || month < 1 || month > 12 || day < 1) {
        return -1;
    }
    is_leap = is_leap_year(year);
    days = dm(month, is_leap);
    if (month == 12) {
        if (day > 31) {
            return -1;
        }
    } else {
        if (days + day > dm(month + 1, is_leap)) {
            return -1;
        }
    }

    return dy(year) + days + day - 1;
}

inline int dy(int year)
{
    int x;

    if (year == 0) {
        return 0;
    } else {
        x = year - 1;
        return (int)(x / 4) - (int)(x / 100) +
                (int)(x / 400) + x * 365 + 366;
    }
}

inline int dm(int month, bool is_leap)
{
    int days[12] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};

    if (month > 2 && is_leap) {
        return days[month - 1] + 1;
    }

    return days[month - 1];
}
}/* end of namespace rytong */
