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
 * $Id$
 *
 *  @file SysLogger.cpp
 *  @brief System logger class.
 *  @author wang.meigong <wang.meigong@rytong.com>
 *  @version 1.0.0
 *  @date Created on 2010-04-08 4:09
 */

#include <stdarg.h>
#include "SysLogger.h"

#define MESSAGE_LENGTH 1024

using namespace rytong;

SysLogger::SysLogger() {
}

SysLogger::~SysLogger() {
}

void SysLogger::open(const char* ident) {
    openlog (ident, LOG_CONS | LOG_PID, LOG_USER);
}

void SysLogger::close() {
    closelog();
}

void SysLogger::info(const char *format, ...) {
    char tmp[MESSAGE_LENGTH];
    va_list ap;
    va_start(ap, format);
    vsnprintf(tmp, MESSAGE_LENGTH, format, ap);
    syslog(LOG_INFO, "%s", tmp);
    va_end(ap);
}

void SysLogger::debug(const char *format, ...) {
#ifdef DEBUG
    char tmp[MESSAGE_LENGTH];
    va_list ap;
    va_start(ap, format);
    vsnprintf(tmp, MESSAGE_LENGTH, format, ap);
    syslog(LOG_DEBUG, "%s", tmp);
    va_end(ap);
#endif
}

void SysLogger::notice(const char *format, ...) {
    char tmp[MESSAGE_LENGTH];
    va_list ap;
    va_start(ap, format);
    vsnprintf(tmp, MESSAGE_LENGTH, format, ap);
    syslog(LOG_INFO, "%s", tmp);
    va_end(ap);
}

void SysLogger::warning(const char *format, ...) {
    char tmp[MESSAGE_LENGTH];
    va_list ap;
    va_start(ap, format);
    vsnprintf(tmp, MESSAGE_LENGTH, format, ap);
    syslog(LOG_INFO, "%s", tmp);
    va_end(ap);
}

void SysLogger::error(const char *format, ...) {
    char tmp[MESSAGE_LENGTH];
    va_list ap;
    va_start(ap, format);
    vsnprintf(tmp, MESSAGE_LENGTH, format, ap);
    syslog(LOG_ERR, "%s", tmp);
    va_end(ap);
}

void SysLogger::crit(const char *format, ...) {
    char tmp[MESSAGE_LENGTH];
    va_list ap;
    va_start(ap, format);
    vsnprintf(tmp, MESSAGE_LENGTH, format, ap);
    syslog(LOG_CRIT, "%s", tmp);
    va_end(ap);
}

void SysLogger::alert(const char *format, ...) {
    char tmp[MESSAGE_LENGTH];
    va_list ap;
    va_start(ap, format);
    vsnprintf(tmp, MESSAGE_LENGTH, format, ap);
    syslog(LOG_ALERT, "%s", tmp);
    va_end(ap);
}
