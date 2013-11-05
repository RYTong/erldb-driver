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
 *  @file SysLogger.h
 *  @brief System logger class.
 *  @author wang.meigong <wang.meigong@rytong.com>
 *  @version 1.0.0
 *  @date Created on 2010-04-08 4:09
 */

#ifndef _RYT_SYS_LOGGER_H
#define _RYT_SYS_LOGGER_H

#include <syslog.h>
#include <stdio.h>

namespace rytong {
/** @brief System logger class.
 */
class SysLogger {
public:
    /** @brief Constructor for the class.
     *  @return None.
     */
    SysLogger();
    
    /** @brief Destructor for the class.
     *  @return None.
     */
    ~SysLogger();
    
    /** @brief Opens a connection to the system logger for a program.
     *  @param ident Prepended to every message, and is typically set to
     *      the program name.    
     *  @return None.
     */
    static void open(const char* ident);
    
    /** @brief Closes the descriptor being used to write to the system logger.
     *  @return None.
     */
    static void close();
    
    /** @brief Generates a informational log message.
     *  @param format The message format, as in printf(3) and any
     *      arguments required by the format, except that the two
     *      character sequence %m will be replaced  by the error 
     *      message string strerror(errno).    
     *  @return None.
     */
    static void info(const char *format, ...);
    
    /** @brief Generates debug-level message. 
     *  @param format The message format, as in printf(3) and any
     *      arguments required by the format, except that the two
     *      character sequence %m will be replaced  by the error 
     *      message string strerror(errno).    
     *  @return None.
     */
    static void debug(const char *format, ...);
    
    /** @brief Generates a log message, the message level 
     *      is normal, but significant, condition. 
     *  @param format The message format, as in printf(3) and any
     *      arguments required by the format, except that the two
     *      character sequence %m will be replaced  by the error 
     *      message string strerror(errno).    
     *  @return None.
     */
    static void notice(const char *format, ...);
    
    /** @brief Generates a log message, the message level 
     *      is warning conditions. 
     *  @param format The message format, as in printf(3) and any
     *      arguments required by the format, except that the two
     *      character sequence %m will be replaced  by the error 
     *      message string strerror(errno).    
     *  @return None.
     */
    static void warning(const char *format, ...);

    /** @brief Generates a log message, the message level 
     *      is error conditions. 
     *  @param format The message format, as in printf(3) and any
     *      arguments required by the format, except that the two
     *      character sequence %m will be replaced  by the error 
     *      message string strerror(errno).    
     *  @return None.
     */
    static void error(const char *format, ...);
   
    /** @brief Generates a log message, the message level 
     *      is critical conditions. 
     *  @param format The message format, as in printf(3) and any
     *      arguments required by the format, except that the two
     *      character sequence %m will be replaced  by the error 
     *      message string strerror(errno).    
     *  @return None.
     */
    static void crit(const char *format, ...);
    
    /** @brief Generates a log message, the message level 
     *      is that action must be taken immediately. 
     *  @param format The message format, as in printf(3) and any
     *      arguments required by the format, except that the two
     *      character sequence %m will be replaced  by the error 
     *      message string strerror(errno).    
     *  @return None.
     */
    static void alert(const char *format, ...);

private:
};
}/* end of namespace rytong */

#endif /* _RYT_SYS_LOGGER_H */
