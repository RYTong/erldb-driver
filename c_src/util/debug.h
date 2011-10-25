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
 * $Id: debug.h 21893 2010-02-02 06:21:48Z deng.lifen $
 *
 *  @file debug.h
 *  @brief Global functions, variables, defines.
 *  @author deng.lifen <deng.lifen@rytong.com>
 *  @version 1.0.0
 *  @date Fri Nov 13 14:41:22 CST 2009
 */

#ifndef _RYT_UTIL_H
#define _RYT_UTIL_H

/** @brief Copyright (c) 2009-2010 Beijing RYTong Information Technologies, Ltd.
 *      All rights reserved.
 */
namespace rytong {

/** @brief close debug */
// #define NDEBUG 
#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>
#include <execinfo.h>

#ifdef NDEBUG
#define rabort(str)
#else // ! NDEBUG
#ifdef rabort
#undef rabort
#endif
#ifdef HAVE_CPP_VAARG
#define rabort(format, args...) do { fprintf(stderr, format, ## args); abort(); } while(0)
#else

inline void rabort(const char* fmt, ...) //__gnu_printf_like(1, 2)
{
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    abort();
}
#endif
#endif // NDEBUG

#ifdef rassert
#undef rassert
#endif

/** @brief This mimics the behavior of C stdlib's assert() but dump an error
 *      message to error log instead of stderr.
 */
#ifdef NDEBUG
#define rassert(str)
#else
#define rassert(expr) \
do { \
    if (!(expr)) { \
        PRINT_STACK(); \
        rabort("%s:%d assertion " #expr " failed in function %s\n", \
            __FILE__, __LINE__, __PRETTY_FUNCTION__); \
    } \
} \
while (0)
#endif

#define DEPTH 100 ///< Stack depth.
/** @brief This macro prints out a stack trace at current location.
 *  @note
 *  - To call this in your code, you MUST include execinfo.h.
 *  - It is preferred that depth is a CONSTANT, although a variable will
 *      also work.
 */
#define PRINT_STACK() \
do { \
    void* callstack[DEPTH]; \
    int num_callers = backtrace(callstack, DEPTH); \
    char** callers = backtrace_symbols(callstack, num_callers); \
    fprintf(stderr, "Call stack: \n"); \
    for (int i = 0; i < num_callers; ++i) { \
        fprintf(stderr, "%s\n", callers[i]); \
    } \
    free(callers); \
} \
while (0)


// // Turn on time printer.
// #define _RYT_TIME_TEST_
// // Turn on log writing.
// #define _RYT_LOG_DEBUG_
// 
// // Only turn on log writing if we are in debugging mode.
// #ifdef NDEBUG
// #define WRITE_LOG(x) 
// #else
// #define WRITE_LOG(x) LogHelper::instance().write(__FILE__, __LINE__, x)
// #endif
}/* end of namespace rytong */

#endif /* _RYT_UTIL_H */

