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
 *  @file TimeVal.h
 *  @brief Calculate time class.
 *  @author cao.xu <cao.xu@rytong.com>
 *  @version 1.0.0
 *  @date Thu Aug 11 14:09:57 CST 2011
 */

#ifndef _RYT_TIMEVAL_H
#define _RYT_TIMEVAL_H

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <iomanip>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <cmath>
#include <strings.h>
#include <limits.h>

using namespace std;

namespace rytong {
inline ostream & operator <<(ostream &o, const timeval &t) {
    o << t.tv_sec << "." << setw(6) << setfill('0') << t.tv_usec;
    return o;
}

/** @brief Calculate time class.
 */
class TimeVal : public timeval {
public:
    // time_t version of infinite time. TimeVal version relies on this.
    static const time_t INFINITE_TIME_T = INT_MAX;

    static const string REX_TIME_FORMAT;
    static const string UTC_TIME_FORMAT;

    inline TimeVal() {
        tv_sec = tv_usec = 0;
    }

    inline TimeVal(const timeval &t) {
        tv_sec = t.tv_sec;
        tv_usec = t.tv_usec;
    }

    explicit inline TimeVal(const double &t) {
        tv_sec = (int) floor(t);
        tv_usec = (int) floor((t - tv_sec)*1e6 + 0.5);
    }

    inline TimeVal(long sec, long usec) {
        tv_sec = sec;
        tv_usec = usec;
    }

    inline TimeVal(const TimeVal& t) {
        tv_sec = t.tv_sec;
        tv_usec = t.tv_usec;
    }

    TimeVal& parse(const char* in_str) {
        char* str = new char[strlen(in_str) + 1];
        strcpy(str, in_str);
        char* p = index(str, '.');
        if (p != NULL) {
            *p++ = '\0';
            tv_usec = strtoul(p, NULL, 10);
        } else {
            tv_usec = 0;
        }
        tv_sec = strtoul(str, NULL, 10);
        delete[] str;
        return *this;
    }

    string format() const;

    bool fromString(const char* in_str);

    string toString(bool printMicroSeconds = false) const;
    string toStringTZ(bool printMicroSeconds = false) const;

    static string toString(time_t, bool printMicroSeconds = false);
    static string toStringTZ(time_t, bool printMicroSeconds = false);

    static bool is_leap_year(int year);

    bool fromISO8601UTC(const char* in_str);
    string toISO8601UTC(bool printMicroSeconds = false) const;

    time_t seconds() const {
        return tv_sec;
    }

    u_int32_t useconds() const {
        return tv_usec;
    }

    inline TimeVal & operator=(const timeval &b) {
        tv_sec = b.tv_sec;
        tv_usec = b.tv_usec;
        return *this;
    }

    inline TimeVal & operator+=(const timeval &b) {
        tv_sec += b.tv_sec;
        tv_usec += b.tv_usec;
        if (tv_usec >= 1000000) {
            tv_sec++;
            tv_usec -= 1000000;
        }
        return *this;
    }

    inline TimeVal & operator-=(const timeval &b) {
        tv_sec -= b.tv_sec;
        tv_usec -= b.tv_usec;
        if (((int) tv_usec) < 0) {
            tv_sec--;
            tv_usec += 1000000;
        }
        return *this;
    }

    inline bool operator==(const timeval &b) const {
        return tv_sec == b.tv_sec && tv_usec == b.tv_usec;
    }

    inline bool operator!=(const timeval &b) const {
        return tv_sec != b.tv_sec || tv_usec != b.tv_usec;
    }

    inline bool operator<(const timeval &b) const {
        return tv_sec < b.tv_sec || (tv_sec == b.tv_sec && tv_usec < b.tv_usec);
    }

    inline bool operator<=(const timeval &b) const {
        return tv_sec < b.tv_sec || (tv_sec == b.tv_sec && tv_usec <= b.tv_usec);
    }

    inline bool operator>=(const timeval &b) const {
        return tv_sec > b.tv_sec || (tv_sec == b.tv_sec && tv_usec >= b.tv_usec);
    }

    inline bool operator>(const timeval &b) const {
        return tv_sec > b.tv_sec || (tv_sec == b.tv_sec && tv_usec > b.tv_usec);
    }

    inline TimeVal & operator=(long sec) {
        tv_sec = sec;
        tv_usec = 0;
        return *this;
    }

    inline TimeVal & operator+=(long sec) {
        tv_sec += sec;
        return *this;
    }

    inline TimeVal & operator-=(long sec) {
        tv_sec -= sec;
        return *this;
    }

    inline bool operator==(int sec) const {
        return tv_sec == sec && tv_usec == 0;
    }

    inline bool operator!=(int sec) const {
        return (tv_sec != sec) || (tv_usec != 0);
    }

    inline bool operator<(int sec) const {
        return tv_sec < sec;
    }

    inline bool operator<=(int sec) const {
        return tv_sec <= sec;
    }

    inline bool operator>(int sec) const {
        return tv_sec > sec;
    }

    inline bool operator>=(int sec) const {
        return tv_sec >= sec;
    }

    // Conversion to double: 52-bit mantissa is enough for microsecond
    // (1e-6 ~ 20bit) level precision with second as 32-bit integer.

    inline double get_dbl() const {
        return (double) tv_sec + ((double) tv_usec) / 1.0e6;
    }

    inline u_int64_t get_LL() const {
        u_int64_t t = tv_sec;
        t *= 1000000;
        t += tv_usec;
        return t;
    }

    inline TimeVal& infinity() {
        tv_sec = INFINITE_TIME_T;
        tv_usec = 0;
        return *this;
    }

    inline static const TimeVal& Infinity() {
        return TimeVal::TIME_INFINITY;
    }

    inline bool isInfinity() const {
        // Note that we are not checking tv_usec
        return tv_sec == INFINITE_TIME_T;
    }

    inline TimeVal &now() {
        gettimeofday(this, NULL);
        return *this;
    }

    /// tracks the event time

    inline static TimeVal &eventTime() {
        return evTime;
    }
    /// set event time manaually, useful when reading history

    inline static TimeVal &setEventTime(TimeVal &time) {
        return evTime = time;
    }
    /// sync Event time to current time

    inline static TimeVal &syncEventTime() {
        return evTime.now();
    }

private:
    string toStringPrivate(bool printMicroSeconds, bool withTZ,
            bool isUTC = false) const;

    // Prohibit accidentally using toString() in place of fromString()
    bool toString(const char*);

    static TimeVal evTime; // tracks the notification time
    static TimeVal TIME_INFINITY;
};

inline TimeVal operator-(const TimeVal& a, const TimeVal& b) {
    TimeVal c = a;
    c -= b;
    return c;
}

inline TimeVal operator+(const TimeVal& a, const TimeVal& b) {
    TimeVal c = a;
    c += b;
    return c;
}

inline TimeVal operator-(const TimeVal& a, long sec) {
    TimeVal c = a;
    c -= sec;
    return c;
}

inline TimeVal operator+(const TimeVal& a, long sec) {
    TimeVal c = a;
    c += sec;
    return c;
}

inline ostream & operator <<(ostream &o, const timespec &t) {
    o << t.tv_sec << "." << setw(9) << setfill('0') << t.tv_nsec;
    return o;
}

/** @brief Nanosecond instead of microsecond.
 */
class TimeSpec : public timespec {
public:

    TimeSpec() {
        tv_sec = tv_nsec = 0;
    }

    TimeSpec(const TimeVal& t) {
        tv_sec = t.tv_sec;
        tv_nsec = t.tv_usec * 1000;
    }

    TimeSpec(const timeval& t) {
        tv_sec = t.tv_sec;
        tv_nsec = t.tv_usec * 1000;
    }

    TimeSpec(const timespec &t) {
        tv_sec = t.tv_sec;
        tv_nsec = t.tv_nsec;
    }

    explicit TimeSpec(const double &t) {
        tv_sec = (int) floor(t);
        tv_nsec = (int) floor((t - tv_sec)*1e9 + 0.5);
    }

    explicit TimeSpec(time_t t) {
        tv_sec = t;
        tv_nsec = 0;
    }

    TimeSpec(long sec, long usec) {
        tv_sec = sec;
        tv_nsec = usec;
    }

    TimeSpec(const TimeSpec& t) {
        tv_sec = t.tv_sec;
        tv_nsec = t.tv_nsec;
    }

    TimeSpec & operator=(const timespec &b) {
        tv_sec = b.tv_sec;
        tv_nsec = b.tv_nsec;
        return *this;
    }

    TimeSpec & operator+=(const timespec &b) {
        tv_sec += b.tv_sec;
        tv_nsec += b.tv_nsec;
        if (tv_nsec >= 1000000000L) {
            tv_sec++;
            tv_nsec -= 1000000000L;
        }
        return *this;
    }

    TimeSpec & operator-=(const timespec &b) {
        tv_sec -= b.tv_sec;
        tv_nsec -= b.tv_nsec;
        if (((int) tv_nsec) < 0) {
            tv_sec--;
            tv_nsec += 1000000000;
        }
        return *this;
    }

    bool operator==(const timespec &b) const {
        return tv_sec == b.tv_sec && tv_nsec == b.tv_nsec;
    }

    bool operator!=(const timespec &b) const {
        return tv_sec != b.tv_sec || tv_nsec != b.tv_nsec;
    }

    bool operator<(const timespec &b) const {
        return tv_sec < b.tv_sec || (tv_sec == b.tv_sec && tv_nsec < b.tv_nsec);
    }

    bool operator<=(const timespec &b) const {
        return tv_sec < b.tv_sec || (tv_sec == b.tv_sec && tv_nsec <= b.tv_nsec);
    }

    bool operator>=(const timespec &b) const {
        return tv_sec > b.tv_sec || (tv_sec == b.tv_sec && tv_nsec >= b.tv_nsec);
    }

    bool operator>(const timespec &b) const {
        return tv_sec > b.tv_sec || (tv_sec == b.tv_sec && tv_nsec > b.tv_nsec);
    }

    TimeSpec & operator=(long sec) {
        tv_sec = sec;
        tv_nsec = 0;
        return *this;
    }

    TimeSpec & operator+=(long sec) {
        tv_sec += sec;
        return *this;
    }

    TimeSpec & operator-=(long sec) {
        tv_sec -= sec;
        return *this;
    }

    bool operator==(int sec) const {
        return tv_sec == sec && tv_nsec == 0;
    }

    bool operator!=(int sec) const {
        return (tv_sec != sec) || (tv_nsec != 0);
    }

    bool operator<(int sec) const {
        return tv_sec < sec;
    }

    bool operator>=(int sec) const {
        return tv_sec >= sec;
    }

    inline operator double() const {
        return (double) tv_sec + ((double) tv_nsec) / 1.0e9;
    }

    TimeSpec &now() {
#ifdef HAVE_CLOCK_GETTIME
        clock_gettime(CLOCK_REALTIME, this);
#else
        TimeVal tm;
        tm.now();
        tv_sec = tm.tv_sec;
        tv_nsec = tm.tv_usec * 1000;
#endif
        return *this;
    }
};
}/* end of namespace rytong */

#endif // TIMEVAL_HH

