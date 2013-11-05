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
 *  @file TimeVal.cpp
 *  @brief Calculate time class.
 *  @author cao.xu <cao.xu@rytong.com>
 *  @version 1.0.0
 *  @date Thu Aug 11 14:10:39 CST 2011
 */

#include <sstream>
#include <string.h>

#include "TimeVal.h"

using namespace rytong;

const string TimeVal::REX_TIME_FORMAT = "%Y-%m-%d %H:%M:%S";
const string TimeVal::UTC_TIME_FORMAT = "%FT%R:%S";

TimeVal TimeVal::evTime(0, 0);
TimeVal TimeVal::TIME_INFINITY(INT_MAX, 0);

string
TimeVal::format() const {
    stringstream ss;
    ss << *this;
    return ss.str();
}

bool TimeVal::fromString(const char* in_str) {
    if (!strcasecmp(in_str, "now")) {
        now();
        return true;
    }

    struct tm lt;
    char *ptr;
    char* str = new char[strlen(in_str) + 1];
    ewp_strcpy(str, in_str);

    u_int32_t usec = 0;
    char* p = index(str, '.');
    if (p != NULL) {
        *p++ = '\0';
        size_t c = strspn(p, "0123456789");
        if (c == 6 && *(p + c) == '\0') {
            usec = strtoul(p, NULL, 10);
        } else {
            delete[] str;
            return false;
        }
    }

    // Accept timestamps in various formats.  For the seconds-since-epoch
    // format, require dates in the 21st century in order to avoid accepting
    // smaller numbers such as an ISO 8601 date alone.
    if ((ptr = strptime(str, "%s", &lt)) && *ptr == '\0' &&
            strtoul(str, NULL, 10) > 978307200UL) {
    } else if ((ptr = strptime(str, REX_TIME_FORMAT.c_str(), &lt)) &&
            (*ptr == '\0')) {
    } else if ((ptr = strptime(str, "%Y%m%d-%H:%M:%S", &lt)) && *ptr == '\0') {
    } else if ((ptr = strptime(str, "%Y%m%d-%H%M%S", &lt)) && *ptr == '\0') {
    } else {
        delete[] str;
        return false;
    }
    delete[] str;

    // Tell mktime to determine if DST.  The input is ambiguous for the
    // duplicated hour at the end of daylight savings (mktime always takes the
    // DST one).  strptime won't parse timezone names yet, so not easy to fix.
    lt.tm_isdst = -1;
    time_t sec = mktime(&lt);
    if (sec == -1)
        return false;
    tv_sec = sec;
    tv_usec = usec;
    return true;
}

string TimeVal::toString(bool printMicroSeconds) const {
    return toStringPrivate(printMicroSeconds, false);
}

string TimeVal::toStringTZ(bool printMicroSeconds) const {
    return toStringPrivate(printMicroSeconds, true);
}

// static method

string TimeVal::toString(time_t t, bool printMicroSeconds) {
    return TimeVal(t).toStringPrivate(printMicroSeconds, false);
}

// static method

string TimeVal::toStringTZ(time_t t, bool printMicroSeconds) {
    return TimeVal(t).toStringPrivate(printMicroSeconds, true);
}

string TimeVal::toStringPrivate(bool printMicroSeconds, bool withTZ,
        bool isUTC) const {
    static const int BUF_CNT = 6;
    static const int BUF_LEN = 48;
    static char buffers[BUF_CNT][BUF_LEN];
    static int turn = 0;

    turn = (turn + 1) % BUF_CNT;
    char* buffer = buffers[turn];
    int len = BUF_LEN;

    struct tm lt;
    int ret;

    time_t t = tv_sec;
    if (isUTC) {
        gmtime_r(&t, &lt);
        ret = strftime(buffer, len, UTC_TIME_FORMAT.c_str(), &lt);
    } else {
        localtime_r(&t, &lt);
        ret = strftime(buffer, len, REX_TIME_FORMAT.c_str(), &lt);
    }
    if (ret == 0)
        return string(); // Empty string

    buffer += ret;
    len -= ret;
    if (printMicroSeconds)
        ret = snprintf(buffer, len, ".%06lu", (long unsigned int) tv_usec);
    else
        ret = 0;
    if (ret >= len)
        return string(); // Empty string

    buffer += ret;
    len -= ret;
    if (withTZ) {
        ret = strftime(buffer, len, " %Z", &lt);
        if (ret == 0)
            return string(); // Empty string
    }

    return string(buffers[turn]);
}

bool TimeVal::fromISO8601UTC(const char* in_str) {
    static bool localAdjustKnown = false;
    static int gmtoff;
    char* str = new char[strlen(in_str) + 1];
    ewp_strcpy(str, in_str);
    char *p = strstr(str, "T");
    if (p) *p = ' ';
    bool ret = fromString(str); // parse UTC as if localtime
    if (ret) {
        if (!localAdjustKnown) { // find difference from UTC
#ifdef _AIX
        gmtoff = 0;
#else
            static struct tm gm;
            strptime("0", "%s", &gm);
            gmtoff = gm.tm_gmtoff; // eg -28800 for PST
#endif
            localAdjustKnown = true;
        }
        tv_sec -= gmtoff; // adjust
    }
    return ret;
}

string TimeVal::toISO8601UTC(bool printMicroSeconds) const {
    return toStringPrivate(printMicroSeconds, false, true);
}

bool TimeVal::is_leap_year(int year) {
    if (year & 0x3) {
        return false;
    } else {
        if (year % 400 == 0) {
            return true;
        } else if (year % 100 == 0) {
            return false;
        } else {
            return true;
        }
    }
}

