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
 *  @file Mutex.h
 *  @brief Wrapper for pthread mutex.
 *  @author cao.xu <cao.xu@rytong.com>
 *  @version 1.0.0
 *  @date Created on 2010-3-8
 */

#ifndef _RYT_MUTEX_H
#define _RYT_MUTEX_H

#include <stdlib.h>
#include <sys/types.h>
#include <errno.h>
#include <time.h>
#include <sys/time.h>
#include <pthread.h>
#include "TimeVal.h"

namespace rytong {
/** @brief Wrapper for pthread mutex.
 *
 *  We only support the default mutex type on linux (i.e., "fast"),
 *  since other types (recursive and error-check) are non-POSIX.
 *  As a result, lock() and unlock() always returns true.
 */
class Mutex {
public:
    /** @brief Constructor for the class.
     *  @return None.
     */
    inline Mutex();
    
    /** @brief Destructor for the class.
     *  @return None.
     */
    inline ~Mutex();
    
    /** @brief Lock the mutex.
     *  @return true.
     */
    inline bool lock();
    
    /** @brief Unlock the mutex.
     *  @return true.
     */
    inline bool unlock();

    /** @brief Get the mutex.
     *  @return The pointer to mutex_.
     */
    pthread_mutex_t* mutex() {
        return &mutex_;
    }
protected:
    pthread_mutex_t mutex_; ///< The mutex to lock.
};

inline Mutex::Mutex() {
    pthread_mutex_init(&mutex_, NULL);
}

inline Mutex::~Mutex() {
    pthread_mutex_destroy(&mutex_);
}

inline bool Mutex::lock() {
    if (pthread_mutex_lock(&mutex_)) {
        return false;
    }
    return true;
}

inline bool Mutex::unlock() {
    if (pthread_mutex_unlock(&mutex_)) {
        return false;
    }
    return true;
}

/** @brief Allow a condition variable to be shared by multiple threads
 *      (hence multiple mutexes).
 */
class Condition {
public:
    /** @brief Constructor for the class.
     *  @return None.
     */
    Condition();
    
    /** @brief Destructor for the class.
     *  @return None.
     */
    ~Condition();
    
    /** @brief Wrapper for pthread_cond_timedwait().
     *  @see ConditionAndMutex::wait. We must not combine lock() and
     *      wait() in the same function.
     */
    inline bool wait(Mutex& mutex);
    
    /** @brief Wrapper for pthread_cond_timedwait().
     *  @note The return value is used to indicate if a timeout has happened.
     *  @see pthread_cond_timedwait().
     */
    inline int timed_wait(Mutex& mutex, u_int32_t milliseconds);
    
    /** @brief Wrapper for pthread_cond_signal().
     *  @note If we do not lock the condition mutex before signaling,
     *      the signal may be lost if the waiting thread has locked the mutex
     *      but has not started to wait on the condition variable. This,
     *      however, is no different than signaling before any thread is
     *      waiting. Therefore, we do not enforce locking before signaling.
     */
    inline bool signal();
    
    /** @brief Wrapper for pthread_cond_broadcast().
     */
    inline bool broadcast();
protected:
    pthread_cond_t condition_;
};

inline Condition::Condition() {
    pthread_cond_init(&condition_, NULL);
}

inline Condition::~Condition() {
    pthread_cond_destroy(&condition_);
}

inline bool Condition::wait(Mutex& mutex) {
    return (pthread_cond_wait(&condition_, mutex.mutex()) == 0);
}

inline int Condition::timed_wait(Mutex& mutex, u_int32_t milliseconds) {
    struct TimeSpec ts;
    ts.now();
    ts += TimeSpec(milliseconds / 1000, (milliseconds % 1000) * 1000000);
    return pthread_cond_timedwait(&condition_, mutex.mutex(), &ts);
}

inline bool Condition::signal() {
    if (pthread_cond_signal(&condition_)) {
        return false;
    }
    return true;
}

inline bool Condition::broadcast() {
    if (pthread_cond_broadcast(&condition_)) {
        return false;
    }
    return true;
}

/** @brief Wrapper for combined pthread condition and mutex.
 */
class ConditionAndMutex : public Condition, public Mutex {
public:
    /** @brief Constructor for the class.
     *  @return None.
     */
    ConditionAndMutex();
    
    /** @brief Call timedWait after locking the mutex. We have to return
     *      the real return code because it tells the caller if this
     *      is a real signal, a timeout, or a signal interrupt.
     *  @param tm Wait time.
     */
    inline int timed_wait(const struct timespec &tm);
    
    /** @brief Call timedWait after locking the mutex. We have to return
     *      the real return code because it tells the caller if this
     *      is a real signal, a timeout, or a signal interrupt.
     *  @param millisecond Wait time.
     */
    inline int timed_wait(u_int32_t millisecond);
    
    /** @brief Call timedWait after locking the mutex. We have to return
     *      the real return code because it tells the caller if this
     *      is a real signal, a timeout, or a signal interrupt.
     *  @param tm Wait time.
     */
    inline int timed_wait(const struct timeval &tm);
    
    /** @brief Wrapper for pthread_cond_wait(), even though it never 
     *      returns an error code according to its man page PTHREAD_COND(3)
     *  @note The canonical usage is to always lock the mutex before
     *      waiting (I.e., a wait() MUST always be bundled with a protected
     *      section. "Programming with POSIX threads", David Butonhof,
     *      pp.77.).  However, we don't bundle them, because lock() MUST be
     *      separated from wait(), and wait() should always be in a loop
     *      testing the predicate, because it may be interrupted by other than
     *      a signal.
     */
    inline bool wait();
    
    /** @brief Lock before signalling to guarantee that we will switch to
     *      the waiting thread if it has higher priority. Otherwise a lower
     *      priority thread may lock the mutex and forbids the waiting thread
     *      from running. The cost of locking is two more context switches
     *      before the waiting thread can run.
     */
    inline bool locked_signal();
    
    /** @brief Wrapper for pthread_cond_broadcast().
     */
    inline bool locked_broadcast();
};

inline ConditionAndMutex::ConditionAndMutex() :
Condition(), Mutex() {
}

inline bool ConditionAndMutex::wait() {
    if (pthread_cond_wait(&condition_, &mutex_)) {
        return false;
    }
    return true;
}

inline int ConditionAndMutex::timed_wait(const struct timespec &tm) {
    return pthread_cond_timedwait(&condition_, &mutex_, &tm);
}

inline int ConditionAndMutex::timed_wait(const struct timeval &tm) {
    struct timespec ts;
    ts.tv_sec = tm.tv_sec;
    ts.tv_nsec = tm.tv_usec * 1000;
    return pthread_cond_timedwait(&condition_, &mutex_, &ts);
}

inline int ConditionAndMutex::timed_wait(u_int32_t milliseconds) {
    struct TimeSpec ts;
    ts.now();
    ts += TimeSpec(milliseconds / 1000, (milliseconds % 1000) * 1000000);
    return timed_wait(ts);
}

inline bool ConditionAndMutex::locked_signal() {
    bool ret;
    if (!lock()) {
        return false;
    }
    if (pthread_cond_signal(&condition_)) {
        ret = false;
    } else {
        ret = true;
    }
    if (!unlock()) {
        ret = false;
    }
    return ret;
}

inline bool ConditionAndMutex::locked_broadcast() {
    bool ret;
    if (!lock()) {
        return false;
    }
    if (pthread_cond_broadcast(&condition_)) {
        ret = false;
    } else {
        ret = true;
    }
    if (!unlock()) {
        ret = false;
    }
    return ret;
}
}/* end of namespace rytong */

#endif  /* _RYT_MUTEX_H */
