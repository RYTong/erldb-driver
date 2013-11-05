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
 *  @file ThreadPool.h
 *  @brief Thread pool class.
 *  @author cao.xu <cao.xu@rytong.com>
 *  @version 1.0.0
 *  @date Created on 2011-4-19
 */

#ifndef THREADPOOL_H_
#define THREADPOOL_H_

#include <ei.h>
#include <map>
#include <queue>
#include <list>
#include <string>
#include <iostream>


#include "../util/Mutex.h"
#include "../util/debug.h"
#include "../util/EiEncoder.h"
#include "../util/SysLogger.h"

using namespace std;

namespace rytong {

/** @brief Async message struct. Async data, work function and free function.
 */
typedef struct erl_async {
    void* async_data;    ///< The data for work and free functions.
    void (*async_invoke)(void*); ///< The work function.
    void (*async_free)(void*); ///< The free function.
} ErlAsync;


typedef queue<ErlAsync*> MsgQueue; ///< The messages' queue.

typedef queue<pthread_t*> ThreadList; ///< The work threads' queue.

/** @brief Thread pool class.
 */
class ThreadPool {
public:
    /** @brief Constructor for the class.
     *  @return None.
     */
    ThreadPool();

    /** @brief Destructor for the class.
     *  @return None.
     */
    ~ThreadPool();

    /** @brief Create the async messages here and add them to the message queue.
     *  @param async_invoke The work function.
     *  @param async_data The data for work and free functions.
     *  @param async_free The free function.
     *  @return If successful.
     *  @retval 0 for success.
     *  @retval -1 for failed.
     */
    int do_async(void(*async_invoke)(void*), void* async_data, void(*async_free)(void*));

    /** @brief Setter function for worker threads of ThreadPool.
     *      Set the length of threads and initialize them.
     *  @param pool The pointer to thread pool.
     *  @param len The thread pool length.
     *  @return None.
     */
    void set(ThreadPool* pool, unsigned long long len);

private:
    /** @brief Add an async message to the queue.
     */
    int async_add(ErlAsync* a);

    /** @brief Get an async message from the queue.
     */
    ErlAsync* async_get();

    /** @brief Main function of work threads.
     *      do a infinite loop: get an async message in the queue, then finish
     *      it, until the system shut down.
     */
    static void* async_main(void* arg);


    /** @brief These functions are not implemented to prohibit copy construction
     *      and assignment by value.
     */
    ThreadPool & operator=(const ThreadPool &);
    ThreadPool(const ThreadPool &);

    Mutex mtx_;
    Condition cv_;
    MsgQueue q_;
    ThreadList thr_list_;
};

}

#endif /* THREADPOOL_H_ */
