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
 *  @file ThreadPool.cpp
 *  @brief Thread pool class.
 *  @author cao.xu <cao.xu@rytong.com>
 *  @version 1.0.0
 *  @date Created on 2011-4-19
 */

#include "ThreadPool.h"

using namespace rytong;

ThreadPool::ThreadPool() {
};

ThreadPool::~ThreadPool() {
    for (unsigned i = 0; i < thr_list_.size(); i++) {
        async_add(NULL);
    }

    while(!thr_list_.empty()){
        pthread_t* thr = thr_list_.front();
        thr_list_.pop();
        pthread_join(*thr, NULL);
        delete thr;
        thr = NULL;
    }

    while (0 != q_.size()) {
        ErlAsync* a = q_.front();
        q_.pop();
        delete a;
    }
}

void ThreadPool::set(ThreadPool* pool, unsigned long long len) {
    for (unsigned long long i = 0; i < len; i++) {
        pthread_t* thr = new pthread_t();
        /* FIXME
         * Should we handle errors if pthread_create failed ?
         */
        pthread_create(thr, NULL, async_main, pool);
        thr_list_.push(thr);
    }
}

int ThreadPool::do_async(void(*async_invoke)(void*), void* async_data,
        void(*async_free)(void*)) {
    ErlAsync* a = new ErlAsync();
    //rassert(a != NULL);
    // cout<<"here to send a job "<<endl;
    if (NULL != a) {
        a->async_data = async_data;
        a->async_invoke = async_invoke;
        a->async_free = async_free;
        //rassert(a->async_invoke != NULL);
        if (0 != async_add(a)) {
            //if there is only one free function, we can directly call it and
            //need no more async_free.
            // cout<<"error when adding to msg queue!\r"<<endl;
            (*a->async_free)(a->async_data);
            delete a;
            return -1;
        }
        // cout << "now the msg length : " <<q_.size() <<endl;
        return 0;
    } else {
        // cout << "error when malloc!" <<endl;
        return -1;
    }
}


int ThreadPool::async_add(ErlAsync* a) {
    // rassert(a != NULL);
    // cout<<"here to add job to the queue"<<endl;
    if (!mtx_.lock()) {
        cout<<"error get lock in async_add\r"<<endl;
        // SysLogger::error("error get lock");
        return -1;
    }
    // cout<<"finish lock"<<endl;
    q_.push(a);
    // cout<<"finish add"<<endl;
    if (1 == q_.size()) {
        cv_.broadcast();
    }
    // cout<<"finish broadcast"<<endl;
    mtx_.unlock();
    // cout<<"finish unlock"<<endl;
    return 0;
}

ErlAsync * ThreadPool::async_get() {
    if (!mtx_.lock()) {
        // cout<<"error get lock in async_get\r"<<endl;
        // SysLogger::error("error get lock");
        return NULL;
    }

    while (0 == q_.size()) {
        cv_.wait(mtx_);
    }
    ErlAsync * a = q_.front();

    // rassert(a != NULL);

    q_.pop();
    mtx_.unlock();
    // cout<< "async_get a job\r"<<endl;
    return a;
}


void* ThreadPool::async_main(void* arg) {
    ErlAsync* a;
    ThreadPool* pool = (ThreadPool*) arg;
    while (a = pool->async_get()) {
         // ((DrvData*) a->async_data)->conn = conn;
        (*a->async_invoke)(a->async_data);
        // (*a->async_free)(a->async_data);
        delete a;
        a = NULL;
    }
    pthread_exit((void *)0);
    return NULL;
}


