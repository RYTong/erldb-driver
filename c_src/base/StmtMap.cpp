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
 * $Id: StmtMap.cpp 22181 2010-02-24 03:24:43Z deng.lifen $
 *
 *  @file StmtMap.cpp
 *  @brief Manage data of prepare statement.
 *  @author cao.xu <cao.xu@rytong.com>
 *  @version 1.0.0
 *  @date Created on 2010-2-23 
 */

#include "base/StmtMap.h"

using namespace rytong;

StmtMap::StmtMap() {
}

StmtMap::~StmtMap() {
    // stmt_map::iterator pos;
    // //FIXME we should add branchs here for database other than mysql
    // if (true) {
    //     while (!map_.empty()) {
    //         pos = map_.begin();
    //         DBOperation::release_stmt(pos->second);
    //         map_.erase(pos);
    //     }
    // }
}

bool StmtMap::add(const string& key, void* value) {
    if (!mutex_.lock()) {
        return false;
    }
    if (map_.insert(make_pair(key, value)).second) {
        mutex_.unlock();
        return true;
    }
    mutex_.unlock();
    return false;
}

void* StmtMap::get(const string& key) {
    if (!mutex_.lock()) {
        return NULL;
    }
    stmt_map::iterator pos = map_.find(key);
    if (pos != map_.end()) {
        mutex_.unlock();
        return pos->second;
    }
    mutex_.unlock();
    return NULL;
}

void* StmtMap::remove(const string& key) {
    if (!mutex_.lock()) {
        return NULL;
    }
    stmt_map::iterator pos = map_.find(key);
    if (pos != map_.end()) {
        map_.erase(pos);
        mutex_.unlock();
        return pos->second;
    }
    mutex_.unlock();
    return NULL;
}

void* StmtMap::pop() {
    if (!mutex_.lock()) {
        return NULL;
    }
    
    if (!map_.empty()) {
        stmt_map::iterator pos = map_.begin();
        void* stmt_data = pos->second;
        map_.erase(pos);
        mutex_.unlock();
        return stmt_data;
    }

    mutex_.unlock();
    return NULL;
}
