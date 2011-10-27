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
 *  @file FieldToTypeMap.h
 *  @brief Storage database fields.
 *  @author cao.xu <cao.xu@rytong.com>
 *  @version 1.0.0
 *  @date Wed Mar 10 14:24:38 CST 2010
 */

#ifndef _RYT_FIELD_TO_TYPE_MAP_H
#define _RYT_FIELD_TO_TYPE_MAP_H

#include <string>
#include "util/MutexMap.h"
using namespace std;

namespace rytong {
/** @brief Storage database fields.
 */
class FieldToTypeMap {
public:
    ~FieldToTypeMap();

    /**
     * Function: instance
     * Description: interface to get the singleton instance of Map.
     * Input: none
     * Output: none
     * Returns: FieldToTypeMap object.
     */
    static FieldToTypeMap& instance() {
        if (NULL == instance_) {
            static MyOSMutex mutex;
            mutex.lock();
            if (NULL == instance_) {
                instance_ = new FieldToTypeMap();
            }
            mutex.unlock();
        }
        return (*instance_);
    }

    /**
     * Function: destroy
     * Description: destroy the unique FieldToTypeMap object.
     * Input: none
     * Output: none
     * Returns: none.
     */
    static void destroy() {
        delete instance_;
    }

    /**
     * Function: add
     * Description: store the mapping into the map, which use 
     *              table_name + field_name as the key and type as value.
     *              we should add mutex if we want this interface to be 
     *              thread-safe.
     * Input: table_name -> table name in database
     *        field_name -> field_name in database
     *        type -> type of field in database
     * Output:
     * Returns: true|false
     */
    bool add(const string& table_name, const string& field_name, int type) {
        string key = table_name + "." + field_name;
        return map_.add(key, (void *)type);
    }

    /**
     * Function: get_type
     * Description: get type according to the key.
     *              we also need add mutex if the map can be changed during.
     * Input: table_name -> table name in database
     *        field_name -> field_name in database
     * Output:
     * Returns: int type of field in database
     */
    int get_type(const string& table_name, const string& field_name) {
        string key = table_name + "." + field_name;
        void * value = map_.get(key);
        if (NULL != value) {
            return (int)value;
        }
        return -1;
    }

private:
    FieldToTypeMap();
    FieldToTypeMap(const FieldToTypeMap& orig);

    static FieldToTypeMap* instance_; ///< singleton instance
    MutexMap map_; ///< data structure to store mapping
};
}/* end of namespace rytong */

#endif /* _RYT_FIELD_TO_TYPE_MAP_H */
