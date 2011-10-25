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
 * $Id: StmtMap.h 22181 2010-02-24 03:24:43Z deng.lifen $
 *
 *  @file StmtMap.h
 *  @brief Manage data of prepare statement.
 *  @author cao.xu <cao.xu@rytong.com>
 *  @version 1.0.0
 *  @date Created on 2010-2-23
 */

#ifndef _RYT_STMT_MAP_H
#define _RYT_STMT_MAP_H

#include <map>
#include <string>
#include "Mutex.h"

using namespace std;

namespace rytong{

typedef map<string, void*>stmt_map; ///< Stmt map.
    
/** @brief Manage data of prepare statement.
 */
class StmtMap {
public:
    /** @brief Constructor for the class.
     *  @return None.
     */
    StmtMap();
    
    /** @brief Destructor for the class.
     *  @return None.
     */
    ~StmtMap();

    /** @brief Add the point to stmt to the map.
     *  @param key Prepare name.
     *  @param value The point to stmt data.
     *  @return true|false.
     */
    bool add(const string& key, void* value);

    /** @brief Get the point to stmt from the map.
     *  @param key Prepare name.
     *  @return Is successful.
     *  @retval true Connect success.
     *  @retval false Connect failed.
     */
    void* get(const string& key);

    /** @brief Remove the point to stmt from the map.
     *  @param key Prepare name.
     *  @return The point to stmt data.
     */
    void* remove(const string& key);
    
    /** @brief Remove the point to stmt from the map.
     *  @return The point to stmt data.
     */
    void* pop();

private:
    StmtMap(const StmtMap& orig);

    stmt_map map_; ///< Stmt map.
    Mutex mutex_; ///< Operation mutex.
};
}/* end of namespace rytong */

#endif /* _RYT_STMT_MAP_H */
