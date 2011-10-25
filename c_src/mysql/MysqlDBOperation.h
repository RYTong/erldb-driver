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
 * $Id: MysqlDBOperation.h 22235 2010-02-25 09:13:30Z deng.lifen $
 *
 *  @file MysqlDBOperation.h
 *  @brief Derived class for mysql to represent operations of database.
 *  @author cao.xu <cao.xu@rytong.com>
 *  @version 1.0.0
 *  @date Created on 2010-1-31
 */

#ifndef _RYT_MYSQL_DATABASE_H
#define _RYT_MYSQL_DATABASE_H

#include <mysql.h>
#include "base/DBOperation.h"
#include "base/StmtMap.h"

namespace rytong {
/** @brief struct StmtData.
 *
 */
typedef struct {
    void* stmt; ///< The point to stmt.
    void* meta_data; ///< The port to select meta data.
} StmtData;

/** @brief Derived class for mysql to represent operations of database.
 */
class MysqlDBOperation : public DBOperation {
public:
    /** @brief Constructor for the class.
     *  @return None.
     */
    MysqlDBOperation();
    
    /** @brief Destructor for the class.
     *  @return None.
     */
    ~MysqlDBOperation();

    static bool release_stmt(void* data) {
        StmtData* stmt_data = (StmtData*)data;
        mysql_free_result((MYSQL_RES*) stmt_data->meta_data);
        if (mysql_stmt_close((MYSQL_STMT*) stmt_data->stmt)) {
            delete stmt_data;;
            return true;
        }
        return false;
    }

    /** @brief Execute interface.
     *  @see DBOperation::exec.
     */
    bool exec(ei_x_buff * const res);

    /** @brief Insert interface.
     *  @see DBOperation::insert.
     */
    bool insert(ei_x_buff * const res);

    /** @brief Update interface.
     *  @see DBOperation::update.
     */
    bool update(ei_x_buff * const res);

    /** @brief Delete interface.
     *  @see DBOperation::del.
     */
    bool del(ei_x_buff * const res);

    /** @brief Select interface.
     *  @see DBOperation::select.
     */
    bool select(ei_x_buff * const res);

    /** @brief Transaction begin interface.
     *  @see DBOperation::trans_begin.
     */
    bool trans_begin(ei_x_buff * const res);

    /** @brief Transaction commit interface.
     *  @see DBOperation::trans_commit.
     */
    bool trans_commit(ei_x_buff * const res);

    /** @brief Transaction rollback interface.
     *  @see DBOperation::trans_rollback.
     */
    bool trans_rollback(ei_x_buff * const res);

    /** @brief Perpare statement init interface.
     *  @see DBOperation::prepare_stat_init.
     */
    bool prepare_stat_init(ei_x_buff * const res);

    /** @brief Perpare statement execute interface.
     * @warning This function is not safe in multithreading.
     *  @see DBOperation::prepare_stat_exec.
     */
    bool prepare_stat_exec(ei_x_buff * const res);

    /** @brief Perpare statement release interface.
     *  @see DBOperation::prepare_stat_release.
     */
    bool prepare_stat_release(ei_x_buff * const res);

    /** @brief Mapping mysql field type.
     *  @param type Mysql type name.
     *  @return Mysql type code.
     */
    int map_mysql_type(const char* type);

private:
    /** These functions are not implemented to prohibit copy construction
     * and assignment by value.
     */
    MysqlDBOperation & operator =(const MysqlDBOperation&);
    MysqlDBOperation(MysqlDBOperation&);

    bool real_query_sql(const char* sql, ei_x_buff * const res, 
            MYSQL * & db_conn);

    /** execute sql string */
    void exec_sql(const char* sql, ei_x_buff * const res);

    /** execute stmt */
    void exec_stmt(StmtData*, FieldValue*, ei_x_buff * const);

    /** call execute stmt */
    void call_exec_stmt(StmtData*, ei_x_buff * const);

    /** encode select result */
    void encode_select_result(MYSQL_RES*, ei_x_buff * const);

    /** encode stmt select result */
    void encode_stmt_result(StmtData*, unsigned int, ei_x_buff * const);

    /** encode one row */
    void encode_one_row(MYSQL_ROW, MYSQL_FIELD*, unsigned int,
            unsigned long *, ei_x_buff * const);

    /** encode date tuple or time tuple */
    inline void encode_date_tuple(unsigned int year, unsigned int month,
            unsigned int day, ei_x_buff * const res) {
        ei_x_encode_tuple_header(res, 3);
        ei_x_encode_long(res, (long) year);
        ei_x_encode_long(res, (long) month);
        ei_x_encode_long(res, (long) day);
    }

    /** @brief Get field type for mysql.
     *  @param fieldtype Field type in erlang.
     *  @return Field type for mysql.
     */
    const enum_field_types get_db_field_type(char field_type) const;

    /** fill value type */
    void fill_value(stringstream & sm, FieldValue & field);

    /** make where */
    void make_where(stringstream & sm);

    /** make fields */
    void make_field(stringstream & sm);

    /** make tables */
    void make_table(stringstream & sm);

    /** make extras */
    void make_extras(stringstream & sm);

    /** decode where clause */
    void decode_expr(stringstream & sm);

    /** decode the tuple in where clause */
    void decode_expr_tuple(stringstream & sm);
};
}/* end of namespace rytong */

#endif /* _RYT_MYSQL_DATABASE_H */
