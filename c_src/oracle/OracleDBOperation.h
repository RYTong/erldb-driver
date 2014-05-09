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
 *  @file OracleDBOperation.h
 *  @brief Derived class for oracle to represent operations of database.
 *  @author wang.meigong <wang.meigong@rytong.com>
 *  @version 1.0.0
 *  @date Created on 2010-2-3
 */

#ifndef _RYT_ORACLE_DB_OPERATION_H
#define _RYT_ORACLE_DB_OPERATION_H

#include <occi.h>
#include <vector>
#include "../util/SysLogger.h"
#include "../base/DBOperation.h"
#include "../base/DBException.h"
#include "../base/StmtMap.h"
#include "OracleConnection.h"

namespace rytong {
static const int MAX_COLUMN_SIZE = 100; ///< Max column size.
static const int MAX_LONG_SIZE = 10000; ///< Max long size.
static const int MAX_LBI_SIZE = 10000; ///< Max lbi size.
static const char* BAD_ARG_ERROR = "bad arg"; ///< Arg error message.

/** @brief Derived class for oracle to represent operations of database.
 */
class OracleDBOperation : public DBOperation {
public:

    /** @brief Param index in ei buffer. */
    typedef std::vector<int> ParamIndex;

    /**  @enum OCCIDataType
     *  @brief OCCI data type.
     */
    enum OCCIDataType {
        EMPTY,       ///< Empty String
        STRING,      ///< String.
        NUMBER,      ///< Number.
        DATE,        ///< Date.
        TIMESTAMP,   ///< Timestamp.
        INTERVAL_YM, ///< Interval year to month.
        INTERVAL_DS, ///< Interval day to second.
        BYTES,       ///< Binary.
        BFILEE,      ///< Bfile.
    };

    /** @brief Constructor for the class.
     *  @return None.
     */
    OracleDBOperation();

    /** @brief Destructor for the class.
     *  @return None.
     */
    ~OracleDBOperation();

    static bool release_stmt(void* data) {
        oracle::occi::Statement* stmt = (oracle::occi::Statement*) data;
        oracle::occi::Connection* conn = stmt->getConnection();
        conn->terminateStatement(stmt);
        return true;
    }

    /** @brief Get singleton env variable.
     *  @attention We should add mutex if we want this function to be thread
     *      safe.
     *  @return The pointer to oracle::occi::Environment.
     */
    static oracle::occi::Environment* get_env_instance();

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
     bool prepare_statement_init(ei_x_buff * const res);

     /** @brief Perpare statement execute interface.
      * @warning This function is not safe in multithreading.
      *  @see DBOperation::prepare_stat_exec.
      */
     bool prepare_stat_exec(ei_x_buff * const res);
     bool prepare_statement_exec(ei_x_buff * const res);

     /** @brief Perpare statement release interface.
      *  @see DBOperation::prepare_stat_release.
      */
     bool prepare_stat_release(ei_x_buff * const res);
     bool prepare_statement_release(ei_x_buff * const res);

    void decode_expr_tuple(stringstream & sm) {
    }

private:
    static oracle::occi::Environment* env_;

    OracleDBOperation & operator =(const OracleDBOperation&);

    OracleDBOperation(OracleDBOperation&);

    /** @brief Make select sql expression.
     *  @param sql The stringstream object, using to store sql statement.
     *  @param param_index The type is vector<int>, using to store param
     *      index which is in the ei buff.
     *  @return None.
     */
    void make_expr(stringstream& sql, ParamIndex& param_index);

    /** @brief Encode one column in the ei_x_buff.
     *  @param type Data type of the column.
     *  @param colIndex Column index in the result set.
     *  @param rset Oracle::occi::ResultSet object, using to store select
     *      result.
     *  @param x The pointer to ei buff.
     *  @return
     */
    int encode_column(int type, unsigned int colIndex,
            oracle::occi::ResultSet* rset, ei_x_buff* x);

    /** @brief Encode select result in the ei_x_buff.
     *  @param result The ei buff where the select result will be encoded.
     *  @param rset Oracle::occi::ResultSet object, using to store select
     *      result.
     *  @return None.
     */
    void encode_select_result(ei_x_buff * const result,
            oracle::occi::ResultSet * const rset);

    /** @brief Decode param from ei buff and set it in the sql statement.
     *  @param stmt -> oracle::occi::Statement object,is the sql statement.
     *        index -> param index in ei buffer.
     *        p_index -> a pointer pointing to ei buffer index,
     *            if NULL it will be set to point to index_.
     *  @param
     *  @return None.
     */
    void decode_and_set_param(oracle::occi::Statement* stmt,
            unsigned int index, int* p_index = NULL);

    /** @brief Get data type from ei buffer.
     *  @param p_index A pointer pointing to ei buffer index,
     *      if NULL it will be set to point to index_.
     *  @return Data type
     */
    int get_data_type(int* p_index = NULL);

    /** @brief Execute query sql statement.
     *  @param stmt Sql statement.
     *  @param rset Result set.
     *  @param res The ei buff.
     *  @return None.
     */
    inline void query(oracle::occi::Statement* stmt,
            oracle::occi::ResultSet* rset, ei_x_buff * const res) {
        rset = stmt->executeQuery();

        encode_select_result(res, rset);
    }

    /** @brief get sql string type.
     *  @param sql Sql string.
     *  @return Is select sql string.
     */
    inline bool is_query(const char* sql) {
        while (*sql == ' ' ||
               *sql == '\t' ||
               *sql == '\n' ||
               *sql == '\r') {
            sql++;
        }
        return strncasecmp(sql, "SELECT", 6) == 0;
    }

    /** @brief Encode some type's data in the ei buffer from result set.
     *  @param colIndex Column index.
     *  @param rset Result set, a oracle::occi::ResultSet object.
     *  @param x The ei buffer.
     *  @return None.
     */
    inline void encode_string(unsigned int colIndex,
            oracle::occi::ResultSet* rset, ei_x_buff * const x) {
        string str = rset->getString(colIndex);
        if(0 == str.compare("NULL")){
            ei_x_encode_atom(x, "undefined");
		}else{
            ei_x_encode_string(x, str.c_str());
		}
    }

    inline void encode_binary(unsigned int colIndex,
            oracle::occi::ResultSet* rset, ei_x_buff * const x) {
        oracle::occi::Bytes bytes = rset->getBytes(colIndex);
        unsigned int length = bytes.length();
        unsigned char* buff = new unsigned char[length];

        bytes.getBytes(buff, length);

        ei_x_encode_binary(x, buff, length);

        delete[] buff;
    }

    inline void encode_double(unsigned int colIndex,
            oracle::occi::ResultSet* rset, ei_x_buff * const x) {
        double data = rset->getDouble(colIndex);

        ei_x_encode_double(x, data);
    }

    inline void encode_float(unsigned int colIndex,
            oracle::occi::ResultSet* rset, ei_x_buff * const x) {
        float data = rset->getFloat(colIndex);

        ei_x_encode_double(x, (double) data);
    }

    inline void encode_chr(unsigned int colIndex,
            oracle::occi::ResultSet* rset, ei_x_buff * const x) {
        encode_string(colIndex, rset, x);
    }

    inline void encode_num(unsigned int colIndex,
            oracle::occi::ResultSet* rset, ei_x_buff * const x) {
        double data = rset->getDouble(colIndex);
        long long intData = (long long) data;

        if (intData == data) {
            ei_x_encode_longlong(x, intData);
        } else {
            ei_x_encode_double(x, data);
        }
    }

    inline void encode_lng( unsigned int colIndex,
            oracle::occi::ResultSet* rset, ei_x_buff * const x) {
        encode_string(colIndex, rset, x);
    }

    inline void encode_dat(unsigned int colIndex,
            oracle::occi::ResultSet* rset, ei_x_buff * const x) {
        oracle::occi::Date date =
                rset->getDate(colIndex);
        int year;
        unsigned int month, day, hour, min, seconds;

        date.getDate(year, month, day, hour, min, seconds);

        ei_x_encode_tuple_header(x, 2);
        ei_x_encode_atom(x, "datetime");
        ei_x_encode_tuple_header(x, 2);

        ei_x_encode_tuple_header(x, 3);
        ei_x_encode_long(x, year);
        ei_x_encode_ulong(x, month);
        ei_x_encode_ulong(x, day);

        ei_x_encode_tuple_header(x, 3);
        ei_x_encode_ulong(x, hour);
        ei_x_encode_ulong(x, min);
        ei_x_encode_ulong(x, seconds);
    }

    inline void encode_ibfloat(unsigned int colIndex,
            oracle::occi::ResultSet* rset, ei_x_buff * const x) {
        encode_float(colIndex, rset, x);
    }

    inline void encode_ibdouble(unsigned int colIndex,
            oracle::occi::ResultSet* rset, ei_x_buff * const x) {
        encode_double(colIndex, rset, x);
    }

    inline void encode_timestamp(unsigned int colIndex,
            oracle::occi::ResultSet* rset, ei_x_buff * const x) {
        oracle::occi::Timestamp timestamp =
                rset->getTimestamp(colIndex);
        int year, tzhour, tzmin;
        unsigned int month, day, hour, min, seconds, fs;

        timestamp.getDate(year, month, day);
        timestamp.getTime(hour, min, seconds, fs);
        timestamp.getTimeZoneOffset(tzhour, tzmin);

        ei_x_encode_tuple_header(x, 2);
        ei_x_encode_atom(x, "timestamp");
        ei_x_encode_tuple_header(x, 3);

        ei_x_encode_tuple_header(x, 3);
        ei_x_encode_long(x, year);
        ei_x_encode_ulong(x, month);
        ei_x_encode_ulong(x, day);

        ei_x_encode_tuple_header(x, 4);
        ei_x_encode_ulong(x, hour);
        ei_x_encode_ulong(x, min);
        ei_x_encode_ulong(x, seconds);
        ei_x_encode_ulong(x, fs);

        ei_x_encode_tuple_header(x, 2);
        ei_x_encode_long(x, tzhour);
        ei_x_encode_long(x, tzmin);
    }

    inline void encode_timestamp_tz(unsigned int colIndex,
            oracle::occi::ResultSet* rset, ei_x_buff * const x) {
        encode_timestamp(colIndex, rset, x);
    }

    inline void encode_timestamp_ltz(unsigned int colIndex,
            oracle::occi::ResultSet* rset, ei_x_buff * const x) {
        encode_timestamp(colIndex, rset, x);
    }

    inline void encode_interval_ym( unsigned int colIndex,
            oracle::occi::ResultSet* rset, ei_x_buff * const x) {
        oracle::occi::IntervalYM interval =
                rset->getIntervalYM(colIndex);
        int year, month;

        year = interval.getYear();
        month = interval.getMonth();

        ei_x_encode_tuple_header(x, 2);
        ei_x_encode_atom(x, "interval_ym");
        ei_x_encode_tuple_header(x, 2);
        ei_x_encode_ulong(x, year);
        ei_x_encode_ulong(x, month);
    }

    inline void encode_interval_ds(unsigned int colIndex,
            oracle::occi::ResultSet* rset, ei_x_buff * const x) {
        oracle::occi::IntervalDS interval =
                rset->getIntervalDS(colIndex);
        int day, hour, min, seconds, fs;

        day = interval.getDay();
        hour = interval.getHour();
        min = interval.getMinute();
        seconds = interval.getSecond();
        fs = interval.getFracSec();

        ei_x_encode_tuple_header(x, 2);
        ei_x_encode_atom(x, "interval_ds");
        ei_x_encode_tuple_header(x, 5);
        ei_x_encode_long(x, day);
        ei_x_encode_long(x, hour);
        ei_x_encode_long(x, min);
        ei_x_encode_long(x, seconds);
        ei_x_encode_long(x, fs);
    }

    inline void encode_bin(unsigned int colIndex,
            oracle::occi::ResultSet* rset,ei_x_buff * const x) {
        encode_binary(colIndex, rset, x);
    }

    inline void encode_lbi(unsigned int colIndex,
            oracle::occi::ResultSet* rset, ei_x_buff * const x) {
        encode_binary(colIndex, rset, x);
    }

    inline void encode_rdd(unsigned int colIndex,
            oracle::occi::ResultSet* rset, ei_x_buff * const x) {
        encode_binary(colIndex, rset, x);
    }

    inline void encode_afc(unsigned int colIndex,
            oracle::occi::ResultSet* rset, ei_x_buff * const x) {
        encode_string(colIndex, rset, x);
    }

    inline unsigned int read_clob(oracle::occi::Clob& clob,
            unsigned char* buff, unsigned int buffLength) {
        unsigned int len = 0;

        try {
            clob.open(oracle::occi::OCCI_LOB_READONLY);
            len = clob.read(buffLength, buff, buffLength);
            clob.close();
        } catch (oracle::occi::SQLException ex) {
            if (ex.getErrorCode() == 24806) {
                clob.setCharSetForm(oracle::occi::OCCI_SQLCS_NCHAR);
                len = clob.read(buffLength, buff, buffLength);
                clob.close();
            } else {
                throw ex;
            }
        }
        return len;
    }

    inline void encode_clob(unsigned int colIndex,
            oracle::occi::ResultSet* rset, ei_x_buff * const x) {
        oracle::occi::Clob clob = rset->getClob(colIndex);
        unsigned int max_len = clob.length() * 4;
        unsigned char* buff = new unsigned char[max_len + 1];
        unsigned int len = read_clob(clob, buff, max_len);

        buff[len] = '\0';

        ei_x_encode_string(x, (const char*) buff);

        delete[] buff;
    }

    inline void encode_blob(unsigned int colIndex,
            oracle::occi::ResultSet* rset, ei_x_buff * const x) {
        oracle::occi::Blob blob = rset->getBlob(colIndex);

        if (blob.isNull()) {
            ei_x_encode_atom(x, "null");
        } else {
            unsigned int max_len = blob.length();
            unsigned char* buff = new unsigned char[max_len];

            blob.open(oracle::occi::OCCI_LOB_READONLY);

            unsigned int len = blob.read(max_len, buff, max_len);

            blob.close();

            ei_x_encode_binary(x, buff, len);

            delete[] buff;
        }
    }

    inline void encode_bfile(unsigned int colIndex,
            oracle::occi::ResultSet* rset, ei_x_buff * const x) {
        oracle::occi::Bfile bfile = rset->getBfile(colIndex);
        string name = bfile.getFileName();
        string dir = bfile.getDirAlias();

        ei_x_encode_tuple_header(x, 2);
        ei_x_encode_atom(x, "bfile");
        ei_x_encode_tuple_header(x, 2);
        ei_x_encode_string(x, name.c_str());
        ei_x_encode_string(x, dir.c_str());
    }

    /** decode tuple header*/
    inline int decode_tuple_header(int* p_index = NULL){
        if (p_index == NULL) {
            p_index = &index_;
        }

        if (ei_decode_tuple_header(buf_, p_index, &size_) == 0) {
            return size_;
        } else {
            DBException ex(BAD_ARG_ERROR);
            throw ex;
        }
    }

    /** decode list header*/
    inline int decode_list_header(int* p_index = NULL) {
        if (p_index == NULL) {
            p_index = &index_;
        }

        if (ei_decode_list_header(buf_, p_index, &size_) == 0) {
            return size_;
        } else {
            DBException ex(BAD_ARG_ERROR);
            throw ex;
        }
    }

    /** decode empty list*/
    inline void decode_empty_list(int* p_index = NULL) {
        if (p_index == NULL) {
            p_index = &index_;
        }

        if (ei_decode_list_header(buf_, p_index, &size_) == -1) {
            DBException ex(BAD_ARG_ERROR);
            throw ex;
        }
    }

    /** skip a term*/
    inline void skip_term(int* p_index = NULL) {
        if (p_index == NULL) {
            p_index = &index_;
        }

        if (ei_skip_term(buf_, p_index) == -1) {
            DBException ex(BAD_ARG_ERROR);
            throw ex;
        }
    }

    /**decode string,if failed throw a DBException*/
    inline void decode_string_with_throw(char*& str,
            int* p_index = NULL) {
        if (p_index == NULL) {
            p_index = &index_;
        }

        if (!decode_string(str, p_index)) {
            DBException ex(BAD_ARG_ERROR);
            throw ex;
        }
    }

     /** Function: decode_and_set_<data type>
     *  @brief decode and set some type's data
     *  @param stmt -> oracle::occi::Statement object,is the sql statement.
     *        index -> param index in ei buffer.
     *        p_index -> a pointer pointing to ei buffer index,
     *            if NULL it will be set to point to index_.
     *  @param
     *  @return None.
     */
    inline void decode_and_set_empty(oracle::occi::Statement* stmt,
            unsigned int index, int* p_index = NULL) {
        if (p_index == NULL) {
            p_index = &index_;
        }
        skip_term(p_index);
        stmt->setString(index, "");
    }

    /** Function: decode_and_set_<data type>
     *  @brief decode and set some type's data
     *  @param stmt -> oracle::occi::Statement object,is the sql statement.
     *        index -> param index in ei buffer.
     *        p_index -> a pointer pointing to ei buffer index,
     *            if NULL it will be set to point to index_.
     *  @param
     *  @return None.
     */
    inline void decode_and_set_string(oracle::occi::Statement* stmt,
            unsigned int index, int* p_index = NULL) {
        if (p_index == NULL) {
            p_index = &index_;
        }

        char* str = NULL;

        decode_string_with_throw(str, p_index);
        stmt->setString(index, str);
        free_string(str);
    }

    inline void decode_and_set_bytes(oracle::occi::Statement* stmt,
            unsigned int index, int* p_index = NULL) {
        if (p_index == NULL) {
            p_index = &index_;
        }

        char* tmp = NULL;
        long len = decode_binary(tmp, p_index);

        if (len != -1) {
            oracle::occi::Bytes bytes((unsigned char*) tmp, len);
            stmt->setBytes(index, bytes);
            free_binary(tmp);
        } else {
            DBException ex(BAD_ARG_ERROR);
            throw ex;
        }
    }

    inline void decode_and_set_number(oracle::occi::Statement* stmt,
            unsigned int index, int* p_index = NULL) {
        if (p_index == NULL) {
            p_index = &index_;
        }

        long integer = -1;
        double decimal;
        int type = get_erl_type(p_index);

        if ((type == ERL_SMALL_INTEGER_EXT ||
            type == ERL_INTEGER_EXT ||
            type ==ERL_SMALL_BIG_EXT ||
            type == ERL_LARGE_BIG_EXT) &&
            decode_integer(integer, p_index)) {
            oracle::occi::Number number(integer);
            stmt->setNumber(index, number);
        } else if (decode_double(decimal, p_index)){
            oracle::occi::Number number(decimal);
            stmt->setNumber(index, number);
        } else {
            DBException ex(BAD_ARG_ERROR);
            throw ex;
        }
    }


    inline void decode_and_set_date(oracle::occi::Statement* stmt,
            unsigned int index, int* p_index = NULL) {
        if (p_index == NULL) {
            p_index = &index_;
        }

        long year, month, day, hour, min, seconds;
        if (decode_tuple_header(p_index) == 2 &&
            decode_tuple_header(p_index) == 3 &&
            decode_integer(year, p_index) &&
            decode_integer(month, p_index) &&
            decode_integer(day, p_index) &&
            decode_tuple_header(p_index) == 3 &&
            decode_integer(hour, p_index) &&
            decode_integer(min, p_index) &&
            decode_integer(seconds, p_index)) {
            oracle::occi::Date date(get_env_instance(), year, month, day, hour,min, seconds);
            stmt->setDate(index, date);
        } else {
            DBException ex(BAD_ARG_ERROR);
            throw ex;
        }
    }

    inline void decode_and_set_timestamp(oracle::occi::Statement* stmt,
            unsigned int index, int* p_index = NULL) {
        if (p_index == NULL) {
            p_index = &index_;
        }

        unsigned long long fs;
        long year, month, day, hour, min, seconds, tzhour, tzmin;
        if (decode_tuple_header(p_index) == 3 &&
            decode_tuple_header(p_index) == 3&&
            decode_integer(year, p_index) &&
            decode_integer(month, p_index) &&
            decode_integer(day, p_index)&&
            decode_tuple_header(p_index) == 4 &&
            decode_integer(hour, p_index) &&
            decode_integer(min, p_index) &&
            decode_integer(seconds, p_index) &&
            decode_integer(fs, p_index)&&
            decode_tuple_header(p_index) == 2 &&
            decode_integer(tzhour, p_index) &&
            decode_integer(tzmin, p_index)) {
            oracle::occi::Timestamp timestamp(get_env_instance(), year, month, day,
                    hour,min, seconds, fs, tzhour, tzmin);
            stmt->setTimestamp(index, timestamp);
        } else {
            DBException ex(BAD_ARG_ERROR);
            throw ex;
        }
    }

    inline void decode_and_set_interval_ym(oracle::occi::Statement* stmt,
            unsigned int index, int* p_index = NULL) {
        if (p_index == NULL) {
            p_index = &index_;
        }

        long year, month;
        if (decode_tuple_header(p_index) == 2 &&
            decode_integer(year, p_index) &&
            decode_integer(month, p_index)) {
            oracle::occi::IntervalYM interval(get_env_instance(), year, month);
            stmt->setIntervalYM(index, interval);
       } else {
            DBException ex(BAD_ARG_ERROR);
            throw ex;
        }
    }

    inline void decode_and_set_interval_ds(oracle::occi::Statement* stmt,
            unsigned int index, int* p_index = NULL) {
        if (p_index == NULL) {
            p_index = &index_;
        }

        long day, hour, min, seconds, fs;
        if (decode_tuple_header(p_index) == 5 &&
            decode_integer(day, p_index) &&
            decode_integer(hour, p_index) &&
            decode_integer(min, p_index) &&
            decode_integer(seconds, p_index) &&
            decode_integer(fs, p_index)) {
            oracle::occi::IntervalDS interval(get_env_instance(), day, hour, min,seconds, fs);
            stmt->setIntervalDS(index, interval);
        } else {
            DBException ex(BAD_ARG_ERROR);
            throw ex;
        }
    }

    inline void decode_and_set_bfile(oracle::occi::Statement* stmt,
            unsigned int index, int* p_index = NULL) {
        if (p_index == NULL) {
            p_index = &index_;
        }

        char* name = NULL;
        char* dir = NULL;

        if (decode_tuple_header(p_index) == 2 &&
            decode_string(name, p_index) &&
            decode_string(dir, p_index)) {
            oracle::occi::Connection* conn =
                (oracle::occi::Connection*) conn_->get_connection();
            oracle::occi::Bfile bfile(conn);
            bfile.setName(dir, name);
            stmt->setBfile(index, bfile);
            free_string(name);
            free_string(dir);
        } else {
            free_string(name);
            free_string(dir);
            DBException ex(BAD_ARG_ERROR);
            throw ex;
        }
    }

    /**get erlang type from ei buffer */
    inline int get_erl_type(int* p_index = NULL) {
        if (p_index == NULL) {
            p_index = &index_;
        }

        if (ei_get_type(buf_, p_index, &type_, &size_) == -1) {
            DBException ex(BAD_ARG_ERROR);
            throw ex;
        }

        return type_;
    }

    /** Function: make_<sql key word>
     *  @brief make sql statement
     *  @param sql -> the stringstream object, using to store sql statement.
     *        param_index -> the type is vector<int>,using to store param
     *            index which is in the ei buff.
     *  @param
     *  @return None.
     */
    inline void make_and(stringstream& sql,
            ParamIndex& param_index) {
        int size = decode_list_header();
        for (int i = 0; i < size; ++i) {
            sql << " (";
            make_expr(sql, param_index);
            sql << ")";
            if (i < size - 1) {
                sql << " AND";
            }
        }
        decode_empty_list();
    }

    inline void make_or(stringstream& sql,
            ParamIndex& param_index) {
        sql << " (";
        make_expr(sql, param_index);
        sql << ") OR(";
        make_expr(sql, param_index);
        sql << ")";
    }

    inline void make_not(stringstream& sql,
            ParamIndex& param_index) {
        sql << " NOT (";
        make_expr(sql, param_index);
        sql << ")";
    }

    inline void make_like(stringstream& sql,
            ParamIndex& param_index) {
        make_expr(sql, param_index);
        sql << " LIKE";
        make_expr(sql, param_index);
    }

    inline void make_as(stringstream& sql,
            ParamIndex& param_index) {
        sql << " AS";
        make_expr(sql, param_index);
    }

    inline void make_equal(stringstream& sql,
            ParamIndex& param_index) {
        make_expr(sql, param_index);
        sql << " =";
        make_expr(sql, param_index);
    }

    inline void make_greater(stringstream& sql,
            ParamIndex& param_index) {
        make_expr(sql, param_index);
        sql << " >";
        make_expr(sql, param_index);
    }

    inline void make_greater_equal(stringstream& sql,
            ParamIndex& param_index) {
        make_expr(sql, param_index);
        sql << " >=";
        make_expr(sql, param_index);
    }

    inline void make_less(stringstream& sql,
            ParamIndex& param_index) {
        make_expr(sql, param_index);
        sql << " <";
        make_expr(sql, param_index);
    }

    inline void make_less_equal(stringstream& sql,
            ParamIndex& param_index) {
        make_expr(sql, param_index);
        sql << " <=";
        make_expr(sql, param_index);
    }

    inline void make_join(stringstream& sql,
            ParamIndex& param_index) {
        make_expr(sql, param_index);
        sql << " JOIN";
        make_expr(sql, param_index);
        sql << " ON";
        make_expr(sql, param_index);
    }

    inline void make_left_join(stringstream& sql,
            ParamIndex& param_index) {
        make_expr(sql, param_index);
        sql << " LEFT JOIN";
        make_expr(sql, param_index);
        sql << " ON";
        make_expr(sql, param_index);
    }

    inline void make_right_join(stringstream& sql,
            ParamIndex& param_index) {
        make_expr(sql, param_index);
        sql << " RIGHT JOIN";
        make_expr(sql, param_index);
        sql << " ON";
        make_expr(sql, param_index);
    }

    inline void make_not_equal(stringstream& sql,
            ParamIndex& param_index) {
        make_expr(sql, param_index);
        sql << " !=";
        make_expr(sql, param_index);
    }

    inline void make_order(stringstream& sql,
            ParamIndex& param_index) {
        sql << " ORDER BY";
        int size = decode_list_header();
        for (int i = 0; i < size; ++i) {
            decode_tuple_header();
            make_expr(sql, param_index);
            long flag;
            if (decode_integer(flag)) {
                sql << (flag == 0 ? "ASC" : "DESC");
                if (i < size - 1) {
                    sql << " ,";
                 }
            } else {
                DBException ex(BAD_ARG_ERROR);
                throw ex;
            }
        }
        decode_empty_list();
    }

    inline void make_limit(stringstream& sql,
            ParamIndex& param_index) {
        long offset, len;
        if (decode_integer(offset)
            && decode_integer(len)) {
            string tmp = sql.str();
            sql.str("");
            sql << "SELECT * FROM ("
                << tmp << ") WHERE ROWNUM > "
                << offset
                << " AND ROWNUM <= "
                << offset + len;
        } else {
            DBException ex(BAD_ARG_ERROR);
            throw ex;
        }
    }

    inline void make_dot(stringstream& sql,
            ParamIndex& param_index) {
        make_expr(sql, param_index);
        sql << ".";
        make_expr(sql, param_index);
    }

    inline void make_group(stringstream& sql,
            ParamIndex& param_index) {
        int size = decode_list_header();
        sql << " GROUP BY";
        for (int i = 0; i < size; ++i) {
            make_expr(sql, param_index);
            if (i < size - 1) {
                sql << " ,";
            }
        }
        decode_empty_list();
    }

    inline void make_having(stringstream& sql,
            ParamIndex& param_index) {
        sql << " HAVING";
        make_expr(sql, param_index);
    }

    inline void make_between(stringstream& sql,
            ParamIndex& param_index) {
        make_expr(sql, param_index);
        sql << " BETWEEN";
        make_expr(sql, param_index);
        sql << " AND";
        make_expr(sql, param_index);
    }

    inline void make_add(stringstream& sql,
            ParamIndex& param_index) {
        sql << " (";
        make_expr(sql, param_index);
        sql << " +";
        make_expr(sql, param_index);
    }

    inline void make_sub(stringstream& sql,
            ParamIndex& param_index) {
        sql << " (";
        make_expr(sql, param_index);
        sql << " -";
        make_expr(sql, param_index);
    }

    inline void make_mul(stringstream& sql,
            ParamIndex& param_index) {
        sql << " (";
        make_expr(sql, param_index);
        sql << " *";
        make_expr(sql, param_index);
    }

    inline void make_div(stringstream& sql,
            ParamIndex& param_index) {
        sql << " (";
        make_expr(sql, param_index);
        sql << " /";
        make_expr(sql, param_index);
    }

    inline void make_fun(stringstream& sql,
            ParamIndex& param_index) {
        make_expr(sql, param_index);
        int type = get_erl_type();
        if (type == ERL_LIST_EXT) {
            int size = decode_list_header();
            sql << " (";
            for (int i = 0; i < size; ++i) {
                make_expr(sql, param_index);
                if (i < size - 1) {
                    sql << " ,";
                }
            }
            sql << " )";
            decode_empty_list();
        } else  {
            char* str = NULL;
            if (decode_string(str)) {
                sql << " (";
                for (unsigned int i = 0; i < strlen(str); ++i) {
                    sql << " " << (int) str[i];
                    if (i < strlen(str) - 1) {
                        sql << " ,";
                    }
                }
                sql << " )";
                free_string(str);
            } else {
                DBException ex(BAD_ARG_ERROR);
                throw ex;
            }
        }
    }

    inline void make_inner_join(stringstream& sql,
            ParamIndex& param_index) {
        make_expr(sql, param_index);
        sql << " INNER JOIN";
        make_expr(sql, param_index);
        sql << " ON";
        make_expr(sql, param_index);
    }

    inline void make_is_null(stringstream& sql,
            ParamIndex& param_index) {
        make_expr(sql, param_index);
        sql << " IS NULL";
    }

    inline void make_is_not_null(stringstream& sql,
            ParamIndex& param_index) {
        make_expr(sql, param_index);
        sql << " IS NOT NULL";
    }
};
}/* end of namespace rytong */
#endif /* _RYT_ORACLE_DB_OPERATION_H */
