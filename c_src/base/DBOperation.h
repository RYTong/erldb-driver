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
 * $Id: DBOperation.h 22235 2010-02-25 09:13:30Z deng.lifen $
 *
 *  @file DBOperation.h
 *  @brief Base class declaration of DBOperation.
 *  @author cao.xu <cao.xu@rytong.com>
 *  @version 1.0.0
 *  @date Created on 2010-1-30
 */

#ifndef _RYT_DB_OPERATION_H
#define _RYT_DB_OPERATION_H

#include <queue>
#include <ei.h>
#include <sstream>
#include "Connection.h"
#include "ConnectionPool.h"
#include "StmtMap.h"
#include "DrvConf.h"

using namespace std;

//----------------------------------------------------------------------
// Constants
//----------------------------------------------------------------------
namespace rytong {
/** @enum SqlKeyword
 *  @brief Sql keyword type.
 */
enum SqlKeyword {
    DB_DRV_SQL_AND = 0,
    DB_DRV_SQL_OR = 1,
    DB_DRV_SQL_NOT = 2,
    DB_DRV_SQL_LIKE = 3,
    DB_DRV_SQL_AS = 4,
    DB_DRV_SQL_EQUAL = 5,
    DB_DRV_SQL_GREATER = 6,
    DB_DRV_SQL_GREATER_EQUAL = 7,
    DB_DRV_SQL_LESS = 8,
    DB_DRV_SQL_LESS_EQUAL = 9,
    DB_DRV_SQL_JOIN = 10,
    DB_DRV_SQL_LEFT_JOIN = 11,
    DB_DRV_SQL_RIGHT_JOIN = 12,
    DB_DRV_SQL_NOT_EQUAL = 13,
    DB_DRV_SQL_ORDER = 14,
    DB_DRV_SQL_LIMIT = 15,
    DB_DRV_SQL_DOT = 16,
    DB_DRV_SQL_GROUP = 17,
    DB_DRV_SQL_HAVING = 18,
    DB_DRV_SQL_BETWEEN = 19,
    DB_DRV_SQL_ADD = 20,
    DB_DRV_SQL_SUB = 21,
    DB_DRV_SQL_MUL = 22,
    DB_DRV_SQL_DIV = 23,
    DB_DRV_SQL_FUN = 24,
    DB_DRV_SQL_INNER_JOIN = 25,
    DB_DRV_SQL_IN = 26,
    DB_DRV_SQL_IS_NULL = 27,
    DB_DRV_SQL_IS_NOT_NULL = 28,
    DB_DRV_SQL_DATETIME = 29,
    DB_DRV_SQL_DATE = 30,
    DB_DRV_SQL_TIME = 31,
	DB_DRV_SQL_TIMESTAMP = 32,
	DB_DRV_SQL_INTERVAL_YM = 33,
	DB_DRV_SQL_INTERVAL_DS = 34,
	DB_DRV_SQL_BFILE = 35
};

/** @brief Field value struct.
 */
typedef struct {
    char erl_type;          ///< Data type.
    unsigned long length;   ///< Data length.
    void* value;            ///< Data value.
} FieldValue;

/** @brief Field struct.
 */
typedef struct {
    char* field_name;       ///< Field name.
    FieldValue field_value; ///< Field value.
} FieldStruct;

/** @brief Abstract base class to represent operations of database.
 *
 *  Give the declaration of the operation interfaces here.
 */
class DBOperation {
public:
    /** @brief Constructor for the class.
     *  @return None.
     */
    DBOperation();

    /** @brief Destructor for the class.
     *  @return None.
     */
    virtual ~DBOperation() = 0;

    /** @brief Create database object.
     *  @param[in] db_type Database type.
     *  @return the pointer to the DBOperation.
     */
    static DBOperation* create(DatabaseType db_type);

    /** @brief Create database object.
     *  @param[in] db The pointer to the DBOperation.
     *  @return None.
     */
    static void destroy(DBOperation* db);

    /** @brief Connection pool init interface.
     *  @param[in] conf The configure struct.
     *  @param[in] conn_pool The pointer to ConnectionPool.
     *  @return If success.
     *  @retval int Successful, return connection pool size.
     *  @retval -1 Failed.
     */
    static int init(DrvConf* conf, ConnectionPool* conn_pool);

    /** @brief Create a connection to the database.
     *  @param[in] conf The configure struct.
     *  @return the pointer to the connection.
     */
    static Connection* create_conn(DrvConf* conf);

    /** @brief Release a connection.
     *  @param[in] conn The pointer to the connection.
     *  @return None.
     */
    static void release_conn(Connection* conn);

    /** @brief conn_ setter.
     *  @param[in] conn The pointer to the connection.
     *  @return None.
     */
    inline void set_conn(Connection* conn) {
        conn_ = conn;
        muli_tran_flag_ = false;
    }

    /** @brief stmt_map_ setter.
     *  @param[in] stmt_map The pointer to the connection.
     *  @return None.
     */
    inline void set_stmt_map(StmtMap* stmt_map) {
        stmt_map_ = stmt_map;
    }

    /** @brief buf_ setter, set buf_, index_, version_.
     *  @param[in] buf The buf_ to set.
     *  @param[in] index The index_ to set.
     *  @param[in] version The version_ to set.
     *  @return None.
     */
    inline void set_buf(char * buf, int index, int version) {
        buf_ = buf;
        index_ = index;
        version_ = version;
    }

    /** @brief Decode conn_ and set conn_.
     *  @return The pointer to Connection object.
     */
    inline Connection* decode_set_conn_tuple() {
        ei_decode_tuple_header(buf_, &index_, &type_);
        if (!decode_set_conn()){
            return NULL;
        }
        muli_tran_flag_ = true;
        return conn_;
    }

    /** @brief Execute all sql command.
     *  @param[out] res Result encapsulated in erlang format.
     *  @return If successful.
     *  @retval true Success.
     *  @retval false Failed.
     */
    virtual bool exec(ei_x_buff * const res) = 0;

    /** @brief Insert interface.
     *  @param[out] res Result encapsulated in erlang format.
     *  @return If successful.
     *  @retval true Success.
     *  @retval false Failed.
     */
    virtual bool insert(ei_x_buff * const res) = 0;

    /** @brief Update interface.
     *  @param[out] res Result encapsulated in erlang format.
     *  @return If successful.
     *  @retval true Success.
     *  @retval false Failed.
     */
    virtual bool update(ei_x_buff * const res) = 0;

    /** @brief Delete interface.
     *  @param[out] res Result encapsulated in erlang format.
     *  @return If successful.
     *  @retval true Success.
     *  @retval false Failed.
     */
    virtual bool del(ei_x_buff * const res) = 0;

    /** @brief Select interface.
     *  @param[out] res Result encapsulated in erlang format.
     *  @return If successful.
     *  @retval true Success.
     *  @retval false Failed.
     */
    virtual bool select(ei_x_buff * const res) = 0;

    /** @brief Transaction begin interface.
     *  @param[out] res Result encapsulated in erlang format.
     *  @return If successful.
     *  @retval true Success.
     *  @retval false Failed.
     */
    virtual bool trans_begin(ei_x_buff * const res) = 0;

    /** @brief Transaction commit interface.
     *  @param[out] res Result encapsulated in erlang format.
     *  @return If successful.
     *  @retval true Success.
     *  @retval false Failed.
     */
    virtual bool trans_commit(ei_x_buff * const res) = 0;

    /** @brief Transaction rollback interface.
     *  @param[out] res Result encapsulated in erlang format.
     *  @return If successful.
     *  @retval true Success.
     *  @retval false Failed.
     */
    virtual bool trans_rollback(ei_x_buff * const res) = 0;

    /** @brief Perpare statement init interface.
     *  @param[out] res Result encapsulated in erlang format.
     *  @return If successful.
     *  @retval true Success.
     *  @retval false Failed.
     */
    virtual bool prepare_stat_init(ei_x_buff * const res) = 0;
    virtual bool prepare_statement_init(ei_x_buff * const res) = 0;

    /** @brief Perpare statement execute interface.
     *  @param[out] res Result encapsulated in erlang format.
     *  @return If successful.
     *  @retval true Success.
     *  @retval false Failed.
     */
    virtual bool prepare_stat_exec(ei_x_buff * const res) = 0;
    virtual bool prepare_statement_exec(ei_x_buff * const res) = 0;

    /** @brief Perpare statement release interface.
     *  @param[out] res Result encapsulated in erlang format.
     *  @return If successful.
     *  @retval true Success.
     *  @retval false Failed.
     */
    virtual bool prepare_stat_release(ei_x_buff * const res) = 0;
    virtual bool prepare_statement_release(ei_x_buff * const res) = 0;

    /** @brief Decode the expression in fields and where clause.
     *  @param[out] sm Result encapsulated in string.
     */
    virtual void decode_expr_tuple(stringstream & sm) = 0;

protected:
    /** @brief Decode string or atom.
     *  @param[out] str Result string to decode.
     *  @param[in] p_index Decode index.
     *  @return If successful.
     *  @retval true Success.
     *  @retval false Failed.
     */
    inline bool decode_string(char* &str, int* p_index = NULL) {
        if (p_index == NULL) {
            p_index = &index_;
        }

        int flag = -1;
        str = NULL;
        if (ei_get_type(buf_, p_index, &type_, &size_) == 0) {
            str = new char[size_ + 1];
            switch (type_) {
                case ERL_NIL_EXT:
                    str[0] = 0;
                    flag = ei_decode_list_header(buf_, p_index, &size_);
                    break;
                case ERL_ATOM_EXT:
                    flag = ei_decode_atom(buf_, p_index, str);
                    break;
                case ERL_STRING_EXT:
                case ERL_LIST_EXT:
                    flag = ei_decode_string(buf_, p_index, str);
                    break;
                default:
                    delete[] str;
                    str = NULL;
            }
        }
        return flag == 0;
    }

    /** @brief Free string or atom.
     *  @param[out] str Result string to decode.
     *  @return None.
     */
    inline void free_string(char* &str) {
        delete [] str;
        str = NULL;
    }

    /** @brief Decode binary.
     *  @param[out] bin Result binary to decode.
     *  @param[in] p_index Decode index.
     *  @return binary_length if decode success else -1.
     */
    inline int decode_binary(char* &bin, int* p_index = NULL) {
        if (p_index == NULL) {
            p_index = &index_;
        }

        int flag = -1;

        if (ei_get_type(buf_, p_index, &type_, &size_) == 0) {
            bin = new char[size_];

            bin_size_ = size_;
            if ((flag = ei_decode_binary(buf_, p_index, bin, &bin_size_))==-1) {
                delete[] bin;
                bin = NULL;
            }
        }

        return flag == -1 ? -1:size_;
    }

    /** @brief Free binary.
     *  @param[out] bin Result binary to decode.
     *  @return None.
     */
    inline void free_binary(char* &bin) {
        delete [] bin;
        bin = NULL;
    }

    /** @brief Decode and set connection.
     *  @return If successful.
     *  @retval true Success.
     *  @retval false Failed.
     */
    inline bool decode_set_conn() {
        if (ei_get_type(buf_, &index_, &type_, &size_) == -1) {
            return false;
        }
        bin_size_ = size_;
        if (type_ != ERL_BINARY_EXT ||
                ei_decode_binary(buf_, &index_, &conn_, &bin_size_) == -1) {
            return false;
        }
        return true;
    }


    /** @brief Decode integer.
     *  @return The int value to decode.
     */
    inline int decode_int() {
        return (int) decode_long();
    }

    /** @brief Decode long.
     *  @return The long value to decode.
     */
    inline int decode_long() {
        long temp;
        ei_decode_long(buf_, &index_, &temp);
        return temp;
    }

    /** @brief Decode float.
     *  @return The float value to decode.
     */
    inline float decode_float() {
        return (float) decode_double();
    }

    /** @brief Decode double.
     *  @return The float value to decode.
     */
    inline double decode_double() {
        double temp;
        ei_decode_double(buf_, &index_, &temp);
        return temp;
    }

    /** @brief Decode double type value from buf_.
     *  @param[out] value Double value.
     *  @param[in] p_index Decode index.
     *  @return If successful.
     *  @retval true Success.
     *  @retval false Failed.
     */
    inline bool decode_double(double& value, int* p_index = NULL) {
        if (p_index == NULL) {
            p_index = &index_;
        }

        return ei_decode_double(buf_, p_index, &value) == 0;
    }

    /** @brief Decode integer value from buf_.
     *  @param[out] value Integer value, long type.
     *  @param[in] p_index Decode index.
     *  @return If successful.
     *  @retval true Success.
     *  @retval false Failed.
     */
    inline bool decode_integer(long& value, int* p_index = NULL) {
        if (p_index == NULL) {
            p_index = &index_;
        }
        return ei_decode_long(buf_, p_index, &value) == 0;
    }

    /** @brief Decode integer value from buf_.
     *  @param[out] value Integer value, unsigned long type.
     *  @param[in] p_index Decode index.
     *  @return If successful.
     *  @retval true Success.
     *  @retval false Failed.
     */
    inline bool decode_integer(unsigned long& value, int* p_index = NULL) {
        if (p_index == NULL) {
            p_index = &index_;
        }

        return ei_decode_ulong(buf_, p_index, &value) == 0;
    }

    /** @brief Decode integer value from buf_.
     *  @param[out] value Integer value, long long type.
     *  @param[in] p_index Decode index.
     *  @return If successful.
     *  @retval true Success.
     *  @retval false Failed.
     */
    inline bool decode_integer(long long& value, int* p_index = NULL) {
        if (p_index == NULL) {
            p_index = &index_;
        }

        return ei_decode_longlong(buf_, p_index, &value) == 0;
    }

    /** @brief Decode integer value from buf_.
     *  @param[out] value Integer value, unsigned long long type.
     *  @param[in] p_index Decode index.
     *  @return If successful.
     *  @retval true Success.
     *  @retval false Failed.
     */
    inline bool decode_integer(unsigned long long& value, int* p_index = NULL) {
        if (p_index == NULL) {
            p_index = &index_;
        }

        return ei_decode_ulonglong(buf_, p_index, &value) == 0;
    }

    /** @brief According to the type of field to decode the field.
     *  @param[out] field_tuple Field struct, contains the field type and value.
     *  @return None.
     */
    inline void decode_field_value(FieldValue & field_tuple) {
        stringstream tuple_sm;
        string tuple_str;
        int tuple_len;

        ei_get_type(buf_, &index_, &type_, &size_);
        field_tuple.erl_type = type_;

        switch (field_tuple.erl_type) {
            case ERL_BINARY_EXT: // binary
                field_tuple.value = (void*) (new char[size_]);
                bin_size_ = size_;
                field_tuple.length = size_;
                ei_decode_binary(buf_, &index_, (char*) field_tuple.value,
                    &bin_size_);
                break;
            case ERL_SMALL_INTEGER_EXT:
            case ERL_INTEGER_EXT: // integer
            case ERL_SMALL_BIG_EXT:
            case ERL_LARGE_BIG_EXT:
                field_tuple.value = (void*) (new long());
                field_tuple.length = sizeof(long);
                ei_decode_long(buf_, &index_, (long*) field_tuple.value);
                break;
            case ERL_FLOAT_EXT: // float
                field_tuple.value = (void*) (new double());
                field_tuple.length = sizeof(double);
                ei_decode_double(buf_, &index_, (double*) field_tuple.value);
                break;
            case ERL_ATOM_EXT: // atom
                field_tuple.value = (void*) (new char[size_ + 1]);
                field_tuple.length = size_;
                ei_decode_atom(buf_, &index_, (char*) field_tuple.value);
                break;
            case ERL_NIL_EXT:
            case ERL_STRING_EXT:
            case ERL_LIST_EXT: // list
                field_tuple.value = (void*) (new char[size_ + 1]);
                field_tuple.length = size_;
                ei_decode_string(buf_, &index_, (char*) field_tuple.value);
                break;
            case ERL_SMALL_TUPLE_EXT:
            case ERL_LARGE_TUPLE_EXT:
                decode_expr_tuple(tuple_sm);
                tuple_str = tuple_sm.str();
                tuple_len = tuple_str.length();

                field_tuple.value = (void*) (new char[tuple_len + 1]);
                field_tuple.length = tuple_len + 1;
                memset(field_tuple.value, 0, tuple_len + 1);
                memcpy(field_tuple.value, tuple_str.c_str(), tuple_len);
                break;
            default:
                field_tuple.value = NULL;
                break;
        }
    }

    /** @brief Decode date and time tuple.
     *  @param[out] sm String stream, to trans date or time.
     *  @param[in] c Connection date or time of the characters, if type is date,
     *      use '-', if type is time, use ':'.
     *  @return None.
     */
    inline void decode_date_tuple(stringstream& sm, char c) {
        sm << decode_int();
        sm << c << decode_int();
        sm << c << decode_int();
    }

    inline int decode_tuple_length(int* p_index = NULL){
        if (p_index == NULL) {
            p_index = &index_;
        }

        if (ei_decode_tuple_header(buf_, p_index, &size_) == 0) {
            return size_;
        } else {
            return -1;
        }
    }

    inline int decode_list_length(int* p_index = NULL){
        if (p_index == NULL) {
            p_index = &index_;
        }

        if (ei_decode_list_header(buf_, p_index, &size_) == 0) {
            return size_;
        } else {
            return -1;
        }
    }

    inline int erl_data_type(int* p_index = NULL) {
        if(p_index == NULL) {
            p_index = &index_;
        }

        return ei_get_type(buf_, p_index, &type_, &size_)
                == 0 ? type_:-1;
    }

    inline bool decode_null() {
        int index = index_;
        char type[10];

        if (ei_get_type(buf_, &index, &type_, &size_) == 0 &&
                size_ == 4 && ei_decode_atom(buf_, &index, type) == 0 &&
                strcmp(type, "NULL") == 0) {
            index_ = index;
            return true;
        }

        return false;
    }

    /** @brief Free the memory of the function decode_field_value application.
     *  @param[out] field_tuple Field struct, contains the field type and value.
     *  @return None.
     */
    inline void free_field_tuple(FieldValue & field_tuple) {
        if (field_tuple.value) {
            switch (field_tuple.erl_type) {
                case ERL_BINARY_EXT:
                case ERL_STRING_EXT:
                case ERL_LIST_EXT:
                case ERL_ATOM_EXT:
                    delete[] (char*) field_tuple.value;
                    break;
                case ERL_SMALL_INTEGER_EXT:
                case ERL_INTEGER_EXT:
                case ERL_SMALL_BIG_EXT:
                case ERL_LARGE_BIG_EXT:
                    delete (long*) field_tuple.value;
                    break;
                case ERL_FLOAT_EXT:
                    delete (double*) field_tuple.value;
                    break;
                default:
                    break;
            }
        }
    }

    /** @brief Decode the statement fields value, contains the field type and
     *      value.
     *  @param[out] fields List of field struct, contains the field type and
     *      value.
     *  @return Field's count.
     */
    int decode_stmt_fields(FieldValue * & fields);

    /** @brief Free the memory of the function decode_stmt_fields application.
     *  @param[out] fields List of field struct, contains the field type and
     *      value.
     *  @param[in] len Field's count.
     *  @return None.
     */
    void free_stmt_fields(FieldValue * & fields, int len);

    /** @brief Decode fields struct, contains the field name, field type and
     *      value.
     *  @param[out] field_list List of field struct, contains the field name,
     *      field type and value
     *  @return Field's count.
     */
    int decode_fields(FieldStruct * & field_list);

    /** @brief Free the memory of the function decode_fields application.
     *  @param[out] field_list List of field struct, contains the field name,
     *      field type and value.
     *  @param[in] len Field's count.
     *  @return None.
     */
    void free_fields(FieldStruct * & field_list, int len);

    Connection *conn_; ///< The point to Connection obj, use to connect database.
    bool muli_tran_flag_; ///< The flag of multiple steps transaction;
    StmtMap *stmt_map_; ///< The pointer to StmtMap.
    char * buf_; ///< The parameters of erlang pass over.
    int type_; ///< Decode type.
    int size_; ///< Decode size.
    int index_; ///< Decode index.
    int version_; ///< Decode version.
    long bin_size_; ///< Decode binary size.

private:
    /*
     * These functions are not implemented to prohibit copy construction
     * and assignment by value.
     */
    DBOperation & operator =(const DBOperation&);
    DBOperation(DBOperation&);
};
}/* end of namespace rytong */

#endif    /* _RYT_DB_OPERATION_H */
