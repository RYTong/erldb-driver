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
 *  @file SybDBOperation.h
 *  @brief Derived class for sybase to represent operations of database.
 *  @author wang.meigong <wang.meigong@rytong.com>
 *  @version 1.0.0
 *  @date Mon May 16 12:37:54 CST 2011
 */

#ifndef _SYBDBOPERATION_H
#define _SYBDBOPERATION_H

#include "SybUtils.h"
#include "base/DBOperation.h"
#include "base/DBException.h"
#include "base/StmtMap.h"
#include "SybConnection.h"

namespace rytong {
/* custom defined erlang data types */
#define MY_ERROR_TYPE       '0'
#define MY_NUMBER_TYPE      '1'
#define MY_DATE_TYPE        '2'
#define MY_TIME_TYPE        '3'
#define MY_DATETIME_TYPE    '4'
#define MY_DATETIME4_TYPE   '5'
#define MY_BIGDATETIME_TYPE '6'
#define MY_BIGTIME_TYPE     '7'

/** @brief Derived class for sybase to represent operations of database.
 */
class SybDBOperation : public DBOperation {    
public:
    /** @brief Defined binary type.
     */
    typedef struct _binary {
        char* value; ///< Binary data.
        long len; ///< Binary length.
    } MyBinary;

    /** @brief Defined datatime type.
     */
    typedef struct _datetime {
        int year;   ///< Year.
        int month;  ///< Month.
        int day;    ///< Day.
        int hour;   ///< Hour.
        int minutes;///< Minutes.
        int seconds;///< Seconds.
        int ms;     ///< Millisecond.
        int Ms;     ///< Microsecond.
    } MyDateTime;
    
    /** @brief Constructor for the class.
     *  @return None.
     */
    SybDBOperation();
    
    /** @brief Destructor for the class.
     *  @return None.
     */
    ~SybDBOperation();
    
    static bool release_stmt(void* data) {
        SybStatement* stmt = (SybStatement*) data;
        stmt->prepare_release();
        delete stmt;
        
        return true;
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
     *  @see DBOperation::prepare_stat_exec.
     */
    bool prepare_stat_exec(ei_x_buff * const res);

    /** @brief Perpare statement release interface.
     *  @see DBOperation::prepare_stat_release.
     */
    bool prepare_stat_release(ei_x_buff * const res);
    
private:
    SybDBOperation & operator =(const SybDBOperation&);

    SybDBOperation(SybDBOperation&);

    /* error types */
    static const char* CONN_NULL_ERROR;
    static const char* STMT_NULL_ERROR;
    static const char* BAD_ARG_ERROR;
    static const char* EXECUTE_SQL_ERROR;

    int limit_row_count_;

    inline bool alloc_binary(MyBinary* bin, long length)
    {
        bin->len = length;
        bin->value = (char*)malloc(length * sizeof(char));
        return bin->value != NULL;
    }

    inline void free_binary(MyBinary* bin)
    {
        free(bin->value);
        bin->value = NULL;
    }

    inline bool to_16hex(char* dest, int len, unsigned long src)
    {
        for (int i = 0; i < len; ++i) {
            dest[i*2 + 1] = src % 16;
            src = src >> 4;
            dest[i*2] = src % 16;
            src = src >> 4;

            if (dest[i*2 + 1] < 10) {
                dest[i*2 + 1] = dest[i*2 + 1] + '0';
            } else {
                dest[i*2 + 1] = dest[i*2 + 1] - 10 + 'a';
            }
            if (dest[i*2] < 10) {
                dest[i*2] = dest[i*2] + '0';
            } else {
                dest[i*2] = dest[i*2] - 10 + 'a';
            }
        }

        return true;
    }


    /** get erlang type from ei buffer */
    inline int get_erl_type(int* p_index = NULL)
    {
        if (p_index == NULL) {
            p_index = &index_;
        }

        return ei_get_type(buf_, p_index, &type_,
                &size_) == 0 ? type_:-1;
    }

    /** decode tuple header */
    inline int decode_tuple_header(int* p_index = NULL){
        if (p_index == NULL) {
            p_index = &index_;
        }

        return ei_decode_tuple_header(buf_, p_index,
                &size_) == 0 ? size_:-1;
    }

     /** decode list header */
    inline int decode_list_header(int* p_index = NULL) {
        if (p_index == NULL) {
            p_index = &index_;
        }

        return ei_decode_list_header(buf_, p_index,
                &size_) == 0 ? size_:-1;
    }

    /** decode empty list */
    inline bool decode_empty_list(int* p_index = NULL) {
        if (p_index == NULL) {
            p_index = &index_;
        }

        return ei_decode_list_header(buf_, p_index, &size_) == 0;
    }

    /** skip a term */
    inline bool skip_term(int* p_index = NULL) {
        if (p_index == NULL) {
            p_index = &index_;
        }

        return ei_skip_term(buf_, p_index) == 0;
    }

    /** skip a term */
    inline bool decode_binary(MyBinary* dest, int* p_index = NULL)
    {
        int retcode;

        if (p_index == NULL) {
            p_index = &index_;
        }

        retcode = ei_get_type(buf_, p_index, &type_, &size_);
        if (retcode != 0) {
            return false;
        }

        if (!alloc_binary(dest, size_)) {
            return false;
        }

        switch (type_) {
            case ERL_STRING_EXT:
                retcode = ei_decode_string(buf_, p_index, dest->value);
                break;

            case ERL_BINARY_EXT:
                retcode = ei_decode_binary(buf_, p_index, dest->value, &dest->len);
                break;

            default:
                retcode = -1;
        }
        if (retcode != 0) {
            free_binary(dest);
            return false;
        }

        return true;
    }

     /** decode internal defined erlang type from ei buffer */
    inline int decode_custom_type(int* p_index = NULL)
    {
        int retcode;
        char type[20];

        if (p_index == NULL) {
            p_index = &index_;
        }

        if (decode_tuple_header(p_index) != 2) {
            return MY_ERROR_TYPE;
        }
        retcode = ei_get_type(buf_, p_index, &type_, &size_);
        if (retcode != 0 || type_ != ERL_ATOM_EXT || size_ > 19) {
            return MY_ERROR_TYPE;
        }
        retcode = ei_decode_atom(buf_, p_index, type);
        if (retcode != 0) {
            return MY_ERROR_TYPE;
        }

        if (strcmp(type, "number") == 0) {
            return MY_NUMBER_TYPE;
        } else if (strcmp(type, "date") == 0) {
            return MY_DATE_TYPE;
        } else if (strcmp(type, "time") == 0) {
            return MY_TIME_TYPE;
        } else if (strcmp(type, "datetime") == 0) {
            return MY_DATETIME_TYPE;
        } else if (strcmp(type, "smalldatetime") == 0) {
            return MY_DATETIME4_TYPE;
        } else if (strcmp(type, "bigdatetime") == 0) {
            return MY_BIGDATETIME_TYPE;
        } else if (strcmp(type, "bigtime") == 0) {
            return MY_BIGTIME_TYPE;
        }

        return MY_ERROR_TYPE;
    }

    inline bool decode_strnumber(char dest[], int destlen, int* p_index = NULL)
    {
        int retcode;

        if (p_index == NULL) {
            p_index = &index_;
        }

        retcode = ei_get_type(buf_, p_index, &type_, &size_);
        if (retcode != 0 || type_ != ERL_STRING_EXT
                || size_ > destlen -1) {
            return false;
        }
        retcode = ei_decode_string(buf_, p_index, dest);

        return retcode == 0;
    }

    inline bool decode_date(MyDateTime* datetime, int* p_index = NULL)
    {
        long year, month, day;

        if (p_index == NULL) {
            p_index = &index_;
        }

        if (decode_tuple_header(p_index) == 3
                && decode_integer(year, p_index)
                && decode_integer(month, p_index)
                && decode_integer(day, p_index)) {
            datetime->year = year;
            datetime->month = month;
            datetime->day = day;
        } else {
            return false;
        }

        return true;
    }

    inline bool decode_time(MyDateTime* datetime, int* p_index = NULL)
    {
        long hour, minutes, seconds, ms;

        if (p_index == NULL) {
            p_index = &index_;
        }

        if (decode_tuple_header(p_index) == 4
                && decode_integer(hour, p_index)
                && decode_integer(minutes, p_index)
                && decode_integer(seconds, p_index)
                && decode_integer(ms, p_index)) {
            datetime->hour = hour;
            datetime->minutes = minutes;
            datetime->seconds = seconds;
            datetime->ms = ms;
        } else {
            return false;
        }

        return true;
    }

    inline bool decode_datetime(MyDateTime* datetime, int* p_index = NULL)
    {
        if (p_index == NULL) {
            p_index = &index_;
        }

        return (decode_tuple_header(p_index) == 2
                && decode_date(datetime, p_index)
                && decode_time(datetime, p_index));
    }

    inline bool decode_datetime4(MyDateTime* datetime, int* p_index = NULL)
    {
        long hour, minutes;
        
        if (p_index == NULL) {
            p_index = &index_;
        }

        if (decode_tuple_header(p_index) == 2 
                && decode_date(datetime, p_index)
                && decode_tuple_header(p_index) == 2
                && decode_integer(hour, p_index)
                && decode_integer(minutes, p_index)) {
            datetime->hour = hour;
            datetime->minutes = minutes;
        } else {
            return false;
        }

        return true;
    }

#ifdef SYBASE_OCS15_5
    inline bool decode_bigdatetime(MyDateTime* datetime, int* p_index = NULL)
    {
        if (p_index == NULL) {
            p_index = &index_;
        }

        return (decode_tuple_header(p_index) == 2
                && decode_date(datetime, p_index)
                && decode_bigtime(datetime, p_index));
    }

    inline bool decode_bigtime(MyDateTime* datetime, int* p_index = NULL)
    {
        long hour, minutes, seconds, ms, Ms;

        if (p_index == NULL) {
            p_index = &index_;
        }

        if (decode_tuple_header(p_index) == 5
                && decode_integer(hour, p_index)
                && decode_integer(minutes, p_index)
                && decode_integer(seconds, p_index)
                && decode_integer(ms, p_index)
                && decode_integer(Ms, p_index)) {
            datetime->hour = hour;
            datetime->minutes = minutes;
            datetime->seconds = seconds;
            datetime->ms = ms;
            datetime->Ms = Ms;
        } else {
            return false;
        }

        return true;
    }
#endif

    /** is null */
    inline bool is_null(int* p_index = NULL) 
    {
        int retcode;
        char type[10];
        
        if (p_index == NULL) {
            p_index = &index_;
        }

        retcode = ei_get_type(buf_, p_index, &type_, &size_);
        if (retcode != 0 || type_ != ERL_ATOM_EXT || size_ != 9) {
            return false;
        }

        if (ei_decode_atom(buf_, p_index, type) != 0
                || strcmp(type, "undefined") !=0) {
            return false;
        }

        return true;
    }

    /** @brief Decode param from ei buff and set it in the sql statement.
     *  @param stmt The pointer to Sybase Statement, is the sql statement.
     *  @param index Param index in ei buffer.
     *  @param p_index A pointer pointing to ei buffer index,
     *      if NULL it will be set to point to index_.
     *  @return None.
     */
    bool decode_and_set_param(SybStatement* stmt, int index)
    {
        bool retcode;
        int param_type;

        if (is_null()) {
            return stmt->set_null(index);
        }

        param_type = stmt->get_param_type(index);
        switch(param_type) {
             /** Binary types */
            case CS_BINARY_TYPE:
                retcode = decode_and_set_binary(stmt, index);
                break;

            case CS_LONGBINARY_TYPE:
                retcode = decode_and_set_longbinary(stmt, index);
                break;

            case CS_VARBINARY_TYPE:
                retcode = decode_and_set_varbinary(stmt, index);
                break;

            /** Bit types */
            case CS_BIT_TYPE:
                retcode = decode_and_set_bit(stmt, index);
                break;

            /** Character types */
            case CS_CHAR_TYPE:
                retcode = decode_and_set_char(stmt, index);
                break;

            case CS_LONGCHAR_TYPE:
                retcode = decode_and_set_longchar(stmt, index);
                break;

            case CS_VARCHAR_TYPE:
                retcode = decode_and_set_varchar(stmt, index);
                break;

            case CS_UNICHAR_TYPE:
                retcode = decode_and_set_unichar(stmt, index);
                break;

            case CS_XML_TYPE:
                retcode = decode_and_set_xml(stmt, index);
                break;

            /** Datetime types */
            case CS_DATE_TYPE:
                retcode = decode_and_set_date(stmt, index);
                break;

            case CS_TIME_TYPE:
                retcode = decode_and_set_time(stmt, index);
                break;

            case CS_DATETIME_TYPE:
                retcode = decode_and_set_datetime(stmt, index);
                break;

            case CS_DATETIME4_TYPE:
                retcode = decode_and_set_datetime4(stmt, index);
                break;

#ifdef SYBASE_OCS15_5
            case CS_BIGDATETIME_TYPE:
                retcode = decode_and_set_bigdatetime(stmt, index);
                break;

            case CS_BIGTIME_TYPE:
                retcode = decode_and_set_bigtime(stmt, index);
                break;
#endif

            /** Numeric types */
            case CS_TINYINT_TYPE:
                retcode = decode_and_set_tinyint(stmt, index);
                break;

            case CS_SMALLINT_TYPE:
                retcode = decode_and_set_smallint(stmt, index);
                break;

            case CS_INT_TYPE:
                retcode = decode_and_set_int(stmt, index);
                break;

            case CS_BIGINT_TYPE:
                retcode = decode_and_set_bigint(stmt, index);
                break;

            case CS_USMALLINT_TYPE:
                retcode = decode_and_set_usmallint(stmt, index);
                break;

            case CS_UINT_TYPE:
                retcode = decode_and_set_uint(stmt, index);
                break;

            case CS_UBIGINT_TYPE:
                retcode = decode_and_set_ubigint(stmt, index);
                break;

            case CS_DECIMAL_TYPE:
                retcode = decode_and_set_decimal(stmt, index);
                break;

            case CS_NUMERIC_TYPE:
                retcode = decode_and_set_numeric(stmt, index);
                break;

            case CS_FLOAT_TYPE:
                retcode = decode_and_set_float(stmt, index);
                break;

            case CS_REAL_TYPE:
                retcode = decode_and_set_real(stmt, index);
                break;

            /** Money types */
            case CS_MONEY_TYPE:
                retcode = decode_and_set_money(stmt, index);
                break;

            case CS_MONEY4_TYPE:
                retcode = decode_and_set_money4(stmt, index);
                break;

            default:
                retcode = false;
                break;
        }

        return retcode;
    }

    bool set_param(SybStatement* stmt, int index, unsigned char data)
    {
        bool retcode;
        int param_type;

        param_type = stmt->get_param_type(index);
        switch(param_type) {
            /** Bit types */
            case CS_BIT_TYPE:
                retcode = stmt->set_bit(index, data);
                break;

            case CS_TINYINT_TYPE:
                retcode = stmt->set_tinyint(index, data);
                break;

            case CS_SMALLINT_TYPE:
                retcode = stmt->set_smallint(index, (short)data);
                break;

            case CS_INT_TYPE:
                retcode = stmt->set_int(index, (int)data);
                break;

            case CS_BIGINT_TYPE:
                retcode = stmt->set_bigint(index, (long)data);
                break;

            case CS_USMALLINT_TYPE:
                retcode = stmt->set_usmallint(index, (unsigned short)data);
                break;

            case CS_UINT_TYPE:
                retcode = stmt->set_uint(index, (unsigned int)data);
                break;

            case CS_UBIGINT_TYPE:
                retcode = stmt->set_ubigint(index, (unsigned long)data);
                break;

            case CS_FLOAT_TYPE:
                retcode = stmt->set_float(index, (double)data);
                break;

            case CS_REAL_TYPE:
                retcode = stmt->set_real(index, (float)data);
                break;

            case CS_MONEY4_TYPE:
                retcode = stmt->set_money4(index, (double)data);
                break;

            default:
                retcode = false;
                break;
        }

        return retcode;
    }

    bool decode_and_set_binary(SybStatement* stmt, int index)
    {
        bool setcode;
        MyBinary bin;
        
        if (!decode_binary(&bin)) {
            return false;
        }
        setcode =  stmt->set_binary(index, (unsigned char*)bin.value, bin.len);
        free_binary(&bin);

        return setcode;
    }

    bool decode_and_set_longbinary(SybStatement* stmt, int index)
    {
        bool setcode;
        MyBinary bin;

        if (!decode_binary(&bin)) {
            return false;
        }

        setcode =  stmt->set_longbinary(index, (unsigned char*)bin.value, bin.len);
        free_binary(&bin);

        return setcode;
    }

    bool decode_and_set_varbinary(SybStatement* stmt, int index)
    {
        bool setcode;
        MyBinary bin;

        if (!decode_binary(&bin)) {
            return false;
        }

        setcode =  stmt->set_varbinary(index, (unsigned char*)bin.value, bin.len);
        free_binary(&bin);

        return setcode;
    }

    bool decode_and_set_bit(SybStatement* stmt, int index)
    {
        int retcode = -1;
        char bit;

        retcode = ei_decode_char(buf_, &index_, &bit);
        if (retcode != 0) {
            return false;
        }

        return stmt->set_bit(index, (unsigned char)bit);
    }

    bool decode_and_set_char(SybStatement* stmt, int index)
    {
        bool setcode;
        MyBinary bin;

        if (!decode_binary(&bin)) {
            return false;
        }

        setcode =  stmt->set_char(index, bin.value, bin.len);
        free_binary(&bin);

        return setcode;
    }

    bool decode_and_set_longchar(SybStatement* stmt, int index)
    {
        bool setcode;
        MyBinary bin;

        if (!decode_binary(&bin)) {
            return false;
        }

        setcode =  stmt->set_longchar(index, (unsigned char*)bin.value, bin.len);
        free_binary(&bin);

        return setcode;
    }

    bool decode_and_set_varchar(SybStatement* stmt, int index)
    {
        bool setcode;
        MyBinary bin;

        if (!decode_binary(&bin)) {
            return false;
        }

        setcode =  stmt->set_varchar(index, (unsigned char*)bin.value, bin.len);
        free_binary(&bin);

        return setcode;
    }

    bool decode_and_set_unichar(SybStatement* stmt, int index)
    {
        int retcode = -1;
        bool setcode;
        long length = 0;
        unsigned short* unichar = NULL;
        MyBinary bin;


        retcode = ei_get_type(buf_, &index_, &type_, &size_);
        if (retcode != 0) {
            return false;
        }
        
        length = size_;
        unichar = (unsigned short*)malloc(length * sizeof(unsigned short));

        if (type_ == ERL_LIST_EXT) {
            length = decode_list_header();
            if (length < 0) {
                free(unichar);
                return false;
            }
            for (int i = 0; i < length; ++i) {
                unsigned long v;
                retcode = ei_decode_ulong(buf_, &index_, &v);
                if (retcode != 0) {
                    free(unichar);
                    return false;
                }
                unichar[i] = (unsigned short) v;
            }
            decode_empty_list();
        } else {
            if (!decode_binary(&bin)) {
                free(unichar);
                return false;
            }
            for (int i = 0; i < length; ++i) {
                unichar[i] = (unsigned short)bin.value[i];
            }
            free_binary(&bin);
        }

        setcode =  stmt->set_unichar(index, unichar, length);
        free(unichar);

        return setcode;
    }

    bool decode_and_set_xml(SybStatement* stmt, int index)
    {
        bool setcode;
        MyBinary bin;

        if (!decode_binary(&bin)) {
            return false;
        }

        setcode =  stmt->set_xml(index, (unsigned char*)bin.value, bin.len);
        free_binary(&bin);

        return setcode;
    }

    bool decode_and_set_date(SybStatement* stmt, int index) 
    {
        MyDateTime datetime;

        if (decode_custom_type() != MY_DATE_TYPE
                || !decode_date(&datetime)) {
            return false;
        }

        return stmt->set_date(index, datetime.year,
                datetime.month, datetime.day);
    }

    bool decode_and_set_time(SybStatement* stmt, int index)
    {
        MyDateTime datetime;

        if (decode_custom_type() != MY_TIME_TYPE
                || !decode_time(&datetime)) {
            return false;
        }

        return stmt->set_time(index, datetime.hour, datetime.minutes,
                datetime.seconds, datetime.ms);
    }

    bool decode_and_set_datetime(SybStatement* stmt, int index)
    {
        MyDateTime datetime;

        if (decode_custom_type() != MY_DATETIME_TYPE
                || !decode_datetime(&datetime)) {
            return false;
        }

        return stmt->set_datetime(index, datetime.year, datetime.month,
                datetime.day, datetime.hour, datetime.minutes,
                datetime.seconds, datetime.ms);
    }

    bool decode_and_set_datetime4(SybStatement* stmt, int index)
    {
        MyDateTime datetime;

        if (decode_custom_type() != MY_DATETIME4_TYPE
                || !decode_datetime4(&datetime)) {
            return false;
        }

        return stmt->set_datetime4(index, datetime.year, datetime.month,
                datetime.day, datetime.hour, datetime.minutes);
    }

#ifdef SYBASE_OCS15_5
    bool decode_and_set_bigdatetime(SybStatement* stmt, int index)
    {
        MyDateTime datetime;

        if (decode_custom_type() != MY_BIGDATETIME_TYPE
                || !decode_bigdatetime(&datetime)) {
            return false;
        }

        return stmt->set_bigdatetime(index, datetime.year, datetime.month,
                datetime.day, datetime.hour, datetime.minutes,
                datetime.seconds, datetime.ms, datetime.Ms);
    }

    bool decode_and_set_bigtime(SybStatement* stmt, int index)
    {
        MyDateTime datetime;

        if (decode_custom_type() != MY_BIGTIME_TYPE
                || !decode_bigtime(&datetime)) {
            return false;
        }

        return stmt->set_bigtime(index, datetime.hour, datetime.minutes,
                datetime.seconds, datetime.ms, datetime.Ms);
    }
#endif
    
    bool decode_and_set_tinyint(SybStatement* stmt, int index)
    {
        int retcode = -1;
        char tinyint;

        retcode = ei_decode_char(buf_, &index_, &tinyint);
        if (retcode != 0) {
            return false;
        }

        return stmt->set_tinyint(index, tinyint);
    }

    bool decode_and_set_smallint(SybStatement* stmt, int index)
    {
        int retcode = -1;
        long longint;

        retcode = ei_decode_long(buf_, &index_, &longint);
        if (retcode != 0) {
            return false;
        }

        return stmt->set_smallint(index, (short)longint);
    }

    bool decode_and_set_int(SybStatement* stmt, int index) 
    {
        int retcode = -1;
        long longint;

        retcode = ei_decode_long(buf_, &index_, &longint);
        if (retcode != 0) {
            return false;
        }

        return stmt->set_int(index, (int)longint);
    }

    bool decode_and_set_bigint(SybStatement* stmt, int index)
    {
        int retcode = -1;
        long bigint;

        retcode = ei_decode_long(buf_, &index_, &bigint);
        if (retcode != 0) {
            return false;
        }

        return stmt->set_bigint(index, bigint);
    }

    bool decode_and_set_usmallint(SybStatement* stmt, int index) 
    {
        int retcode = -1;
        unsigned long longint;

        retcode = ei_decode_ulong(buf_, &index_, &longint);
        if (retcode != 0) {
            return false;
        }

        return stmt->set_usmallint(index, (unsigned short)longint);
    }

    bool decode_and_set_uint(SybStatement* stmt, int index) 
    {
        int retcode = -1;
        unsigned long longint;

        retcode = ei_decode_ulong(buf_, &index_, &longint);
        if (retcode != 0) {
            return false;
        }

        return stmt->set_uint(index, (unsigned int)longint);
    }

    bool decode_and_set_ubigint(SybStatement* stmt, int index)
    {
        int retcode = -1;
        unsigned long bigint;

        retcode = ei_decode_ulong(buf_, &index_, &bigint);
        if (retcode != 0) {
            return false;
        }

        return stmt->set_ubigint(index, bigint);
    }

    bool decode_and_set_decimal(SybStatement* stmt, int index) 
    {
        char str[40];

        if (decode_custom_type() != MY_NUMBER_TYPE
                || !decode_strnumber(str, sizeof(str))) {
            return false;
        }

        return stmt->set_decimal(index, str);
    }

    bool decode_and_set_numeric(SybStatement* stmt, int index) 
    {
        char str[40];

        if (decode_custom_type() != MY_NUMBER_TYPE
                || !decode_strnumber(str, sizeof(str))) {
            return false;
        }

        return stmt->set_numeric(index, str);
    }

    bool decode_and_set_float(SybStatement* stmt, int index) 
    {
        int retcode;
        double data;

        retcode = ei_decode_double(buf_, &index_, &data);
        if (retcode != 0) {
            return false;
        }

        return stmt->set_float(index, (float)data);
    }

    bool decode_and_set_real(SybStatement* stmt, int index) 
    {
        int retcode;
        double data;

        retcode = ei_decode_double(buf_, &index_, &data);
        if (retcode != 0) {
            return false;
        }

        return stmt->set_real(index, data);
    }

    bool decode_and_set_money(SybStatement* stmt, int index)
    {
        char str[30];

        if (decode_custom_type() != MY_NUMBER_TYPE
                || !decode_strnumber(str, sizeof(str))) {
            return false;
        }

        return stmt->set_money(index, str);
    }

    bool decode_and_set_money4(SybStatement* stmt, int index)
    {
        int retcode;
        double data;

        retcode = ei_decode_double(buf_, &index_, &data);
        if (retcode != 0) {
            return false;
        }

        return stmt->set_money4(index, data);
    }

    bool decode_and_input_value(stringstream& stream)
    {
        bool retcode;
        
        if (is_null()) {
            stream << "NULL";
            return true;
        }

        switch (get_erl_type()) {
            case ERL_NIL_EXT:
                if (retcode = decode_empty_list()) {
                    stream << "''";
                }
                break;
                
            case ERL_STRING_EXT:
                retcode = decode_and_input_string(stream);
                break;

            case ERL_LIST_EXT:
                retcode = decode_and_input_list(stream);
                break;

            case ERL_BINARY_EXT:
                retcode = decode_and_input_binary(stream);
                break;

            case ERL_SMALL_INTEGER_EXT:
            case ERL_INTEGER_EXT:
            case ERL_SMALL_BIG_EXT:
            case ERL_LARGE_BIG_EXT:
                retcode = decode_and_input_integer(stream);
                break;
                
            case ERL_FLOAT_EXT:
                retcode = decode_and_input_float(stream);
                break;
                
            case ERL_SMALL_TUPLE_EXT:
                retcode = decode_and_input_cunstom(stream);
                break;

            default:
                retcode =  false;
        }

        return retcode;
    }

    bool decode_and_input_string(stringstream& stream) 
    {
        char* value = NULL;
        
        if (!decode_string(value)) {
            return false;
        }
        stream << " '" << value << "'";
        free_string(value);
        
        return true;
    }

    bool decode_and_input_list(stringstream& stream)
    {
        long len;
        unsigned long data;
        char* value;

        if ((len = decode_list_header()) < 1) {
            return false;
        }

        value = (char*)malloc(len * 4  + 1);
        if (value == NULL) {
            return false;
        }
        for (long i = 0; i < len; ++i) {
            if (ei_decode_ulong(buf_, &index_, &data) != 0
                    || data > 65535) {
                free(value);
                return false;
            }
            to_16hex(value + i * 4, 2, data);
        }
        if (!decode_empty_list()) {
            free(value);
            return false;
        }
        value[len * 4] = 0;

        stream << " 0x" << value;
        free(value);

        return true;
    }

    bool decode_and_input_binary(stringstream& stream)
    {
        MyBinary bin;
        char* value;

        if (!decode_binary(&bin)) {
            return false;
        }
        if (bin.len > 0) {
            value = (char*)malloc(bin.len * 2  + 1);
            if (value == NULL) {
                free_binary(&bin);
                return false;
            }
            for (long i = 0; i < bin.len; ++i) {
                to_16hex(value + i*2, 1, (unsigned long)bin.value[i]);
            }
            value[bin.len * 2] = 0;

            stream << " 0x" << value;
            free_binary(&bin);
            free(value);
        } else {
            stream << " NULL";
        }
        
        return true;
    }

    bool decode_and_input_integer(stringstream& stream)
    {
        int retcode;
        long long value;

        retcode = ei_decode_longlong(buf_, &index_, &value);
        if (retcode != 0) {
            return false;
        }
        stream << " " << value;

        return true;
    }

    bool decode_and_input_float(stringstream& stream)
    {
        int retcode;
        double value;

        retcode = ei_decode_double(buf_, &index_, &value);
        if (retcode != 0) {
            return false;
        }
        stream << " " << value;

        return true;
    }

    bool decode_and_input_cunstom(stringstream& stream)
    {
        bool retcode;
        
        switch (decode_custom_type()) {
            case MY_NUMBER_TYPE:
                retcode = decode_and_input_strnumber(stream);
                break;

            case MY_DATE_TYPE:
                retcode = decode_and_input_date(stream);
                break;

            case MY_TIME_TYPE:
                retcode = decode_and_input_time(stream);
                break;

            case MY_DATETIME_TYPE:
                retcode = decode_and_input_datetime(stream);
                break;

            case MY_DATETIME4_TYPE:
                retcode = decode_and_input_datetime4(stream);
                break;

#ifdef SYBASE_OCS15_5
            case MY_BIGDATETIME_TYPE:
                retcode = decode_and_input_bigdatetime(stream);
                break;

            case MY_BIGTIME_TYPE:
                retcode = decode_and_input_bigtime(stream);
                break;
#endif
                
            default:
                retcode = false;
        }

        return retcode;
    }

    bool decode_and_input_strnumber(stringstream& stream)
    {
        char strnum[40];

        if (!decode_strnumber(strnum, sizeof(strnum))) {
            return false;
        } else {
            stream << " " << strnum;
        }

        return true;
    }

    bool decode_and_input_date(stringstream& stream)
    {
        MyDateTime datetime;
        char data[9];
        int days;

        if (!decode_date(&datetime)) {
            return false;
        }
        days = date_to_days(datetime.year, datetime.month, datetime.day) -
                693961;
        to_16hex(data, 4, (unsigned long)days);
        data[8] = 0;
        stream << " 0x" << data;
        
        return true;
    }

    bool decode_and_input_time(stringstream& stream)
    {
        MyDateTime datetime;
        char data[9];
        int mseconds;

        if (!decode_time(&datetime)) {
            return false;
        }
        mseconds = ((datetime.hour * 60 + datetime.minutes) * 60 +
                datetime.seconds) * 1000 + datetime.ms;
        mseconds = mseconds * 3 / 10;
        to_16hex(data, 4, (unsigned long)mseconds);
        data[8] = 0;
        stream << " 0x" << data;

        return true;
    }

    bool decode_and_input_datetime(stringstream& stream)
    {
        MyDateTime datetime;
        char data[17];
        int days;
        int mseconds;

        if (!decode_datetime(&datetime)) {
            return false;
        }
        days = date_to_days(datetime.year, datetime.month, datetime.day) -
                693961;
        mseconds = ((datetime.hour * 60 + datetime.minutes) * 60 +
                datetime.seconds) * 1000 + datetime.ms;
        mseconds = mseconds * 3 / 10;
        to_16hex(data, 4, (unsigned long)days);
        to_16hex(data + 8, 4, (unsigned long)mseconds);
        data[16] = 0;
        stream << " 0x" << data;

        return true;
    }

    bool decode_and_input_datetime4(stringstream& stream)
    {
        MyDateTime datetime;
        char data[9];
        int days;
        int minutes;

        if (!decode_datetime4(&datetime)) {
            return false;
        }
        days = date_to_days(datetime.year, datetime.month, datetime.day) -
                693961;
        minutes = datetime.hour * 60 + datetime.minutes;
        to_16hex(data, 2, (unsigned long)days);
        to_16hex(data + 4, 2, (unsigned long)minutes);
        data[8] = 0;
        stream << " 0x" << data;

        return true;
    }

#ifdef SYBASE_OCS15_5
    bool decode_and_input_bigdatetime(stringstream& stream)
    {
        MyDateTime datetime;
        char data[17];
        unsigned long bigdatetime;

        if (!decode_bigdatetime(&datetime)) {
            return false;
        }
        bigdatetime = (unsigned long)date_to_days(datetime.year,
                datetime.month, datetime.day);
        bigdatetime = bigdatetime * 24 + datetime.hour;
        bigdatetime = bigdatetime * 60 + datetime.minutes;
        bigdatetime = bigdatetime * 60 + datetime.seconds;
        bigdatetime = bigdatetime * 1000 + datetime.ms;
        bigdatetime = bigdatetime * 1000 + datetime.Ms;
        to_16hex(data, 8, bigdatetime);
        data[16] = 0;
        stream << " 0x" << data;

        return true;
    }

    bool decode_and_input_bigtime(stringstream& stream)
    {
        MyDateTime datetime;
        char data[17];
        unsigned long bigtime;

        if (!decode_bigtime(&datetime)) {
            return false;
        }
        bigtime = datetime.hour * 60 + datetime.minutes;
        bigtime = bigtime * 60 + datetime.seconds;
        bigtime = bigtime * 1000 + datetime.ms;
        bigtime = bigtime * 1000 + datetime.Ms;
        to_16hex(data, 8, bigtime);
        data[16] = 0;
        stream << " 0x" << data;

        return true;
    }
#endif

    bool make_where_expr(stringstream &sqlstream)
    {
        bool retcode;
        char* value;
        int index;
        long key;

        switch (get_erl_type()) {
            case ERL_ATOM_EXT:
                if ((retcode = decode_string(value))) {
                    if (strcmp(value, "undefined") == 0) {
                        sqlstream << " NULL";
                    } else {
                        sqlstream << " " << value;
                    }
                    free_string(value);
                }
                break;
                
            case ERL_NIL_EXT:
                if (retcode = decode_empty_list()) {
                    sqlstream << " ''";
                }
                break;

            case ERL_STRING_EXT:
                retcode = decode_and_input_string(sqlstream);
                break;

            case ERL_LIST_EXT:
                retcode = decode_and_input_list(sqlstream);
                break;

            case ERL_BINARY_EXT:
                retcode = decode_and_input_binary(sqlstream);
                break;

            case ERL_SMALL_INTEGER_EXT:
            case ERL_INTEGER_EXT:
            case ERL_SMALL_BIG_EXT:
            case ERL_LARGE_BIG_EXT:
                retcode = decode_and_input_integer(sqlstream);
                break;

            case ERL_FLOAT_EXT:
                retcode = decode_and_input_float(sqlstream);
                break;

            case ERL_SMALL_TUPLE_EXT:
            case ERL_LARGE_TUPLE_EXT:
                index = index_;
                decode_tuple_header(&index);
                if (decode_integer(key, &index)) {
                    index_ = index;
                    retcode = make_rela_expr(sqlstream, key);
                } else {
                    retcode = decode_and_input_cunstom(sqlstream);
                }
                break;

            default:
                retcode =  false;
        }

        return retcode;
    }

    bool make_rela_expr(stringstream &sqlstream, long key)
    {
        bool retcode;
        
        switch(key) {
            case SQL_AND:
                retcode = make_and(sqlstream);
                break;

            case SQL_OR:
                retcode = make_or(sqlstream);
                break;

            case SQL_NOT:
                retcode = make_not(sqlstream);
                break;

            case SQL_LIKE:
                retcode = make_like(sqlstream);
                break;

            case SQL_AS:
                retcode = make_as(sqlstream);
                break;

            case SQL_EQUAL:
                retcode = make_equal(sqlstream);
                break;

            case SQL_GREATER:
                retcode = make_greater(sqlstream);
                break;

            case SQL_GREATER_EQUAL:
                retcode = make_greater_equal(sqlstream);
                break;

            case SQL_LESS:
                retcode = make_less(sqlstream);
                break;

            case SQL_LESS_EQUAL:
                retcode = make_less_equal(sqlstream);
                break;

            case SQL_JOIN:
                retcode = make_join(sqlstream);
                break;

            case SQL_LEFT_JOIN:
                retcode = make_left_join(sqlstream);
                break;

            case SQL_RIGHT_JOIN:
                retcode = make_right_join(sqlstream);
                break;

            case SQL_NOT_EQUAL:
                retcode = make_not_equal(sqlstream);
                break;

            case SQL_ORDER:
                retcode = make_order(sqlstream);
                break;

            case SQL_LIMIT:
                retcode = make_limit(sqlstream);
                break;

            case SQL_DOT:
                retcode = make_dot(sqlstream);
                break;

            case SQL_GROUP:
                retcode = make_group(sqlstream);
                break;

            case SQL_HAVING:
                retcode = make_having(sqlstream);
                break;

            case SQL_BETWEEN:
                retcode = make_between(sqlstream);
                break;

            case SQL_ADD:
                retcode = make_add(sqlstream);
                break;

            case SQL_SUB:
                retcode = make_sub(sqlstream);
                break;

            case SQL_MUL:
                retcode = make_mul(sqlstream);
                break;

            case SQL_DIV:
                retcode = make_div(sqlstream);
                break;

            case SQL_FUN:
                retcode = make_fun(sqlstream);
                break;

            case SQL_INNER_JOIN:
                retcode = make_inner_join(sqlstream);
                break;

            default:
                retcode = false;
        }

        return retcode;
    }

    bool make_and(stringstream &sqlstream)
    {
        int size;

        if ((size = decode_list_header()) < 0) {
            return false;
        }

        for (int i = 0; i < size; ++i) {
            sqlstream << " (";
            if (!make_where_expr(sqlstream)) {
                return false;
            }
            sqlstream << ")";
            if (i < size - 1) {
                sqlstream << " AND";
            }
        }

        return decode_empty_list();
    }

    bool make_or(stringstream &sqlstream)
    {
        sqlstream << " (";
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        sqlstream << ") OR(";
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        sqlstream << ")";

        return true;
    }

    bool make_not(stringstream &sqlstream)
    {
        sqlstream << " NOT (";
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        sqlstream << ")";

        return true;
    }

    bool make_like(stringstream &sqlstream)
    {
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        sqlstream << " LIKE";

        return make_where_expr(sqlstream);
    }

    bool make_as(stringstream &sqlstream)
    {
        sqlstream << " AS";
        
        return  make_where_expr(sqlstream);
    }

    bool make_equal(stringstream &sqlstream)
    {
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        
        sqlstream << " =";
        
        return  make_where_expr(sqlstream);
    }

    bool make_greater(stringstream &sqlstream)
    {
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        sqlstream << " >";

        return  make_where_expr(sqlstream);
    }

    bool make_greater_equal(stringstream &sqlstream)
    {
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        sqlstream << " >=";

        return  make_where_expr(sqlstream);
    }

    bool make_less(stringstream &sqlstream)
    {
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        sqlstream << " <";

        return  make_where_expr(sqlstream);
    }

    bool make_less_equal(stringstream &sqlstream)
    {
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        sqlstream << " <=";

        return  make_where_expr(sqlstream);
    }

    bool make_join(stringstream &sqlstream)
    {
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        sqlstream << " JOIN";
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        sqlstream << " ON";

        return  make_where_expr(sqlstream);
    }

    bool make_left_join(stringstream &sqlstream)
    {
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        sqlstream << " LEFT JOIN";
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        sqlstream << " ON";

        return  make_where_expr(sqlstream);
    }

    bool make_right_join(stringstream &sqlstream)
    {
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        sqlstream << " RIGHT JOIN";
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        sqlstream << " ON";

        return  make_where_expr(sqlstream);
    }

    bool make_not_equal(stringstream &sqlstream)
    {
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        sqlstream << " !=";
        
        return  make_where_expr(sqlstream);
    }

    bool make_order(stringstream &sqlstream)
    {
        int size;
        long flag;
        
        sqlstream << " ORDER BY";
        if ((size = decode_list_header()) < 0) {
            return false;
        }
        for (int i = 0; i < size; ++i) {
            if (decode_tuple_header() < 0) {
                return false;
            }
            if (!make_where_expr(sqlstream)) {
                return false;
            }
            if (decode_integer(flag)) {
                sqlstream << (flag == 0 ? "ASC" : "DESC");
                if (i < size - 1) {
                    sqlstream << " ,";
                 }
            } else {
                return false;
            }
        }

        return decode_empty_list();
    }

    bool make_limit(stringstream &sqlstream)
    {
        long offset, len;

        /** @attention sybase does not surpport 'limit' key word, so we use
         * 'SET ROWCOUNT len' to limit the select total rows.
         */
        if (!decode_integer(offset) || offset != 0
                || !decode_integer(len) || len < 1) {
            return false;
        }

        limit_row_count_ = len;

        return true;
    }

    bool make_dot(stringstream &sqlstream)
    {
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        sqlstream << ".";

        return make_where_expr(sqlstream);
    }

    bool make_group(stringstream &sqlstream)
    {
        int size;

        if ((size = decode_list_header()) < 0) {
            return false;
        }
        sqlstream << " GROUP BY";
        for (int i = 0; i < size; ++i) {
            if (!make_where_expr(sqlstream)) {
                return false;
            }
            if (i < size - 1) {
                sqlstream << " ,";
            }
        }

        return decode_empty_list();
    }

    bool make_having(stringstream &sqlstream)
    {
        sqlstream << " HAVING";
        
        return make_where_expr(sqlstream);
    }

    bool make_between(stringstream &sqlstream)
    {
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        sqlstream << " BETWEEN";
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        sqlstream << " AND";

        return make_where_expr(sqlstream);
    }

    bool make_add(stringstream &sqlstream)
    {
        sqlstream << " (";
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        sqlstream << " +";

        return make_where_expr(sqlstream);
    }

    bool make_sub(stringstream &sqlstream)
    {
        sqlstream << " (";
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        sqlstream << " -";

        return make_where_expr(sqlstream);
    }

    bool make_mul(stringstream &sqlstream)
    {
        sqlstream << " (";
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        sqlstream << " *";

        return make_where_expr(sqlstream);
    }

    bool make_div(stringstream &sqlstream)
    {
        sqlstream << " (";
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        sqlstream << " /";

        return make_where_expr(sqlstream);
    }

    bool make_fun(stringstream &sqlstream) 
    {
        int type;
        int size;
        
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        type = get_erl_type();
        if (type == ERL_LIST_EXT) {
            if ((size = decode_list_header()) < 0) {
                return false;
            }
            sqlstream << " (";
            for (int i = 0; i < size; ++i) {
                if (!make_where_expr(sqlstream)) {
                    return false;
                }
                if (i < size - 1) {
                    sqlstream << " ,";
                }
            }
            sqlstream << " )";
            if (!decode_empty_list()) {
                return false;
            }
        } else  {
            char* str = NULL;
            if (decode_string(str)) {
                sqlstream << " (";
                for (unsigned int i = 0; i < strlen(str); ++i) {
                    sqlstream << " " << (int) str[i];
                    if (i < strlen(str) - 1) {
                        sqlstream << " ,";
                    }
                }
                sqlstream << " )";
                free_string(str);
            } else {
                return false;
            }
        }

        return true;
    }

    bool make_inner_join(stringstream &sqlstream)
    {
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        sqlstream << " INNER JOIN";
        if (!make_where_expr(sqlstream)) {
            return false;
        }
        sqlstream << " ON";

        return make_where_expr(sqlstream);
    }
};
}/* end of namespace rytong */
#endif  /* _SYBDBOPERATION_H */

