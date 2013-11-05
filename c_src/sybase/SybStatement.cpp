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
 *  @file SybStatement.cpp
 *  @brief Sybase prepare statement class.
 *  @author wang.meigong <wang.meigong@rytong.com>
 *  @version 1.0.0
 *  @date Wen May 4 15:06:54 CST 2011
 */

#include "SybStatement.h"

namespace rytong {
SybStatement::SybStatement(CS_CONNECTION* conn):
        conn_(conn),
        sql_(NULL),
        row_count_(0), 
        desc_dfmt_(NULL),
        param_count_(0),
        is_prepare_(false),
        executed_(false)
{
    id_[0] = '\0';
    if (ct_cmd_alloc(conn_, &cmd_) != CS_SUCCEED) {
        SysLogger::error("execute_cmd: ct_cmd_alloc() failed");
        cmd_ = NULL;
    }
}

SybStatement::SybStatement(CS_CONNECTION* conn, const char* sql):
        conn_(conn),
        row_count_(0),
        desc_dfmt_(NULL),
        param_count_(0),
        is_prepare_(false),
        executed_(false)
{
    int len = strlen(sql);
    id_[0] = '\0';
    
    if (ct_cmd_alloc(conn_, &cmd_) != CS_SUCCEED) {
        SysLogger::error("execute_cmd: ct_cmd_alloc() failed");
        cmd_ = NULL;
    }
    sql_ = (char*)malloc((len + 1) * sizeof(char));
    if (sql_) {
        strcpy(sql_, sql);
    }
}

SybStatement::~SybStatement()
{
    if (cmd_) {
        /**  Clean up the command handle used */
        (void)ct_cmd_drop(cmd_);
        cmd_ = NULL;
    }
    if (desc_dfmt_) {
        free(desc_dfmt_);
        desc_dfmt_ = NULL;
    }
    if (sql_) {
        free(sql_);
        sql_ = NULL;
    }
}

bool SybStatement::execute_cmd()
{   
    return execute_cmd(sql_);
}

bool SybStatement::execute_cmd(const char* sql)
{
    SysLogger::debug("cmd:%s", sql);

    /** execute_cmd does not support prepare statement */
    if (is_prepare_ || cmd_ == NULL) {
        return false;
    }
    reset();
    
    /* Store the command string in it, and send it to the server.*/
    if (ct_command(cmd_, CS_LANG_CMD, (CS_CHAR*)sql, CS_NULLTERM,
            CS_UNUSED) != CS_SUCCEED) {
        SysLogger::error("execute_cmd: ct_command() failed");
        return false;
    }
    if (ct_send(cmd_) != CS_SUCCEED) {
        SysLogger::error("execute_cmd: ct_send() failed");
        return false;
    }

    /** Handle the result, only return success or failure */
    if (handle_command_result() != CS_SUCCEED) {
        return false;
    }

    return true;
}

 /**
  * If this is a prepare statement then we will use ct_dynamic to execute it
  * or use ct_command
  */
bool SybStatement::execute_sql(ei_x_buff* result)
{
    if (is_prepare_) {
        if (!executed_) {
            if (ct_dynamic(cmd_, CS_EXECUTE, id_, CS_NULLTERM,
                    NULL, CS_UNUSED) != CS_SUCCEED) {
                SysLogger::error("execute_sql:ct_dynamic() failed");
                return false;
            }
            executed_ = true;
        }
        if (ct_send(cmd_) != CS_SUCCEED) {
            SysLogger::error("execute_sql:ct_send() failed");
            return false;
        }

         /** Handle the result and encode to ei_x_buff */
        if (handle_sql_result(result) != CS_SUCCEED) {
            return false;
        }
        executed_ = false;
        return true;
    } else {
        return execute_sql(result, sql_);
    }
}

 /** Execute common sql command */
bool SybStatement::execute_sql(ei_x_buff* result, const char* sql)
{
    SysLogger::debug("sql:%s", sql);
    if (cmd_ == NULL) {
        return false;
    }
    reset();

    /* Store the command string in it, and send it to the server.*/
    if (ct_command(cmd_, CS_LANG_CMD, (CS_CHAR*)sql, CS_NULLTERM,
            CS_UNUSED) != CS_SUCCEED) {
        SysLogger::error("execute_sql: ct_command() failed");
        return false;
    }
    if (ct_send(cmd_) != CS_SUCCEED) {
        SysLogger::error("execute_sql: ct_send() failed");
        return false;
    }

    /** Handle the result and encode to ei_x_buff */
    if (handle_sql_result(result) != CS_SUCCEED) {
        return false;
    }

    return true;
}

bool SybStatement::prepare_init(const char* id)
{
    return prepare_init(id, sql_);
}

bool SybStatement::prepare_init(const char* id, const char* sql)
{
    SysLogger::debug("prepare id:%s sql:%s", id, sql);
    if (cmd_ == NULL) {
        return false;
    }
    reset();
    if (strlen(id) +1 > CS_MAX_CHAR) {
        SysLogger::error("prepare identifier is too long");
        return false;
    } else {
        strcpy(id_, id);
        is_prepare_ = true;
    }
    
    if (ct_dynamic(cmd_, CS_PREPARE, (CS_CHAR*)id, CS_NULLTERM,
            (CS_CHAR*)sql, CS_NULLTERM) != CS_SUCCEED) {
        SysLogger::error("prepare_init: ct_dynamic(CS_PREPARE) failed");
        return false;
    }
    if (ct_send(cmd_) != CS_SUCCEED) {
        SysLogger::error("prepare_init: ct_send() failed");
        return false;
    }
    if (handle_command_result() != CS_SUCCEED) {
        return false;
    }
    if (ct_dynamic(cmd_, CS_DESCRIBE_INPUT, (CS_CHAR*)id,
            CS_NULLTERM, NULL, CS_UNUSED) != CS_SUCCEED) {
        SysLogger::error("prepare_init: ct_dynamic(CS_DESCRIBE_INPUT) failed");
        return false;
    }
    if (ct_send(cmd_) != CS_SUCCEED) {
        SysLogger::error("prepare_init: ct_send() failed");
        return false;
    }
    if (handle_describe_result() != CS_SUCCEED) {
        return false;
    }
    
    return true;
}

int SybStatement::get_param_count()
{
    return param_count_;
}

int SybStatement::get_param_type(int index)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return -1;
    } else {
        return desc_dfmt_[index -1].datatype;
    }
}

bool SybStatement::set_binary(int index,
        unsigned char* data, unsigned int len)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    return set_param(dfmt, (CS_VOID*)data, len);
}

bool SybStatement::set_longbinary(int index,
        unsigned char* data, unsigned int len)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    return set_param(dfmt, (CS_VOID*)data, len);
}

bool SybStatement::set_varbinary(int index,
        unsigned char* data, unsigned int len)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_VARBINARY varbinary;
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    if (len > CS_MAX_CHAR) {
        return false;
    } else {
        varbinary.len = len;
        memcpy(varbinary.array, data, len);
    }

    return set_param(dfmt, (CS_VOID*)&varbinary, sizeof(CS_VARBINARY));
}

bool SybStatement::set_bit(int index, unsigned char data)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);
    
    return set_param(dfmt, (CS_VOID*)&data, CS_UNUSED);
}

bool SybStatement::set_char(int index, char* data)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    return set_param(dfmt, (CS_VOID*)data, CS_NULLTERM);
}

bool SybStatement::set_char(int index, char* data, unsigned int len)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }

    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    return set_param(dfmt, (CS_VOID*)data, len);
}

bool SybStatement::set_longchar(int index, unsigned char* data)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    return set_param(dfmt, (CS_VOID*)data, CS_NULLTERM);
}

bool SybStatement::set_longchar(int index,
        unsigned char* data, unsigned int len)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    return set_param(dfmt, (CS_VOID*)data, len);
}

bool SybStatement::set_varchar(int index, unsigned char* data)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_VARCHAR varchar;
    CS_INT len = strlen((const char*)data);
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);
    
    if (len > CS_MAX_CHAR) {
        return false;
    } else {
        varchar.len = len;
        memcpy(varchar.str, data, len);
    }

    return set_param(dfmt, (CS_VOID*)&varchar, 1);
}

bool SybStatement::set_varchar(int index,
        unsigned char* data, unsigned int len)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_VARCHAR varchar;
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    if (len > CS_MAX_CHAR) {
        return false;
    } else {
        varchar.len = len;
        memcpy(varchar.str, data, len);
    }

    return set_param(dfmt, (CS_VOID*)&varchar, 1);
}

bool SybStatement::set_unichar(int index,
        unsigned short* data, unsigned int len)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    return set_param(dfmt, (CS_VOID*)data, len * sizeof(unsigned short));
}

bool SybStatement::set_xml(int index, unsigned char* data)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    return set_param(dfmt, (CS_VOID*)data, CS_NULLTERM);
}

bool SybStatement::set_xml(int index,
        unsigned char* data, unsigned int len)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    return set_param(dfmt, (CS_VOID*)data, len);
}

bool SybStatement::set_date(int index, int data)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    return set_param(dfmt, (CS_VOID*)&data, CS_UNUSED);
}

bool SybStatement::set_date(int index, int year, int month, int day)
{
    int days;
    
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }

    days = date_to_days(year, month, day) - 693961;

    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    return set_param(dfmt, (CS_VOID*)&days, CS_UNUSED);
}

bool SybStatement::set_time(int index, int data)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }

    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    return set_param(dfmt, (CS_VOID*)&data, CS_UNUSED);
}

bool SybStatement::set_time(int index, int hour,
        int minutes, int seconds, int ms)
{
    int mseconds;
    
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }

    mseconds = ((hour * 60 + minutes) * 60 + seconds) * 1000 + ms;

    mseconds = mseconds * 3 / 10;

    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    return set_param(dfmt, (CS_VOID*)&mseconds, CS_UNUSED);
}

bool SybStatement::set_datetime(int index, int days, int time)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_DATETIME datetime;
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    datetime.dtdays = days;
    datetime.dttime = time;
    
    return set_param(dfmt, (CS_VOID*)&datetime, CS_UNUSED);
}

bool SybStatement::set_datetime(int index, int year, int month,
        int day, int hour, int minutes, int seconds, int ms)
{
    int days;
    int mseconds;

    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }

    CS_DATETIME datetime;
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    days = date_to_days(year, month, day) - 693961;
    mseconds = ((hour * 60 + minutes) * 60 + seconds) * 1000 + ms;
    datetime.dtdays = days;
    datetime.dttime = mseconds * 3 / 10;

    return set_param(dfmt, (CS_VOID*)&datetime, CS_UNUSED);
}

bool SybStatement::set_datetime4(int index,
        unsigned short days, unsigned short minutes)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_DATETIME4 datetime4;
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    datetime4.days = days;
    datetime4.minutes = minutes;
    
    return set_param(dfmt, (CS_VOID*)&datetime4, CS_UNUSED);
}

bool SybStatement::set_datetime4(int index, int year,
        int month, int day, int hour, int minutes)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }

    CS_DATETIME4 datetime4;
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    datetime4.days = date_to_days(year, month, day) - 693961;
    datetime4.minutes = hour * 60 + minutes;

    return set_param(dfmt, (CS_VOID*)&datetime4, CS_UNUSED);
}

#ifdef SYBASE_OCS15_5
bool SybStatement::set_bigdatetime(int index, unsigned long data)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    return set_param(dfmt, (CS_VOID*)&data, CS_UNUSED);
}

bool SybStatement::set_bigdatetime(int index, int year, int month,
        int day, int hour, int minutes, int seconds, int ms, int Ms)
{
    unsigned long bigdatetime;
    
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }

    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);
    bigdatetime = (unsigned long)date_to_days(year, month, day);
    bigdatetime = bigdatetime * 24 + hour;
    bigdatetime = bigdatetime * 60 + minutes;
    bigdatetime = bigdatetime * 60 + seconds;
    bigdatetime = bigdatetime * 1000 + ms;
    bigdatetime = bigdatetime * 1000 + Ms;

    return set_param(dfmt, (CS_VOID*)&bigdatetime, CS_UNUSED);
}

bool SybStatement::set_bigtime(int index, unsigned long data)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    return set_param(dfmt, (CS_VOID*)&data, CS_UNUSED);
}

bool SybStatement::set_bigtime(int index, int hour,
        int minutes, int seconds, int ms, int Ms)
{
    unsigned long bigtime;
    
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }

    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);
    bigtime = hour * 60 + minutes;
    bigtime = bigtime * 60 + seconds;
    bigtime = bigtime * 1000 + ms;
    bigtime = bigtime * 1000 + Ms;

    return set_param(dfmt, (CS_VOID*)&bigtime, CS_UNUSED);
}
#endif

bool SybStatement::set_tinyint(int index, unsigned char data)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    return set_param(dfmt, (CS_VOID*)&data, CS_UNUSED);
}

bool SybStatement::set_smallint(int index, short data)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    return set_param(dfmt, (CS_VOID*)&data, CS_UNUSED);
}

bool SybStatement::set_int(int index, int data)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    return set_param(dfmt, (CS_VOID*)&data, CS_UNUSED);
}

bool SybStatement::set_bigint(int index, long data)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    return set_param(dfmt, (CS_VOID*)&data, CS_UNUSED);
}

bool SybStatement::set_usmallint(int index, unsigned short data)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    return set_param(dfmt, (CS_VOID*)&data, CS_UNUSED);
}

bool SybStatement::set_uint(int index, unsigned int data)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    return set_param(dfmt, (CS_VOID*)&data, CS_UNUSED);
}

bool SybStatement::set_ubigint(int index, unsigned long data)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    return set_param(dfmt, (CS_VOID*)&data, CS_UNUSED);
}

bool SybStatement::set_decimal(int index, char* data)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_CONTEXT* context;
    CS_DATAFMT srcfmt;
    CS_DECIMAL dest;
    CS_INT destlen;
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE,
            &context, CS_UNUSED, NULL) != CS_SUCCEED) {
        return CS_FAIL;
    }

    memset(&srcfmt, 0, sizeof (CS_DATAFMT));
    srcfmt.datatype = CS_CHAR_TYPE;
    srcfmt.maxlength = strlen(data);
    srcfmt.format = CS_FMT_NULLTERM;
    srcfmt.locale = NULL;

    if (cs_convert(context, &srcfmt, (CS_VOID *)data, dfmt,
            &dest, &destlen) != CS_SUCCEED) {
        return CS_FAIL;
    }

    return set_param(dfmt, (CS_VOID*)&dest, CS_UNUSED);
}

bool SybStatement::set_decimal(int index, unsigned char precision,
        unsigned char scale, unsigned char* data, unsigned int len)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_DECIMAL decimal;
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    if (len > CS_MAX_NUMLEN) {
        return false;
    } else {
        memcpy(decimal.array, data, len);
        decimal.precision = precision;
        decimal.scale = scale;
    }

    return set_param(dfmt, (CS_VOID*)&decimal, CS_UNUSED);
}

bool SybStatement::set_numeric(int index, char* data)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }

    CS_CONTEXT* context;
    CS_DATAFMT srcfmt;
    CS_DECIMAL dest;
    CS_INT destlen;
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE,
            &context, CS_UNUSED, NULL) != CS_SUCCEED) {
        return CS_FAIL;
    }

    memset(&srcfmt, 0, sizeof (CS_DATAFMT));
    srcfmt.datatype = CS_CHAR_TYPE;
    srcfmt.maxlength = strlen(data);
    srcfmt.format = CS_FMT_NULLTERM;
    srcfmt.locale = NULL;

    if (cs_convert(context, &srcfmt, (CS_VOID *)data, dfmt,
            &dest, &destlen) != CS_SUCCEED) {
        return CS_FAIL;
    }

    return set_param(dfmt, (CS_VOID*)&dest, CS_UNUSED);
}

bool SybStatement::set_numeric(int index, unsigned char precision,
        unsigned char scale, unsigned char* data, unsigned int len)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_NUMERIC numeric;
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    if (len > CS_MAX_NUMLEN) {
        return false;
    } else {
        memcpy(numeric.array, data, len);
        numeric.precision = precision;
        numeric.scale = scale;
    }

    return set_param(dfmt, (CS_VOID*)&numeric, CS_UNUSED);
}

bool SybStatement::set_float(int index, double data)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    return set_param(dfmt, (CS_VOID*)&data, CS_UNUSED);
}

bool SybStatement::set_real(int index, float data)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    return set_param(dfmt, (CS_VOID*)&data, CS_UNUSED);
}

bool SybStatement::set_money(int index, char* data)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }

    CS_CONTEXT* context;
    CS_DATAFMT srcfmt;
    CS_MONEY dest;
    CS_INT destlen;
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE,
            &context, CS_UNUSED, NULL) != CS_SUCCEED) {
        return CS_FAIL;
    }

    memset(&srcfmt, 0, sizeof (CS_DATAFMT));
    srcfmt.datatype = CS_CHAR_TYPE;
    srcfmt.maxlength = strlen(data);
    srcfmt.format = CS_FMT_NULLTERM;
    srcfmt.locale = NULL;

    if (cs_convert(context, &srcfmt, (CS_VOID *)data, dfmt,
            &dest, &destlen) != CS_SUCCEED) {
        return CS_FAIL;
    }

    return set_param(dfmt, (CS_VOID*)&dest, CS_UNUSED);
}

bool SybStatement::set_money(int index, int high, unsigned int low)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_MONEY money;
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    money.mnyhigh = high;
    money.mnylow = low;

    return set_param(dfmt, (CS_VOID*)&money, CS_UNUSED);
}

bool SybStatement::set_money4(int index, double data)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {
        return false;
    }
    
    CS_MONEY4 money;
    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    money.mny4 = (int)(data * 10000);

    return set_param(dfmt, (CS_VOID*)&money, CS_UNUSED);
}

bool SybStatement::set_null(int index)
{
    if (!is_prepare_ || index < 1 || index > param_count_) {

        return false;
    }

    CS_DATAFMT* dfmt = desc_dfmt_ + (index - 1);

    if (dfmt->datatype == CS_BIT_TYPE) {
        CS_BIT bit = 0;
        return set_param(dfmt, &bit, CS_UNUSED);
    }
    
    return set_param(dfmt, NULL, CS_UNUSED);
}

bool SybStatement::prepare_release()
{
    if (!is_prepare_) {
        return false;
    }
        
    /** Deallocate the prepared statement */
    if (ct_dynamic(cmd_, CS_DEALLOC, (CS_CHAR*)id_, CS_NULLTERM,
            NULL, CS_UNUSED) != CS_SUCCEED) {
        SysLogger::error("prepare_release: ct_dynamic(CS_DEALLOC) failed");
        return false;
    }
    if (ct_send(cmd_) != CS_SUCCEED) {
        SysLogger::error("prepare_release: ct_send() failed");
        return false;
    }
    if (handle_command_result() != CS_SUCCEED) {
        return false;
    }

    return true;
}

unsigned int SybStatement::get_affected_rows()
{
    return (unsigned int)row_count_;
}

//private functions

void SybStatement::reset()
{
    if(desc_dfmt_) {
        free(desc_dfmt_);
        desc_dfmt_ = NULL;
    }
    row_count_ = 0;
    param_count_ = 0;
    is_prepare_ = false;
    executed_ = false;
}

CS_RETCODE SybStatement::process_describe_reslut()
{
    if(ct_res_info(cmd_, CS_NUMDATA, &param_count_,
            CS_UNUSED, NULL) != CS_SUCCEED) {
        SysLogger::error("process_describe_reslut: ct_res_info() failed");
        return CS_FAIL;
    }

    if (param_count_ <= 0) {
        return CS_SUCCEED;
    }

    desc_dfmt_ = (CS_DATAFMT*)malloc(param_count_ * sizeof(CS_DATAFMT));
    if (desc_dfmt_ == NULL) {
        SysLogger::error("process_describe_reslut: allocate CS_DATAFMT failed");
        return CS_FAIL;
    }

    for (CS_INT i = 0; i < param_count_; ++i) {
        if (ct_describe(cmd_, i + 1, desc_dfmt_ + i) != CS_SUCCEED) {
            SysLogger::error("process_describe_reslut: ct_describe failed");
            free(desc_dfmt_);
            desc_dfmt_ = NULL;
            return CS_FAIL;
        }
    }

    return CS_SUCCEED;
}

CS_RETCODE SybStatement::process_row_result(ei_x_buff* result)
{
    CS_RETCODE retcode;
    
    /** Find out how many columns there are in this result set.*/
    CS_INT column_count =  get_column_count();

    /** Make sure we have at least one column. */
    if (column_count <= 0) {
        SysLogger::error("process_row_result: have no columns");
        return cancel_current();
    }

    /** Allocate memory for the data element to process. */
    COLUMN_DATA* columns =(COLUMN_DATA *)malloc(column_count
            * sizeof (COLUMN_DATA));
    if (columns == NULL) {
        SysLogger::error("process_row_result: allocate COLUMN_DATA failed");
        return cancel_current();
    }

    for (CS_INT i = 0; i < column_count; ++i) {

        /**
         * Get the column description.  ct_describe() fills the
         * datafmt parameter with a description of the column.
         */
        CS_DATAFMT *dfmt = &columns[i].dfmt;

        memset(dfmt, 0, sizeof(CS_DATAFMT));
        if(ct_describe(cmd_, i + 1, dfmt) != CS_SUCCEED) {
            SysLogger::error("process_row_result: ct_describe failed");
            free_column_data(columns, i);
            return cancel_current();
        }

        columns[i].value = alloc_column_value(dfmt);
        if (columns[i].value == NULL) {
            SysLogger::error("process_row_result: alloc_column_value() failed");
            free_column_data(columns, i);
            return cancel_current();
        }

        if (ct_bind(cmd_, i + 1, dfmt,
                (CS_VOID *)columns[i].value,
                &columns[i].valuelen,
                &columns[i].indicator) != CS_SUCCEED) {
            SysLogger::error("process_row_result: ct_bind() failed");
            free_column_data(columns, i);
            return cancel_current();
        }
    }

    retcode = encode_query_result(result, columns, column_count);
    free_column_data(columns, column_count);
    
    return retcode;
}

CS_VOID* SybStatement::alloc_column_value(CS_DATAFMT *dfmt) {
    CS_VOID *value;

    switch (dfmt->datatype) {
        /** Binary types */
        case CS_BINARY_TYPE:
            value = (CS_VOID*)malloc(dfmt->maxlength * sizeof(CS_BINARY));
            break;

        case CS_LONGBINARY_TYPE:
            value = (CS_VOID*)malloc(dfmt->maxlength * sizeof(CS_LONGBINARY));
            break;

        case CS_VARBINARY_TYPE:
            value = (CS_VOID*)malloc(sizeof(CS_VARBINARY));
            break;

        /** Bit types */
        case CS_BIT_TYPE:
            value = (CS_VOID*)malloc(sizeof(CS_BIT));
            break;

        /** Character types */
        case CS_CHAR_TYPE:
            value = (CS_VOID*)malloc(dfmt->maxlength * sizeof(CS_CHAR));
            break;

        case CS_LONGCHAR_TYPE:
            value = (CS_VOID*)malloc(dfmt->maxlength * sizeof(CS_LONGCHAR));
            break;

        case CS_VARCHAR_TYPE:
            value = (CS_VOID*)malloc(sizeof(CS_VARCHAR));
            break;

        case CS_UNICHAR_TYPE:
            value = (CS_VOID*)malloc(dfmt->maxlength * sizeof(CS_UNICHAR));
            break;

        case CS_XML_TYPE:
            value = (CS_VOID*)malloc(dfmt->maxlength * sizeof(CS_XML));
            break;

        /** Datetime types */
        case CS_DATE_TYPE:
            value = (CS_VOID*)malloc(sizeof(CS_DATE));
            break;

        case CS_TIME_TYPE:
            value = (CS_VOID*)malloc(sizeof(CS_TIME));
            break;

        case CS_DATETIME_TYPE:
            value = (CS_VOID*)malloc(sizeof(CS_DATETIME));
            break;

        case CS_DATETIME4_TYPE:
            value = (CS_VOID*)malloc(sizeof(CS_DATETIME4));
            break;
#ifdef SYBASE_OCS15_5
        case CS_BIGDATETIME_TYPE:
            value = (CS_VOID*)malloc(sizeof(CS_BIGDATETIME));
            break;

        case CS_BIGTIME_TYPE:
            value = (CS_VOID*)malloc(sizeof(CS_BIGTIME));
            break;
#endif
        /** Numeric types */
        case CS_TINYINT_TYPE:
            value = (CS_VOID*)malloc(sizeof(CS_TINYINT));
            break;

        case CS_SMALLINT_TYPE:
            value = (CS_VOID*)malloc(sizeof(CS_SMALLINT));
            break;

        case CS_INT_TYPE:
            value = (CS_VOID*)malloc(sizeof(CS_INT));
            break;

        case CS_BIGINT_TYPE:
            value = (CS_VOID*)malloc(sizeof(CS_BIGINT));
            break;

        case CS_USMALLINT_TYPE:
            value = (CS_VOID*)malloc(sizeof(CS_USMALLINT));
            break;

        case CS_UINT_TYPE:
            value = (CS_VOID*)malloc(sizeof(CS_UINT));
            break;

        case CS_UBIGINT_TYPE:
            value = (CS_VOID*)malloc(sizeof(CS_UBIGINT));
            break;

        case CS_DECIMAL_TYPE:
            value = (CS_VOID*)malloc(sizeof(CS_DECIMAL));
            break;

        case CS_NUMERIC_TYPE:
            value = (CS_VOID*)malloc(sizeof(CS_NUMERIC));
            break;

        case CS_FLOAT_TYPE:
            value = (CS_VOID*)malloc(sizeof(CS_FLOAT));
            break;

        case CS_REAL_TYPE:
            value = (CS_VOID*)malloc(sizeof(CS_REAL));
            break;

        /** Money types */
        case CS_MONEY_TYPE:
            value = (CS_VOID*)malloc(sizeof(CS_MONEY));
            break;

        case CS_MONEY4_TYPE:
            value = (CS_VOID*)malloc(sizeof(CS_MONEY4));
            break;

        /** Text and image types */
        case CS_TEXT_TYPE:
            value = (CS_VOID*)malloc(dfmt->maxlength * sizeof(CS_TEXT));
            break;

        case CS_IMAGE_TYPE:
            value = (CS_VOID*)malloc(dfmt->maxlength * sizeof(CS_IMAGE));
            break;

        case CS_UNITEXT_TYPE:
            value = (CS_VOID*)malloc(dfmt->maxlength * sizeof(CS_UNITEXT));
            break;
        default:
            value = NULL;
            break;
    }

    return value;
}

CS_VOID SybStatement::free_column_data(COLUMN_DATA *columns, CS_INT size)
{
    if (columns) {
        for (CS_INT i = 0; i < size; ++i) {
            free(columns[i].value);
            columns[i].value = NULL;
        }
        free(columns);
    }
}

CS_INT SybStatement::get_row_count()
{
    CS_INT row_count = 0;
    if(ct_res_info(cmd_, CS_ROW_COUNT, &row_count,
            CS_UNUSED, NULL) != CS_SUCCEED) {
        SysLogger::error("get_row_count: ct_res_info() failed");
        return 0;
    }
    return row_count;
}

CS_INT SybStatement::get_column_count()
{
    CS_INT column_count = 0;
    if(ct_res_info(cmd_, CS_NUMDATA, &column_count,
            CS_UNUSED, NULL) != CS_SUCCEED) {
        SysLogger::error("get_column_count: ct_res_info() failed");
        return 0;
    }
    return column_count;
}

CS_RETCODE SybStatement::cancel_current()
{
   return ct_cancel(NULL, cmd_, CS_CANCEL_CURRENT);
}

CS_RETCODE SybStatement::cancel_all()
{
   return ct_cancel(NULL, cmd_, CS_CANCEL_ALL);
}

CS_INT SybStatement::get_column_length(CS_DATAFMT *dfmt)
{
    CS_INT len;

    switch (dfmt->datatype){
        case CS_CHAR_TYPE:
        case CS_LONGCHAR_TYPE:
        case CS_VARCHAR_TYPE:
        case CS_TEXT_TYPE:
        case CS_IMAGE_TYPE:
            len = MIN(dfmt->maxlength, MAX_CHAR_BUF);
            break;

        case CS_UNICHAR_TYPE:
            len = MIN((dfmt->maxlength / 2), MAX_CHAR_BUF);
            break;

        case CS_BINARY_TYPE:
        case CS_VARBINARY_TYPE:
            len = MIN((2 * dfmt->maxlength) + 2, MAX_CHAR_BUF);
            break;

        case CS_BIT_TYPE:
        case CS_TINYINT_TYPE:
            len = 3;
            break;

        case CS_SMALLINT_TYPE:
            len = 6;
            break;

        case CS_INT_TYPE:
            len = 11;
            break;

        case CS_REAL_TYPE:
        case CS_FLOAT_TYPE:
            len = 20;
            break;

        case CS_MONEY_TYPE:
        case CS_MONEY4_TYPE:
            len = 24;
            break;

        case CS_DATETIME_TYPE:
        case CS_DATETIME4_TYPE:
            len = 30;
            break;

        case CS_NUMERIC_TYPE:
        case CS_DECIMAL_TYPE:
            len = (CS_MAX_PREC + 2);
            break;

        default:
            len = 12;
            break;
    }

    return MAX((CS_INT)(strlen(dfmt->name) + 1), len);
}

CS_CHAR* SybStatement::get_agg_op_name(CS_INT op)
{
    switch ((int)op)
    {
        case CS_OP_SUM:
            return "sum";
            break;

        case CS_OP_AVG:
            return "avg";
            break;

        case CS_OP_COUNT:
            return "count";
            break;

        case CS_OP_MIN:
            return "min";
            break;

        case CS_OP_MAX:
            return "max";
            break;

        default:
            return "unknown";
            break;
    }
    return "";
}

CS_RETCODE SybStatement::compute_info(CS_INT index, CS_DATAFMT *data_fmt)
{
    CS_INT agg_op  = 0;

    if (ct_compute_info(cmd_, CS_COMP_OP, index, &agg_op,
        CS_UNUSED, &data_fmt->namelen) != CS_SUCCEED) {
        return CS_FAIL;
    } else {
        strcpy(data_fmt->name, get_agg_op_name(agg_op));
    }

    return CS_SUCCEED;
}

CS_RETCODE SybStatement::handle_describe_result() {
    CS_RETCODE retcode;
    CS_INT restype;
    CS_RETCODE query_code = CS_SUCCEED;

    /** Examine the results coming back. If any errors are seen, the query
     * result code (which we will return from this function) will be set to FAIL.
     */
    while ((retcode = ct_results(cmd_, &restype)) == CS_SUCCEED) {
        switch(restype) {
            case CS_DESCRIBE_RESULT:
                if (process_describe_reslut() != CS_SUCCEED) {
                    return CS_FAIL;
                }
                break;

            case CS_CMD_SUCCEED:
            case CS_CMD_DONE:
                break;

            default:
                /** Unexpected result type. */
                query_code = CS_FAIL;
                break;
        }

        if (query_code == CS_FAIL) {

            /** Terminate results processing and break out of the results loop */
            if (ct_cancel(NULL, cmd_, CS_CANCEL_ALL) != CS_SUCCEED) {
                SysLogger::error("handle_describe_result: ct_cancel() failed");
            }
            break;
        }
    }

    if (retcode != CS_END_RESULTS || query_code != CS_SUCCEED) {
        return CS_FAIL;
    }

    return CS_SUCCEED;
}

CS_RETCODE SybStatement::handle_command_result()
{
    CS_RETCODE retcode;
    CS_INT restype;
    CS_RETCODE query_code = CS_SUCCEED;
    row_count_ = 0;

    /** Examine the results coming back. If any errors are seen, the query
     * result code (which we will return from this function) will be set to FAIL.
     */
    while ((retcode = ct_results(cmd_, &restype)) == CS_SUCCEED) {
        switch(restype) {
            case CS_CMD_SUCCEED:               
            case CS_CMD_DONE:
                break;

            default:
                /** Unexpected result type. */
                query_code = CS_FAIL;
                break;
        }

        if (query_code == CS_FAIL) {

            /** Terminate results processing and break out of the results loop */
            if (ct_cancel(NULL, cmd_, CS_CANCEL_ALL) != CS_SUCCEED) {
                SysLogger::error("handle_command_result: ct_cancel() failed");
            }
            break;
        }
    }

    if (retcode != CS_END_RESULTS || query_code != CS_SUCCEED) {
        return CS_FAIL;
    }

    return CS_SUCCEED;
}

CS_RETCODE SybStatement::handle_sql_result(ei_x_buff* result)
{
    CS_RETCODE retcode;
    CS_INT restype;
    CS_RETCODE query_code = CS_SUCCEED;
    row_count_ = 0;
    CS_INT is_query = 0;

    /** Examine the results coming back. If any errors are seen, the query
     * result code (which we will return from this function) will be set to FAIL.
     */
    while ((retcode = ct_results(cmd_, &restype)) == CS_SUCCEED) {
        switch(restype) {
            case CS_COMPUTE_RESULT:
            case CS_CURSOR_RESULT:
            case CS_PARAM_RESULT:
            case CS_STATUS_RESULT:
            case CS_ROW_RESULT:
                is_query = 1;
                if (process_row_result(result) != CS_SUCCEED) {
                    return false;
                }
                break;

            case CS_CMD_SUCCEED:
                break;
                
            case CS_CMD_DONE:
                row_count_ = get_row_count();
                break;

            default:
                /** Unexpected result type. */
                query_code = CS_FAIL;
                break;
        }

        if (query_code == CS_FAIL) {

            /** Terminate results processing and break out of the results loop */
            if (ct_cancel(NULL, cmd_, CS_CANCEL_ALL) != CS_SUCCEED) {
                SysLogger::error("handle_sql_result: ct_cancel() failed");
            }
            break;
        }
    }

    if (retcode != CS_END_RESULTS || query_code != CS_SUCCEED) {
        return CS_FAIL;
    }

    if (is_query ==0) {
        return  encode_update_result(result, row_count_);
    }

    return CS_SUCCEED;
}

CS_RETCODE SybStatement::encode_query_result(ei_x_buff* result,
        COLUMN_DATA* columns, CS_INT column_count) {
    CS_RETCODE retcode;
    CS_INT rows_read;
    CS_INT row_count = 0;
    
    ei_x_new_with_version(result);
    ei_x_encode_tuple_header(result, 2);
    ei_x_encode_atom(result, "ok");

    int pos = result->index;
    ei_x_encode_list_header(result, 1);

    /** Fetch the rows.  Loop while ct_fetch() returns CS_SUCCEED or
     * CS_ROW_FAIL
     */
    while(((retcode = ct_fetch(cmd_, CS_UNUSED, CS_UNUSED, CS_UNUSED,
            &rows_read)) == CS_SUCCEED) || (retcode == CS_ROW_FAIL)) {

        /** Increment our row count by the number of rows just fetched. */
        row_count = row_count + rows_read;
        

        /** Check if we hit a recoverable error. */
        if (retcode == CS_ROW_FAIL){
            SysLogger::error("encode_query_result: Error on row %d", row_count);
            ei_x_encode_atom(result, "error");
        } else {
            ei_x_encode_list_header(result, column_count);

            /**
             * We have a row. Loop through the columns encode the
             * column values.
             */
            for (CS_INT i = 0; i < column_count; ++i) {
                if (encode_column_data(result, columns + i) != CS_SUCCEED) {
                    return CS_FAIL;
                }
            }
            ei_x_encode_empty_list(result);
        }
    }
    ei_x_encode_empty_list(result);

    ei_x_buff x;
    ei_x_new(&x);
    ei_x_encode_list_header(&x, row_count);
    memcpy(result->buff + pos, x.buff, x.index);
    ei_x_free(&x);

    switch (retcode) {
        case CS_END_DATA:
            retcode = CS_SUCCEED;
            break;

        case CS_FAIL:
            SysLogger::error("encode_query_result: ct_fetch() failed");
            break;

        default:
            /** We got an unexpected return value. */
            SysLogger::error("encode_query_result: ct_fetch() returned an "
                             "expected retcode:%d", retcode);
            break;
    }

    return retcode;
}

CS_RETCODE SybStatement::encode_update_result(ei_x_buff* result, CS_INT row_count)
{
   ei_x_new_with_version(result);
   ei_x_encode_tuple_header(result, 2);
   ei_x_encode_atom(result, "ok");
   ei_x_encode_long(result, row_count);

   return CS_SUCCEED;
}

CS_RETCODE SybStatement::encode_column_data(ei_x_buff* result, COLUMN_DATA *column)
{
    CS_RETCODE retcode;
    CS_DATAFMT *dfmt = &column->dfmt;

    if (column->indicator == 0) {
        switch (dfmt->datatype) {
            
            /** Binary types */
            case CS_BINARY_TYPE:
                retcode = encode_binary(result, dfmt, (CS_BINARY*)column->value, column->valuelen);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_binary() failed");
                    return retcode;
                }
                break;
                
            case CS_LONGBINARY_TYPE:
                retcode = encode_longbinary(result, dfmt, (CS_LONGBINARY*)column->value, column->valuelen);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_longbinary() failed");
                    return retcode;
                }
                break;

            case CS_VARBINARY_TYPE:
                retcode = encode_varbinary(result, dfmt, (CS_VARBINARY*)column->value);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_varbinary() failed");
                    return retcode;
                }
                break;

            /** Bit types */
            case CS_BIT_TYPE:
                retcode = encode_bit(result, dfmt, (CS_BIT*)column->value);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_bit() failed");
                    return retcode;
                }
                break;

            /** Character types */
            case CS_CHAR_TYPE:
                retcode = encode_char(result, dfmt, (CS_CHAR*)column->value, column->valuelen);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_char() failed");
                    return retcode;
                }
                break;
                
            case CS_LONGCHAR_TYPE:
                retcode = encode_longchar(result, dfmt, (CS_LONGCHAR*)column->value, column->valuelen);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_longchar() failed");
                    return retcode;
                }
                break;

            case CS_VARCHAR_TYPE:
                retcode = encode_varchar(result, dfmt, (CS_VARCHAR*)column->value);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_varchar() failed");
                    return retcode;
                }
            case CS_UNICHAR_TYPE:
                retcode = encode_unichar(result, dfmt, (CS_UNICHAR*)column->value, column->valuelen);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_unichar() failed");
                    return retcode;
                }
                break;
                
            case CS_XML_TYPE:
                retcode = encode_xml(result, dfmt, (CS_XML*)column->value, column->valuelen);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_xml() failed");
                    return retcode;
                }
                break;

            /** Datetime types */
            case CS_DATE_TYPE:
                retcode = encode_date(result, dfmt, (CS_DATE*)column->value);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_date() failed");
                    return retcode;
                }
                break;

            case CS_TIME_TYPE:
                retcode = encode_time(result, dfmt, (CS_TIME*)column->value);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_time() failed");
                    return retcode;
                }
                break;

            case CS_DATETIME_TYPE:
                retcode = encode_datetime(result, dfmt, (CS_DATETIME*)column->value);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_datetime() failed");
                    return retcode;
                }
                break;

            case CS_DATETIME4_TYPE:
                retcode = encode_datetime4(result, dfmt, (CS_DATETIME4*)column->value);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_datetime4() failed");
                    return retcode;
                }
                break;

#ifdef SYBASE_OCS15_5
            case CS_BIGDATETIME_TYPE:
                retcode = encode_bigdatetime(result, dfmt, (CS_BIGDATETIME*)column->value);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_bigdatetime() failed");
                    return retcode;
                }
                break;

            case CS_BIGTIME_TYPE:
                retcode = encode_bigtime(result, dfmt, (CS_BIGTIME*)column->value);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_bigtime() failed");
                    return retcode;
                }
                break;
#endif
                
            /** Numeric types */
            case CS_TINYINT_TYPE:
                retcode = encode_tinyint(result, dfmt, (CS_TINYINT*)column->value);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_tinyint() failed");
                    return retcode;
                }
                break;

            case CS_SMALLINT_TYPE:
                retcode = encode_smallint(result, dfmt, (CS_SMALLINT*)column->value);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_smallint() failed");
                    return retcode;
                }
                break;

            case CS_INT_TYPE:
                retcode = encode_int(result, dfmt, (CS_INT*)column->value);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_int() failed");
                    return retcode;
                }
                break;
                
            case CS_BIGINT_TYPE:
                retcode = encode_bigint(result, dfmt, (CS_BIGINT*)column->value);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_bigint() failed");
                    return retcode;
                }
                break;
                
            case CS_USMALLINT_TYPE:
                retcode = encode_usmallint(result, dfmt, (CS_USMALLINT*)column->value);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_usmallint() failed");
                    return retcode;
                }
                break;

            case CS_UINT_TYPE:
                retcode = encode_uint(result, dfmt, (CS_UINT*)column->value);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_uint() failed");
                    return retcode;
                }
                break;
                
            case CS_UBIGINT_TYPE:
                retcode = encode_ubigint(result, dfmt, (CS_UBIGINT*)column->value);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_ubigint() failed");
                    return retcode;
                }
                break;

            case CS_DECIMAL_TYPE:
                retcode = encode_decimal(result, dfmt, (CS_DECIMAL*)column->value);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_decimal() failed");
                    return retcode;
                }
                break;
                
            case CS_NUMERIC_TYPE:
                retcode = encode_numeric(result, dfmt, (CS_NUMERIC*)column->value);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_numeric() failed");
                    return retcode;
                }
                break;

            case CS_FLOAT_TYPE:
                retcode = encode_float(result, dfmt, (CS_FLOAT*)column->value);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_float() failed");
                    return retcode;
                }
                break;
                
            case CS_REAL_TYPE:
                retcode = encode_real(result, dfmt, (CS_REAL*)column->value);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_real() failed");
                    return retcode;
                }
                break;

            /** Money types */
            case CS_MONEY_TYPE:
                retcode = encode_money(result, dfmt, (CS_MONEY*)column->value);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_money() failed");
                    return retcode;
                }
                break;

            case CS_MONEY4_TYPE:
                retcode = encode_money4(result, dfmt, (CS_MONEY4*)column->value);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_money4() failed");
                    return retcode;
                }
                break;

            /** Text and image types */
            case CS_TEXT_TYPE:
                retcode = encode_text(result, dfmt, (CS_TEXT*)column->value, column->valuelen);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_text() failed");
                    return retcode;
                }
                break;
                
            case CS_IMAGE_TYPE:
                retcode = encode_image(result, dfmt, (CS_IMAGE*)column->value, column->valuelen);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_image() failed");
                    return retcode;
                }
                break;
                
            case CS_UNITEXT_TYPE:
                retcode = encode_unitext(result, dfmt, (CS_UNITEXT*)column->value, column->valuelen);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_unitext() failed");
                    return retcode;
                }
                break;

            default:
                retcode = encode_unknown(result);
                if (retcode != CS_SUCCEED) {
                    SysLogger::error("encode_column_data: encode_unknown() failed");
                    return retcode;
                }
                break;
        }
    } else if (column->indicator == -1) {
        retcode = encode_null(result);
        if (retcode != CS_SUCCEED) {
            SysLogger::error("encode_column_data: encode_null() failed");
            return retcode;
        }
    }
    else {
        
        /* Buffer overflow */
        retcode = encode_overflow(result);
        if (retcode != CS_SUCCEED) {
            SysLogger::error("encode_column_data: encode_overflow() failed");
            return retcode;
        }
    }

    return retcode;
}

bool SybStatement::set_param(CS_DATAFMT* dfmt, CS_VOID* data, CS_INT len)
{
    if (!executed_) {
        if (ct_dynamic(cmd_, CS_EXECUTE, id_, CS_NULLTERM,
                NULL, CS_UNUSED) != CS_SUCCEED) {
            SysLogger::error("set_param: ct_dynamic() failed");
            return false;
        }
        executed_ = true;
    }

    dfmt->status = CS_INPUTVALUE;

    if (ct_param(cmd_, dfmt, (CS_VOID *)data, len, 0) != CS_SUCCEED) {
        SysLogger::error("set_param: ct_param() failed");
        return false;
    }
    
    return true;
}

CS_RETCODE SybStatement::encode_binary(ei_x_buff* x,
        CS_DATAFMT* dfmt, CS_BINARY* v, CS_INT len)
{
    return ei_x_encode_binary(x, (const void*)v,
            (long)len) == 0 ? CS_SUCCEED:CS_FAIL;
}

CS_RETCODE SybStatement::encode_longbinary(ei_x_buff* x, 
        CS_DATAFMT* dfmt, CS_LONGBINARY* v, CS_INT len)
{
    return ei_x_encode_binary(x, (const void*)v,
            (long)len) == 0 ? CS_SUCCEED:CS_FAIL;
}

CS_RETCODE SybStatement::encode_varbinary(ei_x_buff* x, 
        CS_DATAFMT* dfmt, CS_VARBINARY* v)
{
    return ei_x_encode_binary(x, (const void*)v->array,
            (long)v->len) == 0 ? CS_SUCCEED:CS_FAIL;
}

CS_RETCODE SybStatement::encode_bit(ei_x_buff* x, 
        CS_DATAFMT* dfmt, CS_BIT* v)
{
    return ei_x_encode_char(x, (char)*v) == 0 ? CS_SUCCEED:CS_FAIL;
}

CS_RETCODE SybStatement::encode_char(ei_x_buff* x, 
        CS_DATAFMT* dfmt, CS_CHAR* v, CS_INT len)
{
    return encode_string(x, (const char*)v, len) == 0 ? CS_SUCCEED:CS_FAIL;
}

CS_RETCODE SybStatement::encode_longchar(ei_x_buff* x, 
        CS_DATAFMT* dfmt, CS_LONGCHAR* v, CS_INT len)
{
    return encode_string(x, (const char*)v, len) == 0 ? CS_SUCCEED:CS_FAIL;
}

CS_RETCODE SybStatement::encode_varchar(ei_x_buff* x, 
        CS_DATAFMT* dfmt, CS_VARCHAR* v)
{
    return encode_string(x, (const char*)v->str,
            (int)v->len) == 0 ? CS_SUCCEED:CS_FAIL;
}

CS_RETCODE SybStatement::encode_unichar(ei_x_buff* x, 
        CS_DATAFMT* dfmt, CS_UNICHAR* v, CS_INT len)
{
    CS_INT i;

    len = len / sizeof(CS_UNICHAR);
    if (ei_x_encode_list_header(x, (long)len)) {
        return CS_FAIL;
    }
    for (i = 0; i < len; ++i) {
        if(ei_x_encode_ulong(x, (unsigned long)v[i])) {
            return CS_FAIL;
        }
    }
    ei_x_encode_empty_list(x);

    return CS_SUCCEED;
}

CS_RETCODE SybStatement::encode_xml(ei_x_buff* x, 
        CS_DATAFMT* dfmt, CS_XML* v, CS_INT len)
{
    return ei_x_encode_string_len(x, (const char*)v,
            len) == 0 ? CS_SUCCEED:CS_FAIL;
}

CS_RETCODE SybStatement::encode_date(ei_x_buff* x,
        CS_DATAFMT* dfmt, CS_DATE* v)
{
    CS_CONTEXT* context;
    CS_DATEREC daterec;

    if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE,
            &context, CS_UNUSED, NULL) != CS_SUCCEED) {
        return CS_FAIL;
    }

    if (cs_dt_crack(context, dfmt->datatype,
            (CS_VOID*)v, &daterec) != CS_SUCCEED) {
        return CS_FAIL;
    }

    ei_x_encode_tuple_header(x, 2);
    ei_x_encode_atom(x, "date");

    ei_x_encode_tuple_header(x, 3);
    ei_x_encode_long(x, daterec.dateyear);
    ei_x_encode_long(x, daterec.datemonth + 1);
    ei_x_encode_long(x, daterec.datedmonth);

    return CS_SUCCEED;
}

CS_RETCODE SybStatement::encode_time(ei_x_buff* x,
        CS_DATAFMT* dfmt, CS_TIME* v)
{
    CS_CONTEXT* context;
    CS_DATEREC daterec;

    if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE,
            &context, CS_UNUSED, NULL) != CS_SUCCEED) {
        return CS_FAIL;
    }

    if (cs_dt_crack(context, dfmt->datatype,
            (CS_VOID*)v, &daterec) != CS_SUCCEED) {
        return CS_FAIL;
    }

    ei_x_encode_tuple_header(x, 2);
    ei_x_encode_atom(x, "time");

    ei_x_encode_tuple_header(x, 4);
    ei_x_encode_long(x, daterec.datehour);
    ei_x_encode_long(x, daterec.dateminute);
    ei_x_encode_long(x, daterec.datesecond);
    ei_x_encode_long(x, daterec.datemsecond);

    return CS_SUCCEED;
}

CS_RETCODE SybStatement::encode_datetime(ei_x_buff* x,
        CS_DATAFMT* dfmt, CS_DATETIME* v)
{
    CS_CONTEXT* context;
    CS_DATEREC daterec;

    if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE,
            &context, CS_UNUSED, NULL) != CS_SUCCEED) {
        return CS_FAIL;
    }

    if (cs_dt_crack(context, dfmt->datatype,
            (CS_VOID*)v, &daterec) != CS_SUCCEED) {
        return CS_FAIL;
    }

    ei_x_encode_tuple_header(x, 2);
    ei_x_encode_atom(x, "datetime");

    ei_x_encode_tuple_header(x, 2);

    ei_x_encode_tuple_header(x, 3);
    ei_x_encode_long(x, daterec.dateyear);
    ei_x_encode_long(x, daterec.datemonth + 1);
    ei_x_encode_long(x, daterec.datedmonth);

    ei_x_encode_tuple_header(x, 4);
    ei_x_encode_long(x, daterec.datehour);
    ei_x_encode_long(x, daterec.dateminute);
    ei_x_encode_long(x, daterec.datesecond);
    ei_x_encode_long(x, daterec.datemsecond);

    return CS_SUCCEED;
}

CS_RETCODE SybStatement::encode_datetime4(ei_x_buff* x,
        CS_DATAFMT* dfmt, CS_DATETIME4* v)
{
    CS_CONTEXT* context;
    CS_DATEREC daterec;

    if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE,
            &context, CS_UNUSED, NULL) != CS_SUCCEED) {
        return CS_FAIL;
    }

    if (cs_dt_crack(context, dfmt->datatype,
            (CS_VOID*)v, &daterec) != CS_SUCCEED) {
        return CS_FAIL;
    }

    ei_x_encode_tuple_header(x, 2);
    ei_x_encode_atom(x, "smalldatetime");

    ei_x_encode_tuple_header(x, 2);

    ei_x_encode_tuple_header(x, 3);
    ei_x_encode_long(x, daterec.dateyear);
    ei_x_encode_long(x, daterec.datemonth + 1);
    ei_x_encode_long(x, daterec.datedmonth);

    ei_x_encode_tuple_header(x, 2);
    ei_x_encode_long(x, daterec.datehour);
    ei_x_encode_long(x, daterec.dateminute);

    return CS_SUCCEED;
}

#ifdef SYBASE_OCS15_5
CS_RETCODE SybStatement::encode_bigdatetime(ei_x_buff* x,
        CS_DATAFMT* dfmt, CS_BIGDATETIME* v)
{
    CS_CONTEXT* context;
    CS_DATEREC daterec;

    if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE,
            &context, CS_UNUSED, NULL) != CS_SUCCEED) {
        return CS_FAIL;
    }

    if (cs_dt_crack(context, dfmt->datatype,
            (CS_VOID*)v, &daterec) != CS_SUCCEED) {
        return CS_FAIL;
    }

    ei_x_encode_tuple_header(x, 2);
    ei_x_encode_atom(x, "bigdatetime");

    ei_x_encode_tuple_header(x, 2);

    ei_x_encode_tuple_header(x, 3);
    ei_x_encode_long(x, daterec.dateyear);
    ei_x_encode_long(x, daterec.datemonth + 1);
    ei_x_encode_long(x, daterec.datedmonth);

    ei_x_encode_tuple_header(x, 5);
    ei_x_encode_long(x, daterec.datehour);
    ei_x_encode_long(x, daterec.dateminute);
    ei_x_encode_long(x, daterec.datesecond);
    ei_x_encode_long(x, daterec.datesecfrac / 1000);
    ei_x_encode_long(x, daterec.datesecfrac % 1000);

    return CS_SUCCEED;
}

CS_RETCODE SybStatement::encode_bigtime(ei_x_buff* x,
        CS_DATAFMT* dfmt, CS_BIGTIME* v)
{
    CS_CONTEXT* context;
    CS_DATEREC daterec;

    if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE,
            &context, CS_UNUSED, NULL) != CS_SUCCEED) {
        return CS_FAIL;
    }

    if (cs_dt_crack(context, dfmt->datatype,
            (CS_VOID*)v, &daterec) != CS_SUCCEED) {
        return CS_FAIL;
    }
    
    ei_x_encode_tuple_header(x, 2);
    ei_x_encode_atom(x, "bigtime");

    ei_x_encode_tuple_header(x, 5);
    ei_x_encode_long(x, daterec.datehour);
    ei_x_encode_long(x, daterec.dateminute);
    ei_x_encode_long(x, daterec.datesecond);
    ei_x_encode_long(x, daterec.datesecfrac / 1000);
    ei_x_encode_long(x, daterec.datesecfrac % 1000);

    return CS_SUCCEED;
}
#endif

CS_RETCODE SybStatement::encode_tinyint(ei_x_buff* x, 
        CS_DATAFMT* dfmt, CS_TINYINT* v)
{
    return ei_x_encode_char(x, (char)*v) == 0 ? CS_SUCCEED:CS_FAIL;
}

CS_RETCODE SybStatement::encode_smallint(ei_x_buff* x, 
        CS_DATAFMT* dfmt, CS_SMALLINT* v)
{
    return ei_x_encode_long(x, (long)*v) == 0 ? CS_SUCCEED:CS_FAIL;
}

CS_RETCODE SybStatement::encode_int(ei_x_buff* x, 
        CS_DATAFMT* dfmt, CS_INT* v)
{    
    return ei_x_encode_long(x, (long)*v) == 0 ? CS_SUCCEED:CS_FAIL;
}

CS_RETCODE SybStatement::encode_bigint(ei_x_buff* x, 
        CS_DATAFMT* dfmt, CS_BIGINT* v)
{   
    return ei_x_encode_long(x, (long)*v) == 0 ? CS_SUCCEED:CS_FAIL;
}

CS_RETCODE SybStatement::encode_usmallint(ei_x_buff* x, 
        CS_DATAFMT* dfmt, CS_USMALLINT* v)
{   
    return ei_x_encode_ulong(x, (unsigned long)*v) == 0 ? CS_SUCCEED:CS_FAIL;
}

CS_RETCODE SybStatement::encode_uint(ei_x_buff* x, 
        CS_DATAFMT* dfmt, CS_UINT* v)
{   
    return ei_x_encode_ulong(x, (unsigned long)*v) == 0 ? CS_SUCCEED:CS_FAIL;
}

CS_RETCODE SybStatement::encode_ubigint(ei_x_buff* x, 
        CS_DATAFMT* dfmt, CS_UBIGINT* v)
{
    return ei_x_encode_ulong(x, (unsigned long)*v) == 0 ? CS_SUCCEED:CS_FAIL;
}

CS_RETCODE SybStatement::encode_decimal(ei_x_buff* x,
        CS_DATAFMT* dfmt, CS_DECIMAL* v)
{
    CS_CONTEXT* context;
    CS_DATAFMT destfmt;
    CS_CHAR dest[79];
    CS_INT destlen;

    if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE,
            &context, CS_UNUSED, NULL) != CS_SUCCEED) {
        return CS_FAIL;
    }

    memset(&destfmt, 0, sizeof (CS_DATAFMT));
    destfmt.datatype = CS_CHAR_TYPE;
    destfmt.maxlength = sizeof(dest);
    destfmt.locale = NULL;

    if (cs_convert(context, dfmt, (CS_VOID *)v, &destfmt,
            dest, &destlen) != CS_SUCCEED) {
        return CS_FAIL;
    }

    ei_x_encode_tuple_header(x, 2);
    ei_x_encode_atom(x, "number");

    ei_x_encode_string_len(x, (const char*)dest, destlen);

    return CS_SUCCEED;
}

CS_RETCODE SybStatement::encode_numeric(ei_x_buff* x,
        CS_DATAFMT* dfmt, CS_NUMERIC* v)
{
    CS_CONTEXT* context;
    CS_DATAFMT destfmt;
    CS_CHAR dest[79];
    CS_INT destlen;

    if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE,
            &context, CS_UNUSED, NULL) != CS_SUCCEED) {
        return CS_FAIL;
    }

    memset(&destfmt, 0, sizeof (CS_DATAFMT));
    destfmt.datatype = CS_CHAR_TYPE;
    destfmt.maxlength = sizeof(dest);
    destfmt.locale = NULL;

    if (cs_convert(context, dfmt, (CS_VOID *)v, &destfmt,
            dest, &destlen) != CS_SUCCEED) {
        return CS_FAIL;
    }

    ei_x_encode_tuple_header(x, 2);
    ei_x_encode_atom(x, "number");

    ei_x_encode_string_len(x, (const char*)dest, destlen);

    return CS_SUCCEED;
}

CS_RETCODE SybStatement::encode_float(ei_x_buff* x, 
        CS_DATAFMT* dfmt, CS_FLOAT* v)
{   
    return ei_x_encode_double(x, *v) == 0 ? CS_SUCCEED:CS_FAIL;
}

CS_RETCODE SybStatement::encode_real(ei_x_buff* x, 
        CS_DATAFMT* dfmt, CS_REAL* v)
{
    return ei_x_encode_double(x, *v) == 0 ? CS_SUCCEED:CS_FAIL;
}

CS_RETCODE SybStatement::encode_money(ei_x_buff* x,
        CS_DATAFMT* dfmt, CS_MONEY* v)
{
    CS_CONTEXT* context;
    CS_DATAFMT destfmt;
    CS_CHAR dest[24];
    CS_INT destlen;

    if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE,
            &context, CS_UNUSED, NULL) != CS_SUCCEED) {
        return CS_FAIL;
    }

    memset(&destfmt, 0, sizeof (CS_DATAFMT));
    destfmt.datatype = CS_CHAR_TYPE;
    destfmt.maxlength = sizeof(dest);
    destfmt.locale = NULL;

    if (cs_convert(context, dfmt, (CS_VOID *)v, &destfmt,
            dest, &destlen) != CS_SUCCEED) {
        return CS_FAIL;
    }

    ei_x_encode_tuple_header(x, 2);
    ei_x_encode_atom(x, "number");

    ei_x_encode_string_len(x, (const char*)dest, destlen);
    
    return CS_SUCCEED;
}

CS_RETCODE SybStatement::encode_money4(ei_x_buff* x,
        CS_DATAFMT* dfmt, CS_MONEY4* v)
{
    CS_CONTEXT* context;
    CS_DATAFMT destfmt;
    CS_FLOAT dest;
    CS_INT destlen;

    if (ct_con_props(conn_, CS_GET, CS_PARENT_HANDLE,
            &context, CS_UNUSED, NULL) != CS_SUCCEED) {
        return CS_FAIL;
    }

    memset(&destfmt, 0, sizeof (CS_DATAFMT));
    destfmt.datatype = CS_FLOAT_TYPE;
    destfmt.maxlength = sizeof(CS_FLOAT);
    destfmt.locale = NULL;

    if (cs_convert(context, dfmt, (CS_VOID *)v, &destfmt,
            &dest, &destlen) != CS_SUCCEED) {
        return CS_FAIL;
    }

    return ei_x_encode_double(x, dest) == 0 ? CS_SUCCEED:CS_FAIL;
}

CS_RETCODE SybStatement::encode_text(ei_x_buff* x, 
        CS_DATAFMT* dfmt, CS_TEXT* v, CS_INT len)
{
    return encode_string(x, (const char*)v, len) == 0 ? CS_SUCCEED:CS_FAIL;
}

CS_RETCODE SybStatement::encode_image(ei_x_buff* x, 
        CS_DATAFMT* dfmt, CS_IMAGE* v, CS_INT len)
{
    return ei_x_encode_binary(x, (const void*)v,
            (long)len) == 0 ? CS_SUCCEED:CS_FAIL;
}

CS_RETCODE SybStatement::encode_unitext(ei_x_buff* x, 
        CS_DATAFMT* dfmt, CS_UNITEXT* v, CS_INT len)
{
    CS_INT i;

    len = len / sizeof(CS_UNICHAR);
    if (ei_x_encode_list_header(x, (long)len)) {
        return CS_FAIL;
    }
    for (i = 0; i < len; ++i) {
        if(ei_x_encode_ulong(x, (unsigned long)v[i])) {
            return CS_FAIL;
        }
    }
    ei_x_encode_empty_list(x);

    return CS_SUCCEED;
}

CS_RETCODE SybStatement::encode_unknown(ei_x_buff* const x)
{
    return ei_x_encode_atom(x, "unknown") == 0 ? CS_SUCCEED:CS_FAIL;
}

CS_RETCODE SybStatement::encode_overflow(ei_x_buff* x)
{
    return ei_x_encode_atom(x, "overflow") == 0 ? CS_SUCCEED:CS_FAIL;
}

CS_RETCODE SybStatement::encode_null(ei_x_buff* x)
{
    return ei_x_encode_atom(x, "undefined") == 0 ? CS_SUCCEED:CS_FAIL;
}
}/* end of namespace rytong */
