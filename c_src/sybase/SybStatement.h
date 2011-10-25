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
 *  @file SybStatement.h
 *  @brief Sybase prepare statement class.
 *  @author wang.meigong <wang.meigong@rytong.com>
 *  @version 1.0.0
 *  @date Wen May 4 15:06:54 CST 2011
 */

#ifndef _SYBSTATEMENT_H
#define _SYBSTATEMENT_H

#include "SybUtils.h"

namespace rytong {
#define MAX(X,Y)    (((X) > (Y)) ? (X) : (Y))
#define MIN(X,Y)    (((X) < (Y)) ? (X) : (Y))
#define MAX_CHAR_BUF    1024

#define TRUNC_SUBST "***"

/** @brief Sybase prepare statement class.
 */
class SybStatement {
    typedef struct _column_data
    {
        CS_DATAFMT dfmt;
        CS_VOID *value;
        CS_INT valuelen;
        CS_SMALLINT indicator;
    } COLUMN_DATA;

public:
    /** @brief Constructor for SybStatement class.
     *  @param context A pointer to a CS_CONTEXT structure.
     *  @param sql A pointer to a sql string.
     *  @return None.
     */
    SybStatement(CS_CONNECTION* context);
    SybStatement(CS_CONNECTION*, const char* sql);

    /** @brief Destructor for SybStatement class.
     *  @return None.
     */
    ~SybStatement();

    /** @brief Execute a language command, only return success or failure.
     *  @return If successful.
     *  @retval true The routine completed successfully.
     *  @retval false The routine failed.
     */
    bool execute_cmd();
    
    /** @brief Execute a language command, only return success or failure.
     *  @param sql A pointer to a sql string, default is the sql in constructor.
     *  @return If successful.
     *  @retval true The routine completed successfully.
     *  @retval false The routine failed.
     */
    bool execute_cmd(const char* sql);

    /** @brief Execute a language command, encode result to the buffer and
     *      return success or failure.
     *  @param result The address of a ei_x_buff variable. execute_sql
     *      encode result to the buffer.
     *  @return If successful.
     *  @retval true The routine completed successfully.
     *  @retval false The routine failed.
     */
    bool execute_sql(ei_x_buff* result);
    
    /** @brief Execute a language command, encode result to the buffer and
     *      return success or failure.
     *  @param result The address of a ei_x_buff variable. execute_sql
     *      encode result to the buffer.
     *  @param sql A pointer to a sql string, default is the sql in constructor.
     *  @return If successful.
     *  @retval true The routine completed successfully.
     *  @retval false The routine failed.
     */
    bool execute_sql(ei_x_buff* result, const char* sql);

    /** @brief Initialize a prepare statement.
     *  @param id A pointer to the statement identifier. This identifier
     *      is defined by the application and must conform to server standards
     *  @return If successful.
     *  @retval true The routine completed successfully.
     *  @retval false The routine failed.
     */
    bool prepare_init(const char* id);
    
    /** @brief Initialize a prepare statement.
     *  @param id A pointer to the statement identifier. This identifier
     *      is defined by the application and must conform to server standards
     *  @param sql A pointer to a sql string, default is the sql in constructor.
     *  @return If successful.
     *  @retval true The routine completed successfully.
     *  @retval false The routine failed.
     */
    bool prepare_init(const char* id, const char* sql);

    /** @brief Get the parameter type to set the parameter with the
     *      appropriate function.
     *  @param index The index of the parameters. The first parameter has
     *      an index of 1,the second an index of 2, and so forth.
     *  @return Unsigned integer to indicate the parameter type if the
     *      routine completed successfully else -1.
     */
    int get_param_type(int index);

    /** @brief Get the total number of parameters to set.
     *  @return The total number of parameters.
     */
    int get_param_count();

    /** @brief Set the parameter.
     *  @param index The index of the parameters. The first parameter has
     *      an index of 1,the second an index of 2, and so forth.
     *  @param data The data to set.
     *  @param len Data length.
     *  @return If successful.
     *  @retval true The routine completed successfully.
     *  @retval false The routine failed.
     */
    bool set_binary(int index, unsigned char* data, unsigned int len);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_longbinary(int index, unsigned char* data, unsigned int len);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_varbinary(int index, unsigned char* data, unsigned int len);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_bit(int index, unsigned char data);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_char(int index, char* data);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_char(int index, char* data, unsigned int len);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_longchar(int index, unsigned char* data);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_longchar(int index, unsigned char* data, unsigned int len);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_varchar(int index, unsigned char* data);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_varchar(int index, unsigned char* data, unsigned int len);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_unichar(int index, unsigned short* data, unsigned int len);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_xml(int index, unsigned char* data);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_xml(int index, unsigned char* data, unsigned int len);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_date(int index, int data);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_date(int index, int year, int month, int day);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_time(int index, int data);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_time(int index, int hour, int minutes, int seconds, int ms);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_datetime(int index, int days, int time);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_datetime(int index, int year, int month, int day,
            int hour, int minutes, int seconds, int ms);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_datetime4(int index, unsigned short days, unsigned short minutes);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_datetime4(int index, int year, int month,
            int day, int hour, int minutes);

#ifdef SYBASE_OCS15_5
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_bigdatetime(int index, unsigned long data);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_bigdatetime(int index, int year, int month, int day,
            int hour, int minutes, int seconds, int ms, int Ms);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_bigtime(int index, unsigned long data);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_bigtime(int index, int hour, int minutes,
            int seconds, int ms, int Ms);
#endif
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_tinyint(int index, unsigned char data);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_smallint(int index, short data);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_int(int index, int data);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_bigint(int index, long data);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_usmallint(int index, unsigned short data);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_uint(int index, unsigned int data);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_ubigint(int index, unsigned long data);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_decimal(int index, char* data);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_decimal(int index, unsigned char precision,
        unsigned char scale, unsigned char* data, unsigned int len);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_numeric(int index, char* data);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_numeric(int index, unsigned char precision,
        unsigned char scale, unsigned char* data, unsigned int len);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_float(int index, double data);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_real(int index, float data);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_money(int index, char* data);
    
    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_money(int index, int high, unsigned int low);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_money4(int index, double data);

    /** @brief Set the parameter.
     *  @see set_binary.
     */
    bool set_null(int index);

    /** @brief Release a prepare statement.
     *  @return If successful.
     *  @retval true The routine completed successfully.
     *  @retval false The routine failed.
     */
    bool prepare_release();

    /** @brief Get the number of affected rows after executing a sql command.
     *  @return The number of affected rows.
     */
    unsigned int get_affected_rows();

private:
    CS_CONNECTION *conn_;
    char* sql_;
    CS_COMMAND *cmd_;
    CS_INT row_count_;

    /** data format of parameters in prepare statement */
    CS_DATAFMT *desc_dfmt_;
    CS_INT param_count_;

    char id_[CS_MAX_CHAR];
    bool is_prepare_;
    bool executed_;

    void reset();

    CS_RETCODE handle_describe_result();

    CS_RETCODE handle_command_result();

    CS_RETCODE handle_sql_result(ei_x_buff* result);

    CS_RETCODE process_describe_reslut();

    CS_RETCODE process_row_result(ei_x_buff* result);

    CS_RETCODE encode_query_result(ei_x_buff* result, COLUMN_DATA *columns, CS_INT column_count);

    CS_RETCODE encode_update_result(ei_x_buff* result, CS_INT row_count);

    CS_RETCODE encode_column_data(ei_x_buff* result, COLUMN_DATA *column);

    CS_VOID* alloc_column_value(CS_DATAFMT *dfmt);

    CS_VOID free_column_data(COLUMN_DATA *columns, CS_INT size);

    CS_INT get_row_count();

    CS_INT get_column_count();

    CS_RETCODE cancel_current();

    CS_RETCODE cancel_all();

    CS_INT get_column_length(CS_DATAFMT *dfmt);

    CS_CHAR* get_agg_op_name(CS_INT op);

    CS_RETCODE compute_info(CS_INT index, CS_DATAFMT *data_fmt);
    
    inline bool set_param(CS_DATAFMT* dfmt, CS_VOID* data, CS_INT len);

    CS_RETCODE encode_binary(ei_x_buff* x, CS_DATAFMT* dfmt, CS_BINARY* v, CS_INT len);

    CS_RETCODE encode_longbinary(ei_x_buff* x, CS_DATAFMT* dfmt, CS_LONGBINARY* v, CS_INT len);

    CS_RETCODE encode_varbinary(ei_x_buff* x, CS_DATAFMT* dfmt, CS_VARBINARY* v);

    CS_RETCODE encode_bit(ei_x_buff* x, CS_DATAFMT* dfmt, CS_BIT* v);

    CS_RETCODE encode_char(ei_x_buff* x, CS_DATAFMT* dfmt, CS_CHAR* v, CS_INT len);

    CS_RETCODE encode_longchar(ei_x_buff* x, CS_DATAFMT* dfmt, CS_LONGCHAR* v, CS_INT len);

    CS_RETCODE encode_varchar(ei_x_buff* x, CS_DATAFMT* dfmt, CS_VARCHAR* v);

    CS_RETCODE encode_unichar(ei_x_buff* x, CS_DATAFMT* dfmt, CS_UNICHAR* v, CS_INT len);

    CS_RETCODE encode_xml(ei_x_buff* x, CS_DATAFMT* dfmt, CS_XML* v, CS_INT len);

    CS_RETCODE encode_date(ei_x_buff* x, CS_DATAFMT* dfmt, CS_DATE* v);

    CS_RETCODE encode_time(ei_x_buff* x, CS_DATAFMT* dfmt, CS_TIME* v);

    CS_RETCODE encode_datetime(ei_x_buff* x, CS_DATAFMT* dfmt, CS_DATETIME* v);

    CS_RETCODE encode_datetime4(ei_x_buff* x, CS_DATAFMT* dfmt, CS_DATETIME4* v);
    
#ifdef SYBASE_OCS15_5
    CS_RETCODE encode_bigdatetime(ei_x_buff* x, CS_DATAFMT* dfmt, CS_BIGDATETIME* v);

    CS_RETCODE encode_bigtime(ei_x_buff* x, CS_DATAFMT* dfmt, CS_BIGTIME* v);
#endif
    
    CS_RETCODE encode_tinyint(ei_x_buff* x, CS_DATAFMT* dfmt, CS_TINYINT* v);

    CS_RETCODE encode_smallint(ei_x_buff* x, CS_DATAFMT* dfmt, CS_SMALLINT* v);

    CS_RETCODE encode_int(ei_x_buff* x, CS_DATAFMT* dfmt, CS_INT* v);

    CS_RETCODE encode_bigint(ei_x_buff* x, CS_DATAFMT* dfmt, CS_BIGINT* v);

    CS_RETCODE encode_usmallint(ei_x_buff* x, CS_DATAFMT* dfmt, CS_USMALLINT* v);

    CS_RETCODE encode_uint(ei_x_buff* x, CS_DATAFMT* dfmt, CS_UINT* v);

    CS_RETCODE encode_ubigint(ei_x_buff* x, CS_DATAFMT* dfmt, CS_UBIGINT* v);

    CS_RETCODE encode_decimal(ei_x_buff* x, CS_DATAFMT* dfmt, CS_DECIMAL* v);

    CS_RETCODE encode_numeric(ei_x_buff* x, CS_DATAFMT* dfmt, CS_NUMERIC* v);

    CS_RETCODE encode_float(ei_x_buff* x, CS_DATAFMT* dfmt, CS_FLOAT* v);

    CS_RETCODE encode_real(ei_x_buff* x, CS_DATAFMT* dfmt, CS_REAL* v);

    CS_RETCODE encode_money(ei_x_buff* x, CS_DATAFMT* dfmt, CS_MONEY* v);

    CS_RETCODE encode_money4(ei_x_buff* x, CS_DATAFMT* dfmt, CS_MONEY4* v);

    CS_RETCODE encode_text(ei_x_buff* x, CS_DATAFMT* dfmt, CS_TEXT* v, CS_INT len);

    CS_RETCODE encode_image(ei_x_buff* x, CS_DATAFMT* dfmt, CS_IMAGE* v, CS_INT len);

    CS_RETCODE encode_unitext(ei_x_buff* x, CS_DATAFMT* dfmt, CS_UNITEXT* v, CS_INT len);

    CS_RETCODE encode_unknown(ei_x_buff* x);

    CS_RETCODE encode_overflow(ei_x_buff* x);

    CS_RETCODE encode_null(ei_x_buff* x);
};
}/* end of namespace rytong */
#endif  /* _SYBSTATEMENT_H */

