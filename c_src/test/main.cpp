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
 *  @file main.cpp
 *  @author wang.meigong <wang.meigong@rytong.com>
 * 
 *  @date Created on 2010-4-5
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <ctpublic.h>
#include "SybConnection.h"
#include "SybStatement.h"

using namespace rytong;

/*
** Global context structure
*/

void test1();
void test2();
void test3();
void ei_x_write(ei_x_buff* x);


int main(int argc, char** argv) {
    test3();
    return (EXIT_SUCCESS);
}

void test1() {
    bool retcode;

    SybConnection conn;
    if (!conn.connect("127.0.0.1", "admin", "123456", "tempdb", 5000)) {
        ex_error("connect failed");
        exit(0);
    }

    SybStatement* stmt = conn.create_statement();
    
    char* drop_table = " \
        if exists(select name from sysobjects where type = 'U' \
            and name = 'test_type') \
        begin \
            drop table test_type \
        end";

    retcode = stmt->execute_cmd(drop_table);
    if (!retcode) {
        ex_error("drop_table failed");
        exit(0);
    }

    char* create_table_sql = "\
        create table test_type(\
            binary_v binary(255) null,\
            varbinary_v varbinary(128) null,\
            bit_v bit default 0,\
            char_v char(255) null,\
            varchar_v varchar(128) null,\
            unichar_v unichar(255) null,\
            univarchar_v univarchar(255) null,\
            date_v date null,\
            time_v time null,\
            datetime_v datetime null,\
            smalldatetime_v smalldatetime null,\
            bigdatetime_v bigdatetime null,\
            bigtime_v bigtime null,\
            tinyint_v tinyint null,\
            smallint_v smallint null,\
            int_v int null,\
            bigint_v bigint null,\
            decimal_v decimal(38,10) null,\
            numeric_v numeric(38,10) null,\
            float_v float null,\
            real_v real null,\
            money_v money null,\
            smallmoney_v smallmoney null,\
        )";
    retcode = stmt->execute_cmd(create_table_sql);
    if (!retcode) {
        ex_error("create_table failed");
        exit(0);
    }

    SybStatement* stmt1 = conn.create_statement("insert into test_type values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
    ei_x_buff update_r;

    if(!stmt1->prepare_init("mycursor1")) {
        ex_error("prepare_init failed");
        exit(0);
    }

    printf("type1:%d\n", stmt1->get_param_type(1));
    if(!stmt1->set_binary(1, (unsigned char*)"123456", 6)) {
        ex_error("set_binary(1) failed");
        exit(0);
    }

    printf("type2:%d\n", stmt1->get_param_type(2));
    if(!stmt1->set_binary(2, (unsigned char*)"中国", 6)) {
        ex_error("set_binary(2) failed");
        exit(0);
    }

    printf("type3:%d\n", stmt1->get_param_type(3));
    if(!stmt1->set_bit(3, 1)) {
        ex_error("set_bit(3) failed");
        exit(0);
    }

    printf("type4:%d\n", stmt1->get_param_type(4));
    if(!stmt1->set_char(4, "wangmeigong")) {
        ex_error("set_char(4) failed");
        exit(0);
    }

    printf("type5:%d\n", stmt1->get_param_type(5));
    if(!stmt1->set_char(5,"zhongguo", 8)) {
        ex_error("set_char(5) failed");
        exit(0);
    }

    unsigned short unichar[10] = {'1','2','3','4','5','6','7'};
    printf("type6:%d\n", stmt1->get_param_type(6));
    if(!stmt1->set_unichar(6, unichar, 3)) {
        ex_error("set_unichar(6) failed");
        exit(0);
    }

    printf("type7:%d\n", stmt1->get_param_type(7));
    if(!stmt1->set_unichar(7, unichar, 7)) {
        ex_error("set_unichar(7) failed");
        exit(0);
    }

    printf("type8:%d\n", stmt1->get_param_type(8));
    if(!stmt1->set_null(8)) {
        ex_error("set_null(8) failed");
        exit(0);
    }

    printf("type9:%d\n", stmt1->get_param_type(9));
    if(!stmt1->set_null(9)) {
        ex_error("set_null(9) failed");
        exit(0);
    }

    printf("type10:%d\n", stmt1->get_param_type(10));
    if(!stmt1->set_null(10)) {
        ex_error("set_null(10) failed");
        exit(0);
    }

    printf("type11:%d\n", stmt1->get_param_type(11));
    if(!stmt1->set_null(11)) {
        ex_error("set_null(11) failed");
        exit(0);
    }

    printf("type12:%d\n", stmt1->get_param_type(12));
    if(!stmt1->set_null(12)) {
        ex_error("set_null(12) failed");
        exit(0);
    }

    printf("type13:%d\n", stmt1->get_param_type(13));
    if(!stmt1->set_null(13)) {
        ex_error("set_null(13) failed");
        exit(0);
    }

    printf("type14:%d\n", stmt1->get_param_type(14));
    if(!stmt1->set_tinyint(14, 255)) {
        ex_error("set_tinyint(14) failed");
        exit(0);
    }

    printf("type15:%d\n", stmt1->get_param_type(15));
    if(!stmt1->set_smallint(15, 2570)) {
        ex_error("set_null(15) failed");
        exit(0);
    }

    printf("type16:%d\n", stmt1->get_param_type(16));
    if(!stmt1->set_int(16, 100000)) {
        ex_error("set_null(16) failed");
        exit(0);
    }

    printf("type17:%d\n", stmt1->get_param_type(17));
    if(!stmt1->set_bigint(17, 10000000000)) {
        ex_error("set_null(17) failed");
        exit(0);
    }

    printf("type18:%d\n", stmt1->get_param_type(18));
    if(!stmt1->set_decimal(18, "1232132123131.9321301")) {
        ex_error("set_null(18) failed");
        exit(0);
    }

    printf("type19:%d\n", stmt1->get_param_type(19));
    if(!stmt1->set_numeric(19, "4234243243.00001")) {
        ex_error("set_null(19) failed");
        exit(0);
    }

    printf("type20:%d\n", stmt1->get_param_type(20));
    if(!stmt1->set_float(20, 12312.12333)) {
        ex_error("set_null(20) failed");
        exit(0);
    }

    printf("type21:%d\n", stmt1->get_param_type(21));
    if(!stmt1->set_real(21, 1232.00001)) {
        ex_error("set_null(21) failed");
        exit(0);
    }

    printf("type22:%d\n", stmt1->get_param_type(22));
    if(!stmt1->set_money(22, "12344535.93933")) {
        ex_error("set_null(22) failed");
        exit(0);
    }

    printf("type23:%d\n", stmt1->get_param_type(23));
    if(!stmt1->set_money4(23, 212.312)) {
        ex_error("set_null(23) failed");
        exit(0);
    }

    if(!stmt1->execute_sql(&update_r)) {
        ex_error("exec_update failed");
        exit(0);
    }

    ei_x_free(&update_r);

    if(!stmt1->prepare_release("mycursor1")) {
        ex_error("prepare_release failed");
        exit(0);
    }

    conn.terminate_statement(stmt1);

    char* select_sql5 = "select * from test_type";
    ei_x_buff select_r;
    if(!stmt->execute_sql(&select_r, select_sql5)) {
        ex_error("select_sql5 failed");
        exit(0);
    }

    ei_x_write(&select_r);

    conn.terminate_statement(stmt);
}

void test2() {
    int index, type, size, version;
    char buff[20] = {131,110,6,0,0,160,114,78,24,9};

    ei_decode_version(buff, &index, &version);

    printf("version:%d\n", version);

    ei_get_type(buff, &index, &type, &size);

    printf("index:%d type:%d, size:%d\n", index, type, size);
}

void test3() {
    bool retcode;

    SybConnection conn;
    if (!conn.connect("127.0.0.1", "admin", "123456", NULL, 5000)) {
        ex_error("connect failed");
        exit(0);
    }

    SybStatement* stmt = conn.create_statement("use tempdb");

    retcode = stmt->execute_cmd();
    if (!retcode) {
        ex_error("use tempdb failed");
        exit(0);
    }

    char* drop_table = " \
        if exists(select name from sysobjects where type = 'U' \
            and name = 'test_type') \
        begin \
            drop table test_type \
        end";

    retcode = stmt->execute_cmd(drop_table);
    if (!retcode) {
        ex_error("drop_table failed");
        exit(0);
    }

    char* create_table_sql = "\
        create table test_type(\
            text_v text null,\
            image_v image,\
            unitext_v unitext\
        )";
    retcode = stmt->execute_cmd(create_table_sql);
    if (!retcode) {
        ex_error("create_table failed");
        exit(0);
    }

    char* insert_sql = "insert into test_type values('wangmeigong', 0x123456, 'xxxxxxxxxxxx')";
    ei_x_buff insert_r;
    if(!stmt->execute_sql(&insert_r, insert_sql)) {
        ex_error("insert_sql failed");
        exit(0);
    }

    char* select_sql = "select * from test_type";
    ei_x_buff select_r;
    if(!stmt->execute_sql(&select_r, select_sql)) {
        ex_error("select_sql failed");
        exit(0);
    }

    ei_x_write(&select_r);

    conn.terminate_statement(stmt);
}

void ei_x_write(ei_x_buff* x) {
    FILE * fp = fopen("/root/Desktop/tmp", "w");
    printf("%d\n", x->index);
    fwrite(x->buff, x->index, 1, fp);
    fclose(fp);
}

