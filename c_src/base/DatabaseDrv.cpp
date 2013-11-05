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
 *  @file DatabaseDrv.cpp
 *  @brief Derived class of AsyncDrv to implment an asynchronous driver
 *      for databases.
 *  @author cao.xu <cao.xu@rytong.com>
 *  @version 1.0.0
 *  @date Created on 2011-4-20
 */

#include "DatabaseDrv.h"
#ifdef USE_MYSQL
#include "../mysql/MysqlDBOperation.h"
#endif
#ifdef USE_ORACLE
#include "../oracle/OracleDBOperation.h"
#endif
#ifdef USE_SYBASE
#include "../sybase/SybDBOperation.h"
#endif
#ifdef USE_DB2
#include "../DB2/DB2Operation.h"
#endif
#ifdef USE_INFORMIX
#include "../informix/InformixOperation.h"
#endif

using namespace rytong;

static DatabaseDrv* drv_instance = NULL;

DatabaseDrv::~DatabaseDrv() {
    release_stmt(stmt_map_, type_);
    delete conn_pool_;
}

void DatabaseDrv::release_stmt(StmtMap* stmt_map, DatabaseType type) {
    void* stmt_data = NULL;
    do {
        stmt_data = stmt_map->pop();
        if (stmt_data != NULL) {
            switch (type) {
        #ifdef USE_MYSQL
                case MYSQL_DB:
                    MysqlDBOperation::release_stmt(stmt_data);
                    break;
        #endif
        #ifdef USE_ORACLE
                case ORACLE_DB:
                    OracleDBOperation::release_stmt(stmt_data);
                    break;
        #endif
        #ifdef USE_SYBASE
                case SYBASE_DB:
                    SybDBOperation::release_stmt(stmt_data);
                    break;
        #endif
        #ifdef USE_DB2
                case DB2_DB:
                    DB2Operation::release_stmt(stmt_data);
                    break;
        #endif
        #ifdef USE_INFORMIX
                case INFORMIX_DB:
                    InformixOperation::release_stmt(stmt_data);
                    break;
        #endif
                default:
                    break;
            }
        }
    } while(stmt_data != NULL);
    delete stmt_map;
}

void DatabaseDrv::io_async(void *arg) {
    DrvData* data = (DrvData*) arg;
    Msg* msg = (Msg*)data->msg;
    DatabaseDrv* drv = msg->db_drv;
    ConnectionPool* conn_pool = drv->get_pool();
    DBOperation* db_oper = DBOperation::create(drv->get_type());
    if(db_oper == NULL) {
        msg->db_action = NULL;
        EiEncoder::encode_error_msg("failed to create db operation",
            &data->res);
    }else {
        msg->db_action = db_oper;
        db_oper->set_buf(data->buf, data->index, data->version);
        Connection * conn = NULL;
        /*
         * Set the working connection for current driver command and
         * call functions in DBOpeartion(through execute_cmd) to
         * finish the job.
         * We handle the connections fetching and returning here,
         * especially for transactions.
         * We also check whether the DBOperations success and
         * will return error messages if they fail.
         */
        if (msg->command >= DRV_TRANSACTION_EXECUTE
            && msg->command <= DRV_TRANSACTION_ROLLBACK
            && msg->command != DRV_TRANSACTION_BEGIN) {
            conn = db_oper->decode_set_conn_tuple();
            // cout << "the trans conn == " << conn <<endl;
            if (NULL != conn) {
                if (!execute_cmd(db_oper, msg->command, &data->res))
                {
                    /**
                     * @fixme should we push back the conn when rollback or
                     * commit fail.
                     */
                    EiEncoder::encode_error_msg("failed to execute drv command",
                        &data->res);
                } else {
                    /*
                     * push back the conn into the pool after transactions
                     * commit or rollback.
                     */
                    if(msg->command == DRV_TRANSACTION_ROLLBACK
                        || msg->command == DRV_TRANSACTION_COMMIT) {
                        conn_pool->push(conn);
                    }
                }
            } else {
                EiEncoder::encode_error_msg("failed to decode connection",
                    &data->res);
            }
        } else {
            conn = conn_pool->pop();
            if (NULL == conn) {
                EiEncoder::encode_error_msg("failed to get connection from "
                    "pool", &data->res);
            } else {
                if (msg->command == DRV_PREPARE
                    || msg->command == DRV_PREPARE_EXECUTE
                    || msg->command == DRV_PREPARE_CANCEL) {
                    db_oper->set_stmt_map(drv->get_stmt_map());
                }

                db_oper->set_conn(conn);
                if (!execute_cmd(db_oper, msg->command, &data->res))
                {
                    // if transaction failed to begin, we push back the
                    // conn into the pool.
                    if (msg->command == DRV_TRANSACTION_BEGIN) {
                     conn_pool->push(conn);
                    }
                    EiEncoder::encode_error_msg("failed to execute drv command",
                        &data->res);
                } else {
                    if(msg->command != DRV_TRANSACTION_BEGIN) {
                        conn_pool->push(conn);
                    }
                }
            }
        }

    }
    /** @note We can't let async_free do the free job of msg,
     * since the drvdata (who hold the msg pointer) may be
     * released first.
     */
    free_msg(arg);
    write(data->pipe_fd[1], "0", 1);
}

void DatabaseDrv::io_async2(void *arg) {
    DrvData* data = (DrvData*) arg;
    Msg* msg = (Msg*)data->msg;
    // DatabaseDrv* drv = msg->db_drv;
    Connection * conn = msg->db_conn;
    // cout << "conn->get_db_type(): " << conn->get_db_type() << "\r\n";
    DBOperation* db_oper = DBOperation::create(conn->get_db_type());
    if(db_oper == NULL) {
        msg->db_action = NULL;
        EiEncoder::encode_error_msg("failed to create db operation",
            &data->res);
    } else {
        msg->db_action = db_oper;
        db_oper->set_buf(data->buf, data->index, data->version);

        if (msg->command == DRV_PREPARE_DB
            || msg->command == DRV_PREPARE_EXECUTE_DB
            || msg->command == DRV_PREPARE_CANCEL_DB) {
            db_oper->set_stmt_map(msg->stmt_map);
        }

        db_oper->set_conn(conn);
        if (!execute_cmd(db_oper, msg->command, &data->res))
        {
            // if transaction failed to begin, we push back the
            // conn into the pool.
            // if (msg->command == DRV_TRANSACTION_BEGIN) {
            //  conn_pool->push(conn);
            // }
            EiEncoder::encode_error_msg("failed to execute drv command",
                &data->res);
        }
    }
    /** @note We can't let async_free do the free job of msg,
     * since the drvdata (who hold the msg pointer) may be
     * released first.
     */
    free_msg(arg);
    write(data->pipe_fd[1], "0", 1);
}

bool DatabaseDrv::execute_cmd(DBOperation* db_oper, DrvCommand cmd,
    ei_x_buff* res) {
    switch (cmd) {
        case DRV_EXECUTE:
        case DRV_EXECUTE_DB:
            return db_oper->exec(res);
        case DRV_INSERT:
        case DRV_INSERT_DB:
            return db_oper->insert(res);
        case DRV_UPDATE:
        case DRV_UPDATE_DB:
            return db_oper->update(res);
        case DRV_DELETE:
        case DRV_DELETE_DB:
            return db_oper->del(res);
        case DRV_SELECT:
        case DRV_SELECT_DB:
            return db_oper->select(res);
        case DRV_TRANSACTION_BEGIN:
        case DRV_TRANS_BEGIN_DB:
            return db_oper->trans_begin(res);
        case DRV_TRANSACTION_COMMIT:
        case DRV_TRANS_COMMIT_DB:
            return db_oper->trans_commit(res);
        case DRV_TRANSACTION_ROLLBACK:
        case DRV_TRANS_ROLLBACK_DB:
            return db_oper->trans_rollback(res);
        case DRV_TRANSACTION_EXECUTE:
            return db_oper->exec(res);
        case DRV_TRANSACTION_INSERT:
            return db_oper->insert(res);
        case DRV_TRANSACTION_UPDATE:
            return db_oper->update(res);
        case DRV_TRANSACTION_DELETE:
            return db_oper->del(res);
        case DRV_TRANSACTION_SELECT:
            return db_oper->select(res);
        case DRV_PREPARE:
            return db_oper->prepare_stat_init(res);
        case DRV_PREPARE_DB:
            return db_oper->prepare_statement_init(res);
        case DRV_PREPARE_EXECUTE:
            return db_oper->prepare_stat_exec(res);
        case DRV_PREPARE_EXECUTE_DB:
            return db_oper->prepare_statement_exec(res);
        case DRV_PREPARE_CANCEL:
            return db_oper->prepare_stat_release(res);
        case DRV_PREPARE_CANCEL_DB:
            return db_oper->prepare_statement_release(res);
        default:
            return false;
    }
}

void DatabaseDrv::finish() {
    SysLogger::debug("Finish driver");
    SysLogger::debug("Delete conn pool");
    // destroy the log helper
    SysLogger::close();
}

ErlDrvSSizeT DatabaseDrv::control(ErlDrvData handle, unsigned int command, char *buf,
    ErlDrvSizeT len, char **rbuf, ErlDrvSizeT rlen) {
    DrvData *data = (DrvData *) handle;
    int i = 0, size = 0, index = 0, version = 0;
    DatabaseDrv* instance = NULL;

    /** @warnning Important since the synchronized return is (*rbuf).
     */
    (*rbuf) = NULL;
    if (command == (unsigned int) DRV_CONNECT) {
        // cout << "here to init the connections " <<endl;
        DrvConf drv_conf;
        decode_init_arg(drv_conf, buf);
        if (0 != check_init_arg(&drv_conf, data->port)) {
            return 0;
        }
        /*
         * FIXME We should also return the number of db
         * connections that had been created successfully,
         * and let the caller decide whether the connect is
         * successful.
         */
        instance = new DatabaseDrv(drv_conf);
        EiEncoder::encode_ok_pointer(instance, &data->res);
        driver_output(data->port, data->res.buff, data->res.index);
        SysLogger::debug("Init driver!");
    } else if (command == (unsigned int) DRV_DISCONNECT) {
        ei_decode_version(buf, &index, &version);
        ei_get_type(buf, &index, &i, &size);
        long bin_size = size;
        ei_decode_binary(buf, &index, &instance, &bin_size);
        // cout <<"decoded drv instance  == " << instance <<endl;
        delete instance;
        instance = NULL;
        EiEncoder::encode_ok_msg("disconnect", &data->res);
        driver_output(data->port, data->res.buff, data->res.index);
    } else if (command == (unsigned int) DRV_INIT_DB) {
        // cout << "here to init the driver " <<endl;
        ei_decode_version(buf, &index, &version);
        ei_get_type(buf, &index, &i, &size);
        unsigned long long thread_len;
        ei_decode_ulonglong(buf, &index, &thread_len);
        drv_instance = new DatabaseDrv(thread_len);
        EiEncoder::encode_ok_pointer(drv_instance, &data->res);
        driver_output(data->port, data->res.buff, data->res.index);
        SysLogger::debug("Init driver!");
    } else if (command == (unsigned int) DRV_CONNECT_DB) {
        DrvConf drv_conf;
        decode_connect_arg(drv_conf, buf);

        Connection* conn = ConnectionPool::create_conn(drv_conf.db_type, drv_conf.host, drv_conf.user,
            drv_conf.password, drv_conf.db_name, drv_conf.port);
        if (conn == NULL) {
            EiEncoder::encode_error_msg("invalid args to start connection", &data->res);
        } else {
            EiEncoder::encode_ok_pointer(conn, &data->res);
        }
        driver_output(data->port, data->res.buff, data->res.index);
        SysLogger::debug("Init conn!");
    } else if (command == (unsigned int) DRV_DISCONNECT_DB) {
        ei_decode_version(buf, &index, &version);
        ei_get_type(buf, &index, &i, &size);
        long bin_size = size;
        Connection* conn;
        ei_decode_binary(buf, &index, &conn, &bin_size);
        // cout <<"decoded drv conn  == " << conn <<endl;
        Connection::destroy(conn);
        EiEncoder::encode_ok_msg("close", &data->res);
        driver_output(data->port, data->res.buff, data->res.index);
    } else if (command == (unsigned int) DRV_STMT_MAP_INIT_DB) {
        StmtMap* stmt_map = new StmtMap();
        if (stmt_map == NULL) {
            EiEncoder::encode_error_msg("error to new stmt map", &data->res);
        } else {
            EiEncoder::encode_ok_pointer(stmt_map, &data->res);
        }
        driver_output(data->port, data->res.buff, data->res.index);
    } else if (command == (unsigned int) DRV_STMT_MAP_DESTORY_DB) {
        ei_decode_version(buf, &index, &version);
        ei_decode_tuple_header(buf, &index, &i);

        ei_get_type(buf, &index, &i, &size);
        long db_type;
        ei_decode_long(buf, &index, &db_type);

        ei_get_type(buf, &index, &i, &size);
        long bin_size = size;
        StmtMap* stmt_map;
        ei_decode_binary(buf, &index, &stmt_map, &bin_size);
        // cout <<"decoded drv stmt_map  == " << stmt_map <<endl;
        release_stmt(stmt_map, (DatabaseType) db_type);
        EiEncoder::encode_ok_msg("close", &data->res);
        driver_output(data->port, data->res.buff, data->res.index);
    // } else if (command >= (unsigned int) DRV_PREPARE_DB && command <= (unsigned int) DRV_PREPARE_CANCEL_DB) {
    //     data->buf = new char[len];
    //     data->index = 0;
    //     data->version = 0;
    //     memcpy(data->buf, buf, len);
    //     /*
    //      * We suppose every request other than connect and disconnect
    //      * is in format of {Drv, Data}, where Drv is the pointer
    //      * of DatabaseDrv created before.
    //      */
    //     ei_decode_version(data->buf, &data->index, &data->version);
    //     ei_decode_tuple_header(data->buf, &data->index, &i);
    //
    //     long bin_size = size;
    //
    //     Connection* conn;
    //     ei_get_type(data->buf, &data->index, &i, &size);
    //     ei_decode_binary(data->buf, &data->index, &conn, &bin_size);
    //
    //     StmtMap* stmt_map;
    //     ei_get_type(data->buf, &data->index, &i, &size);
    //     ei_decode_binary(data->buf, &data->index, &stmt_map, &bin_size);
    //
    //     Msg* msg = new Msg();
    //     msg->db_drv = drv_instance;
    //     msg->command = (DrvCommand) command;
    //     // cout << "cmd: " << msg->command << "\r\n";
    //     msg->db_conn = conn;
    //     msg->stmt_map = stmt_map;
    //     data->msg = msg;
    //     /* FIXME We should handle errors happen in the VM thread, like
    //      * failure of async_add, since it will cause the calling process
    //      * hang. The process/1 should return a error if sth is wrong and
    //      * then we will send an error message to the erlang process .
    //      */
    //     drv_instance->process2(data);
    } else if (command >= (unsigned int) DRV_EXECUTE_DB && command <= (unsigned int) DRV_PREPARE_CANCEL_DB) {
        data->buf = new char[len];
        data->index = 0;
        data->version = 0;
        memcpy(data->buf, buf, len);
        /*
         * We suppose every request other than connect and disconnect
         * is in format of {Drv, Data}, where Drv is the pointer
         * of DatabaseDrv created before.
         */
        ei_decode_version(data->buf, &data->index, &data->version);
        ei_decode_tuple_header(data->buf, &data->index, &i);
        ei_get_type(data->buf, &data->index, &i, &size);
        long bin_size = size;
        Connection* conn;
        ei_decode_binary(data->buf, &data->index, &conn, &bin_size);

        Msg* msg = new Msg();
        msg->db_drv = drv_instance;
        msg->command = (DrvCommand) command;
        // cout << "cmd: " << msg->command << "\r\n";
        msg->db_conn = conn;
        data->msg = msg;
        /* FIXME We should handle errors happen in the VM thread, like
         * failure of async_add, since it will cause the calling process
         * hang. The process/1 should return a error if sth is wrong and
         * then we will send an error message to the erlang process .
         */
        drv_instance->process2(data);
    } else {
        data->buf = new char[len];
        data->index = 0;
        data->version = 0;
        memcpy(data->buf, buf, len);
        /*
         * We suppose every request other than connect and disconnect
         * is in format of {Drv, Data}, where Drv is the pointer
         * of DatabaseDrv created before.
         */
        ei_decode_version(data->buf, &data->index, &data->version);
        ei_decode_tuple_header(data->buf, &data->index, &i);
        ei_get_type(data->buf, &data->index, &i, &size);
        long bin_size = size;
        ei_decode_binary(data->buf, &data->index, &instance, &bin_size);
        // cout <<"decoded drv instance  == " << instance << "and the type: "
        //     << instance->get_type() << endl;
        Msg* msg = new Msg();
        msg->db_drv = instance;
        msg->command = (DrvCommand) command;
        // cout << "cmd: " << msg->command << "\r\n";
        data->msg = msg;
        /* FIXME We should handle errors happen in the VM thread, like
         * failure of async_add, since it will cause the calling process
         * hang. The process/1 should return a error if sth is wrong and
         * then we will send an error message to the erlang process .
         */
        instance->process(data);
    }
    return 0;
}

void DatabaseDrv::free_msg(void *handle) {
    DrvData *drv = (DrvData *) handle;
    Msg* msg = (Msg *) drv->msg;
    if (NULL != msg->db_action)
        DBOperation::destroy(msg->db_action);
    delete msg;
    msg = NULL; //set pointer to NULL after free it
}
