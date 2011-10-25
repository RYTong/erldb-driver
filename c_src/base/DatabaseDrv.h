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
 *  @file DatabaseDrv.h
 *  @brief Derived class of AsyncDrv to implment an asynchronous driver
 *      for databases.
 *  @author cao.xu <cao.xu@rytong.com>
 *  @version 1.0.0
 *  @date Created on 2011-4-19
 */

#ifndef DATABASEDRV_H_
#define DATABASEDRV_H_
#include "base/DrvConf.h"
#include "base/ThreadPool.h"
#include "base/DBOperation.h"
#include "base/ConnectionPool.h"
#include "base/StmtMap.h"
#include "base/AsyncDrv.h"

using namespace std;

namespace rytong {

/** @enum DrvCommand
 *  @brief driver commands.
 */ 
enum DrvCommand {
    DRV_EXECUTE = 1, ///< execute sql command
    DRV_INSERT = 2, ///< insert command
    DRV_UPDATE = 3, ///< update command
    DRV_DELETE = 4, ///< delete command
    DRV_SELECT = 5, ///< select command
    DRV_TRANSACTION_EXECUTE = 6, ///< execute sql command in transaction
    DRV_TRANSACTION_INSERT = 7, ///< insert command in transaction
    DRV_TRANSACTION_UPDATE = 8, ///< update command in transaction
    DRV_TRANSACTION_DELETE = 9, ///< delete command in transaction
    DRV_TRANSACTION_SELECT = 10, ///< select command in transaction
    DRV_TRANSACTION_BEGIN = 11, ///< begin transaction command
    DRV_TRANSACTION_COMMIT = 12, ///< commit transaction command
    DRV_TRANSACTION_ROLLBACK = 13, ///< rollback transaction command
    DRV_PREPARE = 14, ///< prepare command
    DRV_PREPARE_EXECUTE = 15, ///< execute statement command
    DRV_PREPARE_CANCEL = 16, ///< unprepare command
    DRV_CONNECT = 17, ///< init driver command
    DRV_DISCONNECT = 18, ///< init driver command
};

/** @brief Derived class of AsyncDrv to implment an asynchronous driver
 *      for databases.
 */
class DatabaseDrv :public AsyncDrv{
public:
    /** @brief Constructor for the class.
     *  @param conf Database connect args.
     *  @return None.
     */
    DatabaseDrv(DrvConf conf):
        AsyncDrv(conf.thread_len){
            type_ = conf.db_type;
            conn_pool_ = new ConnectionPool(conf);
            stmt_map_ = new StmtMap();
        // cout << "finish to construte dbdrv"<<endl;
    };
    
    /** @brief Destructor for the class.
     *  @return None.
     */
    ~DatabaseDrv();

    /** @brief erl_driver callback function.
     * Do the clear work when driver is unloaded or the simulator crashes.
     *  @return None.
     */
    static void finish();
    
    /** @brief Distribute the port commands to different code block.
     *      Since we are operating in binary mode, the return value from control
     *      is irrelevant, as long as it is not negative.
     *  @param drv_data Erlang driver data.
     *  @param command Port command.
     *  @param buf Erlang data buf.
     *  @param len Erlang data length.
     *  @param rbuf Return buf.
     *  @param return buf length.
     *  @return If success.
     */
    static int control(ErlDrvData drv_data, unsigned int command, char *buf,
            int len, char **rbuf, int rlen);


    /** @brief Call do_async fo thr_pool_ to process the drv data.
     *  @param data Driver data.
     *  @return None.
     */
    void process(DrvData* data) {
        thr_pool_.do_async(io_async, data, free_msg);
    }

    /** @brief Getter function for conn_pool_.
     *  @return The pointer to ConnectionPool.
     */
    ConnectionPool * get_pool() {
        return conn_pool_;
    }
    
    /** @brief Getter function for stmt_map_.
     *  @return The pointer to StmtMap.
     */
    StmtMap * get_stmt_map() {
        return stmt_map_;
    }

    /** @brief Getter function for type_.
     *  @return Database type.
     */
    DatabaseType  get_type() {
        return type_;
    }

    /** @brief Check init args.
     *  @param conf Configure args.
     *  @param port Erlang Driver Port.
     *  @return if success. 
     *  @retval 0 is success.
     *  @retval -1 is error.
     */
    static inline int check_init_arg(DrvConf *conf, ErlDrvPort port) {
        if (conf->max_queue_len < 1) {
            reply_err(port, "Bad arg in init!");
            return -1;
        }
        if (conf->thread_len > conf->max_thread_len) {
            reply_err(port, "thread_list_len_initialization is overlong!");
            return -1;
        }
        return 0;
    }

    /** @brief Decode init args.
     *  @param conf Configure arg.
     *  @param buf Source string.
     *  @return None.
     */
    static inline void decode_init_arg(DrvConf &conf, const char* buf) {
        int i, size;
        int index = 0;
        int version = 0;
        memset(conf.host, 0, ARG_LENGTH);
        memset(conf.user, 0, ARG_LENGTH);
        memset(conf.password, 0, ARG_LENGTH);
        memset(conf.db_name, 0, ARG_LENGTH);

        ei_decode_version(buf, &index, &version);
        ei_decode_tuple_header(buf, &index, &i);

        // decode driver
        ei_get_type(buf, &index, &i, &size);
        long db_type;
        ei_decode_long(buf, &index, &db_type);
        conf.db_type = (DatabaseType) db_type;
        // cout << "the db_type :" << conf.db_type <<endl;

        // decode host
        ei_get_type(buf, &index, &i, &size);
        ei_decode_string(buf, &index, conf.host);
        // cout << "the host :" << conf.host <<endl;
        
        // decode user
        ei_get_type(buf, &index, &i, &size);
        ei_decode_string(buf, &index, conf.user);
        // cout << "the user :" << conf.user <<endl;

        // decode password
        ei_get_type(buf, &index, &i, &size);
        ei_decode_string(buf, &index, conf.password);
        // cout << "the password :" << conf.password <<endl;

        // decode database name
        ei_get_type(buf, &index, &i, &size);
        ei_decode_string(buf, &index, conf.db_name);
        // cout << "the db_name :" << conf.db_name <<endl;

        // decode port
        unsigned long port;
        ei_get_type(buf, &index, &i, &size);
        ei_decode_ulong(buf, &index, &port);
        conf.port = (unsigned int) port;
        // cout << "the port :" << conf.port <<endl;

        // decode poolsize
        ei_get_type(buf, &index, &i, &size);
        long poolsize;
        ei_decode_long(buf, &index, &poolsize);
        conf.poolsize = (int) poolsize;
        // cout << "the poolsize :" << conf.poolsize <<endl;

        // decode thread length
        ei_get_type(buf, &index, &i, &size);
        ei_decode_ulonglong(buf, &index, &conf.thread_len);
        // cout << "the thread_len :" << conf.thread_len <<endl;

        // decode max thread length
        ei_get_type(buf, &index, &i, &size);
        ei_decode_ulonglong(buf, &index, &conf.max_thread_len);
        // cout << "the max_thread_len :" << conf.max_thread_len <<endl;

        // decode max queue length
        ei_get_type(buf, &index, &i, &size);
        ei_decode_ulonglong(buf, &index, &conf.max_queue_len);
        // cout << "the max_queue_len :" << conf.max_queue_len <<endl;
    }


private:
    // the async_invoke callback for thr_pool_
    static void io_async(void *);
    // the async_free callback for thr_pool_
    static void free_msg(void *);
    static bool execute_cmd(DBOperation* oper, DrvCommand cmd, ei_x_buff* res);

    DatabaseType type_; // Database type.
    ConnectionPool* conn_pool_; // Connection Pool instance.
    StmtMap* stmt_map_;

};

/** @brief Message structure to contain specific parameters
 *      for DatabaseDrv.
 *
 *  The resourses in the struct should be released
 *  in callback async_free. For DatabaseDrv is
 *  free_msg(void*).
 */
typedef struct {
    DrvCommand command; ///< Driver command.
    DBOperation* db_action; ///< The pointer to DBOperation.
    DatabaseDrv* db_drv; ///< The pointer to DatabaseDrv.
} Msg;


}

#endif /* DATABASEDRV_H_ */
