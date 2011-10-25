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
 *  @file DrvConf.h
 *  @brief Config struct.
 *  @author cao.xu <cao.xu@rytong.com>
 *  @version 1.0.0
 *  @date Created on 2011-4-20
 */

#ifndef DRVCONF_H_
#define DRVCONF_H_

namespace rytong {

static const int ARG_LENGTH = 20; ///< Arg length for DrvConf.

/** @enum DatabaseType
 *  @brief Database type.
 */
enum DatabaseType {
    MYSQL_DB = 0,   ///< Mysql flag.
    ORACLE_DB = 1,  ///< Oracle flag.
    SYBASE_DB = 2,  ///< Sybase flag.
    DB2_DB = 3      ///< DB2 flag.
};

/** @brief struct DrvConf.
 */
typedef struct {
    unsigned long long thread_len;      ///< Thread length.
    unsigned long long max_thread_len;  ///< Max thread length..
    unsigned long long max_queue_len;   ///< Max data queue length.
    DatabaseType db_type;       ///< Database type.
    char host[ARG_LENGTH];      ///< Database host.
    char user[ARG_LENGTH];      ///< Database user.
    char password[ARG_LENGTH];  ///< Database password.
    char db_name[ARG_LENGTH];   ///< Database name.
    unsigned int port;          ///< Database port.
    int poolsize;               ///< The size of connection pool.
} DrvConf;

}

#endif /* DRVCONF_H_ */
