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
 *  @file SybConnection.h
 *  @brief Sybase connection class.
 *  @author wang.meigong <wang.meigong@rytong.com>
 *  @version 1.0.0
 *  @date @date Wen May 4 15:06:54 CST 2011
 */

#ifndef _SYBCONNECTION_H
#define _SYBCONNECTION_H

#include "base/Connection.h"
#include "SybUtils.h"
#include "SybStatement.h"
#include <vector>
using std::vector;

namespace rytong {
#define EX_CTLIB_VERSION    CS_CURRENT_VERSION

/** @brief Sybase connection class.
 */
class SybConnection : public Connection {
public:
    /** @brief Constructor for the class.
     *  @return None.
     */
    SybConnection();

    /** @brief Destructor for the class.
     *  @return None.
     */
    ~SybConnection();

    /** @brief Get sybase connection.
     *  @see Connection::get_connection.
     */
    void* get_connection();

    /** @brief Disconnect sybase database.
     *  @see Connection::disconnect.
     */
    bool disconnect();

    /** @brief Connect to sybase database.
     *  @see Connection::connect.
     */
    bool connect(const char *host, const char *user, const char *password,
            const char *db_name, unsigned int port);

    SybStatement* get_statement();

    /** @brief Create a statement in this connection. 
     * default is the sql in constructor.
     *  @return The point to a SybStatement object.
     */
    SybStatement* create_statement();
    
    /** @brief Create a statement in this connection.
     *  @param sql A pointer to a sql string.
     *  @return The point to a SybStatement object.
     */
    SybStatement* create_statement(const char* sql);

    /** @brief Terminate the statement in this connection.
     *  @param stmt A pointer to a SybStatement object.
     *  @return None.
     */
    void terminate_statement(SybStatement* stmt);

    /** @brief Set the maximum row of query result in this connection.
     *  @param count The maximum row which you set to.
     *  @return Is successful.
     *  @retval true The routine completed successfully.
     *  @retval false The routine failed.
     */
    bool set_limit_row_count(int count);

    /** @brief Get the maximum row of query result in this connection.
     *  @return The maximum row of query result.
     */
    int get_limit_row_count();

private:
    static CS_CONTEXT *sContext_;
    CS_CONNECTION *conn_;
    SybStatement* stmt_;
    int limit_row_count_;

    SybConnection(const SybConnection &);
    SybConnection & operator=(const SybConnection &);

    CS_RETCODE init_();
    CS_RETCODE connect_(const char *app, const char *host, const char *user,
            const char *pwd, const char *db);
    CS_RETCODE con_cleanup_(CS_RETCODE status);
    CS_RETCODE ctx_cleanup_(CS_RETCODE status);

};
}/* end of namespace rytong */

#endif  /* _SYBCONNECTION_H */


