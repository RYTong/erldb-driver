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
 * $Id: DBException.h 21889 2010-04-06 16:40:06Z wang.meigong $
 *
 *  @file DBException.h
 *  @brief Exception handler class.
 *  @author wang.meigong <wang.meigong@rytong.com>
 *  @version 1.0.0
 *  @date Tue Apr 6 16:35:04 CST 2010
 */

#ifndef _RYT_DB_EXCEPTION_H
#define _RYT_DB_EXCEPTION_H

namespace rytong{
/** @brief Exception handler class.
 */
class DBException : public exception {
public:
    /** @brief Constructor for the class.
     *  @return None.
     */
    DBException() throw() : what_(NULL){
    }
    
    /** @brief Constructor for the class.
     *  @param what The message to be throw.
     *  @return None.
     */
    DBException(const char* what) throw() {
        what_ = new(nothrow) char[strlen(what) + 1];
        if (what_ != NULL) {
          strcpy(what_, what);
        }
    }
    
    /** @brief Constructor for the class.
     *  @param ex The exception to be throw.
     *  @return None.
     */
     DBException(const DBException& ex) throw() {
        what_ = new(nothrow) char[strlen(ex.what()) + 1];
        if (what_ != NULL) {
          strcpy(what_, ex.what());
        }
    }
    
    /** @brief Destructor for the class.
     *  @return None.
     */
    ~DBException()throw() {
        delete[] what_;
    }
    
    /** @brief Overload operator '='.
     *  @param ex The exception to be throw.
     *  @return None.
     */
    void operator=(const DBException &ex) {
        delete[] what_;
        what_ = new(nothrow) char[strlen(ex.what()) + 1];
        if (what_ != NULL) {
          strcpy(what_, ex.what());
        }
    }

    /** @brief Get the exception message.
     *  @return Exception message.
     */
    const char* what() const throw() {
        return what_;
    }

private:
    char* what_;

};
}/* end of namespace rytong */

#endif /* _RYT_DB_EXCEPTION_H */

