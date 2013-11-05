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
 *  @file AsyncDrv.h
 *  @brief Base class of asynchronous drivers.
 *  @author cao.xu <cao.xu@rytong.com>
 *  @version 1.0.0
 *  @date Mon Apr 25 14:24:38 CST 2011
 */

#ifndef _RYT_ASYNCDRV_H_
#define _RYT_ASYNCDRV_H_

#include <erl_driver.h>
#include <iostream>
#include <unistd.h>

/*
* R15B changed several driver callbacks to use ErlDrvSizeT and
* ErlDrvSSizeT typedefs instead of int.
* This provides missing typedefs on older OTP versions.
*/
#if ERL_DRV_EXTENDED_MAJOR_VERSION < 2
typedef int ErlDrvSizeT;
typedef int ErlDrvSSizeT;
#endif

#include "ThreadPool.h"

// lib
#include "../util/EiEncoder.h"
#include "../util/debug.h"

namespace rytong {

const long MAX_RES_LEN = 1024 * 1024 * 1024; ///< The max res size.


/** @brief Driver data struct.
 */
typedef struct {
    ErlDrvPort port;///< erlang port
    int pipe_fd[2]; ///< pipe used by driver to communicate with worker
    ei_x_buff res;  ///< res will be encoded in worker and returned to erlang
    char* buf;      ///< buf passed from erlang
    int index;      ///< decoding index of buf
    int version;    ///< version of buf xxx:is it necessary?
    void* msg;      ///< message passed to worker
} DrvData;

/** @brief Base class of asynchronous drivers.
 *
 *  We give a framework of asynchronous driver and define general callback
 *  functions for ErlDrvEntry, including start, stop and ready_input. The
 *  derived class may define their own version of callback functions, or use
 *  these directly when define the ErlDrvEntry. Generally, the derived class
 *  should implement ErlDrvEntry callback function control(...), and ThreadPool
 *  callback functions async_invoke(...) and async_free(...) to finish the whole
 *  driver.
 */
class AsyncDrv {
public:
    /** @brief Constructor for the class.
     *  @param len Connection pool length to be init.
     *  @return None.
     */
    AsyncDrv(unsigned long long len = 2) {
        // call set function of thr_pool_ to finish the work threads'
        // initialization.
        thr_pool_.set(&thr_pool_, len);
    }

    /** @brief Destructor for the class.
     *  @return None.
     */
    ~AsyncDrv() {
    }

    /** @brief Port start, called when port is opened.
     *      We allocate a DrvData structure here, which will be setted in
     *      control(fun of derived class) and used in worker.
     *  @param port Erlang Driver Port.
     *  @param command Port command.
     *  @return ErlDrvData.
     */
    static ErlDrvData start(ErlDrvPort port, char *command);

    /** @brief Port stop, called when port is closed.
     *      We deallocated the DrvData structure here except
     *      the msg, which should be freed in worker.
     *  @param drv_data Erlang driver data.
     *  @return None.
     */
    static void stop(ErlDrvData drv_data);

    /** @brief Function that will be executed right after the pipe being
     *      written by the worker. We send the encoded res back to
     *      erlang here.
     *  @param handle Erlang driver data.
     *  @param event Erlang driver event.
     *  @return None.
     */
    static void ready_input(ErlDrvData handle, ErlDrvEvent event);


    /** @brief Helper function to reply err messages to erlang.
     *  @param port Erlang driver port.
     *  @param msg Error message.
     *  @return None.
     */
    static void reply_err(ErlDrvPort port, const char* msg);

protected:
    ThreadPool thr_pool_; ///< A ThreadPool instance.

};
}
#endif /* _RYT_ASYNCDRV_H_ */
