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
 *  @file AsyncDrv.cpp
 *  @brief Base class of asynchronous drivers.
 *  @author cao.xu <cao.xu@rytong.com>
 *  @version 1.0.0
 *  @date Mon Apr 25 14:24:38 CST 2011
 */

#include "AsyncDrv.h"
using namespace rytong;

ErlDrvData AsyncDrv::start(ErlDrvPort port, char *command) {
    // cout << "open port " <<endl;
    if (NULL == port) {
        return ERL_DRV_ERROR_GENERAL;
    }
    DrvData* data;
    data = (DrvData*) driver_alloc(sizeof (DrvData));
    //rassert(data != NULL);
    if (NULL == data) {
        return ERL_DRV_ERROR_GENERAL;
    }
    set_port_control_flags(port, PORT_CONTROL_FLAG_BINARY);
    data->port = port;
    int r = pipe(data->pipe_fd);
    if (r != 0) {
        driver_free((void *) data);
        return ERL_DRV_ERROR_GENERAL;
    }
    driver_select((ErlDrvPort) (data->port), (ErlDrvEvent) (data->pipe_fd[0]),
        DO_READ, 1);

    /*
     * It is always a good habit to do the initialization for vars.
     */
    data->res.buff = NULL;
    data->buf = NULL;
    return (ErlDrvData) data;
}

void AsyncDrv::stop(ErlDrvData handle) {
    DrvData* data = (DrvData*) handle;
    //rassert(data != NULL);
    driver_select((ErlDrvPort) (data->port), (ErlDrvEvent) (data->pipe_fd[0]),
        DO_READ, 0);
    close(data->pipe_fd[0]);
    close(data->pipe_fd[1]);
    
    /*
     * Since we do res.buff = NULL during start, now
     * we can check the value before free it to avoid free error.
     */
    if(NULL != data->res.buff) {
        ei_x_free(&data->res);
    }
    delete[] data->buf;
    driver_free(data);
}

void AsyncDrv::ready_input(ErlDrvData handle, ErlDrvEvent event) {
    // cout << "the async ready for input "<<endl;
    DrvData *drv = (DrvData *) handle;
    //rassert(drv != NULL);
    // we use long here for 64-bits compatibility, given the
    // the assumption event will not overflow.
    int fd = (long) event;
    int read_result;
    char buff[1024];
    read_result = read(fd, buff, sizeof (buff));

    // cout <<"the length of res : " << drv->res.index <<endl;
    if (drv->res.index > MAX_RES_LEN) {
        reply_err(drv->port, "res is too big to return");
    } else {
        driver_output(drv->port, drv->res.buff, drv->res.index);
    }
}

void AsyncDrv::reply_err(ErlDrvPort port, const char* msg) {
    //rassert(port != NULL);
    ei_x_buff result;
    EiEncoder::encode_error_msg(msg, &result);
    driver_output(port, result.buff, result.index);
    ei_x_free(&result);
}
