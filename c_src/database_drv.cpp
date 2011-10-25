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
 * $Id: database_drv.cpp 22285 2010-02-26 09:52:22Z wang.meigong $
 *
 *  @file database_drv.cpp
 *  @brief Draft of recomposition of db drv. 
 *  @author cao.xu <cao.xu@rytong.com>
 *  @version 1.0.0
 *  @date Created on 2010-1-30
 */

#include "base/DatabaseDrv.h"

using namespace rytong;


/** @brief Main entry of driver, where define the driver's name and callbacks.
 */
static ErlDrvEntry db_driver_entry = {
    NULL,
    DatabaseDrv::start,
    DatabaseDrv::stop,
    NULL, /** output */
    DatabaseDrv::ready_input, /** ready_input */
    NULL, /** ready_output */
    (char*)"database_drv",
    DatabaseDrv::finish,
    NULL, /** handle */
    DatabaseDrv::control,
    NULL, /** timeout */
    NULL, /** outputv */

    NULL, /** ready_async */
    NULL, /** flush */
    NULL, /** call */
    NULL, /** event */
    ERL_DRV_EXTENDED_MARKER,
    ERL_DRV_EXTENDED_MAJOR_VERSION,
    ERL_DRV_EXTENDED_MINOR_VERSION,
    ERL_DRV_FLAG_USE_PORT_LOCKING,
    NULL, /** handle2 */
    NULL /** process_exit */
};

#ifdef __cplusplus
extern "C" { // shouldn't this be in the DRIVER_INIT macro?
#endif

    DRIVER_INIT(database_drv) {
        return &db_driver_entry;
    }
#ifdef __cplusplus
}
#endif



