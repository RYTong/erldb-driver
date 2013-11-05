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
 */

#ifndef _INFORMIX_UTILS_H
#define _INFORMIX_UTILS_H

#include "../util/SysLogger.h"
//#include <cstdlib>
//#include <cstdio>
//#include <cstring>
//#include <sqlcli1.h>
//#include <ei.h>
//#include <erl_interface.h>

namespace rytong {

#define LOG_INFM_ERROR(message) \
SysLogger::error("\n\rline  = %d\n\rfile  = %s\n\r%s\n\r", __LINE__, __FILE__, message);

#define LOG_INFM_DEBUG(format, arg...) \
SysLogger::debug(format, ## arg);

}/* end of namespace rytong */
#endif  /* _INFORMIX_UTILS_H */

