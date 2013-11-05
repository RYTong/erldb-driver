dnl -*- Mode: autoconf; -*-
dnl autoconf library script for detection of DB2 libraries.
dnl
dnl Copyright (c) 2009-2010 Beijing RYTong Information Technologies, Ltd.
dnl All rights reserved.
dnl
dnl No part of this source code may be copied, used, or modified
dnl without the express written consent of RYTong.

# Detecting DB2 headers

AC_ARG_WITH(db2,
    AS_HELP_STRING([--with-db2], [turn on db2 client on system]),
    [with_db2='yes'],
    [with_db2='no'])

if test "$with_db2" != 'no' ; then
AC_MSG_CHECKING([DB2 headers])
DB2_INCL=""
for d in \
  /home/zhub/sqllib/include \
  /usr/include/sqllib\include \
  /opt/local/sqllib\include \
; do
    if test -r $d/sqlcli.h ; then
       DB2_INCL=$d
    fi
done
AC_MSG_RESULT([$DB2_INCL])
if test "x$DB2_INCL" = "x" ; then
   AC_MSG_ERROR([Cannot find DB2!])
fi

# Detecting DB2 libraries
AC_MSG_CHECKING([DB2 library])
LIBPATH=lib
if test `uname -p` = "x86_64" ; then
    LIBPATH=lib64
fi
for d in \
    /home/zhub/sqllib/$LIBPATH \
    /usr/local/sqllib/$LIBPATH \
    /opt/local/lib/sqllib\$LIBPATH \
    /opt/local/lib/db2/sqllib\$LIBPATH \
    /usr/local/db2/sqllib\$LIBPATH \
; do
    if test -r $d/libdb2.so ; then
       DB2_LIB=$d
    fi
done
AC_MSG_RESULT([$DB2_LIB])
if test "x$DB2_LIB" = "x" ; then
   AC_MSG_ERROR([Cannot find DB2!])
fi

DB=""
AC_MSG_RESULT([$DB])
if test "x$MYSQL_LIB" != "x" ; then
   DB="db"
fi

DB2_CFLAGS="-DUSE_DB2 -I${DB2_INCL}"
DB2_SRC="c_src/DB2/DB2Connection.cpp c_src/DB2/DB2Operation.cpp"
DB2_LIBS="-L$DB2_LIB -ldb2"

AC_SUBST(DB2_SRC)
AC_SUBST(DB2_CFLAGS)
AC_SUBST(DB2_LIBS)
fi