dnl Configuration script for Erlang Database Driver.
dnl 
dnl Copyright (c) 2009-2010 Beijing RYTong Information Technologies, Ltd.
dnl All rights reserved.
dnl
dnl No part of this source code may be copied, used, or modified
dnl without the express written consent of RYTong.

AC_INIT(c_src/database_drv.cpp, 1.0)
echo "Configuring ${PACKAGE_STRING}"

dnl If we need global configuration variables, we can put them in these
dnl header files to be included in C/Erlang files.
dnl AC_CONFIG_HEADER([include/db_driver.hrl])

dnl
dnl Configuration templates
dnl
AC_PROG_CC
AC_PROG_CXX
AC_PROG_CC_STDC

dnl Assuming current dir is the top source dir.
SRC_TOP=$PWD
AC_SUBST(SRC_TOP)

dnl Common setup
builtin(include, ./build/configure.in.shlib)
builtin(include, ./build/configure.in.erlang)
builtin(include, ./build/configure.in.debug)

dnl Libraries
DB=""
AC_MSG_RESULT([$DB])
builtin(include, ./build/configure.in.mysql)
builtin(include, ./build/configure.in.oracle)
builtin(include, ./build/configure.in.sybase)
AC_SUBST(DB)

if test "x$DB" == "x" ; then
   echo "Please add your driver, --with-mysql, --with-oracle or --with-sybase."
   exit 1
fi

dnl Put everything together and generate output.

dnl
dnl Generate some output variables.
dnl
AC_SUBST(CFLAGS)
AC_SUBST(CPPFLAGS, ${CPPFLAGS})

AC_SUBST(ERLC_FLAGS)
AC_SUBST(EMAKE_FLAGS)

dnl
dnl Output files.
dnl
AC_OUTPUT(Makefile Emakefile)
