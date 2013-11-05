%% Copyright (c) 2009-2010 Beijing RYTong Information Technologies, Ltd.
%% All rights reserved.
%%
%% The contents of this file are subject to the Erlang Database Driver
%% Public License Version 1.0, (the "License"); you may not use this
%% file except in compliance with the License. You should have received
%% a copy of the Erlang Database Driver Public License along with this
%% software. If not, it can be retrieved via the world wide web at
%% http://www.rytong.com/.
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%%
%%% -------------------------------------------------------------------
%%% File    : test_util.erl
%%% @author deng.lifen <deng.lifen@rytong.com>
%%% @doc The db driver's test functions.
%%% @end
%%% -------------------------------------------------------------------

-module(test_util).

-export([start/1,
         stop/1,
         start_server/0,
         stop_server/0,
         create_test_table/1,
         drop_test_table/1,
         create_sql/1,
         drop_sql/1]).

-include_lib("eunit/include/eunit.hrl").

start(Table) ->
    start_server(),
    %% error_logger:format("create_test_table~n", []),
    create_test_table(Table).

stop(Table) ->
    drop_test_table(Table),
    stop_server().

start_server() ->
    %% error_logger:format("start db server~n", []),
    EwpConfigFile = ewp_file_util:get_ewp_config(),
    ewp_conf_util:read_ewp_conf(EwpConfigFile),
    db_server:start(),
    %% error_logger:format("init connect~n", []),
    db_server:init_default(test).

stop_server() ->
    %% error_logger:format("stop db server~n", []),
    db_server:stop().

create_test_table(Table) ->
    drop_test_table(Table),
    ?assert({ok, 0} == db_api:execute_sql(create_sql(Table))).

drop_test_table(Table) ->
    ?assert({ok, 0} == db_api:execute_sql(drop_sql(Table))).

create_sql(Table) when is_atom(Table) ->
    create_sql(atom_to_list(Table));
create_sql(Table) when is_list(Table) ->
    "CREATE TABLE " ++ Table ++ " (
        id int(11) NOT NULL auto_increment,
        fbit bit(4) default NULL,
        ftinyint tinyint(3) default NULL,
        fsmallint smallint(6) default NULL,
        fmediunint mediumint(9) default NULL,
        fint int(11) default NULL,
        fbigint bigint(20) default NULL,
        ffloat float default NULL,
        fdouble double default NULL,
        fdecimal decimal(15,12) default NULL,
        fdate date default NULL,
        fdatetime datetime default NULL,
        ftimestamp timestamp NULL default NULL,
        ftime time default NULL,
        fyear year(4) default NULL,
        fchar char(1) default NULL,
        fvarchar varchar(255) default NULL,
        fbinary binary(255) default NULL,
        fvarbinary varbinary(255) default NULL,
        ftinytext tinytext,
        ftext text,
        fmediumtext mediumtext,
        flongtext longtext,
        ftinyblob tinyblob,
        fblob blob,
        fmediumblob mediumblob,
        flongblob longblob,
        PRIMARY KEY  (id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8".

drop_sql(Table) when is_atom(Table) ->
    drop_sql(atom_to_list(Table));
drop_sql(Table) when is_list(Table) ->
    "DROP TABLE IF EXISTS " ++ Table.