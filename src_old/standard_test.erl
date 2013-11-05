%% Copyright (c) 2009-2010 Beijing RYTong Information Technologies, Ltd.
%% All rights reserved.
%%
%% No part of this source code may be copied, used, or modified
%% without the express written consent of RYTong.
-module(standard_test).

%%
%% Include files
%%
-include_lib("eunit/include/eunit.hrl").

%%
%% Exported Functions
%%
-export([common_test/1]).


%%
%% API Functions
%%

mysql_test_() ->
    %% mysql_specific_test(),
     common_test(mysql_test_config()).

common_test(Config) ->
    {spawn,[{setup, 
             fun () -> setup(Config) end,
             fun cleanup/1,
             fun(X) -> [basic_sql(X),
                        basic_api_test(X),
                        datatype_test(X),
                        transaction_test(X),
                        prepare_statement_test(X),
                        long_sql_test(X)
                        ] 
             end
            }]
    }.


%%
%% Local Functions
%%

mysql_test_config() ->
    {ewp_development, [{driver, mysql},
                       {database, "ewp_development"},
                       {host, "localhost"},
                       {port, 3306},
                       {password, "l1ghtp@l3"},
                       {user, "lpdba"},
                       {poolsize, 4}]}.

%% Setup the test environment.
setup({Name, Props} = Config) ->
    db_server:start(),
    ewp_db:init_db(ewp_db:get_dbconf([Config])),
    DbType = proplists:get_value(driver, Props),
    {ok, Tab} = create_test_table(DbType, Name),
    {Name, Tab}.

create_test_table(mysql, Name) ->
    SQL = "CREATE TABLE mysql_test_table (
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
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8",
    {ok, 0} = db_api:execute_sql(SQL,[{db_name, Name}]),
    {ok, mysql_test_table};

%% TODO
create_test_table(_Other, _Name) ->
    to_do.

%% Cleanup.

cleanup({Name,Tab}) ->
    {ok, 0} = db_api:execute_sql("drop table "++ atom_to_list(Tab), [{db_name, Name}]),
    db_server:stop().


%% Simple SQL statement test
basic_sql({Name, Tab}) ->
    {"The tests of SQL statements that all database types support",
     fun() ->
             TableName = atom_to_list(Tab),
             ?assertMatch({ok, 1},
                           db_api:execute_sql("insert into " ++ TableName ++ " (fvarchar, ffloat, fint) values('denglf', 24, 123)", [{db_name, Name}])),
              ?assertMatch({ok, 
                            [["denglf"]]}, db_api:execute_sql("select fvarchar from " ++ TableName ++ " where fint = 123", [{db_name, Name}])),
              ?assertMatch({ok, 1}, 
                           db_api:execute_sql("update " ++ TableName ++ " set ffloat=22.2 where fvarchar = 'denglf'", [{db_name, Name}])),
              ?assertMatch({ok, [[22.2, 123]]}, 
                           db_api:execute_sql("select ffloat,fint from " ++ TableName ++ " where fint = 123", [{db_name, Name}])),
              ?assertMatch({ok, 1},
                            db_api:execute_sql("delete from " ++ TableName ++ " where fvarchar = 'denglf'", [{db_name, Name}])),
              ?assertMatch({ok, []}, 
                           db_api:execute_sql("select * from " ++ TableName ++ " where fint = 123", [{db_name, Name}]))
     end}.

%%  Basic API test : insert, update, delete, select
basic_api_test({Name, Tab}) ->
    {"The tests of APIs that all database types support",
     fun() ->
             []
     end}.

%% Basic datatype test
datatype_test(X) ->
    {"The tests of datatypes that all database types support",
     fun() ->
             []
     end}.

%% Transaction test
transaction_test(X) ->
    {"The tests of transactions that all database types support",
     fun() ->
             []
     end}.

%% Perpared statement test
prepare_statement_test(X) ->
    {"The tests of prepare statements that all database types support",
     fun() ->
             []
     end}.

%% Long SQL test
long_sql_test(X) ->
    {"The tests of long SQL statement that all database types support",
     fun() ->
             []
     end}.

%% Complicated query test
complicated_query_test(X) ->
    {"The tests of complicated query statements that all database types support",
     fun() ->
             []
     end}.


    

