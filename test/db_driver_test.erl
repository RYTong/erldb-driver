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
%%% File    : db_test.erl
%%% @author deng.lifen <deng.lifen@rytong.com>
%%% @doc The db driver's test cases.
%%% @end
%%% -------------------------------------------------------------------

-module(db_driver_test).

-include_lib("eunit/include/eunit.hrl").

-define(TEST_TABLE, db_driver_test_table).

%% test case
basic_test_() ->
    [?_assert(1 == 1)].

%% execute_sql test case.
exec_sql_test() ->
    error_logger:info_msg("=========== exec_sql_test ===========~n", []),
    TableName = atom_to_list(?TEST_TABLE),
    test_util:start(TableName),
    ?assert({ok, 1} == db_api:execute_sql("insert into " ++ TableName ++ " (fvarchar, ffloat, fint) values('denglf', 24, 123)")),
    ?assert({ok, [["denglf"]]} == db_api:execute_sql("select fvarchar from " ++ TableName ++ " where fint = 123")),
    ?assert({ok, 1} == db_api:execute_sql("update " ++ TableName ++ " set ffloat=22.2 where fvarchar = 'denglf'")),
    ?assert({ok, [[22.2, 123]]} == db_api:execute_sql("select ffloat,fint from " ++ TableName ++ " where fint = 123")),
    ?assert({ok, 1} == db_api:execute_sql("delete from " ++ TableName ++ " where fvarchar = 'denglf'")),
    ?assert({ok, []} == db_api:execute_sql("select * from " ++ TableName ++ " where fint = 123")),
    test_util:stop(TableName).
 
execute_test() ->
    error_logger:info_msg("=========== execute_test ===========~n", []),
    TableName = ?TEST_TABLE,
    test_util:start(TableName),
    ?assert({ok, 1} == db_api:insert(TableName, [{fvarchar, "hello"}, {ffloat, 12.25}, {fint, 1}])),
    ?assert({ok, 1} == db_api:insert(TableName, [{fvarchar, "insert test"}, {ffloat, 32}, {fint, 2}])),
    ?assert({ok, [[2], [1]]} == db_api:select(TableName, {fint, '>', 0},
        [{fields, fint}, {extras, [{order, {fint, desc}}]}])),
    ?assert({ok, [[2, 44.25]]} == db_api:select(TableName, [], [{fields, [{count, ['*']}, {sum, [ffloat]}]}])),
    ?assert({ok, 1} == db_api:update(TableName, [{fvarchar, "update test"}, {fint, 2}], {fvarchar, '=', "hello"})),
    ?assert({ok, [[2], [2]]} == db_api:select(TableName, [], [{fields, fint},
                                                   {extras, [{order, {fint, desc}}]}])),
    ?assert({ok, [[2]]} == db_api:select(TableName, [], [{fields, fint}, 
                                              {extras, [{order, {fint, desc}}]},
                                              {distinct, true}])),
    ?assert({ok, 1} == db_api:delete(TableName, {fvarchar, '=', "insert test"})),
    ?assert({ok, [[14.25]]} == db_api:select(TableName, [], [{fields, {fint, '+', ffloat}}])),
    ?assert({ok, 1} == db_api:delete(TableName, [])),
    ?assert({ok, []} == db_api:select(TableName, [])),
    ?assert({ok, 1} == db_api:insert(TableName, [
        {fbit, 1},
        {ftinyint, 1},
        {fsmallint, 200},
        {fmediunint, 1323},
        {fint, 38524},
        {fbigint, 2233434},
        {ffloat, 238954.345},
        {fdouble, 335623.276212},
        {fdecimal, 45656.12},
        {fdate, {date, {2010, 3, 24}}},
        {fdatetime, {datetime, {{2010, 3, 24}, {11, 19, 30}}}},
        {ftimestamp, {datetime, {{2010, 3, 1}, {2, 10, 30}}}},
        {ftime, {time, {11, 45, 22}}},
        {fyear, 2010},
        {fchar, 97},
        {fvarchar, "test varchar"},
        {fbinary, "fjdsgjnkdgbdf dfdfg\r\n isfdk"},
        {fvarbinary, "dkjsor klsjfsdfj"},
        {ftinytext, "dsfgd"},
        {ftext, ";ljkdf"},
        {fmediumtext, "dfuyejksf"},
        {flongtext, "indtvdf"},
        {ftinyblob, <<"hg">>},
        {fblob, <<34,56,0,54,75>>},
        {fmediumblob, <<97,98,99,100>>},
        {flongblob, <<"sdfidsigyrertkjhejkrgweur3[5940766%^#$^&(;lgf khjfgh">>}
    ])),
    ?assert({ok, 1} == db_api:update(TableName, [
        {fbit, 254},
        {ftinyint, 6},
        {fsmallint, 34},
        {fmediunint, 5465},
        {fint, 34354},
        {fbigint, 5646436},
        {ffloat, 4546.343},
        {fdouble, 546355.275312},
        {fdecimal, 3535353.12},
        {fdate, {date, {2010, 3, 3}}},
        {fdatetime, {datetime, {{2010, 3, 8}, {11, 45, 30}}}},
        {ftimestamp, {datetime, {{2010, 1, 1}, {2, 10, 30}}}},
        {ftime, {time, {11, 36, 22}}},
        {fyear, 5},
        {fchar, 99},
        {fvarchar, "test1 varchar"},
        {fbinary, "eyye5ret343r dfdfg\r\n isfdk"},
        {fvarbinary, "!@$%^&*( klsjfsdfj"},
        {ftinytext, "fdgdg"},
        {ftext, "gdljkdf"},
        {fmediumtext, "fhgkgky6u"},
        {flongtext, "urtyr56ru565467yr5yht"},
        {ftinyblob, <<"ytg">>},
        {fblob, <<34,56,0,55,75>>},
        {fmediumblob, <<97,98,99,101>>},
        {flongblob, <<"sdfidgfgdrtrur3[5940766%^#$^&(;lgf khjfgh">>}
    ], {fyear, '=', 2010})),
    ?assert({ok, 0} == db_api:delete(TableName, {'and', [{fsmallint, '=', 200},
        {fdatetime, '=', {datetime, {{2010, 3, 24}, {11, 19, 30}}}}]})),
    test_util:stop(TableName).

datetime_test() ->
    error_logger:info_msg("=========== datetime_test ===========~n", []),
    TableName = ?TEST_TABLE,
    test_util:start(TableName),
    ?assert({ok, 1} == db_api:insert(TableName, [{fvarchar, "date"}, 
        {fint, 23},
        {ffloat, 1.25},
        {fdatetime, {datetime, {{2010, 2, 15}, {1, 2, 3}}}},
        {fdate, {date, {2010, 2, 3}}},
        {ftime, {time, {5, 6, 7}}}])),
    ?assert({ok, [["date", {datetime, {{2010, 2, 15}, {1, 2, 3}}}]]} == db_api:select(TableName,
        {'and', [{fdatetime, '=', {datetime, {{2010, 2, 15}, {1, 2, 3}}}}, {ftime, '=', {time, {5, 6, 7}}}]},
        [{fields, [fvarchar, fdatetime]}])),
    ?assert({ok, 1} == db_api:update(TableName, [{fdate, {date, {2010, 12, 27}}}], 
        {'and', [{fdatetime, '=', {datetime, {{2010, 2, 15}, {1, 2, 3}}}}, {ftime, '=', {time, {5, 6, 7}}}]})),
    ?assert({ok, [["date", {date, {2010, 12, 27}}]]} == db_api:select(TableName,
        {'and', [{fdatetime, '=', {datetime, {{2010, 2, 15}, {1, 2, 3}}}}, {ftime, '=', {time, {5, 6, 7}}}]},
        [{fields, [fvarchar, fdate]}])),
    ?assert({ok, 1} == db_api:delete(TableName, 
        {'and', [{fdatetime, '=', {datetime, {{2010, 2, 15}, {1, 2, 3}}}}, {ftime, '=', {time, {5, 6, 7}}}]})),
    ?assert({ok, []} == db_api:select(TableName, [])),
    test_util:stop(TableName).

binary_test() ->
    error_logger:info_msg("=========== binary_test ===========~n", []),
    TableName = ?TEST_TABLE,
    test_util:start(TableName),
    ?assert({ok, 1} == db_api:insert(TableName, [{fvarchar, "测试编码"}, {fblob, <<1,2,0,3,7,0,23>>}])),
    ?assert({ok, [["测试编码", <<1,2,0,3,7,0,23>>]]} == db_api:select(TableName, [], [{fields, [fvarchar, fblob]}])),
    test_util:stop(TableName).

long_sql_test() ->
    error_logger:info_msg("=========== long_sql_test ===========~n", []),
    TableName = atom_to_list(?TEST_TABLE),
    test_util:start(TableName),
    N = 1000,
    LongData = lists:foldl(
            fun(Id, Acc) ->
                Acc ++ ",("++ integer_to_list(Id) ++
                    ",123,'2010-10-25 10:58:20','test long insert sql',123.123)"
            end
        , "", lists:seq(2,N)),
    Sql = "INSERT INTO " ++ TableName ++ "(id, fint, ftime, fvarchar, ffloat)"
        " VALUES(1,123,'2010-10-25 10:58:20','test long insert sql',123.123)"
        ++ LongData,
    ?assert({ok, N} == db_api:execute_sql(Sql)),
    test_util:stop(TableName).

trans_test() ->
    error_logger:info_msg("=========== trans_test ===========~n", []),
    TableName = ?TEST_TABLE,
    test_util:start(TableName),
    ?assert({atomic, ok} == db_api:transaction(fun trans_fun/3, [])),
    ?assertMatch({aborted,{{error,_Err}, {rollback_result,{ok,"ROLLBACK"}}}},
        db_api:transaction(fun trans_failed_fun/3, [])),
    ?assert({ok, []} == db_api:select(TableName, [])),
    test_util:stop(TableName).

trans_fun(_DbName, Conn, _Arg) ->
    TableName = ?TEST_TABLE,
    ?assert({ok, 1} == db_api:trans_insert(TableName, [{fvarchar, "hello"}, {ffloat, 12.25}, {fint, 1}], Conn)),
    ?assert({ok, 1} == db_api:trans_insert(TableName, [{fvarchar, "insert test"}, {ffloat, 32}, {fint, 2}], Conn)),
    ?assert({ok, [[2], [1]]} == db_api:trans_select(TableName, {fint, '>', 0}, Conn,
        [{fields, fint},
         {extras, [{order, {fint, desc}}]}])),
    ?assert({ok, [[2, 44.25]]} == db_api:trans_select(TableName, [], Conn,
        [{fields, [{count, ['*']}, {sum, [ffloat]}]}])),
    ?assert({ok, 1} == db_api:trans_update(TableName, [{fvarchar, "update test"}, {fint, 2}], {fvarchar, '=', "hello"}, Conn)),
    ?assert({ok, [[2], [2]]} == db_api:trans_select(TableName, [], Conn,
        [{fields, fint},
         {extras, [{order, {fint, desc}}]}])),
    ?assert({ok, [[2]]} == db_api:trans_select(TableName, [], Conn,
             [{fields, fint},
              {extras, [{order, {fint, desc}}]},
              {distinct, true}])),
    ?assert({ok, 1} == db_api:trans_delete(TableName, {fvarchar, '=', "insert test"}, Conn)),
    ?assert({ok, [[14.25]]} == db_api:trans_select(TableName, [], Conn,
        [{fields, {fint, '+', ffloat}}])),
    ?assert({ok, 1} == db_api:trans_delete(TableName, [], Conn)).

trans_failed_fun(_DbName, Conn, _Arg) ->
    TableName = ?TEST_TABLE,
    ?assert({ok, 1} == db_api:trans_insert(TableName, [{id, 1}, {fvarchar, "trans test"}], Conn)),
    db_api:trans_insert(TableName, [{id, 1}, {fvarchar, "trans test"}], Conn),
    db_api:trans_insert(TableName, [{id, 2}, {fvarchar, "trans test"}], Conn).

stmt_test() ->
    error_logger:info_msg("=========== stmt_test ===========~n", []),
    TableName = atom_to_list(?TEST_TABLE),
    PInsert = "p_insert",
    PSelect = "p_select",
    test_util:start(TableName),
    ?assert({ok, PInsert} == db_api:prepare(PInsert, "insert into " ++ TableName ++ " (fvarchar, fint, ffloat) values(?, ?, ?)")),
    ?assert({ok, PSelect} == db_api:prepare(PSelect, "select fvarchar from " ++ TableName ++ " where fint = ?")),
    ?assert({ok, 1} == db_api:prepare_execute(PInsert, ["denglf", 1, 100])),
    ?assert({ok, [["denglf"]]} == db_api:prepare_execute(PSelect, [1])),
    ?assert({ok,"close stmt"} == db_api:unprepare(PInsert)),
    ?assert({ok,"close stmt"} == db_api:unprepare(PSelect)),
    test_util:stop(TableName).

%% fixme: connect_test should be updated after new implement of db driver
connect_test() ->
    error_logger:info_msg("=========== connect_test ===========~n", []),
    TableName = atom_to_list(?TEST_TABLE),
    test_util:start(TableName),
    db_api:execute_sql("drop database if exists connect_test_1"),
    db_api:execute_sql("drop database if exists connect_test_2"),
    db_api:execute_sql("create database connect_test_1"),
    db_api:execute_sql("create database connect_test_2"),
    ConnArg = db_server:get_db_config(test),
    A1 = lists:keyreplace(database, 1, ConnArg, {database, "connect_test_1"}),
    A2 = lists:keyreplace(database, 1, ConnArg, {database, "connect_test_2"}),
    {ok, C1} = db_driver:connect(A1),
    {ok, C2} = db_driver:connect(A2),
    CreateSql = test_util:create_sql(TableName),
    ?assert({ok, 0} == db_driver:execute_sql(C1, CreateSql)),
    ?assert({ok, 0} == db_driver:execute_sql(C2, CreateSql)),
    ?assert({ok, 1} == db_driver:execute_sql(C1, "insert into " ++ TableName ++ " (fvarchar, ffloat, fint) values('denglf', 24, 123)")),
    ?assert({ok, [["denglf"]]} == db_driver:execute_sql(C1, "select fvarchar from " ++ TableName ++ " where fint = 123")),
    ?assert({ok, []} == db_driver:execute_sql(C2, "select * from " ++ TableName)),
    ?assert({ok, 1} == db_driver:insert(C2, ?TEST_TABLE, [{fvarchar, "hello"}, {ffloat, 12.25}, {fint, 1}])),
    ?assert({ok, 1} == db_driver:insert(C2, ?TEST_TABLE, [{fvarchar, "insert test"}, {ffloat, 32}, {fint, 2}])),
    ?assert({ok, [[2], [1]]} == db_driver:select(C2, ?TEST_TABLE, {fint, '>', 0},
        [{fields, fint},
         {extras, [{order, {fint, desc}}]}])),
    db_driver:disconnect(C1),
    db_driver:disconnect(C2),
    db_api:execute_sql("drop database connect_test_1"),
    db_api:execute_sql("drop database connect_test_2"),
    test_util:stop(TableName).
