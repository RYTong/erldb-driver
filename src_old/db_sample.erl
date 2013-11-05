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
%%% File    : db_sample.erl
%%% @author deng.lifen <deng.lifen@rytong.com>
%%% @doc Example of the db driver.
%%% @end
%%% -------------------------------------------------------------------

-module(db_sample).
-compile(export_all).

-define(SAMPLE_TABLE, sample_test).
-define(SAMPLE_TABLE_1, sample_test_1).

api_test() ->
    TableName = ?SAMPLE_TABLE,
    test_util:start(TableName),
    R1 = db_api:execute_sql("show tables"),
    R2 = db_api:insert(TableName, [{fvarchar, "hello"}, {ffloat, 12.25}, {fint, 1}]),
    R3 = db_api:insert(TableName, [{fvarchar, "insert test"}, {ffloat, 32}, {fint, 2}]),
    R4 = db_api:select(TableName, {fint, '>', 0},
        [{fields, fint}, {extras, [{order, {fint, desc}}]}]),
    R5 = db_api:select(TableName, [], [{fields, [{count, ['*']}, {sum, [ffloat]}]}]),
    R6 = db_api:update(TableName, [{fvarchar, "update test"}, {fint, 2}], {fvarchar, '=', "hello"}),
    R7 = db_api:select(TableName, [], [{fields, fint},
                                                   {extras, [{order, {fint, desc}}]}]),
    R8 = db_api:select(TableName, [], [{fields, fint},
                                              {extras, [{order, {fint, desc}}]},
                                              {distinct, true}]),
    R9 = db_api:delete(TableName, {fvarchar, '=', "insert test"}),
    R10 = db_api:select(TableName, [], [{fields, {fint, '+', ffloat}}]),
    R11 = db_api:delete(TableName, []),
    R12 = db_api:select(TableName, []),
    R13 = db_api:insert(TableName, [
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
    ]),
    R14 = db_api:update(TableName, [
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
    ], {fyear, '=', 2010}),
    R15 = db_api:delete(TableName, {'and', [{fsmallint, '=', 200},
        {fdatetime, '=', {datetime, {{2010, 3, 24}, {11, 19, 30}}}}]}),
    output([R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R12, R13, R14, R15]),
    test_util:stop(TableName).

driver_test() ->
    %% start db driver.
    Pid = db_driver:start(),

    %% create a connection.
    ConnArg = db_server:get_db_config(test),
    {ok, ConnPool} = db_driver:connect(ConnArg),
    io:format("conn: ~p~n", [ConnPool]),

    %% execute sql string.
    Version = db_driver:execute_sql(ConnPool, "select version()"),
    io:format("database version: ~p~n", [Version]),
    Tables = db_driver:execute_sql(ConnPool, "show tables"),
    io:format("tables: ~p~n", [Tables]),

    %% create test table, drop old if exists.
    CreateSql = test_util:create_sql(?SAMPLE_TABLE),
    CreateSql1 = test_util:create_sql(?SAMPLE_TABLE_1),
    DropSql = test_util:drop_sql(?SAMPLE_TABLE),
    DropSql1 = test_util:drop_sql(?SAMPLE_TABLE_1),
    db_driver:execute_sql(ConnPool, DropSql),
    db_driver:execute_sql(ConnPool, DropSql1),
    db_driver:execute_sql(ConnPool, CreateSql),
    db_driver:execute_sql(ConnPool, CreateSql1),

    %% insert sample.
    insert(ConnPool),

    %% select sample.
    select(ConnPool),

    %% update sample.
    update(ConnPool),

    %% delete sample.
    delete(ConnPool),

    %% prepare statement sample.
    prepare_stmt(ConnPool),

    %% transaction sample.
    transaction(ConnPool),

    %% drop test table.
    db_driver:execute_sql(ConnPool, DropSql),
    db_driver:execute_sql(ConnPool, DropSql1),

    %% destroy a connect.
    db_driver:disconnect(ConnPool),

    %% stop db driver.
    db_driver:stop(Pid).

insert(ConnPool) ->
    R1 = db_driver:insert(ConnPool, ?SAMPLE_TABLE, [
        {fbit, 1},
        {ftinyint, 1},
        {fsmallint, 200},
        {fmediunint, 1323},
        {fint, 38524},
        {fbigint, 2233434},
        {ffloat, 238954.345},
        {fdouble, 3.1415926535},
        {fdecimal, 3.1415926535},
        {fdate, {date, {2010, 3, 24}}},
        {fdatetime, {datetime, {{2010, 3, 24}, {11, 19, 30}}}},
        {ftimestamp, {datetime, {{2010, 3, 1}, {2, 10, 30}}}},
        {ftime, {time, {11, 45, 22}}},
        {fyear, 2010},
        {fchar, 97},
        {fvarchar, "insert"},
        {fbinary, "fjdsgjnkdgbdf dfdfg\r\n isfdk"},
        {fvarbinary, "dkjsor klsjfsdfj"},
        {ftinytext, "dsfgd"},
        {ftext, "have fun!"},
        {fmediumtext, "test medium text"},
        {flongtext, "this is a long text"},
        {ftinyblob, <<"tiny">>},
        {fblob, <<34,56,0,54,75>>},
        {fmediumblob, <<97,98,99,100>>},
        {flongblob, <<"a long blob: a-z0-9~n\\`!@#$%^&*()_+-=,./;'[]<>?:|{}">>}
    ]),
    R2 = db_driver:insert(ConnPool, ?SAMPLE_TABLE,
        [{fvarchar, "insert"}, {ftinyint, 2}, {fint, 10}]),
    R3 = db_driver:insert(ConnPool, ?SAMPLE_TABLE_1,
        [{fvarchar, "insert"}, {ftinyint, 3}, {fint, 20}]),
    output([R1, R2, R3]).

select(ConnPool) ->
    R1 = db_driver:select(ConnPool, ?SAMPLE_TABLE,
        {fblob, '=', <<34,56,0,54,75>>}, []),
    R2 = db_driver:select(ConnPool, ?SAMPLE_TABLE, {'and', [
        {fdatetime, '=', {datetime, {{2010, 3, 24}, {11, 19, 30}}}},
        {ftime, '=', {time, {11, 45, 22}}}]}, []),
    R3 = db_driver:select(ConnPool, ?SAMPLE_TABLE,
        {{fvarchar, '=', "insert"}, 'or', {ftinyint, '=', 1}},
        [{fields, [fvarchar, ftinyint]},
         {extras, [{order, {ftinyint, desc}}, {limit, 1}]},
         {distinct, true}]),
    R4 = db_driver:select(ConnPool, ?SAMPLE_TABLE, [],
        [{fields, [{count, ['*']}, {sum, [ftinyint]}]}]),
    R5 = db_driver:select(ConnPool, ?SAMPLE_TABLE, {ftinyint, '=', 1},
        [{fields, {ftinyint, '+', fint}}]),
    R6 = db_driver:select(ConnPool, ?SAMPLE_TABLE, [], []),
    R7 = db_driver:select(ConnPool, [?SAMPLE_TABLE, {?SAMPLE_TABLE_1, as, t}],
            {'and', [{{?SAMPLE_TABLE, '.', fvarchar}, '=', {t, '.', fvarchar}},
                     {{?SAMPLE_TABLE, '.', ftinyint}, '=', 1}]},
            [{fields, [{?SAMPLE_TABLE, '.', fvarchar},
                       {?SAMPLE_TABLE, '.', ftinyint},
                       {t, '.', fint}]}]),
    output([R1, R2, R3, R4, R5, R6, R7]).

update(ConnPool) ->
    R1 = db_driver:update(ConnPool, ?SAMPLE_TABLE, [
        {fbit, 254},
        {ftinyint, 2},
        {fsmallint, 34},
        {fmediunint, 5465},
        {fint, {{{fint, '-', 8524}, '/', 300}, '*', 2}},
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
    ], {ftinyint, '=', 2}),
    R2 = db_driver:select(ConnPool, ?SAMPLE_TABLE, [], []),
    output([R1, R2]).

delete(ConnPool) ->
    R1 = db_driver:delete(ConnPool, ?SAMPLE_TABLE, {ftinyint, '=', 2}),
    R2 = db_driver:select(ConnPool, ?SAMPLE_TABLE, [], []),
    R3 = db_driver:delete(ConnPool, ?SAMPLE_TABLE, []),
    R4 = db_driver:select(ConnPool, ?SAMPLE_TABLE, [], []),
    output([R1, R2, R3, R4]).

transaction(ConnPool) ->
    R1 = db_driver:transaction(ConnPool, fun trans_fun/3, "hello transaction"),
    R2 = db_driver:transaction(ConnPool, fun trans_failed/3, ["some args"]),
    R3 = db_driver:select(ConnPool, ?SAMPLE_TABLE, {fvarchar, '=', "trans"},
        []),
    output([R1, R2, R3]).

trans_fun(ConnPool, Conn, Arg) ->
    R1 = Arg,
    R2 = db_driver:trans_insert(ConnPool, ?SAMPLE_TABLE,
        [{fvarchar, "trans"}, {ftinyint, 12}], Conn),
    R3 = db_driver:trans_update(ConnPool, ?SAMPLE_TABLE,
        [{fbinary, <<"update">>}, {ftinyint, 5}], {ftinyint, '=', 12}, Conn),
    R4 = db_driver:trans_select(ConnPool, ?SAMPLE_TABLE,
        {fvarchar, '=', "trans"}, Conn, []),
    output([R1, R2, R3, R4]).

trans_failed(ConnPool, Conn, _Arg) ->
    R1 = db_driver:trans_insert(ConnPool, ?SAMPLE_TABLE,
        [{id, 100}, {fvarchar, "trans"}, {ftinyint, 22}], Conn),
    R2 = db_driver:trans_select(ConnPool, ?SAMPLE_TABLE,
        {fvarchar, '=', "trans"}, Conn, []),
    R3 = db_driver:trans_insert(ConnPool, ?SAMPLE_TABLE,
        [{id, 100}, {fvarchar, "trans"}, {ftinyint, 23}], Conn),
    R4 = db_driver:trans_select(ConnPool, ?SAMPLE_TABLE,
        {fvarchar, '=', "trans"}, Conn, []),
    output([R1, R2, R3, R4]).


prepare_stmt(ConnPool) ->
    Table = atom_to_list(?SAMPLE_TABLE),
    PreInsert = "prepare_insert",
    R1 = db_driver:prepare(ConnPool, PreInsert, "insert into " ++ Table
        ++ " (fvarchar, ftinyint) values(?, ?)"),
    R2 = db_driver:prepare_execute(ConnPool, PreInsert, ["prepare_stmt", 31]),
    R3 = db_driver:prepare_execute(ConnPool, PreInsert, ["prepare_stmt", 32]),
    R4 = db_driver:unprepare(ConnPool, PreInsert),

    PreSelect = "prepare_select",
    R5 = db_driver:prepare(ConnPool, PreSelect, "select * from " ++ Table
        ++ " where fvarchar = ?"),
    R6 = db_driver:prepare_execute(ConnPool, PreSelect, ["prepare_stmt"]),
    R7 = db_driver:unprepare(ConnPool, PreSelect),
    output([R1, R2, R3, R4, R5, R6, R7]).

output(L) ->
    output(L, 1).
output([], _N) ->
    ok;
output([Res | L], N) ->
    error_logger:format("Res ~p = ~p~n", [N, Res]),
    output(L, N + 1).