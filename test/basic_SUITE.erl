%% common_test suite for basic

-module(basic_SUITE).
-include_lib("common_test/include/ct.hrl").

-compile(export_all).

-define(POOL, list_to_atom(lists:concat([pool, ?MODULE]))).

%%--------------------------------------------------------------------
%% Function: suite() -> Info
%%
%% Info = [tuple()]
%%   List of key/value pairs.
%%
%% Description: Returns list of tuples to set default properties
%%              for the suite.
%%
%% Note: The suite/0 function is only meant to be used to return
%% default data values, not perform any other operations.
%%--------------------------------------------------------------------
suite() ->
    [
        {timetrap, {seconds, 20}},
        {require, test}
        ].

%%--------------------------------------------------------------------
%% Function: groups() -> [Group]
%%
%% Group = {GroupName,Properties,GroupsAndTestCases}
%% GroupName = atom()
%%   The name of the group.
%% Properties = [parallel | sequence | Shuffle | {RepeatType,N}]
%%   Group properties that may be combined.
%% GroupsAndTestCases = [Group | {group,GroupName} | TestCase]
%% TestCase = atom()
%%   The name of a test case.
%% Shuffle = shuffle | {shuffle,Seed}
%%   To get cases executed in random order.
%% Seed = {integer(),integer(),integer()}
%% RepeatType = repeat | repeat_until_all_ok | repeat_until_all_fail |
%%              repeat_until_any_ok | repeat_until_any_fail
%%   To get execution of cases repeated.
%% N = integer() | forever
%%
%% Description: Returns a list of test case group definitions.
%%--------------------------------------------------------------------
groups() -> [].

%%--------------------------------------------------------------------
%% Function: all() -> GroupsAndTestCases
%%
%% GroupsAndTestCases = [{group,GroupName} | TestCase]
%% GroupName = atom()
%%   Name of a test case group.
%% TestCase = atom()
%%   Name of a test case.
%%
%% Description: Returns the list of groups and test cases that
%%              are to be executed.
%%
%%      NB: By default, we export all 1-arity user defined functions
%%--------------------------------------------------------------------
all() ->
    [ {exports, Functions} | _ ] = ?MODULE:module_info(),
    [ FName || {FName, _} <- lists:filter(
                fun ({module_info,_}) -> false;
                    ({all,_}) -> false;
                    ({init_per_suite,1}) -> false;
                    ({end_per_suite,1}) -> false;
                    ({_,1}) -> true;
                    ({_,_}) -> false
                end, Functions)].

%%--------------------------------------------------------------------
%% Function: init_per_suite(Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%%
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for skipping the suite.
%%
%% Description: Initialization before the suite.
%%
%% Note: This function is free to add any key/value pairs to the Config
%% variable, but should NOT alter/remove any existing entries.
%%--------------------------------------------------------------------
init_per_suite(Config) ->
    ok = db_app:start(),
    ct:log("~p-~p: Config = ~p~n", [?MODULE, ?LINE, Config]),
    ConnArg = ct:get_config(test),
    ok = db_app:add_pool(?POOL, ConnArg),
    ct:comment("Start db application and add pool."),
    Config.

%%--------------------------------------------------------------------
%% Function: end_per_suite(Config0) -> void() | {save_config,Config1}
%%
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%%
%% Description: Cleanup after the suite.
%%--------------------------------------------------------------------
end_per_suite(_Config) ->
    ok = db_app:remove_pool(?POOL),
    ok = db_app:stop(),
    ct:comment("Remove and stop db application."),
    ok.

%%--------------------------------------------------------------------
%% Function: init_per_group(GroupName, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%%
%% GroupName = atom()
%%   Name of the test case group that is about to run.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding configuration data for the group.
%% Reason = term()
%%   The reason for skipping all test cases and subgroups in the group.
%%
%% Description: Initialization before each test case group.
%%--------------------------------------------------------------------
init_per_group(_group, Config) ->
    Config.

%%--------------------------------------------------------------------
%% Function: end_per_group(GroupName, Config0) ->
%%               void() | {save_config,Config1}
%%
%% GroupName = atom()
%%   Name of the test case group that is finished.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding configuration data for the group.
%%
%% Description: Cleanup after each test case group.
%%--------------------------------------------------------------------
end_per_group(_group, Config) ->
    Config.

%%--------------------------------------------------------------------
%% Function: init_per_testcase(TestCase, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%%
%% TestCase = atom()
%%   Name of the test case that is about to run.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for skipping the test case.
%%
%% Description: Initialization before each test case.
%%
%% Note: This function is free to add any key/value pairs to the Config
%% variable, but should NOT alter/remove any existing entries.
%%--------------------------------------------------------------------
init_per_testcase(TestCase, Config) ->
    ct:log("~p-~p: TestCase = ~p~n", [?MODULE, ?LINE, TestCase]),
    Table = atom_to_list(TestCase) ++ "_table",
    {ok, 0} = db_app:execute_sql("DROP TABLE IF EXISTS " ++ Table),
    {ok, 0} = db_app:execute_sql("CREATE TABLE " ++ Table ++ " (
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
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8"),
    ct:comment("Create table."),
    [{table_name, Table}|Config].

%%--------------------------------------------------------------------
%% Function: end_per_testcase(TestCase, Config0) ->
%%               void() | {save_config,Config1} | {fail,Reason}
%%
%% TestCase = atom()
%%   Name of the test case that is finished.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for failing the test case.
%%
%% Description: Cleanup after each test case.
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, Config) ->
    Table = ?config(table_name, Config),
    {ok, _} = db_app:execute_sql("DROP TABLE IF EXISTS " ++ Table),
    ct:comment("Drop table."),
    Config.

%% execute_sql test case.
exec_sql(Config) ->
    Table = ?config(table_name, Config),
    {ok, 1} = db_app:execute_sql("insert into " ++ Table ++ " (fvarchar, ffloat, fint) values('denglf', 24, 123)"),
    {ok, [["denglf"]]} = db_app:execute_sql("select fvarchar from " ++ Table ++ " where fint = 123"),
    {ok, 1} = db_app:execute_sql("update " ++ Table ++ " set ffloat=22.2 where fvarchar = 'denglf'"),
    {ok, [[22.2, 123]]} = db_app:execute_sql("select ffloat,fint from " ++ Table ++ " where fint = 123"),
    {ok, 1} = db_app:execute_sql("delete from " ++ Table ++ " where fvarchar = 'denglf'"),
    {ok, []} = db_app:execute_sql("select * from " ++ Table ++ " where fint = 123"),
    {comment, "Test the basic SQL statements."}.

execute_param(Config) ->
    Table = ?config(table_name, Config),
    {ok, 1} = db_app:execute_param("insert into " ++ Table ++ " (fvarchar, ffloat, fint) values(?, ?, ?)", ["denglf", 24, 128]),
    {ok, [["denglf"]]} = db_app:execute_param("select fvarchar from " ++ Table ++ " where fint = ?", [128]),
    {ok, 1} = db_app:execute_param("update " ++ Table ++ " set ffloat=? where fvarchar = ?", [22.2, "denglf"]),
    {ok, [[22.2, 128]]} = db_app:execute_param("select ffloat,fint from " ++ Table ++ " where fint = 128", []),
    {ok, 1} = db_app:execute_param("delete from " ++ Table ++ " where fvarchar = ?", ["denglf"]),
    {ok, []} = db_app:execute_param("select * from " ++ Table ++ " where fint = ?", [128]),
    {comment, "Test the execute SQL statements."}.

execute(Config) ->
    Table = list_to_atom(?config(table_name, Config)),
    {ok, 1} = db_app:insert(Table, [{fvarchar, "hello"}, {ffloat, 12.25}, {fint, 1}]),
    {ok, 1} = db_app:insert(Table, [{fvarchar, "insert test"}, {ffloat, 32}, {fint, 2}]),
    {ok, [[2], [1]]} = db_app:select(Table, {fint, '>', 0}, [
                {fields, fint},
                {extras, [{order, {fint, desc}}]}]),
    {ok, [[2, 44.25]]} = db_app:select(Table, [], [
                {fields, [{count, ['*']},
                        {sum, [ffloat]}]}]),
    {ok, 1} = db_app:update(Table, [
                {fvarchar, "update test"},
                {fint, {{{fint, '*', 2}, '+', 1}, '-', fint}}],
            {fvarchar, '=', "hello"}),
    {ok, [[2], [2]]} = db_app:select(Table, [], [
                {fields, fint},
                {extras, [{order, {fint, desc}}]}]),
    {ok, [[2]]} = db_app:select(Table, [], [
                {fields, fint},
                {extras, [{order, {fint, desc}}]},
                {distinct, true}]),
    {ok, 1} = db_app:delete(Table, {fvarchar, '=', "insert test"}),
    {ok, [[14.25]]} = db_app:select(Table, [], [{fields, {fint, '+', ffloat}}]),
    {ok, 1} = db_app:delete(Table, []),
    {ok, []} = db_app:select(Table, []),
    {ok, 1} = db_app:insert(Table, [
                {fbit, 1},
                {ftinyint, 1},
                {fsmallint, 200},
                {fmediunint, 1323},
                {fint, 38524},
                {fbigint, 2233434},
                {ffloat, 238954.345},
                {fdouble, 335623.276212},
                {fdecimal, 456.228364385638},
                {fdate, {date, {2010, 3, 24}}},
                {fdatetime, {datetime, {{2010, 3, 24}, {11, 19, 30}}}},
                {ftimestamp, {datetime, {{2010, 3, 1}, {2, 10, 30}}}},
                {ftime, {time, {11, 45, 22}}},
                {fyear, 2010},
                {fchar, "1"},
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
    {ok, 1} = db_app:update(Table, [
                {fbit, 15},
                {ftinyint, 6},
                {fsmallint, 34},
                {fmediunint, 5465},
                {fint, 34354},
                {fbigint, 5646436},
                {ffloat, 4546.343},
                {fdouble, 546355.275312},
                {fdecimal, 35.3535312},
                {fdate, {date, {2010, 3, 3}}},
                {fdatetime, {datetime, {{2010, 3, 8}, {11, 45, 30}}}},
                {ftimestamp, {datetime, {{2010, 1, 1}, {2, 10, 30}}}},
                {ftime, {time, {11, 36, 22}}},
                {fyear, 5},
                {fchar, "b"},
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
    {ok, 0} = db_app:delete(Table, {'and', [
                    {fsmallint, '=', 200},
                    {fdatetime, '=', {datetime, {{2010, 3, 24}, {11, 19, 30}}}}]}),
    {comment, "Test of basic data types and insert, update, delete, and select operations."}.

long_sql(Config) ->
    Table = ?config(table_name, Config),
    N = 1000,
    LongData = lists:foldl(
            fun(Id, Acc) ->
                    Acc ++ ",("++ integer_to_list(Id) ++
                    ",123,'2010-10-25 10:58:20','test long insert sql',123.123)"
            end
            , "", lists:seq(2,N)),
    Sql = "INSERT INTO " ++ Table ++ "(id, fint, ftime, fvarchar, ffloat)"
        " VALUES(1,123,'2010-10-25 10:58:20','test long insert sql',123.123)"
        ++ LongData,
    {ok, N} = db_app:execute_sql(Sql),
    {comment, "Test long SQL statements."}.

datetime(Config) ->
    Table = list_to_atom(?config(table_name, Config)),
    {ok, 1} = db_app:insert(Table, [{fvarchar, "date"},
                {fint, 23},
                {ffloat, 1.25},
                {fdatetime, {datetime, {{2010, 2, 15}, {1, 2, 3}}}},
                {fdate, {date, {2010, 2, 3}}},
                {ftime, {time, {5, 6, 7}}}]),
    {ok, [["date", {datetime, {{2010, 2, 15}, {1, 2, 3}}}]]} = db_app:select(Table,
            {'and', [{fdatetime, '=', {datetime, {{2010, 2, 15}, {1, 2, 3}}}}, {ftime, '=', {time, {5, 6, 7}}}]},
            [{fields, [fvarchar, fdatetime]}]),
    {ok, 1} = db_app:update(Table, [{fdate, {date, {2010, 12, 27}}}],
            {'and', [{fdatetime, '=', {datetime, {{2010, 2, 15}, {1, 2, 3}}}}, {ftime, '=', {time, {5, 6, 7}}}]}),
    {ok, [["date", {date, {2010, 12, 27}}]]} = db_app:select(Table,
            {'and', [{fdatetime, '=', {datetime, {{2010, 2, 15}, {1, 2, 3}}}}, {ftime, '=', {time, {5, 6, 7}}}]},
            [{fields, [fvarchar, fdate]}]),
    {ok, 1} = db_app:delete(Table,
            {'and', [{fdatetime, '=', {datetime, {{2010, 2, 15}, {1, 2, 3}}}}, {ftime, '=', {time, {5, 6, 7}}}]}),
    {ok, []} = db_app:select(Table, []),
    {comment, "Test datetime data type."}.

binary(Config) ->
    Table = list_to_atom(?config(table_name, Config)),
    {ok, 1} = db_app:insert(Table, [{fvarchar, "测试编码"}, {fblob, <<1,2,0,3,7,0,23>>}]),
    {ok, [["测试编码", <<1,2,0,3,7,0,23>>]]} = db_app:select(Table, [], [{fields, [fvarchar, fblob]}]),
    {comment, "Test binary data type."}.

trans(Config) ->
    TableName = list_to_atom(?config(table_name, Config)),
    {atomic, ok} = db_app:transaction(
            fun() ->
                    Table = TableName,
                    {ok, 1} = db_app:insert(Table, [{fvarchar, "hello"}, {ffloat, 12.25}, {fint, 1}]),
                    {ok, 1} = db_app:insert(Table, [{fvarchar, "insert test"}, {ffloat, 32}, {fint, 2}]),
                    {ok, [[2], [1]]} = db_app:select(Table, {fint, '>', 0},
                            [{fields, fint},
                                {extras, [{order, {fint, desc}}]}]),
                    {ok, [[2, 44.25]]} = db_app:select(Table, [],
                            [{fields, [{count, ['*']}, {sum, [ffloat]}]}]),
                    {ok, 1} = db_app:update(Table,
                            [{fvarchar, "update test"}, {fint, {fint, '+', 1}}], {fvarchar, '=', "hello"}),
                    {ok, [[2], [2]]} = db_app:select(Table, [],
                            [{fields, fint},
                                {extras, [{order, {fint, desc}}]}]),
                    {ok, [[2]]} = db_app:select(Table, [],
                            [{fields, fint},
                                {extras, [{order, {fint, desc}}]},
                                {distinct, true}]),
                    {ok, 1} = db_app:delete(Table, {fvarchar, '=', "insert test"}),
                    {ok, [[14.25]]} = db_app:select(Table, [],
                            [{fields, {fint, '+', ffloat}}]),
                    {ok, 1} = db_app:delete(Table, []),
                    ok
            end),
    {aborted, {{error, _Err}, {rollback_result, {ok, "ROLLBACK"}}}} = db_app:transaction(
            fun() ->
                    Table = TableName,
                    {ok, 1} = db_app:insert(Table, [{id, 1}, {fvarchar, "trans test"}]),
                    db_app:insert(Table, [{id, 1}, {fvarchar, "trans test"}]),
                    db_app:insert(Table, [{id, 2}, {fvarchar, "trans test"}])
            end),
    {ok, []} = db_app:select(TableName, []),
    {comment, "Test transactions."}.

connect(Config) ->
    TableName = ?config(table_name, Config),
    Table = list_to_atom(TableName),
    {ok, _} = db_app:execute_sql("drop database if exists test_connect_1"),
    {ok, _} = db_app:execute_sql("drop database if exists test_connect_2"),
    {ok, 1} = db_app:execute_sql("create database test_connect_1"),
    {ok, 1} = db_app:execute_sql("create database test_connect_2"),
    ConnArg = ct:get_config(test),
    A1 = lists:keydelete(default_pool, 1, lists:keyreplace(database, 1, ConnArg, {database, "test_connect_1"})),
    A2 = lists:keydelete(default_pool, 1, lists:keyreplace(database, 1, ConnArg, {database, "test_connect_2"})),
    C1 = {test_connect_1},
    C2 = {test_connect_2},
    ok = db_app:add_pool(C1, A1),
    ok = db_app:add_pool(C2, A2),
    CreateSql = "CREATE TABLE " ++ TableName ++ " (
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
    {ok, 0} = db_app:execute_sql(CreateSql, [{pool, C1}]),
    {ok, 0} = db_app:execute_sql(CreateSql, [{pool, C2}]),
    {ok, 1} = db_app:execute_param("insert into " ++ TableName ++ " (fvarchar, ffloat, fint) values(?,?,?)", ["denglf", 24, 123], [{pool, C1}]),
    {ok, [["denglf"]]} = db_app:select(Table, {fint, '>', 0}, [{fields, fvarchar}, {pool, C1}]),
    {ok, []} = db_app:select(Table, [], [{pool, C2}]),

    {ok, 1} = db_app:insert(Table, [{fvarchar, "hello"}, {ffloat, 12.25}, {fint, 1}], [{pool, C2}]),
    {ok, 1} = db_app:insert(Table, [{fvarchar, "insert test"}, {ffloat, 3.2}, {fint, 2}], [{pool, C2}]),
    {ok, 1} = db_app:update(Table, [{fint, 3}], {fvarchar, '=', "insert test"}, [{pool, C2}]),
    {ok, [[3, 3.2], [1, 12.25]]} = db_app:select(Table, {fint, '>', 0},
            [{pool, C2},
                {fields, [fint, ffloat]},
                {extras, [{order, {fint, desc}}]}]),
    ok = db_app:remove_pool(C1),
    %% ok = db_app:remove_pool(C2),
    {ok, 1} = db_app:execute_sql("drop database test_connect_1"),
    {ok, 1} = db_app:execute_sql("drop database test_connect_2"),
    {comment, "Test connect pools."}.

stmt(Config) ->
    Table = ?config(table_name, Config),
    PInsert = p_insert,
    PSelect = p_select,
    ok = db_app:prepare(PInsert, "insert into " ++ Table ++ " (fvarchar, fint) values(?, ?)"),
    ok = db_app:prepare(PSelect, "select fvarchar from " ++ Table ++ " where fint = ?"),
    {ok, 1} = db_app:prepare_execute(PInsert, ["denglf", 1]),
    {ok, [["denglf"]]} = db_app:prepare_execute(PSelect, [1]),
    {error, _} = db_app:prepare(PSelect, "select fint from " ++ Table ++ " where fvarchar = 'denglf'"),
    {ok, [["denglf"]]} = db_app:prepare_execute(PSelect, [1]),
    {comment, "Test prepare statements."}.

stored_procedure(Config) ->
    Table = ?config(table_name, Config),
    {ok, 1} = db_app:execute_sql("insert into " ++ Table ++ " (fvarchar, fint) values('denglf', 123)"),
    {ok, 1} = db_app:execute_sql("insert into " ++ Table ++ " (fvarchar, fint) values('denglf', 456)"),
    {ok, 1} = db_app:execute_sql("insert into " ++ Table ++ " (fvarchar, fint) values('denglf', 789)"),
    Procedure = "procedure" ++ Table,
    {_, _} = db_app:execute_sql("drop procedure " ++ Procedure),
    {ok, _} = db_app:execute_sql("create procedure " ++ Procedure ++ "() begin select fvarchar,fint from " ++ Table ++ " limit 2; end"),
    {ok, [["denglf",123],["denglf",456]]} = db_app:execute_sql("call " ++ Procedure ++ "();"),
    {ok, 0} = db_app:execute_sql("drop procedure " ++ Procedure),
    {comment, "Test procedure."}.

change_pool_size(Config) ->
    PoolId = list_to_atom(?config(table_name, Config)),
    ConnArg = lists:keydelete(default_pool, 1, ct:get_config(test)),
    ok = db_app:add_pool(PoolId, lists:keyreplace(poolsize, 1, ConnArg, {poolsize, 5})),
    5 = db_app:get_pool_info(PoolId, size),
    ok = db_app:increment_pool_size(PoolId, 10),
    15 = db_app:get_pool_info(PoolId, size),
    ok = db_app:decrement_pool_size(PoolId, 8),
    7 = db_app:get_pool_info(PoolId, size),
    ok = db_app:decrement_pool_size(PoolId, 8),
    0 = db_app:get_pool_info(PoolId, size),
    ok = db_app:remove_pool(PoolId),
    {error, pool_not_found} = (catch(db_app:get_pool_info(PoolId, size))),
    {comment, "Test change pool size."}.

get_table_schema(Config) ->
    Table = ?config(table_name, Config),
    PoolId = list_to_atom(Table),
    ConnArg = lists:keydelete(default_pool, 1, ct:get_config(test)),
    ok = db_app:add_pool(PoolId, lists:keyreplace(poolsize, 1, ConnArg, {poolsize, 5})),
    Keys = db_app:get_table_schema(PoolId, Table),
    db_app:refresh_table_schemas(),
    Keys = db_app:get_table_schema(Table),
    ct:log("Keys = ~p~n", [Keys]),
    {comment, "Test get keys."}.


test_basic() ->
    [{userdata,[{doc,"Testing the basic module"}]}].

test_basic(Config) ->
    Table = ?config(table_name, Config),
    {ok, 1} = db_app:execute_sql("insert into " ++ Table ++ " (id, fvarchar) values(1, 'a')"),
    {ok, [[1, "a"]]} = db_app:execute_sql("select id,fvarchar from " ++ Table),
    {comment, "Test case."}.
