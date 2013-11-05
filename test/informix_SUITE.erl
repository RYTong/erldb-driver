%% common test for informix


-module(informix_SUITE).
-include_lib("common_test/include/ct.hrl").

-compile(export_all).

-define(POOL, list_to_atom(lists:concat([pool, ?MODULE]))).

suite() ->
    [
        {timetrap, {seconds, 30}},
        {require, informix}
    ].

all() ->
    [
        execute_sql %% execute_sql api
        ,sql_api %% insert, update, delete and select api
        ,expression %% expressions
        ,datetime %% datetime data
        ,lob %% text, byte, clob and blob data
        ,long_sql %% execute_sql using string with long length
        ,trans %% transaction api
        ,stmt %% prepare statement api
        ,stored_procedure %% execute_sql using stored procedure
%        ,change_pool_size %% change connection pool
    ].

init_per_suite(Config) ->
    ok = db_app:start(),
    ct:log("~p-~p: Config = ~p~n", [?MODULE, ?LINE, Config]),
    ConnArg = ct:get_config(informix),
    ok = db_app:add_pool(?POOL, ConnArg),
    ct:comment("Start db application and add pool."),
    Config.

end_per_suite(_Config) ->
    ok = db_app:remove_pool(?POOL),
    ok = db_app:stop(),
    ct:comment("Remove and stop db application."),
    ok.

init_per_testcase(TestCase, Config) ->
    ct:log("~p-~p: TestCase = ~p~n", [?MODULE, ?LINE, TestCase]),
    Table = atom_to_list(TestCase) ++ "_table",
    {ok, _} = db_app:execute_sql("DROP TABLE IF EXISTS " ++ Table),
    {ok, _} = db_app:execute_sql("create table " ++ Table ++ " (
  fchar char(20) default NULL,
  fnchar nchar(20) default NULL,
  fvarchar varchar(20,20) default NULL,
  fnvarchar nvarchar(20,20) default NULL,

  fint integer default NULL,
  fsmall smallint default NULL,
  fserial serial,

  ffloat float default NULL,
  fsmallfloat smallfloat default NULL,

  fserial8 serial8 ,
  fint8 int8 default NULL,
  fdecimal decimal(32) default NULL,
  fmoney money(16,2) default NULL,

  fdate date default NULL,
  fdatetime datetime year to fraction(3) default NULL,
  finterval1 interval year to month default NULL,
  finterval2 interval day to fraction(3) default NULL,

  fbyte byte default NULL,
  fblob blob default NULL,
  ftext text default NULL,
  fclob clob default NULL)"),
    [{table_name, Table}|Config].

end_per_testcase(_TestCase, Config) ->
    Config.


execute_sql(Config) ->
    Table = ?config(table_name, Config),
    {ok, 1} = db_app:execute_sql("insert into " ++ Table ++ " (fvarchar, ffloat, fint) values('denglf', 24, 123)"),
    {ok, [["denglf"]]} = db_app:execute_sql("select fvarchar from " ++ Table ++ " where fint = 123"),
    {ok, 1} = db_app:execute_sql("update " ++ Table ++ " set ffloat=22.2 where fvarchar = 'denglf'"),
    {ok, [[22.2, 123]]} = db_app:execute_sql("select ffloat,fint from " ++ Table ++ " where fint = 123"),
    {ok, 1} = db_app:execute_sql("delete from " ++ Table ++ " where fvarchar = 'denglf'"),
    {ok, []} = db_app:execute_sql("select * from " ++ Table ++ " where fint = 123"),
    {comment, "Test the basic SQL statements."}.

sql_api(Config) ->
    Table = list_to_atom(?config(table_name, Config)),
    Int = 38524,
    Small = 200,
    {ok, 1} = db_app:insert(Table, [
        {fchar, "fchar"},
        {fnchar, "fnchar"},
        {fvarchar, "fvarchar"},
        {fnvarchar, "fnvarchar"},
        {fint, Int},
        {fsmall, Small},
        {ffloat, 335623.276212},
        {fsmallfloat, 238954.345}
    ]),
    {ok, [[1]]} = db_app:select(Table, [], [{fields, [fserial]}]),
    {ok, []} = db_app:select(Table, {fchar, '=', undefined}),
    Num = Int + Small,
    {ok, 1} = db_app:update(Table, [{fint, {fint, '+', fsmall}}], {fserial, '=', 1}),
    {ok, [[Num]]} = db_app:select(Table, {fserial, '=', 1}, [{fields, [fint]}]),

    CNString = "好的，亲！",
    {ok, 1} = db_app:insert(Table, [{fvarchar, CNString}]),
    {ok, [[CNString]]} = db_app:select(Table, {fserial, '=', 2}, [{fields, [fvarchar]}]),
    {comment, "Test of api of insert, delete, update and select."}.

expression(Config) ->
    Table = list_to_atom(?config(table_name, Config)),
    Table2 = list_to_atom(?config(table_name, Config) ++ "2"),
    {ok, 1} = db_app:insert(Table, [{fvarchar, "group1"}, {fint, 1}]),
    {ok, 1} = db_app:insert(Table, [{fvarchar, "group1"}, {fint, 3}]),
    {ok, 1} = db_app:insert(Table, [{fvarchar, "group2"}, {fint, 1}]),
    {ok, 1} = db_app:insert(Table, [{fvarchar, "group2"}, {fint, 3}]),
    {ok, [[3]]} = db_app:select(Table, [], [{fields, [{max, [fint]}]}]),
    {ok, [["group1", 3], ["group2", 3]]} = db_app:select(Table, [],
        [{fields, [fvarchar, fint]},
            {extras, [{group, [fvarchar, fint]}, {having, {fint, in, [3, 4]}}, {order, {fvarchar, asc}}]}
        ]),

    {ok, _} = db_app:execute_sql("drop table if exists " ++ atom_to_list(Table2)),
    {ok, _} = db_app:execute_sql("create table " ++ atom_to_list(Table2) ++ "(
        fvarchar varchar(20) default NULL,
        fint integer default NULL)"),
    {ok, 1} = db_app:insert(Table2, [{fvarchar, "group3"}, {fint, 1}]),
    {ok, 1} = db_app:insert(Table2, [{fvarchar, "group3"}, {fint, 3}]),
    JoinExpr = {{Table, as, t1}, join, {Table2, as, t2}, {{t1, '.', fint}, '=', {t2, '.', fint}}},
    Fields = [{t1, '.', fvarchar}, {t2, '.', fvarchar}],
    OrderByExpr = {order, {{t1, '.', fvarchar}, asc}},
    {ok, [["group1", "group3"], ["group2", "group3"]]} =
        db_app:select(JoinExpr, [], [{fields, Fields}, {distinct, true}, {extras, [OrderByExpr]}]),

    AndExpr = {'and', [{fint, '>', 1}, {fint, '<', 5}]},
    LikeExpr = {fvarchar, like, "%1"},
    {ok, [["group1", 3], ["group2", 1], ["group2", 3]]} =
        db_app:select(Table, {AndExpr, 'or', {'not', LikeExpr}}, [{fields, [fvarchar, fint]}]),

    {comment, "Test of expression"}.


datetime(Config) ->
    Table = list_to_atom(?config(table_name, Config)),
    Date = date(),
    {Year, Month, Day} = Date,
    DateStr = lists:flatten(io_lib:format("~p/~p/~p", [Month, Day, Year])),
    ct:log("~p-~p: DateStr = ~p~n", [?MODULE, ?LINE, DateStr]),
    {ok, 1} = db_app:insert(Table, [{fdate, {date, Date}}]),
    {ok, 1} = db_app:execute_sql("insert into " ++ ?config(table_name, Config) ++ " (fdate) values ('" ++ DateStr ++ "')"),
    DateRes = {ok, lists:duplicate(2, [{date, Date}])},
    DateRes = db_app:select(Table, {fdate, '=', {date, Date}}, [{fields, [fdate]}]),

    {Hour, Min, Sec} = time(),
    Datetime = {datetime, {Date,{Hour, Min, Sec}}},
    DatetimeStr = lists:flatten(io_lib:format("~p-~p-~p ~p:~p:~p", [Year, Month, Day, Hour, Min, Sec])),
    ct:log("~p-~p: DatetimeStr = ~p~n", [?MODULE, ?LINE, DatetimeStr]),
    {ok, 1} = db_app:insert(Table, [{fdatetime, Datetime}]),
    {ok, 1} = db_app:execute_sql("insert into " ++ ?config(table_name, Config) ++ " (fdatetime) values ('" ++ DatetimeStr ++ "')"),
    DatetimeRes = {ok, lists:duplicate(2, [{datetime, {Date,{Hour, Min, Sec}}}])},
    DatetimeRes =
        db_app:select(Table, {fdatetime, '=', Datetime}, [{fields, [fdatetime]}]),

    Interval1 = {datetime, {{Year, Month, undefined},{undefined, undefined, undefined}}},
    Interval1Str = lists:flatten(io_lib:format("~p-~p", [Year, Month])),
    ct:log("~p-~p: Interval1Str = ~p~n", [?MODULE, ?LINE, Interval1Str]),
    {ok, 1} = db_app:insert(Table, [{finterval1, Interval1}]),
    {ok, 1} = db_app:execute_sql("insert into " ++ ?config(table_name, Config) ++ " (finterval1) values ('" ++ Interval1Str ++ "')"),
    {ok, [[{datetime, {{Year, Month, _},_}}], [{datetime, {{Year, Month, _},_}}]]} =
        db_app:select(Table, {finterval1, '=',Interval1}, [{fields, [finterval1]}]),

    SecStr = integer_to_list(Sec) ++ ".123",
    Interval2 = {datetime, {{undefined, undefined, Day},{Hour, Min, {"number", SecStr}}}},
    Interval2Str = lists:flatten(io_lib:format("~p ~p:~p:~p.~p", [Day, Hour, Min, Sec, 123])),
    ct:log("~p-~p: Interval2Str = ~p~n", [?MODULE, ?LINE, Interval2Str]),
    {ok, 1} = db_app:insert(Table, [{finterval2, Interval2}]),
    {ok, 1} = db_app:execute_sql("insert into " ++ ?config(table_name, Config) ++ " (finterval2) values ('" ++ Interval2Str ++ "')"),
    {ok, [[{datetime, {{_, _, Day}, {Hour, Min, {"number", SecStr}}}}], [{datetime, {{_, _, Day}, {Hour, Min, {"number", SecStr}}}}]]} =
        db_app:select(Table, {finterval2, '=',Interval2}, [{fields, [finterval2]}]),

    {comment, "Test of datetime data"}.

lob(Config) ->
    Table = list_to_atom(?config(table_name, Config)),
    Binary = <<1,2,0,3,7,0,23>>,
    String = "大对象类型数据",
    {ok, 1} = db_app:insert(Table, [{fbyte, Binary}, {fblob, {"blob", Binary}}, {ftext, {"text", String}}, {fclob, {"clob", String}}]),
    {ok, [[Binary, Binary, String, String]]} =
        db_app:select(Table, [], [{fields, [fbyte, fblob, ftext, fclob]}]),
    {comment, "Test of text, byte, clob and blob data"}.

long_sql(Config) ->
    try
        Table = ?config(table_name, Config),
        N = 1000,
        Sql = " INSERT INTO " ++ Table ++ " (fint, fdatetime, fvarchar, ffloat)"
            " VALUES (123,'2010-10-25 10:58:20','test long sql',123.123);",
        ct:log("~p-~p: Sql = ~p~n", [?MODULE, ?LINE, Sql]),
        LongData =
            lists:foldl(
            fun(_Id, Acc) ->
                    Acc ++ Sql
            end
                        , "", lists:seq(1,N)),

        {ok, 1} = db_app:execute_sql(LongData, [{timeout, 15*1000}]),
        Num = integer_to_list(N) ++ ".0",
        {ok, [[{"number", Num}]]} =
            db_app:select(list_to_atom(Table), [], [{fields, [{count, ['*']}]}]),
        {comment, "Test long SQL statements."}
    catch
        _Type:Err ->
            ct:log("~p-~p: Err = ~p~n", [?MODULE, ?LINE, Err]),
            {skip, Err}
    end.

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
                {ok, [[{"number", "2.0"}, 44.25]]} = db_app:select(Table, [],
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
                {ok, 1} = db_app:insert(Table, [{fvarchar, "trans test1"}]),
                db_app:insert(Table, [{fdate, "2013/5/5"}, {fvarchar, "trans test2"}]),
                db_app:insert(Table, [{fvarchar, "trans test3"}])
        end),
    {ok, []} = db_app:select(TableName, []),
    {comment, "Test transactions."}.

change_pool_size(Config) ->
    PoolId = list_to_atom(?config(table_name, Config)),
    ConnArg = lists:keydelete(default_pool, 1, ct:get_config(informix)),
    ok = db_app:add_pool(PoolId, lists:keyreplace(poolsize, 1, ConnArg, {poolsize, 2})),
    2 = db_app:get_pool_info(PoolId, size),
    ok = db_app:increment_pool_size(PoolId, 2),
    4 = db_app:get_pool_info(PoolId, size),
    ok = db_app:decrement_pool_size(PoolId, 3),
    1 = db_app:get_pool_info(PoolId, size),
    ok = db_app:decrement_pool_size(PoolId, 3),
    0 = db_app:get_pool_info(PoolId, size),
    ok = db_app:remove_pool(PoolId),
    {error, pool_not_found} = (catch(db_app:get_pool_info(PoolId, size))),
    {comment, "Test change pool size."}.

stored_procedure(Config) ->
    Table = ?config(table_name, Config),
    Sql = "insert into " ++ Table ++ " (fvarchar, fint) values('procedure', 123)",
    Procedure = "procedure" ++ Table,
    {_, _} = db_app:execute_sql("drop procedure " ++ Procedure),
    {ok, _} = db_app:execute_sql("create procedure " ++ Procedure ++ "() begin " ++ Sql ++ " ; end end procedure"),
    {ok, _} = db_app:execute_sql("call " ++ Procedure ++ "();"),
    {ok, [["procedure", 123]]} = db_app:select(list_to_atom(Table), [], [{fields, [fvarchar, fint]}]),
    {ok, _} = db_app:execute_sql("drop procedure " ++ Procedure),
    {comment, "Test procedure."}.

stmt(Config) ->
    Table = ?config(table_name, Config),
    PInsert = p_insert,
    PUpdate = p_update,
    PSelect = p_select,
    ok = db_app:prepare(PInsert, "insert into " ++ Table ++ " (fvarchar, fint) values(?, ?)"),
    ok = db_app:prepare(PUpdate, "update " ++ Table ++ " set fvarchar=? where fint = ?"),
    ok = db_app:prepare(PSelect, "select fvarchar from " ++ Table ++ " where fint=?"),
    {error, _} = db_app:prepare(PUpdate, "update " ++ Table ++ " set fvarchar=?"),
    {ok, 1} = db_app:prepare_execute(PInsert, ["stmt test", 1]),
    {ok, [["stmt test"]]} = db_app:prepare_execute(PSelect, [1]),
%        db_app:select(list_to_atom(Table), {fint, '=', 1}, [{fields, [fvarchar]}]),
    {ok, 1} = db_app:prepare_execute(PUpdate, ["test stmt", 1]),
    {ok, [["test stmt"]]} = db_app:prepare_execute(PSelect, [1]),
%        db_app:select(list_to_atom(Table), {fint, '=', 1}, [{fields, [fvarchar]}]),
    {comment, "Test prepare statements."}.
