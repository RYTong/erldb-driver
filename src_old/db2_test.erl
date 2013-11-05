-module(db2_test).

-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

start() ->
    PrivDir = ewp_file_util:get_priv_dir(),
    DBServerSpec = ewp_sup:server_tuple(db_server, [PrivDir]),
    {ok, _} = supervisor:start_child(ewp_sup, DBServerSpec),
    db_server:connect('test',
                      [{driver, db2},
                       {host, "127.0.0.1"},
                       {user, "db2inst1"},
                       {password, "rytong2010"},
                       {database, "test"},
                       {poolsize, 32}]),
    db_server:make_default('test').

create_table() ->
    db_api:execute_sql("DROP TABLE DATATYPES_TEST"),
    SQL = "CREATE TABLE DATATYPES_TEST
    (
        ID BIGINT NOT NULL PRIMARY KEY,
        BIGINT_V BIGINT,
        BLOB_V BLOB(1048576),
        CHAR_V CHAR(8),
        CHAR_FOR_BIT_DATA_V CHAR(8) FOR BIT DATA,
        CLOB_V CLOB(1048576),
        DATE_V DATE,
        DBCLOB_V DBCLOB(1048576),
        DECFLOAT_V DECFLOAT(34),
        DECIMAL_V DECIMAL(31,9),
        DOUBLE_V DOUBLE,
        GRAPHIC_V GRAPHIC(8),
        INTEGER_V INTEGER,
        LONG_VARCHAR_V LONG VARCHAR,
        LONG_VARCHAR_FOR_BIT_DATA_V LONG VARCHAR FOR BIT DATA,
        LONG_VARGRAPHIC_V LONG VARGRAPHIC,
        REAL_V REAL,
        SMALLINT_V SMALLINT,
        TIME_V TIME,
        TIMESTAMP_V TIMESTAMP,
        VARCHAR_V VARCHAR(128),
        VARCHAR_FOR_BIT_DATA_V VARCHAR(128) FOR BIT DATA,
        VARGRAPHIC_V VARGRAPHIC(128),
        XML_V XML
    )",
    db_api:execute_sql(SQL).

datatype_test() ->
    %% BIGINT
    datatype_test("BIGINT_V", -9223372036854775808),
    datatype_test("BIGINT_V", 9223372036854775807),
    datatype_test("BIGINT_V", crypto:rand_uniform(0, 9223372036854775807)),
    datatype_test("BIGINT_V", -crypto:rand_uniform(0, 9223372036854775808)),

    %% BLOB(1048576)
    datatype_test("BLOB_V", crypto:rand_bytes(1048576)),

    %% CHAR(8)
    datatype_test("CHAR_V", rand_string(8)),

    %% CHAR(8) FOR BIT DATA
    datatype_test("CHAR_FOR_BIT_DATA_V", crypto:rand_bytes(8)),

    %% CLOB_V CLOB(1048576)
    datatype_test("CLOB_V",  rand_string(1048576)),

    %% DATE
    datatype_test("DATE_V",  {date, {1, 1, 1}}),
    datatype_test("DATE_V",  {date, {9999, 12, 31}}),
    datatype_test("DATE_V",  {date, {2012, 9, 6}}),

    %% DBCLOB_V DBCLOB(1048576)
    datatype_test("DBCLOB_V",  rand_dbstring(1048576)),

    %% DECFLOAT_V DECFLOAT(34)
    datatype_test("DECFLOAT_V",  "123456789.123456789"),

    %% DECIMAL_V DECIMAL(31,9)
    datatype_test("DECIMAL_V",  "9999999999999999999999.999999999"),

    %% DOUBLE_V DOUBLE
    datatype_test("DOUBLE_V",  123456.999999),

    %% GRAPHIC_V GRAPHIC(8)
    datatype_test("GRAPHIC_V",  rand_dbstring(8)),

    %% INTEGER_V INTEGER
    datatype_test("INTEGER_V", -2147483648),
    datatype_test("INTEGER_V", 2147483647),
    datatype_test("INTEGER_V", crypto:rand_uniform(0, 2147483647)),
    datatype_test("INTEGER_V", -crypto:rand_uniform(0, 2147483648)),

    %% LONG_VARCHAR_V LONG VARCHAR
    datatype_test("LONG_VARCHAR_V",  rand_string(32700)),

    %% LONG_VARCHAR_FOR_BIT_DATA_V LONG VARCHAR FOR BIT DATA
    datatype_test("LONG_VARCHAR_FOR_BIT_DATA_V", crypto:rand_bytes(32700)),

    %% LONG_VARGRAPHIC_V LONG VARGRAPHIC
    datatype_test("LONG_VARGRAPHIC_V",  rand_dbstring(16350)),

    %% REAL_V REAL
    %datatype_test("REAL_V",  1.1),

    %% SMALLINT_V SMALLINT
    datatype_test("SMALLINT_V", -32768),
    datatype_test("SMALLINT_V", 32767),
    datatype_test("SMALLINT_V", crypto:rand_uniform(0, 32767)),
    datatype_test("SMALLINT_V", -crypto:rand_uniform(0, 32768)),

    %% TIME_V TIME
    datatype_test("TIME_V",  {time, {0, 0, 0}}),
    datatype_test("TIME_V",  {time, {23, 59, 59}}),
    datatype_test("TIME_V",  {time, {14, 8, 12}}),

    %% TIMESTAMP_V TIMESTAMP
    datatype_test("TIMESTAMP_V",  {datetime, {{1,1,1}, {0,0,0}}}),
    datatype_test("TIMESTAMP_V",  {datetime, {{9999, 12, 31}, {23, 59, 59}}}),
    datatype_test("TIMESTAMP_V",  {datetime, {{2012,9,6}, {14,12,12}}}),

    %% VARCHAR_V VARCHAR(128)
    datatype_test("VARCHAR_V",  rand_string(128)),

    %% VARCHAR_FOR_BIT_DATA_V VARCHAR(128) FOR BIT DATA
    datatype_test("VARCHAR_FOR_BIT_DATA_V",  crypto:rand_bytes(128)),

    %% VARGRAPHIC_V VARGRAPHIC(128)
    datatype_test("VARGRAPHIC_V",  rand_dbstring(128)),

    %% XML_V XML
    datatype_test("XML_V",  <<"<?xml version=\"1.0\" encoding=\"UTF-8\" ?><data>test</data>">>),
    ok.

datatype_test(ColumnName, Value) ->
    ID = list_to_integer(lists:concat(tuple_to_list(now()))),
    ?assert({ok, 1} == db_api:execute_param("INSERT INTO DATATYPES_TEST(ID, " ++
        ColumnName ++ ") VALUES(?, ?)", [ID, Value])),
    ?assert({ok, [[Value]]} == db_api:execute_param("SELECT " ++ ColumnName ++ 
        " FROM DATATYPES_TEST WHERE ID=?", [ID])).

perf_test(0, _) ->
    ok;
perf_test(N, M) ->
    spawn(?MODULE, loop, [M]),
    perf_test(N - 1, M).

loop(M) ->
    B = now(),
    perf_test(M),
    E = now(),
    io:format("time : ~10.2f ms~n", [timer:now_diff(E, B) / 1000]),
    loop(M).

perf_test(0) ->
    ok;
perf_test(M) ->
    try
        %% BIGINT
        datatype_test("BIGINT_V", -9223372036854775808),
        datatype_test("BIGINT_V", 9223372036854775807),
        datatype_test("BIGINT_V", crypto:rand_uniform(0, 9223372036854775807)),
        datatype_test("BIGINT_V", -crypto:rand_uniform(0, 9223372036854775808)),

        %% BLOB(1048576)
        datatype_test("BLOB_V", crypto:rand_bytes(1200)),

        %% CHAR(8)
        datatype_test("CHAR_V", rand_string(8)),

        %% CHAR(8) FOR BIT DATA
        datatype_test("CHAR_FOR_BIT_DATA_V", crypto:rand_bytes(8)),

        %% CLOB_V CLOB(1048576)
        datatype_test("CLOB_V",  rand_string(1200)),

        %% DATE
        datatype_test("DATE_V",  {date, {1, 1, 1}}),
        datatype_test("DATE_V",  {date, {9999, 12, 31}}),
        datatype_test("DATE_V",  {date, {2012, 9, 6}}),

        %% DBCLOB_V DBCLOB(1048576)
        datatype_test("DBCLOB_V",  rand_dbstring(1200)),

        %% DECFLOAT_V DECFLOAT(34)
        datatype_test("DECFLOAT_V",  "123456789.123456789"),

        %% DECIMAL_V DECIMAL(31,9)
        datatype_test("DECIMAL_V",  "9999999999999999999999.999999999"),

        %% DOUBLE_V DOUBLE
        datatype_test("DOUBLE_V",  123456.999999),

        %% GRAPHIC_V GRAPHIC(8)
        datatype_test("GRAPHIC_V",  rand_dbstring(8)),

        %% INTEGER_V INTEGER
        datatype_test("INTEGER_V", -2147483648),
        datatype_test("INTEGER_V", 2147483647),
        datatype_test("INTEGER_V", crypto:rand_uniform(0, 2147483647)),
        datatype_test("INTEGER_V", -crypto:rand_uniform(0, 2147483648)),

        %% LONG_VARCHAR_V LONG VARCHAR
        datatype_test("LONG_VARCHAR_V",  rand_string(12)),

        %% LONG_VARCHAR_FOR_BIT_DATA_V LONG VARCHAR FOR BIT DATA
        datatype_test("LONG_VARCHAR_FOR_BIT_DATA_V", crypto:rand_bytes(12)),

        %% LONG_VARGRAPHIC_V LONG VARGRAPHIC
        datatype_test("LONG_VARGRAPHIC_V",  rand_dbstring(12)),

        %% REAL_V REAL
        %datatype_test("REAL_V",  1.1),

        %% SMALLINT_V SMALLINT
        datatype_test("SMALLINT_V", -32768),
        datatype_test("SMALLINT_V", 32767),
        datatype_test("SMALLINT_V", crypto:rand_uniform(0, 32767)),
        datatype_test("SMALLINT_V", -crypto:rand_uniform(0, 32768)),

        %% TIME_V TIME
        datatype_test("TIME_V",  {time, {0, 0, 0}}),
        datatype_test("TIME_V",  {time, {23, 59, 59}}),
        datatype_test("TIME_V",  {time, {14, 8, 12}}),

        %% TIMESTAMP_V TIMESTAMP
        datatype_test("TIMESTAMP_V",  {datetime, {{1,1,1}, {0,0,0}}}),
        datatype_test("TIMESTAMP_V",  {datetime, {{9999, 12, 31}, {23, 59, 59}}}),
        datatype_test("TIMESTAMP_V",  {datetime, {{2012,9,6}, {14,12,12}}}),

        %% VARCHAR_V VARCHAR(128)
        datatype_test("VARCHAR_V",  rand_string(128)),

        %% VARCHAR_FOR_BIT_DATA_V VARCHAR(128) FOR BIT DATA
        datatype_test("VARCHAR_FOR_BIT_DATA_V",  crypto:rand_bytes(128)),

        %% VARGRAPHIC_V VARGRAPHIC(128)
        datatype_test("VARGRAPHIC_V",  rand_dbstring(128)),

        %% XML_V XML
        datatype_test("XML_V",  <<"<?xml version=\"1.0\" encoding=\"UTF-8\" ?><data>test</data>">>)

        %%ID = lists:concat(tuple_to_list(now())),
        %%{ok, 1} = db_api:execute_sql("INSERT INTO DATATYPES_TEST(ID, BIGINT_V) VALUES(" ++ ID ++ ", 9223372036854775807)"),
        %%{ok, [[9223372036854775807]]} = db_api:execute_sql("SELECT BIGINT_V FROM DATATYPES_TEST WHERE ID=" ++ ID)
    catch
        Type:Error ->
            error_logger:format("~p:~p ~p", [Type, Error, erlang:get_stacktrace()]),
            exit(xxxxx)
    end,
    perf_test(M - 1).
    

tran_test() ->
    Fun =
        fun (_DbName, Conn, _Arg) ->
            db_api:trans_execute_sql("insert into test_clob values('wangmeigong')", Conn),
            db_api:trans_execute_sql("update test_clob set values='20000000000'", Conn)
        end,
    db_api:transaction(Fun, []).

%%------------------------------------------------------------------------------
%% Internal Functions
%%------------------------------------------------------------------------------
rand_string(N) when N =< 0 ->
    "";
rand_string(N) ->
    lists:foldl(fun (0, Acc) -> [crypto:rand_uniform(1, 256) | Acc];
                    (X, Acc) -> [X | Acc]
                end, [], binary_to_list(crypto:rand_bytes(N))).

rand_dbstring(N) when N =< 0 ->
    "";
rand_dbstring(N) ->
    rand_dbstring(binary_to_list(crypto:rand_bytes(N * 2)), []).
    %% [crypto:rand_uniform(1, 65536 div 2) | rand_dbstring(N - 1)].

rand_dbstring([], Acc) ->
    Acc;
rand_dbstring([A, B | Rest], Acc) ->
    C = A * 128 + B div 2,
    if C == 0 ->
           rand_dbstring(Rest, [crypto:rand_uniform(1, 32768) | Acc]);
       true   -> 
           rand_dbstring(Rest, [C | Acc])
    end.

test(X) when is_list(X) ->
    ID = list_to_integer(lists:concat(tuple_to_list(now()))),
    db_api:execute_param("insert into datatypes_test(id, dbclob_v) values(?, ?)", [ID, X]),
    {ok, [[Y]]} = db_api:execute_param("select dbclob_v from datatypes_test where id=?", [ID]),
    compare(X, Y);
test(N) when is_integer(N) ->
    ID = list_to_integer(lists:concat(tuple_to_list(now()))),
    X = rand_dbstring(N),
    db_api:execute_param("insert into datatypes_test(id, dbclob_v) values(?, ?)", [ID, X]),
    {ok, [[Y]]} = db_api:execute_param("select dbclob_v from datatypes_test where id=?", [ID]),
    compare(X, Y),
    {ID, X}.

compare([], []) ->
    ok;
compare([H|T1], [H|T2]) ->
    compare(T1, T2);
compare([H1|T1], [H2|T2]) ->
    io:format("~p(~p) ~p(~p)~n", [H1, 1000000 - length(T1), H2, 1000000 - length(T2)]),
    compare(T1, T2).

get_tables() ->
    {ok, [[CurrentSchema]]} = db_api:execute_sql("VALUES CURRENT SCHEMA"),
    {ok, Rows} = db_api:execute_sql("SELECT DISTINCT TABSCHEMA, TABNAME FROM SYSCAT.TABLES"),
    lists:foldl(
        fun ([TabSchema, TabName], Acc) ->
                 case string:strip(TabSchema) of
                     CurrentSchema -> 
                         [TabName, CurrentSchema ++ "." ++ TabName | Acc];
                     _             -> 
                         [string:strip(TabSchema) ++ "." ++ TabName | Acc]
                 end
                 
        end, [], Rows).

get_columns(Name) ->
    {ok, [[CS]]} = db_api:execute_sql("VALUES CURRENT SCHEMA"),
    {TabSchema, TabName} =
        case string:tokens(Name, ".") of
            [TS, TN] -> {TS, TN};
            [TN]     -> {CS, TN}
        end,
    {ok, Rows} = db_api:execute_sql("SELECT COLNAME from SYSCAT.COLUMNS where TABSCHEMA='" 
                                   ++ TabSchema ++ "' AND TABNAME='" ++ TabName ++ "' ORDER BY COLNO"),
    lists:map(fun ([ColName]) -> ColName end, Rows).
