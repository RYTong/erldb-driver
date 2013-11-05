

# Module db_app #
* [Description](#description)
* [Function Index](#index)
* [Function Details](#functions)


The driver's Application.
__Behaviours:__ [`application`](application.md).

__Authors:__ deng.lifen ([`deng.lifen@rytong.com`](mailto:deng.lifen@rytong.com)).
<a name="description"></a>

## Description ##
<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#add_pool-2">add_pool/2</a></td><td>Synchronous call to the connection manager to add a pool.</td></tr><tr><td valign="top"><a href="#decrement_pool_size-2">decrement_pool_size/2</a></td><td>Synchronous call to the connection manager to shrink a pool.</td></tr><tr><td valign="top"><a href="#delete-2">delete/2</a></td><td>Call delete/3.</td></tr><tr><td valign="top"><a href="#delete-3">delete/3</a></td><td>Delete one or more records from the database,
and return the number of rows deleted.</td></tr><tr><td valign="top"><a href="#execute_param-2">execute_param/2</a></td><td>Call execute_param/3.</td></tr><tr><td valign="top"><a href="#execute_param-3">execute_param/3</a></td><td>Send a query with parameters to the driver and wait for the result.</td></tr><tr><td valign="top"><a href="#execute_sql-1">execute_sql/1</a></td><td>Call execute_sql/2.</td></tr><tr><td valign="top"><a href="#execute_sql-2">execute_sql/2</a></td><td>Send a query to the driver and wait for the result.</td></tr><tr><td valign="top"><a href="#get_driver-1">get_driver/1</a></td><td>Get driver name from connection arguments.</td></tr><tr><td valign="top"><a href="#get_pool_info-2">get_pool_info/2</a></td><td>Get pool information.</td></tr><tr><td valign="top"><a href="#get_table_schema-1">get_table_schema/1</a></td><td>Call get_table_schema/2.</td></tr><tr><td valign="top"><a href="#get_table_schema-2">get_table_schema/2</a></td><td>Get table schemas from databases.</td></tr><tr><td valign="top"><a href="#get_table_schemas-1">get_table_schemas/1</a></td><td>Get column_name of all tables.</td></tr><tr><td valign="top"><a href="#increment_pool_size-2">increment_pool_size/2</a></td><td>Synchronous call to the connection manager to enlarge a pool.</td></tr><tr><td valign="top"><a href="#init_conn_arg_record-1">init_conn_arg_record/1</a></td><td>The connection parameters conversion, converted into a record db_conn:conn_args().</td></tr><tr><td valign="top"><a href="#insert-2">insert/2</a></td><td>Call insert/3.</td></tr><tr><td valign="top"><a href="#insert-3">insert/3</a></td><td>Insert a record into database.</td></tr><tr><td valign="top"><a href="#prepare-2">prepare/2</a></td><td>Register a prepared statement with the server db_stmt.</td></tr><tr><td valign="top"><a href="#prepare_execute-2">prepare_execute/2</a></td><td>Call prepare_execute/3.</td></tr><tr><td valign="top"><a href="#prepare_execute-3">prepare_execute/3</a></td><td>Execute a prepared statement it has prepared.</td></tr><tr><td valign="top"><a href="#refresh_table_schemas-0">refresh_table_schemas/0</a></td><td>Call refresh_table_schemas/1.</td></tr><tr><td valign="top"><a href="#refresh_table_schemas-1">refresh_table_schemas/1</a></td><td>Get all table schemas from database, and stored in db_conn_server.</td></tr><tr><td valign="top"><a href="#remove_pool-1">remove_pool/1</a></td><td>Synchronous call to the connection manager to remove a pool.</td></tr><tr><td valign="top"><a href="#select-2">select/2</a></td><td>Call select/3.</td></tr><tr><td valign="top"><a href="#select-3">select/3</a></td><td>Find the records for the Where and Extras expressions.</td></tr><tr><td valign="top"><a href="#start-0">start/0</a></td><td>Start the database application.</td></tr><tr><td valign="top"><a href="#start-2">start/2</a></td><td>Application initialization callback function.</td></tr><tr><td valign="top"><a href="#stop-0">stop/0</a></td><td>Stop the database application.</td></tr><tr><td valign="top"><a href="#stop-1">stop/1</a></td><td>The callback function when the application stops.</td></tr><tr><td valign="top"><a href="#transaction-1">transaction/1</a></td><td>Call transaction/2.</td></tr><tr><td valign="top"><a href="#transaction-2">transaction/2</a></td><td>Execute a transaction.</td></tr><tr><td valign="top"><a href="#update-3">update/3</a></td><td>Call update/4.</td></tr><tr><td valign="top"><a href="#update-4">update/4</a></td><td>Update one or more records from the database,
and return the number of rows updated.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="add_pool-2"></a>

### add_pool/2 ###


<pre><code>
add_pool(PoolId::atom(), ConnArg::<a href="#type-record">record()</a> | <a href="db_conn.md#type-conn_args">db_conn:conn_args()</a>) -&gt; ok | no_return()
</code></pre>

<br></br>



Synchronous call to the connection manager to add a pool.


Creates a pool record, opens n=Size connections and calls
db_conn_server:add_pool/1 to make the pool known to the pool management,
it is translated into a blocking gen-server call.
<a name="decrement_pool_size-2"></a>

### decrement_pool_size/2 ###


<pre><code>
decrement_pool_size(PoolId::atom(), Num::integer()) -&gt; ok | no_return()
</code></pre>

<br></br>



Synchronous call to the connection manager to shrink a pool.



This reduces the connections by up to n=Num, but it only drops and closes available
connections that are not in use at the moment that this function is called. Connections
that are waiting for a server response are never dropped. In heavy duty, this function
may thus do nothing.


If Num is higher than the amount of connections or the amount of available connections,
exactly all available connections are dropped and closed.
<a name="delete-2"></a>

### delete/2 ###


<pre><code>
delete(Table::atom(), Where::<a href="db_conn.md#type-where_expr">db_conn:where_expr()</a>) -&gt; <a href="db_conn.md#type-result">db_conn:result()</a>
</code></pre>

<br></br>


Call delete/3.

__See also:__ [delete/3](#delete-3).
<a name="delete-3"></a>

### delete/3 ###


<pre><code>
delete(Table::atom(), Where::<a href="db_conn.md#type-where_expr">db_conn:where_expr()</a>, Opts::<a href="db_conn.md#type-options">db_conn:options()</a>) -&gt; <a href="db_conn.md#type-result">db_conn:result()</a>
</code></pre>

<br></br>


Delete one or more records from the database,
and return the number of rows deleted.
<a name="execute_param-2"></a>

### execute_param/2 ###


<pre><code>
execute_param(Sql::string(), ParamList::<a href="db_conn.md#type-param_list">db_conn:param_list()</a>) -&gt; <a href="db_conn.md#type-result">db_conn:result()</a>
</code></pre>

<br></br>


Call execute_param/3.

__See also:__ [execute_param/3](#execute_param-3).
<a name="execute_param-3"></a>

### execute_param/3 ###


<pre><code>
execute_param(Sql::string(), ParamList::<a href="db_conn.md#type-param_list">db_conn:param_list()</a>, Opts::<a href="db_conn.md#type-options">db_conn:options()</a>) -&gt; <a href="db_conn.md#type-result">db_conn:result()</a>
</code></pre>

<br></br>


Send a query with parameters to the driver and wait for the result.
<a name="execute_sql-1"></a>

### execute_sql/1 ###


<pre><code>
execute_sql(Sql::string()) -&gt; <a href="db_conn.md#type-result">db_conn:result()</a>
</code></pre>

<br></br>


Call execute_sql/2.

__See also:__ [execute_sql/2](#execute_sql-2).
<a name="execute_sql-2"></a>

### execute_sql/2 ###


<pre><code>
execute_sql(Sql::string(), Opts::<a href="db_conn.md#type-options">db_conn:options()</a>) -&gt; <a href="db_conn.md#type-result">db_conn:result()</a>
</code></pre>

<br></br>


Send a query to the driver and wait for the result.
<a name="get_driver-1"></a>

### get_driver/1 ###


<pre><code>
get_driver(PoolId::atom()) -&gt; atom()
</code></pre>

<br></br>


Get driver name from connection arguments.
<a name="get_pool_info-2"></a>

### get_pool_info/2 ###


<pre><code>
get_pool_info(PoolId::atom(), Info::atom()) -&gt; term()
</code></pre>

<br></br>



Get pool information.


Info: conn_args | table_schemas | size
<a name="get_table_schema-1"></a>

### get_table_schema/1 ###


<pre><code>
get_table_schema(Table::list() | atom()) -&gt; list() | no_return()
</code></pre>

<br></br>


Call get_table_schema/2.

__See also:__ [get_table_schema/2](#get_table_schema-2).
<a name="get_table_schema-2"></a>

### get_table_schema/2 ###


<pre><code>
get_table_schema(PoolId::atom(), Table::list() | atom()) -&gt; list() | no_return()
</code></pre>

<br></br>



Get table schemas from databases.


Stored the schemas in db_conn_server when the server without them.
<a name="get_table_schemas-1"></a>

### get_table_schemas/1 ###


<pre><code>
get_table_schemas(PoolId::atom()) -&gt; list() | no_return()
</code></pre>

<br></br>


Get column_name of all tables.
<a name="increment_pool_size-2"></a>

### increment_pool_size/2 ###


<pre><code>
increment_pool_size(PoolId::atom(), Num::integer()) -&gt; ok | no_return()
</code></pre>

<br></br>



Synchronous call to the connection manager to enlarge a pool.


This opens n=Num new connections and adds them to the pool of id PoolId.
<a name="init_conn_arg_record-1"></a>

### init_conn_arg_record/1 ###


<pre><code>
init_conn_arg_record(ConnArgList::<a href="db_conn.md#type-conn_args">db_conn:conn_args()</a>) -&gt; <a href="#type-record">record()</a>
</code></pre>

<br></br>


The connection parameters conversion, converted into a record db_conn:conn_args().
<a name="insert-2"></a>

### insert/2 ###


<pre><code>
insert(Table::atom(), ParamList::<a href="db_conn.md#type-param_list">db_conn:param_list()</a>) -&gt; <a href="db_conn.md#type-result">db_conn:result()</a>
</code></pre>

<br></br>


Call insert/3.

__See also:__ [insert/3](#insert-3).
<a name="insert-3"></a>

### insert/3 ###


<pre><code>
insert(Table::atom(), ParamList::<a href="db_conn.md#type-param_list">db_conn:param_list()</a>, Opts::<a href="db_conn.md#type-options">db_conn:options()</a>) -&gt; <a href="db_conn.md#type-result">db_conn:result()</a>
</code></pre>

<br></br>


Insert a record into database.
<a name="prepare-2"></a>

### prepare/2 ###


<pre><code>
prepare(PrepareName::string(), Sql::string()) -&gt; <a href="db_conn.md#type-result">db_conn:result()</a>
</code></pre>

<br></br>


Register a prepared statement with the server db_stmt.
<a name="prepare_execute-2"></a>

### prepare_execute/2 ###


<pre><code>
prepare_execute(PrepareName::string(), Value::list()) -&gt; <a href="db_conn.md#type-result">db_conn:result()</a>
</code></pre>

<br></br>


Call prepare_execute/3.

__See also:__ [prepare_execute/3](#prepare_execute-3).
<a name="prepare_execute-3"></a>

### prepare_execute/3 ###


<pre><code>
prepare_execute(PrepareName::string(), Value::list(), Opts::<a href="db_conn.md#type-options">db_conn:options()</a>) -&gt; <a href="db_conn.md#type-result">db_conn:result()</a>
</code></pre>

<br></br>


Execute a prepared statement it has prepared.
<a name="refresh_table_schemas-0"></a>

### refresh_table_schemas/0 ###


<pre><code>
refresh_table_schemas() -&gt; ok | no_return()
</code></pre>

<br></br>


Call refresh_table_schemas/1.

__See also:__ [refresh_table_schemas/1](#refresh_table_schemas-1).
<a name="refresh_table_schemas-1"></a>

### refresh_table_schemas/1 ###


<pre><code>
refresh_table_schemas(PoolId::atom()) -&gt; ok | no_return()
</code></pre>

<br></br>


Get all table schemas from database, and stored in db_conn_server.
<a name="remove_pool-1"></a>

### remove_pool/1 ###


<pre><code>
remove_pool(PoolId::atom()) -&gt; ok | no_return()
</code></pre>

<br></br>


Synchronous call to the connection manager to remove a pool.
<a name="select-2"></a>

### select/2 ###


<pre><code>
select(TableList::<a href="db_conn.md#type-tables">db_conn:tables()</a>, Where::<a href="db_conn.md#type-where_expr">db_conn:where_expr()</a>) -&gt; <a href="db_conn.md#type-result">db_conn:result()</a>
</code></pre>

<br></br>


Call select/3.

__See also:__ [select/3](#select-3).
<a name="select-3"></a>

### select/3 ###


<pre><code>
select(TableList::<a href="db_conn.md#type-tables">db_conn:tables()</a>, Where::<a href="db_conn.md#type-where_expr">db_conn:where_expr()</a>, Opts::<a href="db_conn.md#type-options">db_conn:options()</a>) -&gt; <a href="db_conn.md#type-result">db_conn:result()</a>
</code></pre>

<br></br>



Find the records for the Where and Extras expressions.


If no records match the conditions, the function returns {ok, []}.
<a name="start-0"></a>

### start/0 ###


<pre><code>
start() -&gt; ok | no_return()
</code></pre>

<br></br>


Start the database application.
<a name="start-2"></a>

### start/2 ###

`start(StartType, StartArgs) -> any()`

Application initialization callback function. Start the supervisor.
<a name="stop-0"></a>

### stop/0 ###


<pre><code>
stop() -&gt; ok | no_return()
</code></pre>

<br></br>


Stop the database application.
<a name="stop-1"></a>

### stop/1 ###

`stop(State) -> any()`

The callback function when the application stops.
<a name="transaction-1"></a>

### transaction/1 ###


<pre><code>
transaction(Fun::function()) -&gt; <a href="db_conn.md#type-result">db_conn:result()</a>
</code></pre>

<br></br>


Call transaction/2.

__See also:__ [transaction/2](#transaction-2).
<a name="transaction-2"></a>

### transaction/2 ###


<pre><code>
transaction(Fun::function(), Opts::<a href="db_conn.md#type-options">db_conn:options()</a>) -&gt; <a href="db_conn.md#type-result">db_conn:result()</a>
</code></pre>

<br></br>


Execute a transaction.
<a name="update-3"></a>

### update/3 ###


<pre><code>
update(Table::atom(), ParamList::<a href="db_conn.md#type-param_list">db_conn:param_list()</a>, Where::<a href="db_conn.md#type-where_expr">db_conn:where_expr()</a>) -&gt; <a href="db_conn.md#type-result">db_conn:result()</a>
</code></pre>

<br></br>


Call update/4.

__See also:__ [update/4](#update-4).
<a name="update-4"></a>

### update/4 ###


<pre><code>
update(Table::atom(), ParamList::<a href="db_conn.md#type-param_list">db_conn:param_list()</a>, Where::<a href="db_conn.md#type-where_expr">db_conn:where_expr()</a>, Opts::<a href="db_conn.md#type-options">db_conn:options()</a>) -&gt; <a href="db_conn.md#type-result">db_conn:result()</a>
</code></pre>

<br></br>


Update one or more records from the database,
and return the number of rows updated.
