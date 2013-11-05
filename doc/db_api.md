

# Module db_api #
* [Description](#description)
* [Function Index](#index)
* [Function Details](#functions)


The driver's APIs.
__Authors:__ deng.lifen ([`deng.lifen@rytong.com`](mailto:deng.lifen@rytong.com)).
<a name="description"></a>

## Description ##
<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#add_pool-2">add_pool/2</a></td><td>Call db_app:add_pool/2.</td></tr><tr><td valign="top"><a href="#decrement_pool_size-2">decrement_pool_size/2</a></td><td>Call db_app:decrement_pool_size/2.</td></tr><tr><td valign="top"><a href="#delete-2">delete/2</a></td><td>Call db_app:delete/2.</td></tr><tr><td valign="top"><a href="#delete-3">delete/3</a></td><td>Call db_app:delete/3.</td></tr><tr><td valign="top"><a href="#execute_param-2">execute_param/2</a></td><td>Call db_app:execute_param/2.</td></tr><tr><td valign="top"><a href="#execute_param-3">execute_param/3</a></td><td>Call db_app:execute_param/3.</td></tr><tr><td valign="top"><a href="#execute_sql-1">execute_sql/1</a></td><td>Call db_app:execute_sql/1.</td></tr><tr><td valign="top"><a href="#execute_sql-2">execute_sql/2</a></td><td>Call db_app:execute_sql/2.</td></tr><tr><td valign="top"><a href="#get_driver-1">get_driver/1</a></td><td>Call db_app:get_driver/1.</td></tr><tr><td valign="top"><a href="#get_pool_info-2">get_pool_info/2</a></td><td>Call db_app:get_pool_info/2.</td></tr><tr><td valign="top"><a href="#get_table_schema-1">get_table_schema/1</a></td><td>Call db_app:get_table_schema/1.</td></tr><tr><td valign="top"><a href="#get_table_schema-2">get_table_schema/2</a></td><td>Call db_app:get_table_schema/2.</td></tr><tr><td valign="top"><a href="#get_table_schemas-1">get_table_schemas/1</a></td><td>Call db_app:get_table_schemas/1.</td></tr><tr><td valign="top"><a href="#increment_pool_size-2">increment_pool_size/2</a></td><td>Call db_app:increment_pool_size/2.</td></tr><tr><td valign="top"><a href="#init_conn_arg_record-1">init_conn_arg_record/1</a></td><td>Call db_app:init_conn_arg_record/1.</td></tr><tr><td valign="top"><a href="#insert-2">insert/2</a></td><td>Call db_app:insert/2.</td></tr><tr><td valign="top"><a href="#insert-3">insert/3</a></td><td>Call db_app:insert/3.</td></tr><tr><td valign="top"><a href="#prepare-2">prepare/2</a></td><td>Call db_app:prepare/2.</td></tr><tr><td valign="top"><a href="#prepare_execute-2">prepare_execute/2</a></td><td>Call db_app:prepare_execute/2.</td></tr><tr><td valign="top"><a href="#prepare_execute-3">prepare_execute/3</a></td><td>Call db_app:prepare_execute/3.</td></tr><tr><td valign="top"><a href="#refresh_table_schemas-0">refresh_table_schemas/0</a></td><td>Call db_app:refresh_table_schemas/0.</td></tr><tr><td valign="top"><a href="#refresh_table_schemas-1">refresh_table_schemas/1</a></td><td>Call db_app:refresh_table_schemas/1.</td></tr><tr><td valign="top"><a href="#remove_pool-1">remove_pool/1</a></td><td>Call db_app:remove_pool/1.</td></tr><tr><td valign="top"><a href="#select-2">select/2</a></td><td>Call db_app:select/2.</td></tr><tr><td valign="top"><a href="#select-3">select/3</a></td><td>Call db_app:select/3.</td></tr><tr><td valign="top"><a href="#start-0">start/0</a></td><td>Call db_app:start/0.</td></tr><tr><td valign="top"><a href="#stop-0">stop/0</a></td><td>Call db_app:stop/0.</td></tr><tr><td valign="top"><a href="#transaction-1">transaction/1</a></td><td>Call db_app:transaction/1.</td></tr><tr><td valign="top"><a href="#transaction-2">transaction/2</a></td><td>Call db_app:transaction/2.</td></tr><tr><td valign="top"><a href="#update-3">update/3</a></td><td>Call db_app:update/3.</td></tr><tr><td valign="top"><a href="#update-4">update/4</a></td><td>Call db_app:update/4.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="add_pool-2"></a>

### add_pool/2 ###

`add_pool(PoolId, ConnArg) -> any()`

Call db_app:add_pool/2.

__See also:__ [db_app:add_pool/2](db_app.md#add_pool-2).
<a name="decrement_pool_size-2"></a>

### decrement_pool_size/2 ###

`decrement_pool_size(PoolId, Num) -> any()`

Call db_app:decrement_pool_size/2.

__See also:__ [db_app:decrement_pool_size/2](db_app.md#decrement_pool_size-2).
<a name="delete-2"></a>

### delete/2 ###

`delete(TableName, Where) -> any()`

Call db_app:delete/2.

__See also:__ [db_app:delete/2](db_app.md#delete-2).
<a name="delete-3"></a>

### delete/3 ###

`delete(TableName, Where, Opts) -> any()`

Call db_app:delete/3.

__See also:__ [db_app:delete/3](db_app.md#delete-3).
<a name="execute_param-2"></a>

### execute_param/2 ###

`execute_param(Sql, ParamList) -> any()`

Call db_app:execute_param/2.

__See also:__ [db_app:execute_param/2](db_app.md#execute_param-2).
<a name="execute_param-3"></a>

### execute_param/3 ###

`execute_param(Sql, ParamList, Opts) -> any()`

Call db_app:execute_param/3.

__See also:__ [db_app:execute_param/3](db_app.md#execute_param-3).
<a name="execute_sql-1"></a>

### execute_sql/1 ###

`execute_sql(String) -> any()`

Call db_app:execute_sql/1.

__See also:__ [db_app:execute_sql/1](db_app.md#execute_sql-1).
<a name="execute_sql-2"></a>

### execute_sql/2 ###

`execute_sql(String, Opts) -> any()`

Call db_app:execute_sql/2.

__See also:__ [db_app:execute_sql/2](db_app.md#execute_sql-2).
<a name="get_driver-1"></a>

### get_driver/1 ###

`get_driver(PoolId) -> any()`

Call db_app:get_driver/1.

__See also:__ [db_app:get_driver/1](db_app.md#get_driver-1).
<a name="get_pool_info-2"></a>

### get_pool_info/2 ###

`get_pool_info(PoolId, Info) -> any()`

Call db_app:get_pool_info/2.

__See also:__ [db_app:get_pool_info/2](db_app.md#get_pool_info-2).
<a name="get_table_schema-1"></a>

### get_table_schema/1 ###

`get_table_schema(Table) -> any()`

Call db_app:get_table_schema/1.

__See also:__ [db_app:get_table_schema/1](db_app.md#get_table_schema-1).
<a name="get_table_schema-2"></a>

### get_table_schema/2 ###

`get_table_schema(PoolId, Table) -> any()`

Call db_app:get_table_schema/2.

__See also:__ [db_app:get_table_schema/2](db_app.md#get_table_schema-2).
<a name="get_table_schemas-1"></a>

### get_table_schemas/1 ###

`get_table_schemas(PoolId) -> any()`

Call db_app:get_table_schemas/1.

__See also:__ [db_app:get_table_schemas/1](db_app.md#get_table_schemas-1).
<a name="increment_pool_size-2"></a>

### increment_pool_size/2 ###

`increment_pool_size(PoolId, Num) -> any()`

Call db_app:increment_pool_size/2.

__See also:__ [db_app:increment_pool_size/2](db_app.md#increment_pool_size-2).
<a name="init_conn_arg_record-1"></a>

### init_conn_arg_record/1 ###

`init_conn_arg_record(ConnArgList) -> any()`

Call db_app:init_conn_arg_record/1.

__See also:__ [db_app:init_conn_arg_record/1](db_app.md#init_conn_arg_record-1).
<a name="insert-2"></a>

### insert/2 ###

`insert(TableName, ParamList) -> any()`

Call db_app:insert/2.

__See also:__ [db_app:insert/2](db_app.md#insert-2).
<a name="insert-3"></a>

### insert/3 ###

`insert(TableName, ParamList, Opts) -> any()`

Call db_app:insert/3.

__See also:__ [db_app:insert/3](db_app.md#insert-3).
<a name="prepare-2"></a>

### prepare/2 ###

`prepare(PrepareName, Sql) -> any()`

Call db_app:prepare/2.

__See also:__ [db_app:prepare/2](db_app.md#prepare-2).
<a name="prepare_execute-2"></a>

### prepare_execute/2 ###

`prepare_execute(PrepareName, Value) -> any()`

Call db_app:prepare_execute/2.

__See also:__ [db_app:prepare_execute/2](db_app.md#prepare_execute-2).
<a name="prepare_execute-3"></a>

### prepare_execute/3 ###

`prepare_execute(PrepareName, Value, Opts) -> any()`

Call db_app:prepare_execute/3.

__See also:__ [db_app:prepare_execute/3](db_app.md#prepare_execute-3).
<a name="refresh_table_schemas-0"></a>

### refresh_table_schemas/0 ###

`refresh_table_schemas() -> any()`

Call db_app:refresh_table_schemas/0.

__See also:__ [db_app:refresh_table_schemas/0](db_app.md#refresh_table_schemas-0).
<a name="refresh_table_schemas-1"></a>

### refresh_table_schemas/1 ###

`refresh_table_schemas(PoolId) -> any()`

Call db_app:refresh_table_schemas/1.

__See also:__ [db_app:refresh_table_schemas/1](db_app.md#refresh_table_schemas-1).
<a name="remove_pool-1"></a>

### remove_pool/1 ###

`remove_pool(PoolId) -> any()`

Call db_app:remove_pool/1.

__See also:__ [db_app:remove_pool/1](db_app.md#remove_pool-1).
<a name="select-2"></a>

### select/2 ###

`select(TableList, Where) -> any()`

Call db_app:select/2.

__See also:__ [db_app:select/2](db_app.md#select-2).
<a name="select-3"></a>

### select/3 ###

`select(TableList, Where, Opts) -> any()`

Call db_app:select/3.

__See also:__ [db_app:select/3](db_app.md#select-3).
<a name="start-0"></a>

### start/0 ###

`start() -> any()`

Call db_app:start/0.

__See also:__ [db_app:start/0](db_app.md#start-0).
<a name="stop-0"></a>

### stop/0 ###

`stop() -> any()`

Call db_app:stop/0.

__See also:__ [db_app:stop/0](db_app.md#stop-0).
<a name="transaction-1"></a>

### transaction/1 ###

`transaction(Fun) -> any()`

Call db_app:transaction/1.

__See also:__ [db_app:transaction/1](db_app.md#transaction-1).
<a name="transaction-2"></a>

### transaction/2 ###

`transaction(Fun, Opts) -> any()`

Call db_app:transaction/2.

__See also:__ [db_app:transaction/2](db_app.md#transaction-2).
<a name="update-3"></a>

### update/3 ###

`update(TableName, ParamList, Where) -> any()`

Call db_app:update/3.

__See also:__ [db_app:update/3](db_app.md#update-3).
<a name="update-4"></a>

### update/4 ###

`update(TableName, ParamList, Where, Opts) -> any()`

Call db_app:update/4.

__See also:__ [db_app:update/4](db_app.md#update-4).
