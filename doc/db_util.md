

# Module db_util #
* [Description](#description)
* [Function Index](#index)
* [Function Details](#functions)


The public library.
__Authors:__ deng.lifen ([`deng.lifen@rytong.com`](mailto:deng.lifen@rytong.com)).
<a name="description"></a>

## Description ##
<a name="index"></a>

## Function Index ##


<table width="100%" border="1" cellspacing="0" cellpadding="2" summary="function index"><tr><td valign="top"><a href="#check_lib_path-0">check_lib_path/0</a></td><td>Check the library path is exist.</td></tr><tr><td valign="top"><a href="#command-2">command/2</a></td><td>Send command to database library.</td></tr><tr><td valign="top"><a href="#command_t-2">command_t/2</a></td><td>Send command to database library.</td></tr><tr><td valign="top"><a href="#drv_code-1">drv_code/1</a></td><td>Transform driver name to driver code.</td></tr><tr><td valign="top"><a href="#drv_name-1">drv_name/1</a></td><td>Transform driver code to driver name.</td></tr><tr><td valign="top"><a href="#get_table_schemas-4">get_table_schemas/4</a></td><td>Get column names of all tables.</td></tr><tr><td valign="top"><a href="#is_true-1">is_true/1</a></td><td>Determine whether is true.</td></tr><tr><td valign="top"><a href="#load-2">load/2</a></td><td>Load database driver library, and then loop waiting to the message of
the process to stop.</td></tr><tr><td valign="top"><a href="#make_expr-1">make_expr/1</a></td><td>The expression format conversion for database library.</td></tr><tr><td valign="top"><a href="#make_pararmlist-1">make_pararmlist/1</a></td><td>The where statement format conversion for database library.</td></tr></table>


<a name="functions"></a>

## Function Details ##

<a name="check_lib_path-0"></a>

### check_lib_path/0 ###


<pre><code>
check_lib_path() -&gt; string() | no_return()
</code></pre>

<br></br>


Check the library path is exist.
<a name="command-2"></a>

### command/2 ###


<pre><code>
command(Cmd::integer(), Data::term()) -&gt; ReturnType::term()
</code></pre>

<br></br>


Send command to database library.
<a name="command_t-2"></a>

### command_t/2 ###


<pre><code>
command_t(Cmd::integer(), Data::term()) -&gt; ReturnType::term() | no_return()
</code></pre>

<br></br>


Send command to database library.
If responce is {error, Error}, throw it.
<a name="drv_code-1"></a>

### drv_code/1 ###


<pre><code>
drv_code(Driver::atom()) -&gt; integer()
</code></pre>

<br></br>


Transform driver name to driver code.
<a name="drv_name-1"></a>

### drv_name/1 ###


<pre><code>
drv_name(Driver::integer()) -&gt; atom()
</code></pre>

<br></br>


Transform driver code to driver name.
<a name="get_table_schemas-4"></a>

### get_table_schemas/4 ###


<pre><code>
get_table_schemas(PoolId::atom(), DbType::atom(), DbName::atom(), TableList::list()) -&gt; list()
</code></pre>

<br></br>


Get column names of all tables.
<a name="is_true-1"></a>

### is_true/1 ###


<pre><code>
is_true(Parameter::term()) -&gt; 1 | 0
</code></pre>

<br></br>



Determine whether is true.



If Parameter is true, return 1.



If Parameter is integer and Parameter > 0, return 1.


Others return 0.
<a name="load-2"></a>

### load/2 ###


<pre><code>
load(DrvPath::string(), Parent::pid()) -&gt; none()
</code></pre>

<br></br>


Load database driver library, and then loop waiting to the message of
the process to stop.
<a name="make_expr-1"></a>

### make_expr/1 ###


<pre><code>
make_expr(Expr::term()) -&gt; term()
</code></pre>

<br></br>


The expression format conversion for database library.
<a name="make_pararmlist-1"></a>

### make_pararmlist/1 ###


<pre><code>
make_pararmlist(ParamList::term()) -&gt; term()
</code></pre>

<br></br>


The where statement format conversion for database library.
