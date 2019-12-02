// HBase: Google Product, a very big table, above HDFS in architecture

/* HBase component:
1. row key
2. column group <includes 1 or more column names>
3. column name <one table can have 1 or more column groups>
4. unit <row key -> column group -> column name locates a unit, a unit can contain multiple values>
5. timestamp <why need timestamp? Because HBase is based on HDFS, HDFS only allow one time write and read multiple times, if user wants to modify a value in an unit, HBase will direct to new values but also keep the old value>
*/

// start HBase

./bin/start-hbase.sh

// start HBase shell

./bin/hbase shell

// if any tables existed in HBase, do below command to remove it

hbase > disable 'student'
hbase > drop 'student'

// Create a table in HBase
/* 
1. create '<table name>', '<column group>'
2. put '<table name>', '<row key>', '<column group>:<column name>', '<column value>'
*/

hbase > create 'student', 'info'
hbase > put 'student', '1', 'info:name','Simon'
hbase > put 'student', '1', 'info:gender','M'
hbase > put 'student', '1', 'info:age','27'

hbase > put 'student', '2', 'info:name','Jane'
hbase > put 'student', '2', 'info:gender','F'
hbase > put 'student', '2', 'info:age','20'

