#Slony-I Failover / Switchover 

Perl script to assist with performing switchover and failover of replication sets 
in PostgreSQL databases replicated using Slony-I.

The script can be run in interactive mode to suggest switchover or failover and
will create a slonik script to perform the suggested action.

It's hard to put together a script for all situations as different Slony 
configurations can have different complexities (hence the existence of slonik), 
but this script is intended to be used for building and running slonik scripts 
to move all sets from one node to another.

There is also an autofailover mode which will sit and poll each node and perform
a failover of failed nodes.  This mode should be assumed as experimental, as 
there can be quite a few decisions to be made when failing over different setups.

##Example usage

Launch interactive mode with command line parameters:

```bash
$ ./slony_failover.pl -h localhost -db TEST -cl test_replication
```

Launch with configuration file, a minimal configuration file would be:

    slony_database_host = localhost
    slony_database_name = TEST
    slony_cluster_name = test_replication

```bash
$ ./slony_failover.pl -f slony_failover.conf
```
Run as a daemon in debian:

```bash
$ sudo cp init.debian /etc/init.d/slony_failover
$ cp slony_failover.conf /var/slony/failover/slony_failover.conf
$ sudo chmod +x /etc/init.d/slony_failover
$ sudo update-rc.d slony_failover start 99 2 3 4 5 . stop 24 0 1 6 
$ sudo invoke-rc.d slony_failover start
```

##Command line parameters

```bash
$ ./failover.pl [options]
```

|Switch    | Description
|----------|------------------------------------------
|-f        |Read all configuration from config file
|-h        |Host running PostgreSQL instance to read state of Slony-I cluster from
|-p        |Port of above PostgreSQL database instance
|-db       |Name of above PostgreSQL database instance 
|-cl       |Name of Slony-I cluster
|-u        |User to connect to above PostgreSQL database instance
|-P        |Password for above user (Use .pgpass instead where possible)
|-i        |Print information about slony cluster and exit

##Configuration file parameters

| Section     | Parameter                                    | Type                          | Default                         | Comment
|:------------|:-------------------------------------------- |:------------------------------|:--------------------------------|:-----------------------------------
| General     |**lang**                                      | en/fr                         | *'en'*                          | The language to print messages in, currently only english and french
| General     |**prefix_directory**                          | /full/path/to/directory       | *'/tmp/slony_failovers'*        | Working directory for script to generate slonik scripts and log files
| General     |**separate_working_directory**                | boolean                       | *'true'*                        | Append a separate working directory to the prefix_directory for each run
| General     |**slonik_path**                               | /full/path/to/bin/directory   | *null*                          | Slonik binary if not in current path
| General     |**pid_filename**                              | /path/to/pidfile              | *'/var/run/slony_failover.pid'* | Pid file to use when running in autofailover mode
| General     |**enable_try_blocks**                         | boolean                       | *false*                         |    Write slonik script with try blocks where possible to aid error handling
| General     |**lockset_method**                            | single/multiple               | *'multiple'*                    | Write slonik script that locks all sets
| General     |**pull_aliases_from_comments**                | boolean                       | *false*                         | If true, script will pull text from comment fields and use to generate
|             |                                              |                               |                                 | possibly meaningful aliases for nodes and sets.
|             |                                              |                               |                                 | For sl_set this uses the entire comment, and sl_node text in parentheses.
| General     |**log_line_prefix**                           | text                          | *null*                          | Prefix to add to log lines, special values:
|             |                                              |                               |                                 |     %p = process ID
|             |                                              |                               |                                 |     %t = timestamp without milliseconds
|             |                                              |                               |                                 |     %m = timestamp with milliseconds
| General     |**failover_offline_subscriber_only**          | boolean                       | *false*                         | If set to true any subscriber only nodes that are unavailable at the time 
|             |                                              |                               |                                 | of failover will also be failed over.  If false any such nodes will be 
|             |                                              |                               |                                 | excluded from the preamble and not failed over, however this may be problematic
|             |                                              |                               |                                 | especially in the case where the most up to date node is the unavailable one.
| General     |**drop_failed_nodes**                         | boolean                       | *false*                         | After failover automatically drop the failed nodes.
| Slon Config |**slony_database_host**                       | IP Address/Hostname           | *null*                          | PostgreSQL Hostname of database to read Slony configuration from
| Slon Config |**slony_database_port**                       | integer                       | *5432*                          | PostgreSQL Port of database to read Slony configuration from
| Slon Config |**slony_database_name**                       | name                          | *null*                          | PostgreSQL database name to read Slony configuration from 
| Slon Config |**slony_database_user**                       | username                      | *'slony'*                       | Username to use to connect when reading Slony configuration
| Slon Config |**slony_database_password**                   | password                      | *''*                            | Recommended to leave blank and use .pgpass file
| Slon Config |**slony_cluster_name**                        | name                          | *null*                          | Name of Slony-I cluster to read configuration for 
| Logging     |**enable_debugging**                          | boolean                       | *false*                         | Enable printing of debug messages to stdout
| Logging     |**log_filename**                              | base file name                | *'failover.log'*                | File name to use for script process logging, special values as per strftime spec
| Logging     |**log_to_postgresql**                         | boolean                       | *false*                         | Store details of failover script runs in a postgresql database
| Logging     |**log_database_host**                         | IP Address/Hostname           | *null*                          | PostgreSQL Hostname of logging database
| Logging     |**log_database_port**                         | integer                       | *null*                          | PostgreSQL Port of logging database
| Logging     |**log_database_name**                         | name                          | *null*                          | PostgreSQL database name of logging database
| Logging     |**log_database_user**                         | username                      | *null*                          | Username to use to connect when logging to database
| Logging     |**log_database_password**                     | password                      | *''*                            | Recommended to leave blank and use .pgpass file
| Autofailover|**enable_autofailover**                       | boolean                       | *'false'*                       | Rather than interactive mode sit and watch the cluster state for failed
|             |                                              |                               |                                 | origin/forwarding nodes; upon detection trigger automated failover.
|             |                                              |                               |                                 |
| Autofailover|**autofailover_forwarding_providers**         | boolean                       | *'false'*                       | If true a failure of a pure forwarding provider will also trigger failover
| Autofailover|**autofailover_config_any_node**              | boolean                       | *'true'*                        | After reading the initial cluster configuration, subsequent reads of the configuration 
|             |                                              |                               |                                 | will use conninfo read from sl_subscribe to read from any node.
| Autofailover|**autofailover_poll_interval**                | integer                       | 500                             | How often to check for failure of nodes (milliseconds)
| Autofailover|**autofailover_node_retry**                   | integer                       | 2                               | When failure is detected, retry this many times before initiating failover
| Autofailover|**autofailover_sleep_time**                   | integer                       | 1000                            | Interval between retries (milliseconds)

Changes
-------

* 08/04/2012 - Hash together some ideas for interactive failover perl script
* 04/11/2012 - Experiment with different use of try blocks (currently can't use multiple lock sets indide try)
* 13/04/2014 - Update to work differently for Slony 2.2+
* 05/05/2014 - Experiment with autofailover ideas

Licence
-------
See the LICENCE file.
