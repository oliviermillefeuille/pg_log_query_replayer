# PG Log Query Replayer

This tool replays SELECT queries that have been logged by a PostgreSQL logging system instance on a specific PG instance.
It supports two PG log file formats

## Run the tool

```bash
ruby json_query_replayer.rb [mandatory parameters] [options]
```

Mandatory parameters
```text
    --logfile LOGFILE
    --pghost PGHOST
    --pgdatabase PGDATABASE
    --pguser PGUSER
    --pgport PGPORT
```

Options
```text
    --pgpassword <PGPASSWORD>
    --skiplines Number of lines to skip from the beginning of the file
    --maxlines Maximum line number at which the replay stops
```

## Output

The results of will be printed on STDOUT. The output will consist of three sections, with the last two sections being displayed only if the tool is able to read the entire log file. 

All sections of the output data will be in CSV format for easy capture, and each section will begin with a header. The first section will contain information about the replayed queries and their statistics. The second section will display the top 100 queries sorted by their total cost, with the queries with the highest total cost on top. The third section will show the top 100 queries sorted by their count, with the queries that occur most frequently on top.

In this version, these 4 fields from the query plan are being captured:
```Actual Total Time, Total Cost, Shared Hit Blocks, Shared Read Blocks```

See an example below:
```
# REPLAYING LOG FILE 
elapsed_in_secs,execution_number,line_number,fingerprint,count,cost,avg_cost,time,avg_time,shared_hit_blocks,avg_shared_hit_blocks,shared_read_blocks,avg_shared_read_blocks
1.729327,1,1,5f1ab0fa5dd08863,1,0.049,0.049,8.44,8.44,4.0,4.0,0.0,0.0
2.091389,2,4,232416985003ca0b,1,0.048,0.048,8.44,8.44,7.0,7.0,0.0,0.0
2.227272,3,12,595e9ed2fb1128af,1,0.003,0.003,8.16,8.16,2.0,2.0,0.0,0.0
2.38604,4,15,595e9ed2fb1128af,2,0.006,0.0045000000000000005,8.16,8.16,2.0,2.0,0.0,0.0
2.469703,5,33,98ee9ba3a3c76056,1,0.393,0.393,70.18,70.18,16.0,16.0,0.0,0.0
2.563886,6,53,e78fe2c08de5f079,1,0.031,0.031,78.15,78.15,22.0,22.0,0.0,0.0
2.623094,7,62,595e9ed2fb1128af,3,0.008,0.005666666666666667,8.16,8.16,2.0,2.0,0.0,0.0
2.679911,8,70,595e9ed2fb1128af,4,0.007,0.006,8.16,8.16,2.0,2.0,0.0,0.0
2.730294,9,73,5f1ab0fa5dd08863,2,0.039,0.044,8.44,8.44,4.0,4.0,0.0,0.0
2.778994,10,91,232416985003ca0b,2,0.018,0.033,8.44,8.44,4.0,5.5,0.0,0.0
2.872408,11,101,e78fe2c08de5f079,2,0.028,0.0295,78.15,78.15,22.0,22.0,0.0,0.0
2.93116,12,115,595e9ed2fb1128af,5,0.006,0.006,8.16,8.16,2.0,2.0,0.0,0.0
2.980132,13,121,5f1ab0fa5dd08863,3,0.02,0.036,8.44,8.44,4.0,4.0,0.0,0.0
3.027672,14,130,232416985003ca0b,3,0.015,0.027,8.44,8.44,4.0,5.0,0.0,0.0
```

## Example with JSON LOG format (read by the json_query_replayer)

In the following PG json log file (all private information have been deleted but its structure remains the same)
The replay will execute the SELECT queries found on lines: 26 and 38

```
023-04-25 00:00:17 UTC:192.168.0.10(43274):[unknown]@[unknown]:[31209]:LOG:  connection received: ...
2023-04-25 00:00:20 UTC:192.168.0.10(51506):monitoring@db:[642]:STATEMENT:  SELECT buffers_sent_last_minute*8/60 AS warm_rate_kbps,
	100*(1.0-buffers_sent_last_scan/buffers_found_last_scan) AS warm_percent
	FROM aurora_ccm_status();

2023-04-25 00:00:21 UTC:192.168.0.10(44966):role@db:[31198]:LOG:  temporary file: path "base/pgsql_tmp/pgsql_tmp31198.0", size 63268165
2023-04-25 00:00:21 UTC:192.168.0.10(44966):role@db:[31198]:STATEMENT:  refresh materialized view concurrently instant_messaging_searchable_profiles_v7
2023-04-25 00:00:21 UTC:192.168.0.10(58088):role@db:[31195]:LOG:  duration: 5098.147 ms  plan:
	{
	  "Query Text": "refresh materialized view concurrently ...",
	  "Plan": {
	    "Node Type": "Unique",
	    "Parallel Aware": false,
	    ...
	  }
	}
2023-04-25 00:00:21 UTC:192.168.0.10(44966):role@db:[31198]:LOG:  temporary file: path "base/pgsql_tmp/pgsql_tmp31198.4", size 11943540
2023-04-25 00:00:23 UTC:192.168.0.10(51506):monitoring@db:[642]:STATEMENT:  SELECT buffers_sent_last_minute*8/60 AS warm_rate_kbps,
	100*(1.0-buffers_sent_last_scan/buffers_found_last_scan) AS warm_percent
	FROM aurora_ccm_status();

2023-04-25 00:00:24 UTC:192.168.0.10(44966):role@db:[31198]:LOG:  temporary file: path "base/pgsql_tmp/pgsql_tmp31198.5", size 11915508
2023-04-25 00:00:24 UTC:192.168.0.10(44966):role@db:[31198]:STATEMENT:  refresh materialized view concurrently instant_messaging_searchable_profiles_v7 
2023-04-25 00:00:25 UTC:192.168.0.10(58986):role@db:[31218]:LOG:  duration: 6356.041 ms  plan:
	{
	  "Query Text": "SELECT ...",
	  "Plan": {
	    "Node Type": "Aggregate",
	    "Strategy": "Hashed",
	    "Partial Mode": "Simple",
	    "Parallel Aware": false,
		...
	  }
	}
2023-04-25 00:00:29 UTC:192.168.0.10(34694):role@db:[31205]:LOG:  temporary file: path "base/pgsql_tmp/pgsql_tmp31205.0", size 29627352
2023-04-25 00:00:29 UTC:192.168.0.10(34694):role@db:[31205]:LOG:  duration: 5695.872 ms  plan:
	{
	  "Query Text": "SELECT ...",
	  "Plan": {
	    "Node Type": "Nested Loop",
	    "Parallel Aware": false,
	    "Join Type": "Left",
	    "Startup Cost": 100245.80,
		...
	  }
	}
2023-04-25 00:00:36 UTC:192.168.0.10(44966):role@db:[31198]:LOG:  temporary file: path "base/pgsql_tmp/pgsql_tmp31198.8", size 34584116
```
