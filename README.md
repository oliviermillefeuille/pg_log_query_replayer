# PG Log Query Replayer

This tool replays queries that have been logged by a PostgreSQL logging system instance on a specific PG instance.

## Run the tool

```bash
ruby log_query_replayer.rb [mandatory parameters] [options]
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
