# Simple Write Test for Spanner

Run with this command:

```shell
mvn exec:java
```

## Schema

Requires this schema (PostgreSQL):

```sql
CREATE TABLE all_types (
  col_bigint bigint NOT NULL,
  col_bool boolean,
  col_bytea bytea,
  col_float8 double precision,
  col_int bigint,
  col_numeric numeric,
  col_timestamptz timestamp with time zone,
  col_date date,
  col_varchar character varying(100),
  col_jsonb jsonb,
  PRIMARY KEY(col_bigint)
);

CREATE CHANGE STREAM all_types_change_stream
FOR all_types;
```
