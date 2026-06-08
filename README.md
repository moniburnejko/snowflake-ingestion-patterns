# snowflake-ingestion-patterns

a small reference collection of ways to land and transform data in snowflake, built while learning the trade-offs between them during my snowflake upskilling. each pattern is self-contained, so you can read one without the others.

two ingestion patterns are covered:

- **streams and tasks** (`snowflake_sql/core_stream_task`): the classic change-tracking plus scheduled-task approach
- **dynamic tables** (`snowflake_sql/dynamic_tables`): declarative pipelines, including scd2 history and deduplication

infrastructure is provisioned with terraform, and there's a small dbt sandbox for the transformation side.

## layout

| path | what's there |
|---|---|
| `snowflake_sql/core_stream_task` | streams and tasks ingestion |
| `snowflake_sql/dynamic_tables` | dynamic tables, scd2, dedup |
| `snowflake_sql/archive` | earlier versions kept for reference |
| `terraform/` | snowflake resources (databases, schemas, warehouses, roles) |
| `dbt_fun/` | dbt sandbox |
| `scripts/` | helper scripts |

## requirements

- a snowflake account with rights to create databases, warehouses, and tasks
- terraform 1.x
- dbt (optional, only for the `dbt_fun` sandbox)

## getting started

provision the snowflake objects:

```bash
cd terraform
terraform init
terraform apply
```

then run the sql for the pattern you want to try. for dynamic tables:

```sql
-- run the files in snowflake_sql/dynamic_tables in order
```

tip: start with `core_stream_task` for the most common pattern, or `dynamic_tables` if you want fewer moving parts to manage.
