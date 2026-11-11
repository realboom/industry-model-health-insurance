# Databricks notebook source
# MAGIC %md
# MAGIC # Set Delta table properties on silver source tables
# MAGIC
# MAGIC Idempotently sets `properties` on every table in `tables`. Used by both jobs:
# MAGIC
# MAGIC | Job | Properties | Why |
# MAGIC |---|---|---|
# MAGIC | `hi-silver-history` | `delta.enableChangeDataFeed=true` | The SCD2 pipeline streams each table's Change Data Feed |
# MAGIC | `hi-gold-refresh` | `delta.enableRowTracking=true`, `delta.enableDeletionVectors=true` | Lets the gold materialized views refresh **incrementally** instead of recomputing |
# MAGIC
# MAGIC **Reinstall warning:** the model installer uses `CREATE OR REPLACE TABLE`, which resets these
# MAGIC properties. Both jobs re-apply them on every run; see the README reinstall warning for the
# MAGIC history streams.

# COMMAND ----------

dbutils.widgets.text("silver_catalog", "health_insurance", "Silver catalog")
dbutils.widgets.text("tables", "member.identity", "Tables (<domain>.<table>, comma-separated)")
dbutils.widgets.text("properties", "delta.enableChangeDataFeed=true", "Properties (key=value, comma-separated)")

# COMMAND ----------

catalog = dbutils.widgets.get("silver_catalog").strip()
tables = [t.strip() for t in dbutils.widgets.get("tables").split(",") if t.strip()]
wanted = dict(kv.strip().split("=", 1) for kv in dbutils.widgets.get("properties").split(",") if kv.strip())

failures = []
for t in tables:
    schema, table = t.split(".", 1)
    fq = f"`{catalog}`.`{schema}`.`{table}`"
    try:
        current = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {fq}").collect()}
        todo = {k: v for k, v in wanted.items() if current.get(k, "").lower() != v.lower()}
        if not todo:
            print(f"ok          {fq}")
            continue
        props = ", ".join(f"'{k}' = '{v}'" for k, v in todo.items())
        spark.sql(f"ALTER TABLE {fq} SET TBLPROPERTIES ({props})")
        print(f"set         {fq}: {props}")
    except Exception as e:
        failures.append(t)
        print(f"FAILED      {fq}: {str(e)[:300]}")

if failures:
    raise Exception(f"Could not set properties on: {failures}")
