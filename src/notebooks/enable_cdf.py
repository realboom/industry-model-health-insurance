# Databricks notebook source
# MAGIC %md
# MAGIC # Enable Change Data Feed on silver master tables
# MAGIC
# MAGIC The SCD Type 2 history pipeline (`hi-silver-scd2`) streams each table's Change Data Feed.
# MAGIC This sets `delta.enableChangeDataFeed = true` on every table in `tables`. It is idempotent
# MAGIC and runs before each history refresh.
# MAGIC
# MAGIC **Reinstall warning:** the model installer uses `CREATE OR REPLACE TABLE`. Replacing a
# MAGIC table resets its properties (CDF off) and breaks the history stream. After a reinstall,
# MAGIC re-run this job and see the README reinstall warning before relying on new history.

# COMMAND ----------

dbutils.widgets.text("silver_catalog", "health_insurance", "Silver catalog")
dbutils.widgets.text("tables", "member.identity", "Tables (<domain>.<table>, comma-separated)")

# COMMAND ----------

catalog = dbutils.widgets.get("silver_catalog").strip()
tables = [t.strip() for t in dbutils.widgets.get("tables").split(",") if t.strip()]

failures = []
for t in tables:
    schema, table = t.split(".", 1)
    fq = f"`{catalog}`.`{schema}`.`{table}`"
    try:
        props = {r["key"]: r["value"] for r in spark.sql(f"SHOW TBLPROPERTIES {fq}").collect()}
        if props.get("delta.enableChangeDataFeed", "false").lower() == "true":
            print(f"already on  {fq}")
        else:
            spark.sql(f"ALTER TABLE {fq} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
            print(f"enabled     {fq}")
    except Exception as e:
        failures.append(t)
        print(f"FAILED      {fq}: {str(e)[:300]}")

if failures:
    raise Exception(f"Could not enable CDF on: {failures}")
