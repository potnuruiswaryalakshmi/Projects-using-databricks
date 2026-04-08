# Databricks notebook source
# MAGIC %run ./fmcg_Bronze

# COMMAND ----------

loading_bronze = LoadingBronzeData(
    spark, full_load_file_path, mode="overwrite", file_path_type="parent"
)
loading_bronze.load_files_convert_tables()

# COMMAND ----------

loading_bronze = LoadingBronzeData(
    spark, full_load_file_path, mode="overwrite", file_path_type="child"
)
loading_bronze.load_files_convert_tables()
