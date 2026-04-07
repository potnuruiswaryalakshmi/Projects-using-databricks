# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
li = ['dim_customers','dim_gross_price','dim_products','fact_orders']

# COMMAND ----------

for file in li:
    df = spark.read.format('csv').option('header',True).load(f"/Volumes/fmcg/data_volumes/fmcg_data/Parent_Company/full_load/{file}.csv")
    df.display()

# COMMAND ----------

li_child_files = ['customers','gross_price','products','landing/orders_2025_07_01']

# COMMAND ----------

for file in li_child_files:
    df1 = spark.read.format('csv').option('header',True).load(f"/Volumes/fmcg/data_volumes/fmcg_data/child_company/{file}.csv")
    df1.display()

# COMMAND ----------

li = ['dim_customers','dim_gross_price','dim_products','fact_orders']
li_child_files = ['customers','gross_price','products','landing/orders_2025_07_01']

for file,file1 in zip(li,li_child_files):
    columns_of_parent_company = spark.read.format('csv').option('header',True).load(f"/Volumes/fmcg/data_volumes/fmcg_data/Parent_Company/full_load/{file}.csv").limit(1)
    columns_of_child_company = spark.read.format('csv').option('header',True).load(f"/Volumes/fmcg/data_volumes/fmcg_data/child_company/{file1}.csv").limit(1)
    print('parent_company_columns')
    columns_of_parent_company.display()
    print('child_company_columns')
    columns_of_child_company.display()

# COMMAND ----------

columns_of_child_company = spark.read.format('csv').option('header',True).load(f"/Volumes/fmcg/data_volumes/fmcg_data/child_company/products.csv")
columns_of_child_company.display()

# COMMAND ----------

file_path_parent = "/Volumes/fmcg/data_volumes/fmcg_data/Parent_Company/full_load/table_name.csv"
file_path_child = "/Volumes/fmcg/data_volumes/fmcg_data/child_company/table_name.csv"

# COMMAND ----------

dbutils.fs.ls("/Volumes/fmcg/data_volumes/fmcg_data/Parent_Company/full_load/")

# COMMAND ----------

# Copying a single file — recurse=True is not needed
dbutils.fs.cp(
    "/Volumes/fmcg/data_volumes/fmcg_data/child_company/landing",
    "/Volumes/fmcg/data_volumes/fmcg_data/child_company/full_load/landing",
    recurse=True
)

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("fmcg.fmcg.fmcg_bronze.{table_name}")

# COMMAND ----------

list_file_path = dbutils.fs.ls("/Volumes/fmcg/data_volumes/fmcg_data/child_company/landing")
df = None 
for file_info in list_file_path:
    print(file_info.path)
    df_o = spark.read.format('csv').option('header', True).load(file_info.path)
    if df is None:
        df = df_o
    else:
        df = df.unionByName(df_o)
