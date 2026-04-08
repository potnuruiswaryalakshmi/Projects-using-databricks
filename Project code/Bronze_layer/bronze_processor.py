# Databricks notebook source
# MAGIC %run ../Config/fmcg_common_config

# COMMAND ----------

from datetime import datetime,timedelta
from pyspark.sql.functions import *

class LoadingBronzeData:
    def __init__(self,spark,file_path,mode,file_path_type):
        self.spark = spark
        self.file_path = file_path
        self.mode = mode
        self.file_path_type = file_path_type
    def load_data(self,file_name,file_path):
        df = None
        datetime_orders = (datetime.now()-timedelta(days=128)).strftime('%Y-%m-%d')
        if self.mode == 'append' and self.file_path_type == 'child':
            date = datetime_orders.replace('-','_')
            df = self.spark.read.format('csv').option('header', True).load(f"{file_path}/orders_{date}.csv")
        else:
            df = self.spark.read.format('csv').option('header',True).load(file_path.replace("table_name",file_name))
            if self.mode == 'append':
                df = df.filter(col('date') == datetime_orders)
        if self.mode == 'append':
            df = df.withColumn('upt',lit(datetime_orders))
        else:
            df = df.withColumn('upt',lit('2025-11-30'))
        return df
    def load_data_into_table(self,df,file_name):
        print(file_name,"writing into fmcg bronze")
        if not self.spark.catalog.tableExists(f"fmcg.fmcg_bronze.{self.file_path_type}_{file_name}"):
            df.write.format("delta").mode("overwrite").saveAsTable(f"fmcg.fmcg_bronze.{self.file_path_type}_{file_name}")
        else:
            df.write.format("delta").mode(self.mode).saveAsTable(f"fmcg.fmcg_bronze.{self.file_path_type}_{file_name}")
    def load_orders_data(self,file_path):
        list_file_path = dbutils.fs.ls(f"{file_path}")
        df = None 
        for file_info in list_file_path:
            print(file_info.path)
            df_o = self.spark.read.format('csv').option('header', True).load(file_info.path)
            if df is None:
                df = df_o
            else:
                df = df.unionByName(df_o)
        df = df.withColumn('upt',lit('2025-11-30'))
        return df
    def load_files_convert_tables(self):
        if self.file_path_type == "parent":
            file_path = self.file_path['parent']
        if self.file_path_type == "child":
            file_path = self.file_path["child"]
        files_list = dbutils.fs.ls(file_path)
        for file_info in files_list:
            file_name = file_info.name.split(".")[0]
            if "landing" in file_name:
                df = self.load_orders_data(file_info.path)
                file_name = "orders"
            else:
                df = self.load_data(file_name,file_info.path)
                if "orders/" in file_name:
                    file_name = "orders"
            self.load_data_into_table(df,file_name)
