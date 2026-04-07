# Databricks notebook source
file_path = {"parent": "/Volumes/fmcg/data_volumes/fmcg_data/Parent_Company/full_load/",
              "child": "/Volumes/fmcg/data_volumes/fmcg_data/child_company/full_load/"}

# COMMAND ----------

class LoadingBronzeData:
    def __init__(self,spark,file_path):
        self.spark = spark
        self.file_path = file_path
    def load_data(self,file_name,file_path):
        df = self.spark.read.format('csv').option('header',True).load(file_path.replace("table_name",file_name))
        return df
    def load_data_into_table(self,df,file_name):
        print(file_name,"writing into fmcg bronze")
        if not spark.catalog.tableExists(f"fmcg.fmcg_bronze.{file_name}"):
            df.write.format("delta").mode("overwrite").saveAsTable(f"fmcg.fmcg_bronze.{file_name}")
        df.write.format("delta").mode("overwrite").saveAsTable(f"fmcg.fmcg_bronze.{file_name}")
    def load_orders_data(self,file_path):
        list_file_path = dbutils.fs.ls(f"{file_path}")
        df = None 
        for file_info in list_file_path:
            print(file_info.path)
            df_o = spark.read.format('csv').option('header', True).load(file_info.path)
            if df is None:
                df = df_o
            else:
                df = df.unionByName(df_o)
        return df
    def load_files_convert_tables(self,file_path_type):
        if file_path_type == "parent":
            file_path = self.file_path['parent']
        if file_path_type == "child":
            file_path = self.file_path["child"]
        files_list = dbutils.fs.ls(file_path)
        for file_info in files_list:
            file_name = file_info.name.split(".")[0]
            if "landing" in file_name:
                df = self.load_orders_data(file_info.path)
                file_name = "landing"
            else:
                df = self.load_data(file_name,file_info.path)
            self.load_data_into_table(df,file_name)

# COMMAND ----------

loading_bronze = LoadingBronzeData(spark,file_path)
loading_bronze.load_files_convert_tables(file_path_type="child")
