# Databricks notebook source
#External location path

bronze_path = 'abfss://medallion-zone@devtrafficadls.dfs.core.windows.net/bronze'
silver_path = 'abfss://medallion-zone@devtrafficadls.dfs.core.windows.net/silver'
gold_path = 'abfss://medallion-zone@devtrafficadls.dfs.core.windows.net/gold'
checkpoint_path = 'abfss://databricks-checkpoints@devtrafficadls.dfs.core.windows.net/'
landing_path = 'abfss://landing-zone@devtrafficadls.dfs.core.windows.net/'

# COMMAND ----------

def create_schema(layer, environment, path):
    print(f"Using {environment}_traffic")
    
    # Set the current catalog
    spark.sql(f"USE CATALOG {environment}_traffic")
    
    # Create schema with managed location
    print(f"Creating {layer}_db in {environment}_traffic")
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {layer}_db 
        MANAGED LOCATION '{path}/{layer}_db'
    """)

# COMMAND ----------

def drop_duplicates(df):
    print("Dropping duplicates from table: ", end ='')
    deduped_df = df.dropDuplicates()

    print("Successful")
    return deduped_df

# COMMAND ----------

def handle_null_values(df, columns):
    print("Changing null values to None or 0: ", end ='')

    string_df = df.fillna("None", subset = columns)
    clean_df = string_df.fillna(0, subset = columns)

    print("Successful")
    return clean_df