# Databricks notebook source
# MAGIC %run "../traffic_project/00. project_variables_functions"

# COMMAND ----------

dbutils.widgets.text(name='environment', defaultValue='', label='Enter the current environment')
env = dbutils.widgets.get("environment")

# COMMAND ----------

def read_bronze_roads(environment):
    print("Reading bronze roads table: ", end ='')
    read_bronze_roads = spark.readStream \
        .table(f"{environment}_traffic.bronze_db.raw_roads")
    
    print("Successful")
    return read_bronze_roads

# COMMAND ----------

def assign_road_category(df):
    print("Assigning road category: ", end ='')
    from pyspark.sql.functions import when, col

    category_df = df.withColumn("Road_Category_Name", \
        when(col("Road_Category") == "TA", "Class A Trunk Road") \
        .when(col("Road_Category") == "TM", "Class A Trunk Motor") \
        .when(col("Road_Category") == "PA", "Class A Principal Road") \
        .when(col("Road_Category") == "PM", "Class A Principal Motorway") \
        .when(col("Road_Category") == "M", "Class B Road") \
        .otherwise("NA")
        )

    print("Succesfull")
    return category_df

# COMMAND ----------

def assign_road_type(df):
    print("Assigning road type: ", end ='')
    from pyspark.sql.functions import when, col

    type_df = df.withColumn("Road_Type", \
        when(col("Road_Category_Name").like("%Class A%"), "Major") \
        .when(col("Road_Category_Name").like("%Class B%"), "Minor") \
        .otherwise("NA")
        )

    print("Succesfull")
    return type_df

# COMMAND ----------

def write_silver_roads(df, environment):
    print(f"Writing data into {environment}_traffic silver roads data: ", end ='')
    silver_df = df.writeStream \
        .format("delta") \
        .option("checkpointLocation", f"{checkpoint_path}/silver_roads/checkpoint_progress") \
        .outputMode("append") \
        .queryName("silver_roads_write_stream") \
        .trigger(availableNow = True) \
        .toTable(f"{environment}_traffic.silver_db.roads_data")
    
    silver_df.awaitTermination()
    print("Succesful")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calling all functions

# COMMAND ----------

# Read bronze traffic
bronze_roads_df = read_bronze_roads(env)

# Assing road category
deduped_traffic = drop_duplicates(bronze_roads_df)

# Replace null values
all_columns = deduped_traffic.schema.names
nonull_traffic = handle_null_values(deduped_traffic,all_columns)

# Assign road category
road_category = assign_road_category(nonull_traffic)

# Assign road type
road_type = assign_road_type(road_category)

# Write to silver traffic table
write_silver_roads(road_type, env)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev_traffic.silver_db.roads_data