# Databricks notebook source
# MAGIC %run "../traffic_project/00. project_variables_functions"

# COMMAND ----------

dbutils.widgets.text(name='environment', defaultValue='', label='Enter the current environment')
env = dbutils.widgets.get("environment")

# COMMAND ----------

def read_silver_traffic(environment):
    print("Reading silver traffic table: ", end ='')
    read_silver_traffic = spark.readStream \
        .table(f"{environment}_traffic.silver_db.traffic_data")
    
    print("Successful")
    return read_silver_traffic

# COMMAND ----------

def read_silver_roads(environment):
    print("Reading silver roads table: ", end ='')
    read_silver_roads = spark.readStream \
        .table(f"{environment}_traffic.silver_db.roads_data")
    
    print("Successful")
    return read_silver_roads

# COMMAND ----------

def calc_vehicle_intensity(df):
    print("Calculating vehicle intensity: ", end ='')
    from pyspark.sql.functions import col

    calc_df = df.withColumn("Vehicle_Intensity", col("Motor_Vehicle_Count") / col("Link_length_km"))

    print("Successful")
    return calc_df

# COMMAND ----------

def create_load_time(df):
    print("Creating Load column: ", end ='')
    from pyspark.sql.functions import current_timestamp

    df_time = df.withColumn("Load_Time", current_timestamp())

    print("Successful")
    return df_time

# COMMAND ----------

def write_gold_traffic(df, environment):
    print(f"Writing data into {environment}_traffic gold traffic data: ", end ='')
    gold_df = df.writeStream \
        .format("delta") \
        .option("checkpointLocation", f"{checkpoint_path}/gold_traffic/checkpoint_progress") \
        .outputMode("append") \
        .queryName("gold_traffic_write_stream") \
        .trigger(availableNow = True) \
        .toTable(f"{environment}_traffic.gold_db.traffic_data")
    
    gold_df.awaitTermination()
    print("Succesful")

# COMMAND ----------

def write_gold_roads(df, environment):
    print(f"Writing data into {environment}_traffic gold roads data: ", end ='')
    gold_df = df.writeStream \
        .format("delta") \
        .option("checkpointLocation", f"{checkpoint_path}/gold_roads/checkpoint_progress") \
        .outputMode("append") \
        .queryName("gold_roads_write_stream") \
        .trigger(availableNow = True) \
        .toTable(f"{environment}_traffic.gold_db.roads_data")
    
    gold_df.awaitTermination()
    print("Succesful")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calling functions

# COMMAND ----------

# Read Silver Data
sl_traffic = read_silver_traffic(env)
sl_roads = read_silver_roads(env)

# Transformation
traffic_intensity_df = calc_vehicle_intensity(sl_traffic)
loadtime_traffic = create_load_time(traffic_intensity_df)

loadtime_roads = create_load_time(sl_roads)

# Load into gold tables
write_gold_traffic(loadtime_traffic, env)
write_gold_roads(loadtime_roads, env)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev_traffic.gold_db.traffic_data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev_traffic.gold_db.roads_data