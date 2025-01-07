# Databricks notebook source
# MAGIC %run "../traffic_project/00. project_variables_functions"

# COMMAND ----------

dbutils.widgets.text(name='environment', defaultValue='', label='Enter the current environment')
env = dbutils.widgets.get("environment")

# COMMAND ----------

def read_bronze_traffic(environment):
    print("Reading bronze traffic table: ", end ='')
    read_bronze_traffic = spark.readStream \
        .table(f"{environment}_traffic.bronze_db.raw_traffic")
    
    print("Successful")
    return read_bronze_traffic

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

# COMMAND ----------

def count_ev_vehicles(df):
    print("Counting all ev vehicles: ", end ='')
    from pyspark.sql.functions import col

    ev_df = df.withColumn("Electric_Vehicle_Count", col("EV_Car") + col("EV_Bike"))

    print("Succesfull")
    return ev_df

# COMMAND ----------

def count_motor_vehicles(df):
    print("Counting all motor vehicles: ", end ='')
    from pyspark.sql.functions import col

    motor_df = df.withColumn("Motor_Vehicle_Count", col("Electric_Vehicle_Count") + col("Two_wheeled_motor_vehicles") + col("Cars_and_taxis") + \
        col("Buses_and_coaches") + col("LGV_Type") + col("HGV_Type"))

    print("Succesfull")
    return motor_df

# COMMAND ----------

def create_transformed_time(df):
    print("Creating transformed_time column: ", end ='')
    from pyspark.sql.functions import current_timestamp

    df_time = df.withColumn("Transformed_Time", current_timestamp())

    print("Successful")
    return df_time

# COMMAND ----------

def write_silver_traffic(df, environment):
    print(f"Writing data into {environment}_traffic silver traffic data: ", end ='')
    silver_df = df.writeStream \
        .format("delta") \
        .option("checkpointLocation", f"{checkpoint_path}/silver_traffic/checkpoint_progress") \
        .outputMode("append") \
        .queryName("silver_traffic_write_stream") \
        .trigger(availableNow = True) \
        .toTable(f"{environment}_traffic.silver_db.traffic_data")
    
    silver_df.awaitTermination()
    print("Succesful")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calling all functions

# COMMAND ----------

# Read bronze traffic
bronze_traffic_df = read_bronze_traffic(env)

# Drop duplicates
deduped_traffic = drop_duplicates(bronze_traffic_df)

# Replace null values
all_columns = deduped_traffic.schema.names
nonull_traffic = handle_null_values(deduped_traffic,all_columns)

# Count ev
ev_df = count_ev_vehicles(nonull_traffic)

# Count Motor
motor_df = count_motor_vehicles(ev_df)

# Create transform time column
time_df = create_transformed_time(motor_df)

# Write to silver traffic table
write_silver_traffic(time_df, env)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dev_traffic.silver_db.traffic_data