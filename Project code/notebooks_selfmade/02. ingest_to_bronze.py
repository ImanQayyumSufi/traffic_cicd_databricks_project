# Databricks notebook source
# MAGIC %run "../traffic_project/00. project_variables_functions"

# COMMAND ----------

dbutils.widgets.text(name="environment", defaultValue="", label="Enter the current environment")
env = dbutils.widgets.get("environment")

# COMMAND ----------

print(env)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Traffic Data

# COMMAND ----------

def read_traffic_data():

    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
    from pyspark.sql.functions import current_timestamp

    traffic_schema = StructType([
        StructField("Record_ID", IntegerType(), True),
        StructField("Count_point_id", IntegerType(), True),
        StructField("Direction_of_travel", StringType(), True),
        StructField("Year", IntegerType(), True),
        StructField("Count_date", StringType(), True),
        StructField("hour", IntegerType(), True),
        StructField("Region_id", IntegerType(), True),
        StructField("Region_name", StringType(), True),
        StructField("Local_authority_name", StringType(), True),
        StructField("Road_name", StringType(), True),
        StructField("Road_Category_ID", IntegerType(), True),
        StructField("Start_junction_road_name", StringType(), True),
        StructField("End_junction_road_name", StringType(), True),
        StructField("Latitude", DoubleType(), True),
        StructField("Longitude", DoubleType(), True),
        StructField("Link_length_km", DoubleType(), True),
        StructField("Pedal_cycles", IntegerType(), True),
        StructField("Two_wheeled_motor_vehicles", IntegerType(), True),
        StructField("Cars_and_taxis", IntegerType(), True),
        StructField("Buses_and_coaches", IntegerType(), True),
        StructField("LGV_Type", IntegerType(), True),
        StructField("HGV_Type", IntegerType(), True),
        StructField("EV_Car", IntegerType(), True),
        StructField("EV_Bike", IntegerType(), True)
    ])

    raw_traffic_stream = spark.readStream \
            .format("cloudFiles") \
            .option("cloudFiles.format", "csv") \
            .option("cloudFiles.schemaLocation", f"{checkpoint_path}/raw_traffic/traffic_schema") \
            .option("header", "true") \
            .schema(traffic_schema) \
            .load(f"{landing_path}/raw_traffic") \
            .withColumn("Extract_Time", current_timestamp())
    print("Read Successful")
     
    return raw_traffic_stream

# COMMAND ----------

def write_traffic_data(read_stream, environment):

    read_traffic_df = read_stream.writeStream \
            .format("delta") \
            .option("checkpointLocation", f"{checkpoint_path}/raw_traffic/checkpoint_progress") \
            .outputMode("append") \
            .queryName("raw_traffic_write_stream") \
            .trigger(availableNow = True) \
            .toTable(f"{environment}_traffic.bronze_db.raw_traffic")

    read_traffic_df.awaitTermination()

    print("Write Successful")

# COMMAND ----------

read_traffic_df = read_traffic_data()
write_traffic_data(read_traffic_df, env)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Roads Data

# COMMAND ----------

def read_roads_data():

    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

    roads_schema = StructType([
            StructField('Road_ID',IntegerType()),
            StructField('Road_Category_Id',IntegerType()),
            StructField('Road_Category',StringType()),
            StructField('Region_ID',IntegerType()),
            StructField('Region_Name',StringType()),
            StructField('Total_Link_Length_Km',DoubleType()),
            StructField('Total_Link_Length_Miles',DoubleType()),
            StructField('All_Motor_Vehicles',DoubleType())
        ])
    
    read_roads_stream = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("cloudFiles.schemaLocation",f"{checkpoint_path}/raw_roads/roads_schema") \
        .option("header", "true") \
        .schema(roads_schema) \
        .load(f"{landing_path}/raw_roads")

    print("Read Successful")
     
    return read_roads_stream

# COMMAND ----------

def write_road_data(read_stream, environment):

    write_traffic_data = read_stream.writeStream \
        .format("delta") \
        .option("checkpointLocation", f"{checkpoint_path}/raw_roads/checkpoint_progress") \
        .outputMode("append") \
        .queryName("raw_roads_write_stream") \
        .trigger(availableNow = True) \
        .toTable(f"{environment}_traffic.bronze_db.raw_roads")

    write_traffic_data.awaitTermination()

    print("Write Successful")

# COMMAND ----------

read_road_df = read_roads_data()
write_road_data(read_road_df, env)