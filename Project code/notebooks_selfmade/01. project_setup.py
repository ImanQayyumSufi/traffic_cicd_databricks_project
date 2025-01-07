# Databricks notebook source
# MAGIC %run "../traffic_project/00. project_variables_functions"

# COMMAND ----------

dbutils.widgets.text(name='environment', defaultValue='', label='Enter the current environment')
env = dbutils.widgets.get("environment")

# COMMAND ----------

print(env)

# COMMAND ----------

create_schema("bronze", env, bronze_path)

# COMMAND ----------

create_schema("silver", env, silver_path)

# COMMAND ----------

create_schema("gold", env, gold_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Bronze Table

# COMMAND ----------

def create_table_raw_traffic(environment):
    print(f"Creating raw_traffic table in {environment}_traffic")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {environment}_traffic.bronze_db.raw_traffic (
        Record_ID INT,
        Count_point_id INT,
        Direction_of_travel VARCHAR(255),
        Year INT,
        Count_date VARCHAR(255),
        hour INT,
        Region_id INT,
        Region_name VARCHAR(255),
        Local_authority_name VARCHAR(255),
        Road_name VARCHAR(255),
        Road_Category_ID INT,
        Start_junction_road_name VARCHAR(255),
        End_junction_road_name VARCHAR(255),
        Latitude DOUBLE,
        Longitude DOUBLE,
        Link_length_km DOUBLE,
        Pedal_cycles INT,
        Two_wheeled_motor_vehicles INT,
        Cars_and_taxis INT,
        Buses_and_coaches INT,
        LGV_Type INT,
        HGV_Type INT,
        EV_Car INT,
        EV_Bike INT,
        Extract_Time TIMESTAMP
        );"""
    )

# COMMAND ----------

def create_table_raw_road(environment):
    print(f"Creating raw_road table in {environment}_traffic")
    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {environment}_traffic.bronze_db.raw_roads (
                Road_ID INT,
                Road_Category_Id INT,
                Road_Category VARCHAR(255),
                Region_ID INT,
                region_Name VARCHAR(255),
                Total_Link_Length_Km DOUBLE,
                Total_Link_Length_Miles DOUBLE,
                All_Motor_Vehicles DOUBLE
                );"""
            )