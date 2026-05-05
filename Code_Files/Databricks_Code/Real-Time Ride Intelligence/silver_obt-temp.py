# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stream Rides Transformation

# COMMAND ----------

rides_schema = StructType([
    StructField('ride_id', StringType(), True),
    StructField('confirmation_number', StringType(), True),
    StructField('passenger_id', StringType(), True),
    StructField('driver_id', StringType(), True),
    StructField('vehicle_id', StringType(), True),
    StructField('pickup_location_id', StringType(), True),
    StructField('dropoff_location_id', StringType(), True),
    StructField('vehicle_type_id', LongType(), True),
    StructField('vehicle_make_id', LongType(), True),
    StructField('payment_method_id', LongType(), True),
    StructField('ride_status_id', LongType(), True),
    StructField('pickup_city_id', LongType(), True),
    StructField('dropoff_city_id', LongType(), True),
    StructField('cancellation_reason_id', LongType(), True),
    StructField('passenger_name', StringType(), True),
    StructField('passenger_email', StringType(), True),
    StructField('passenger_phone', StringType(), True),
    StructField('driver_name', StringType(), True),
    StructField('driver_rating', DoubleType(), True),
    StructField('driver_phone', StringType(), True),
    StructField('driver_license', StringType(), True),
    StructField('vehicle_model', StringType(), True),
    StructField('vehicle_color', StringType(), True),
    StructField('license_plate', StringType(), True),
    StructField('pickup_address', StringType(), True),
    StructField('pickup_latitude', DoubleType(), True),
    StructField('pickup_longitude', DoubleType(), True),
    StructField('dropoff_address', StringType(), True),
    StructField('dropoff_latitude', DoubleType(), True),
    StructField('dropoff_longitude', DoubleType(), True),
    StructField('distance_miles', DoubleType(), True),
    StructField('duration_minutes', LongType(), True),
    StructField('booking_timestamp', TimestampType(), True),
    StructField('pickup_timestamp', StringType(), True),
    StructField('dropoff_timestamp', StringType(), True),
    StructField('base_fare', DoubleType(), True),
    StructField('distance_fare', DoubleType(), True),
    StructField('time_fare', DoubleType(), True),
    StructField('surge_multiplier', DoubleType(), True),
    StructField('subtotal', DoubleType(), True),
    StructField('tip_amount', DoubleType(), True),
    StructField('total_fare', DoubleType(), True),
    StructField('rating', DoubleType(), True)
])

# COMMAND ----------

df = spark.read.table("uber.bronze.rides_raw")

df_parsed = df.withColumn("parsed_rides", from_json(col("rides"), rides_schema))\
                .select("parsed_rides.*")

display(df_parsed)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Jinja Template For OBT

# COMMAND ----------

# pip install Jinja2

# COMMAND ----------

jinja_config = [
    {
        "table" : "uber.silver.stg_rides stg_rides",
        "select" : 'stg_rides.ride_id, stg_rides.confirmation_number, stg_rides.passenger_id, stg_rides.driver_id, stg_rides.vehicle_id, stg_rides.pickup_location_id, stg_rides.dropoff_location_id, stg_rides.vehicle_type_id, stg_rides.vehicle_make_id, stg_rides.payment_method_id, stg_rides.ride_status_id, stg_rides.pickup_city_id, stg_rides.dropoff_city_id, stg_rides.cancellation_reason_id, stg_rides.passenger_name, stg_rides.passenger_email, stg_rides.passenger_phone, stg_rides.driver_name, stg_rides.driver_rating, stg_rides.driver_phone, stg_rides.driver_license, stg_rides.vehicle_model, stg_rides.vehicle_color, stg_rides.license_plate, stg_rides.pickup_address, stg_rides.pickup_latitude, stg_rides.pickup_longitude, stg_rides.dropoff_address, stg_rides.dropoff_latitude, stg_rides.dropoff_longitude, stg_rides.distance_miles, stg_rides.duration_minutes, stg_rides.booking_timestamp, stg_rides.pickup_timestamp, stg_rides.dropoff_timestamp, stg_rides.base_fare, stg_rides.distance_fare, stg_rides.time_fare, stg_rides.surge_multiplier, stg_rides.subtotal, stg_rides.tip_amount, stg_rides.total_fare, stg_rides.rating',
        "where" : ""
    },
    {
        "table" : "uber.bronze.map_vehicle_makes map_vehicle_makes",
        "select" : "map_vehicle_makes.vehicle_make",
        "where" : "",
        "on" : "stg_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id"
    },
    {
        "table" : "uber.bronze.map_vehicle_types map_vehicle_types",
        "select" : "map_vehicle_types.vehicle_type,map_vehicle_types.description,map_vehicle_types.base_rate,map_vehicle_types.per_mile,map_vehicle_types.per_minute",
        "where" : "",
        "on" : "stg_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id"
    },
    {
        "table" : "uber.bronze.map_ride_statuses map_ride_statuses",
        "select" : "map_ride_statuses.ride_status",
        "where" : "",
        "on" : "stg_rides.ride_status_id = map_ride_statuses.ride_status_id"
    },
    {
        "table" : "uber.bronze.map_payment_methods map_payment_methods",
        "select" : "map_payment_methods.payment_method, map_payment_methods.is_card, map_payment_methods.requires_auth",
        "where" : "",
        "on" : "stg_rides.payment_method_id = map_payment_methods.payment_method_id"
    },
    {
        "table" : "uber.bronze.map_cities map_cities",
        "select" : "map_cities.city as pickup_city, map_cities.state, map_cities.region, map_cities.updated_at as city_updated_at",
        "where" : "",
        "on" : "stg_rides.pickup_city_id = map_cities.city_id"
    },
    {
        "table" : "uber.bronze.map_cancellation_reasons map_cancellation_reasons",
        "select" : "map_cancellation_reasons.cancellation_reason",
        "where" : "",
        "on" : "stg_rides.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id"
    }
]

# COMMAND ----------

from jinja2 import Template

jinja_str = """

    SELECT 
        {% for config in jinja_config %}
            {{ config.select }} 
                {% if not loop.last %}
                    ,
                {% endif %}
        {% endfor %}
    FROM 
        {% for config in jinja_config %}
            {% if loop.first %}
                {{ config.table }}
            {% else %}
                LEFT JOIN {{ config.table }} ON {{ config.on }}
            {% endif %}
        {% endfor %}
   
        {% for  config in jinja_config %}

            {% if loop.first %}
               {% if config.where != "" %} 
                WHERE
                {% endif %}
            {% endif %}

            {{ config.where }}
                {% if not loop.last %}
                  {% if config.where != "" %}
                    AND
                  {% endif %}  
                {% endif %}
        {% endfor %}
"""


template = Template(jinja_str)
rendered_template = template.render(jinja_config=jinja_config)
print(rendered_template)

# COMMAND ----------

template = Template(jinja_str)
rendered_template = template.render(jinja_config=jinja_config)
display(spark.sql(rendered_template))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Testing Gold Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC select fact.ride_id, fact.base_fare, dim.region from uber.gold.fact as fact
# MAGIC LEFT JOIN uber.gold.dim_location as dim 
# MAGIC ON 
# MAGIC     fact.pickup_city_id = dim.pickup_city_id
# MAGIC     AND dim.`__END_AT` IS NULL

# COMMAND ----------

