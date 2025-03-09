# BigData 2025 Project Repository

![TartuLogo](./images/logo_ut_0.png)

Project [Big Data](https://courses.cs.ut.ee/2025/bdm/spring/Main/HomePage) is provided by [University of Tartu](https://courses.cs.ut.ee/).

---
# Project 1: Analyzing New York City Taxi Data

**Students**  
- Juan Gonzalo Quiroz Cadavid  
- Priit Peterson  
- Shivam Maheshwari  
- Venkata Narayana Bommanaboina  

**Affiliation**: University of Tartu  
**Date**: 2025-03-09  

---

## Master Plan

- **Geospatial preparation**  
  Load the GeoJSON file, sort polygons by descending area, and broadcast it across Spark workers.

- **Data features**  
  - **Borough name**: For each (lat, lon) on pickup/dropoff, use a UDF to find which polygon contains the point.  
  - **Duration**: Calculate the time difference between pickup and dropoff.

- **Cleansing data**  
  Remove rows with invalid lat/lon or negative/very large durations (> 4 hours).

- **Window data**  
  Partition by taxi ID, sort by pickup time, compute idle time between trips, etc.

---

## 1. Geospatial Preparation

The file `nyc_boroughs.geojson` follows the RFC 7946 standard. It has 5 boroughs (Manhattan, Bronx, Brooklyn, Queens, Staten Island) represented by ~104 polygons.

Example code to read and broadcast:

    import json
    from shapely.geometry import shape
    
    with open("./data/nyc_boroughs.geojson", "r") as f:
        geojson_data = json.load(f)
    
    data_geomtries = []
    for feature in geojson_data["features"]:
        boroughCode = feature["properties"]["boroughCode"]
        boroughName = feature["properties"]["borough"]
        poly_shape = shape(feature["geometry"])
        data_geomtries.append({
            "boroughCode": boroughCode,
            "borough": boroughName,
            "area": poly_shape.area,
            "polygon": poly_shape
        })
    
    # Sort polygons by area descending
    sorted_geoms = sorted(
        data_geomtries,
        key=lambda x: x["area"],
        reverse=True
    )
    
    # Broadcast to Spark workers
    broadcast_geoms = spark.sparkContext.broadcast(sorted_geoms)

---

## 2. Data Features

### 2.1 Borough Name

We define a Python function that checks which polygon covers a given point:

    from shapely.geometry import Point
    from pyspark.sql.types import StringType
    import pyspark.sql.functions as F
    
    def getBorough(lat, lon):
        if lat is None or lon is None:
            return None
        point = Point(lon, lat)
        for item in broadcast_geoms.value:
            if item["polygon"].covers(point):
                return item["borough"]
        return "Unknown"
    
    getBoroughUDF = F.udf(getBorough, StringType())

Then we apply it to create pickup/dropoff borough columns:

    trip_df = (trip_df
      .withColumn("pickup_borough",
                  getBoroughUDF(F.col("pickup_latitude"), F.col("pickup_longitude")))
      .withColumn("dropoff_borough",
                  getBoroughUDF(F.col("dropoff_latitude"), F.col("dropoff_longitude")))
    )

### 2.2 Duration

Compute `Duration` by converting date/time to Unix timestamps:

    trip_df = trip_df.withColumn(
        "duration",
        F.unix_timestamp(F.col("dropoff_datetime"), "dd-MM-yy HH:mm")
        - F.unix_timestamp(F.col("pickup_datetime"), "dd-MM-yy HH:mm")
    )

---

## 3. Cleansing Data

Remove rows with invalid or extreme values:

    trip_df = (trip_df
      .filter(F.col("taxi_id").isNotNull())
      .filter(
         (F.col("pickup_latitude") != 0) &
         (F.col("pickup_longitude") != 0) &
         (F.col("dropoff_latitude") != 0) &
         (F.col("dropoff_longitude") != 0)
      )
      .filter(F.col("duration") > 0)
      # optional:
      # .filter(F.col("duration") <= 4 * 3600)
    )

---

## 4. Query 1: Utilization per Taxi

Utilization = Total Ride Time / (Total Ride Time + Total Idle Time)

Steps:

1. Partition by `taxi_id`, order by `pickup_ts`.
2. Compute idle time if gap <= 4 hours.
3. Sum ride time + idle time.
4. Calculate ratio.

Example:

    from pyspark.sql.window import Window
    
    w = Window.partitionBy("taxi_id").orderBy("pickup_ts")
    
    trip_df = trip_df.withColumn(
        "prev_dropoff_ts",
        F.lag("dropoff_ts").over(w)
    )
    
    idle_expr = F.when(
        (F.col("pickup_ts") - F.col("prev_dropoff_ts") <= 4 * 3600) &
        (F.col("pickup_ts") - F.col("prev_dropoff_ts") >= 0),
        F.col("pickup_ts") - F.col("prev_dropoff_ts")
    ).otherwise(F.lit(0))
    
    trip_df = trip_df.withColumn("idle_time", idle_expr)
    
    util_df = trip_df.groupBy("taxi_id").agg(
        F.sum("duration").alias("total_ride_time"),
        F.sum("idle_time").alias("total_idle_time")
    )
    
    util_df = util_df.withColumn(
        "utilization",
        F.col("total_ride_time") / 
        (F.col("total_ride_time") + F.col("total_idle_time"))
    )
    
    util_df.show(10, truncate=False)

---

## 5. Query 2: Average Time to Find Next Fare per Destination Borough

1. Partition by `taxi_id`, order by `pickup_ts`.
2. Compute `time_to_next_fare = next_pickup_ts - dropoff_ts` if gap <= 4 hours.
3. Group by `dropoff_borough` to get the average.

Example:

    w2 = Window.partitionBy("taxi_id").orderBy("pickup_ts")
    
    trip_df = trip_df.withColumn(
        "next_pickup_ts",
        F.lead("pickup_ts").over(w2)
    )
    
    trip_df = trip_df.withColumn(
        "time_to_next_fare",
        F.when(
            (F.col("next_pickup_ts") - F.col("dropoff_ts") <= 4*3600) &
            (F.col("next_pickup_ts") - F.col("dropoff_ts") >= 0),
            F.col("next_pickup_ts") - F.col("dropoff_ts")
        ).otherwise(F.lit(None))
    )
    
    wait_time_df = trip_df.groupBy("dropoff_borough") \
        .agg(F.avg("time_to_next_fare").alias("avg_time_to_next_fare"))
    
    wait_time_df.show(10, truncate=False)

---

## 6. Query 3: Trips Starting and Ending in the Same Borough

Simple filter + count:

    same_borough_count = trip_df.filter(
        F.col("pickup_borough") == F.col("dropoff_borough")
    ).count()
    
    print("Number of same-borough trips:", same_borough_count)

If you want a **per-borough** breakdown:

    same_borough_df = (trip_df
       .filter(F.col("pickup_borough") == F.col("dropoff_borough"))
       .groupBy("pickup_borough")
       .count()
    )
    same_borough_df.show()

---

## 7. Query 4: Trips Starting in One Borough and Ending in Another

    diff_borough_count = trip_df.filter(
        F.col("pickup_borough") != F.col("dropoff_borough")
    ).count()
    
    print("Number of different-borough trips:", diff_borough_count)

---

## Conclusion

1. We **loaded** and **cleansed** NYC Taxi data.
2. We **enriched** each trip with borough info using Shapely + a broadcast approach.
3. We **computed**:
   - Utilization per taxi
   - Average time to next fare (by dropoff borough)
   - Same-borough vs. different-borough trip counts
4. We **visualized** results with basic plots (histograms, bar charts).

This completes our analysis of NYC Taxi data using **PySpark** and **Shapely** for geospatial enrichment. 
