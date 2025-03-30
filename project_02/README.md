# Project 2: The DEBS GRAND CHALLENGE 2015

**Students**  
- Juan Gonzalo Quiroz Cadavid  
- Priit Peterson  
- Shivam Maheshwari  
- Venkata Narayana Bommanaboina  

**Affiliation**: University of Tartu  

---

## Overall Architecture

For this project, the next system architecture were used over docker compose, Kafka,
zooKeeper and PySpak as independent containers, communication goes through docker
compose networking. Container to container communication was made possible by
docker compose networking DNS.

## Master Plan

- **Data Ingestion & Cleansing**
  
      – Load a portion (e.g., 1 GB) of the NYC Taxi trip dataset.
      – Remove invalid rows (e.g., null or zero coordinates, unknown driver IDs) and
      cast columns to appropriate types.
- **Grid Cell Computation**
  
      – Map each latitude/longitude coordinate to a cell ID.
      – For Query 1, use a 500 m grid (cell IDs range from 1.1 to 300.300).
      – For Query 2, use a 250 m grid (cell IDs range from 1.1 to 600.600).
- **Query 1: Frequent Routes**
  
      – Compute the 10 most frequent routes in the last 30 minutes, where a route
      is identified by (start cell, end cell).
      – Update results only when the top-10 changes, and output one row containing:
          . pickup datetime
          . dropoff datetime
          . start cell id n
          . end cell id n
          . delay
- **Query 2: Profitable Areas**
  
      – Calculate profitability for each area as:
      median fare + tip in last 15 minutes
      number of empty taxis in last 30 minutes
      – Report the 10 most profitable areas. If fewer than 10 exist, fill in NULL for
      missing cells. Output one row for each update that includes:
          . pickup datetime
          . dropoff datetime
          . profitable cell id n
          . empty taxies in cell id n
          . median profit in cell id n
          . profitability of cell n
          . delay
- **Streaming Setup & Output**
  
      – Use PySpark Structured Streaming with foreachBatch to process microbatches.
      – Maintain global state to detect changes in top-10 routes or profitable areas.
      – Write final results to a table or console for each batch, including the delay
      metric (system time minus ingest time)

---

## Query 0: Data Cleansing and Setup

### Set up
First, we defined the spark session. We decided to configure spark to work with Delta
lake by:
  1. Adding its JARS package into Spark runtime.
  2. Registering Delta SQL commands into SQL extensions.
  3. Making it the default Catalog for all tables.

          from delta import configure_spark_with_delta_pip
          builder = SparkSession.builder.appName("project2_debs_grand_challenge") \
              .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
              .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
              .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
              .config("spark.local.dir", "./temp")
          spark = configure_spark_with_delta_pip(builder).getOrCreate()

### Reading and cleaning
We decided to read the CSV without headers, in order to inject the headers as we want
to name, Then, we took 10% of the data, (Around 1Gb)

      columns = [
          "medallion", "hack_license", "pickup_datetime", "dropoff_datetime",
          "trip_time_in_secs", "trip_distance", "pickup_longitude", "pickup_latitude",
          "dropoff_longitude", "dropoff_latitude", "payment_type", "fare_amount",
          "surcharge", "mta_tax", "tip_amount", "tolls_amount", "total_amount"
      ]
      
      df = (
          spark.read
              .option("header", "false")
              .option("inferSchema", True)
              .csv("data/sorted_data.csv")
      )
      df = df.toDF(*columns)
      df.printSchema()
      
      #Take 1GB of original data
      df_sample = df.sample(withReplacement=False, fraction=0.1)

After reading, we clean the data by the next operations, which leave us with 13.4
Million elements to analyze, a before cleaning it was 17.3 Million data, which
means almost 4 Million data was corrupted

  1. Changing format of pickup and dropoff date time to timestamp Colum type. 
  2. Removing null or 0.0 columns and unknown licenses or drivers. 
  3. Convert relevant columns to appropriate data types for numerical computations.

    #converting pickup and dropoff datetimes to to_timestamp
    df_sample = df_sample.withColumn("pickup_datetime", to_timestamp(col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss")) \
                         .withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"))
    
    #Removing null or 0.0 columns and unknown licenses or drivers
    df_clean = df_sample.filter(
        (col("medallion").isNotNull()) & (col("medallion") != "0") & (col("medallion") != "UNKNOWN") &
        (col("hack_license").isNotNull()) & (col("hack_license") != "0") & (col("hack_license") != "UNKNOWN") &
        (col("pickup_datetime").isNotNull()) &
        (col("dropoff_datetime").isNotNull()) &
        (col("trip_time_in_secs").isNotNull()) & (col("trip_time_in_secs") != 0) &
        (col("trip_distance").isNotNull()) & (col("trip_distance") != 0) &
        (col("pickup_longitude").isNotNull()) & (col("pickup_longitude") != 0.0) &
        (col("pickup_latitude").isNotNull()) & (col("pickup_latitude") != 0.0) &
        (col("dropoff_longitude").isNotNull()) & (col("dropoff_longitude") != 0.0) &
        (col("dropoff_latitude").isNotNull()) & (col("dropoff_latitude") != 0.0) &
        (col("trip_distance").cast("float") > 0) &
        (col("fare_amount").cast("float") > 0)
    )
    
    # Convert relevant columns to appropriate data types for numerical computations
    df_clean = df_clean.withColumn("trip_time_in_secs", col("trip_time_in_secs").cast("int")) \
                       .withColumn("trip_distance", col("trip_distance").cast("float")) \
                       .withColumn("fare_amount", col("fare_amount").cast("float")) \
                       .withColumn("surcharge", col("surcharge").cast("float")) \
                       .withColumn("mta_tax", col("mta_tax").cast("float")) \
                       .withColumn("tip_amount", col("tip_amount").cast("float")) \
                       .withColumn("tolls_amount", col("tolls_amount").cast("float"))



## Query 1:  Frequent Routes

### Defining cells and computing
#### Part 1: Top 10 most frequent routes during the last 30 minutes.
In order to get the cells for each Lat/Lon point, we:
  1. Moved the center of the firs cell to the corner of the cell (by 0.250 Km to the
  North, 0.250Km to the West).
  2. Shared the new P0.0 across all nodes using Spark context. 
  3. For upcoming lat/lon attributes, we will only need to calculate the distance to the
  Point 0 and then divide it by 0.5 Km on both lat and long distances to get the
  cell location.
  4. Defined a new UDF function for calculating the cells locations. 
  5. Applied the UDF as a new column for all elements in the DF. 
  6. Filtered by -1 -1, which means the (Lat, Lon) pair is located outside the boundaries. After cleaning, 54K rows were removed. 
  7. Grouped them by tumbling time window of 30 mins, start and end cell and Ordered
  then by count.

          def latlon_distance(latA, lonA, latB, lonB):        
              degree_km = 111.32
              
              lat_dist = abs(latB - latA) * degree_km
              lon_dist = abs(lonB - lonA) * (degree_km * cos(radians(latA)))
          
              return lat_dist, lon_dist
          
          def move_point(latA, lonA, north_km=0.250, east_km=0.250):
              km_per_deg_lat = 111.32
              km_per_deg_lon = 111.32 * cos(radians(latA))
          
              new_lat = latA + (north_km / km_per_deg_lat)
              new_lon = lonA + (east_km / km_per_deg_lon)
          
              return new_lat, new_lon
          
          pRoot = [41.474937, -74.913585]
          p0 = move_point(pRoot[0], pRoot[1], 0.250 , 0.250)
          broadcast_center = spark.sparkContext.broadcast(p0)
          
          def getCells(center, lat, lon, upTo=300):
              distLat, distLon = latlon_distance(center[0], center[1], lat, lon)
              if distLat == -1 or distLon == -1:
                  return -1, -1
          
              cellLat, cellLon = int(distLat//0.5), int(distLon//0.5)
          
              if distLat > upTo or distLon > upTo:
                  return -1, -1
          
              return cellLat, cellLon 
          
          def getCellString(lat, lon):
              center = broadcast_center.value
              cellLat, cellLon = getCells(center, lat, lon)
              return f"{cellLat}_{cellLon}"
          
          getCellUDF = F.udf(getCellString, StringType())
          
          df_routes = df_clean.withColumn(
              "start_cell",
              getCellUDF(col("pickup_latitude"),col("pickup_longitude"))
              
          ).withColumn(
              "end_cell",
              getCellUDF(col("dropoff_latitude"),col("dropoff_longitude"))
              
          )
          
          df_routes_cleaned = df_routes.filter((col("end_cell") != "-1_-1") & (col("start_cell") != "-1_-1"))
          
          
          df_frequent_routes_by_time_window = (
              df_routes_cleaned
                  .groupBy(F.window("pickup_datetime", "30 minutes"), "start_cell", "end_cell").count()
                  .withColumnRenamed("count", "Number_of_Rides")
                  .withColumnRenamed("window", "Time_window")
          ) 
          top10_routes_by_time_window = df_frequent_routes_by_time_window.orderBy(col("Number_of_Rides").desc()).limit(10)
          top10_routes_by_time_window.show()


#### Part 2: Query results must be updated whenever any of the 10 most frequent routes change.

To address this query, we:
1. Created a Stream dataframe by writing into disk and loading it as a Stream. 
2. Using the stream, we run a writeStream process on batches. 
3. On every batch, we filter the data for only the last 30 mins, base on their drop off
time. 
4. aggregated the output into a single table.

        # Writing the cleansed data to a Delta table in a writable directory.
        df_clean.write.format("delta").mode("overwrite").save("/tmp/delta/taxi_data")
        df_stream = spark.readStream.format("delta").load("/tmp/delta/taxi_data")
        
        # Define your grid constants for the 500m x 500m grid
        grid_origin_lat = 41.474937
        grid_origin_lon = -74.913585
        delta_lat = 0.0045   # Approximate degrees for 500m in latitude
        delta_lon = 0.0060   # Approximate degrees for 500m in longitude
        
        # This is your foreachBatch function to process each micro-batch
        def process_batch(batch_df, batch_id):
            # Skip empty batches
            if batch_df.rdd.isEmpty():
                return
        
            # Compute the 30-minute window based on the batch’s max dropoff
            max_dropoff = batch_df.agg({"dropoff_datetime": "max"}).collect()[0][0]
            if max_dropoff is None:
                return
            ref_time = max_dropoff - timedelta(minutes=30)
        
            # Compute grid cell IDs for pickup and dropoff using the 500m grid
            batch_df = batch_df.withColumn(
                "pickup_cell_east", floor((col("pickup_longitude") - lit(grid_origin_lon)) / lit(delta_lon)) + 1
            ).withColumn(
                "pickup_cell_south", floor((lit(grid_origin_lat) - col("pickup_latitude")) / lit(delta_lat)) + 1
            ).withColumn(
                "start_cell", concat(col("pickup_cell_east").cast("int"), lit("."), col("pickup_cell_south").cast("int"))
            )
            batch_df = batch_df.withColumn(
                "dropoff_cell_east", floor((col("dropoff_longitude") - lit(grid_origin_lon)) / lit(delta_lon)) + 1
            ).withColumn(
                "dropoff_cell_south", floor((lit(grid_origin_lat) - col("dropoff_latitude")) / lit(delta_lat)) + 1
            ).withColumn(
                "end_cell", concat(col("dropoff_cell_east").cast("int"), lit("."), col("dropoff_cell_south").cast("int"))
            )
        
            # Filter out trips that are out-of-bounds (only consider cells 1 to 300)
            batch_df = batch_df.filter(
                (col("pickup_cell_east").between(1, 300)) &
                (col("pickup_cell_south").between(1, 300)) &
                (col("dropoff_cell_east").between(1, 300)) &
                (col("dropoff_cell_south").between(1, 300))
            )
        
            # Filter for trips with dropoff_datetime >= ref_time (last 30 minutes)
            df_last30 = batch_df.filter(col("dropoff_datetime") >= F.lit(ref_time))
            print(f"Window filter: dropoff_datetime >= {ref_time}")
            print("df_last30 count =", df_last30.count())
            df_last30.show(5)
        
            # Aggregate routes and get top 10 most frequent
            df_frequent_routes = df_last30.groupBy("start_cell", "end_cell") \
                .count() \
                .withColumnRenamed("count", "Number_of_Rides")
            top10_routes = df_frequent_routes.orderBy(col("Number_of_Rides").desc()).limit(10)
            top10_list = top10_routes.collect()
        
            # Determine a triggering event and compute delay
            # Choose the event with the maximum dropoff_datetime as the trigger
            trigger_row = batch_df.orderBy(col("dropoff_datetime").desc()).limit(1).collect()[0]
            trigger_pickup = trigger_row["pickup_datetime"]
            trigger_dropoff = trigger_row["dropoff_datetime"]
            ingest_time = trigger_row["ingest_time"]
            processing_time = datetime.now()
            delay = (processing_time - ingest_time).total_seconds()
        
            # Build the output row
            output_row = {
                "pickup_datetime": trigger_pickup,
                "dropoff_datetime": trigger_dropoff,
                "delay": delay
            }
            for i in range(10):
                if i < len(top10_list):
                    route = top10_list[i]
                    output_row[f"start_cell_id_{i+1}"] = route["start_cell"]
                    output_row[f"end_cell_id_{i+1}"] = route["end_cell"]
                else:
                    output_row[f"start_cell_id_{i+1}"] = None
                    output_row[f"end_cell_id_{i+1}"] = None
        
            print(f"Update for batch {batch_id} :", output_row)
            
            # Define the output schema explicitly
            output_schema = StructType([
                StructField("pickup_datetime", TimestampType(), True),
                StructField("dropoff_datetime", TimestampType(), True),
                StructField("start_cell_id_1", StringType(), True),
                StructField("end_cell_id_1", StringType(), True),
                StructField("start_cell_id_2", StringType(), True),
                StructField("end_cell_id_2", StringType(), True),
                StructField("start_cell_id_3", StringType(), True),
                StructField("end_cell_id_3", StringType(), True),
                StructField("start_cell_id_4", StringType(), True),
                StructField("end_cell_id_4", StringType(), True),
                StructField("start_cell_id_5", StringType(), True),
                StructField("end_cell_id_5", StringType(), True),
                StructField("start_cell_id_6", StringType(), True),
                StructField("end_cell_id_6", StringType(), True),
                StructField("start_cell_id_7", StringType(), True),
                StructField("end_cell_id_7", StringType(), True),
                StructField("start_cell_id_8", StringType(), True),
                StructField("end_cell_id_8", StringType(), True),
                StructField("start_cell_id_9", StringType(), True),
                StructField("end_cell_id_9", StringType(), True),
                StructField("start_cell_id_10", StringType(), True),
                StructField("end_cell_id_10", StringType(), True),
                StructField("delay", DoubleType(), True)
            ])
            
            # Create the result DataFrame using the explicit schema
            result_df = spark.createDataFrame([output_row], schema=output_schema)
            
            # Write the result_df as a table (it will create the table if it doesn't exist)
            result_df.write.mode("append").saveAsTable("frequent_routes")
            
            # Optional: Show the result DataFrame
            result_df.show(truncate=False)
        
        # Ensure that your streaming DataFrame has proper types and includes an ingest_time column
        df_stream = df_stream.withColumn("pickup_datetime", to_timestamp(col("pickup_datetime"), "yyyy-MM-dd HH:mm:ss"))
        df_stream = df_stream.withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"))
        df_stream = df_stream.withColumn("ingest_time", current_timestamp())
        
        # Use trigger(once=True) to process existing data exactly one time
        query = (
            df_stream.writeStream
            .trigger(once=True)  #This means the query to run just once
            .foreachBatch(process_batch)
            .outputMode("append")
            .start()
        )
        
        query.awaitTermination()
        
        spark.sql("SELECT * FROM frequent_routes").show()

---

## Query 2. Profitable Areas

In this query, we compute which cells (or areas) offer the highest current profit opportunity. Profitability is defined as
`Profitability = median(fare + tip in last 15 min)/number of empty taxis in last 30 min`
A taxi is considered empty in a cell if its last dropoff happened there in the preceding 30 minutes and there has been no subsequent pickup. The system reports the top ten areas at each update, placing them in columns such as profitable cell id 1 through profitable cell id 10. If fewer than ten cells meet the criteria, the remaining columns are NULL.

### Part 1: 500m grid

Listing 16 shows how each micro-batch of streaming data is processed to find the most profitable 500 m cells. The code begins by determining the latest `dropoff_datetime` in the batch (`max_dropoff`) and setting up two time windows: one of 15 minutes to calculate the median profit, and another of 30 minutes to count how many taxis remain empty in each cell. In the snippet, `profit_df` uses the `pickup_longitude` and `pickup_latitude` columns to assign each trip to a pickup cell, which is then grouped to find the median of `fare amount + tip amount`. Meanwhile, a second aggregation locates each taxi’s most recent dropoff cell (within 30 minutes) in order to calculate the number of currently empty taxis. After a join merges these values, the system computes `profitability = median profit/empty taxis`, sorts in descending order, then extracts the top ten cells as a single output row. A delay is appended to measure the processing latency between reading the triggering event and writing the table row.

      # Grid constants for the 500m x 500m grid
      grid_origin_lat = 41.474937
      grid_origin_lon = -74.913585
      delta_lat = 0.0045   # Approximate degrees for 500m in latitude
      delta_lon = 0.0060   # Approximate degrees for 500m in longitude
      
      def process_batch_query2(batch_df, batch_id):
          # Skip empty batches
          if batch_df.rdd.isEmpty():
              print(f"Batch {batch_id}: Empty batch.")
              return
      
          # Compute reference times based on the batch’s maximum dropoff_datetime
          max_dropoff = batch_df.agg({"dropoff_datetime": "max"}).collect()[0][0]
          if max_dropoff is None:
              print(f"Batch {batch_id}: max_dropoff is None.")
              return
          # For profit: consider trips ending in the last 15 minutes
          ref_time_profit = max_dropoff - timedelta(minutes=15)
          # For empty taxis: consider taxis whose last dropoff was within the last 30 minutes
          ref_time_empty = max_dropoff - timedelta(minutes=30)
          print(f"Batch {batch_id}: ref_time_profit = {ref_time_profit}, ref_time_empty = {ref_time_empty}")
      
          # Compute profit aggregate per area (using pickup location)
          profit_df = batch_df.filter(col("dropoff_datetime") >= F.lit(ref_time_profit)) \
              .withColumn("profit", col("fare_amount") + col("tip_amount")) \
              .withColumn(
                  "pickup_cell_east",
                  floor((col("pickup_longitude") - lit(grid_origin_lon)) / lit(delta_lon)) + 1
              ).withColumn(
                  "pickup_cell_south",
                  floor((lit(grid_origin_lat) - col("pickup_latitude")) / lit(delta_lat)) + 1
              ).withColumn(
                  "pickup_cell",
                  concat(col("pickup_cell_east").cast("int"), lit("."), col("pickup_cell_south").cast("int"))
              )
          profit_agg = profit_df.groupBy("pickup_cell") \
              .agg(F.expr("approx_percentile(profit, 0.5) as median_profit"))
          
          # Compute empty taxi aggregate per area (using dropoff location)
          w = Window.partitionBy("medallion").orderBy(col("dropoff_datetime").desc())
          last_dropoff_df = batch_df.withColumn("rn", F.row_number().over(w)) \
              .filter(col("rn") == 1)
          empty_df = last_dropoff_df.filter(col("dropoff_datetime") >= F.lit(ref_time_empty)) \
              .withColumn(
                  "dropoff_cell_east",
                  floor((col("dropoff_longitude") - lit(grid_origin_lon)) / lit(delta_lon)) + 1
              ).withColumn(
                  "dropoff_cell_south",
                  floor((lit(grid_origin_lat) - col("dropoff_latitude")) / lit(delta_lat)) + 1
              ).withColumn(
                  "dropoff_cell",
                  concat(col("dropoff_cell_east").cast("int"), lit("."), col("dropoff_cell_south").cast("int"))
              )
          empty_agg = empty_df.groupBy("dropoff_cell") \
              .agg(F.countDistinct("medallion").alias("empty_taxis"))
          
          # Join the two aggregates on the cell identifier.
          area_df = profit_agg.join(empty_agg, profit_agg.pickup_cell == empty_agg.dropoff_cell, "inner") \
              .select(profit_agg.pickup_cell.alias("cell_id"), "median_profit", "empty_taxis") \
              .filter(col("empty_taxis") > 0) \
              .withColumn("profitability", col("median_profit") / col("empty_taxis"))
          
          top10_areas = area_df.orderBy(col("profitability").desc()).limit(10)
          top10_list = top10_areas.collect()
          
          # Determine a triggering event and compute processing delay.
          trigger_row = batch_df.orderBy(col("dropoff_datetime").desc()).limit(1).collect()[0]
          trigger_pickup = trigger_row["pickup_datetime"]
          trigger_dropoff = trigger_row["dropoff_datetime"]
          # Use asDict() to safely check for "ingest_time"
          trigger_row_dict = trigger_row.asDict()
          if "ingest_time" in trigger_row_dict:
              ingest_time = trigger_row_dict["ingest_time"]
          else:
              ingest_time = trigger_dropoff  # fallback if missing
          processing_time = datetime.now()
          delay = (processing_time - ingest_time).total_seconds()
          
          # Build the output row.
          output_row = {
              "pickup_datetime": trigger_pickup,
              "dropoff_datetime": trigger_dropoff,
              "delay": delay
          }
          for i in range(10):
              if i < len(top10_list):
                  area = top10_list[i]
                  output_row[f"profitable_cell_id_{i+1}"] = area["cell_id"]
                  output_row[f"empty_taxies_in_cell_id_{i+1}"] = str(area["empty_taxis"])
                  output_row[f"median_profit_in_cell_id_{i+1}"] = area["median_profit"]
                  output_row[f"profitability_of_cell_{i+1}"] = area["profitability"]
              else:
                  output_row[f"profitable_cell_id_{i+1}"] = None
                  output_row[f"empty_taxies_in_cell_id_{i+1}"] = None
                  output_row[f"median_profit_in_cell_id_{i+1}"] = None
                  output_row[f"profitability_of_cell_{i+1}"] = None
      
          print(f"Update for batch {batch_id}:", output_row)
          
          fields = [
              StructField("pickup_datetime", TimestampType(), True),
              StructField("dropoff_datetime", TimestampType(), True)
          ]
          for i in range(10):
              fields.extend([
                  StructField(f"profitable_cell_id_{i+1}", StringType(), True),
                  StructField(f"empty_taxies_in_cell_id_{i+1}", StringType(), True),
                  StructField(f"median_profit_in_cell_id_{i+1}", DoubleType(), True),
                  StructField(f"profitability_of_cell_{i+1}", DoubleType(), True)
              ])
          fields.append(StructField("delay", DoubleType(), True))
          output_schema = StructType(fields)
          
          result_df = spark.createDataFrame([output_row], schema=output_schema)
          # Write the result_df as a table
          result_df.write.mode("append").saveAsTable("most_profitable_areas_result")
          
          result_df.show(truncate=False)
      
      # Ensure your streaming DataFrame has an ingest_time column.
      df_stream = df_stream.withColumn("ingest_time", current_timestamp())
      
      query2 = (
          df_stream.writeStream
          .trigger(once=True) 
          .foreachBatch(process_batch_query2)
          .outputMode("append")
          .start()
      )
      
      query2.awaitTermination()
      
      spark.sql("SELECT * FROM most_profitable_areas_result").show()

Whenever a batch includes rides for multiple cells, several columns can be filled. If only one cell meets the criteria, the rest appear as NULL. This behavior is normal because the specification requires that unoccupied columns be blank.


### Part 2: 250m grid

Part 2 uses the same reasoning, but cell coordinates are calculated with half the deltasso that each cell measures about 250 m. The same time windows apply: 15 minutes for median profit, 30 minutes for empty taxi counts, and top-ten output columns that fill with NULL if fewer than ten exist.

      # Grid Constants for 250m x 250m grid
      # The grid’s center of cell 1.1 remains at (41.474937, -74.913585).
      # For 250m resolution, we use half the previous deltas:
      new_delta_lat = 0.0045 / 2    # ≈0.00225
      new_delta_lon = 0.0060 / 2    # ≈0.0030
      
      # Define the foreachBatch function for Query 2 Part 2
      def process_batch_query2_part2(batch_df, batch_id):
          # Skip empty batches
          if batch_df.rdd.isEmpty():
              print(f"Batch {batch_id}: Empty batch.")
              return
      
          # Compute reference times using batch’s maximum dropoff_datetime.
          max_dropoff = batch_df.agg({"dropoff_datetime": "max"}).collect()[0][0]
          if max_dropoff is None:
              print(f"Batch {batch_id}: max_dropoff is None.")
              return
          # For profit computation, consider trips that ended in the last 15 minutes.
          ref_time_profit = max_dropoff - timedelta(minutes=15)
          # For empty taxis, consider taxis whose last dropoff was within the last 30 minutes.
          ref_time_empty = max_dropoff - timedelta(minutes=30)
          print(f"Batch {batch_id}: ref_time_profit = {ref_time_profit}, ref_time_empty = {ref_time_empty}")
      
          # Compute profit aggregate per area (using pickup location).
          # Only consider trips with dropoff_datetime >= ref_time_profit.
          profit_df = batch_df.filter(col("dropoff_datetime") >= F.lit(ref_time_profit)) \
              .withColumn("profit", col("fare_amount") + col("tip_amount")) \
              .withColumn(
                  "pickup_cell_east",
                  floor((col("pickup_longitude") - lit(grid_origin_lon)) / lit(new_delta_lon)) + 1
              ).withColumn(
                  "pickup_cell_south",
                  floor((lit(grid_origin_lat) - col("pickup_latitude")) / lit(new_delta_lat)) + 1
              ).withColumn(
                  "pickup_cell",
                  concat(col("pickup_cell_east").cast("int"), lit("."), col("pickup_cell_south").cast("int"))
              )
          profit_agg = profit_df.groupBy("pickup_cell") \
              .agg(F.expr("approx_percentile(profit, 0.5) as median_profit"))
          
          # Compute empty taxi aggregate per area (using dropoff location).
          # For each taxi (medallion), take the latest dropoff event.
          w = Window.partitionBy("medallion").orderBy(col("dropoff_datetime").desc())
          last_dropoff_df = batch_df.withColumn("rn", F.row_number().over(w)) \
              .filter(col("rn") == 1)
          empty_df = last_dropoff_df.filter(col("dropoff_datetime") >= F.lit(ref_time_empty)) \
              .withColumn(
                  "dropoff_cell_east",
                  floor((col("dropoff_longitude") - lit(grid_origin_lon)) / lit(new_delta_lon)) + 1
              ).withColumn(
                  "dropoff_cell_south",
                  floor((lit(grid_origin_lat) - col("dropoff_latitude")) / lit(new_delta_lat)) + 1
              ).withColumn(
                  "dropoff_cell",
                  concat(col("dropoff_cell_east").cast("int"), lit("."), col("dropoff_cell_south").cast("int"))
              )
          empty_agg = empty_df.groupBy("dropoff_cell") \
              .agg(F.countDistinct("medallion").alias("empty_taxis"))
          
          # Join the aggregates on the cell identifier.
          # (We assume the area is defined by the same grid cell for pickup and dropoff.)
          area_df = profit_agg.join(empty_agg, profit_agg.pickup_cell == empty_agg.dropoff_cell, "inner") \
              .select(profit_agg.pickup_cell.alias("cell_id"), "median_profit", "empty_taxis") \
              .filter(col("empty_taxis") > 0) \
              .withColumn("profitability", col("median_profit") / col("empty_taxis"))
          
          # Get the top 10 areas by profitability.
          top10_areas = area_df.orderBy(col("profitability").desc()).limit(10)
          top10_list = top10_areas.collect()
          
          # Determine a triggering event and compute processing delay.
          # Choose the event with maximum dropoff_datetime as the trigger.
          trigger_row = batch_df.orderBy(col("dropoff_datetime").desc()).limit(1).collect()[0]
          trigger_pickup = trigger_row["pickup_datetime"]
          trigger_dropoff = trigger_row["dropoff_datetime"]
          # Convert the row to a dictionary for safe field access.
          trigger_row_dict = trigger_row.asDict()
          # Use ingest_time if available; otherwise, use trigger_dropoff as fallback.
          ingest_time = trigger_row_dict["ingest_time"] if "ingest_time" in trigger_row_dict else trigger_dropoff
          processing_time = datetime.now()
          delay = (processing_time - ingest_time).total_seconds()
          
          # Build the output row.
          # The required output columns are:
          # pickup_datetime, dropoff_datetime, then for each of the 10 areas:
          # profitable_cell_id_i, empty_taxies_in_cell_id_i, median_profit_in_cell_id_i, profitability_of_cell_i, and finally delay.
          output_row = {
              "pickup_datetime": trigger_pickup,
              "dropoff_datetime": trigger_dropoff,
              "delay": delay
          }
          for i in range(10):
              if i < len(top10_list):
                  area = top10_list[i]
                  output_row[f"profitable_cell_id_{i+1}"] = area["cell_id"]
                  output_row[f"empty_taxies_in_cell_id_{i+1}"] = area["empty_taxis"]  # as integer
                  output_row[f"median_profit_in_cell_id_{i+1}"] = area["median_profit"]
                  output_row[f"profitability_of_cell_{i+1}"] = area["profitability"]
              else:
                  output_row[f"profitable_cell_id_{i+1}"] = None
                  output_row[f"empty_taxies_in_cell_id_{i+1}"] = None
                  output_row[f"median_profit_in_cell_id_{i+1}"] = None
                  output_row[f"profitability_of_cell_{i+1}"] = None
      
          print(f"Update for batch {batch_id}:", output_row)
          
          # Define the output schema.
          out_fields = [
              StructField("pickup_datetime", TimestampType(), True),
              StructField("dropoff_datetime", TimestampType(), True)
          ]
          for i in range(10):
              out_fields.extend([
                  StructField(f"profitable_cell_id_{i+1}", StringType(), True),
                  StructField(f"empty_taxies_in_cell_id_{i+1}", IntegerType(), True),
                  StructField(f"median_profit_in_cell_id_{i+1}", DoubleType(), True),
                  StructField(f"profitability_of_cell_{i+1}", DoubleType(), True)
              ])
          out_fields.append(StructField("delay", DoubleType(), True))
          output_schema = StructType(out_fields)
          
          result_df = spark.createDataFrame([output_row], schema=output_schema)
          # Write the result_df as a table
          result_df.write.mode("append").saveAsTable("profitable_areas_streaming_result")
          
          result_df.show(truncate=False)
      
      # Ensure your streaming DataFrame (streaming_df) has an ingest_time column.
      df_stream = df_stream.withColumn("ingest_time", current_timestamp())
      
      # Set up the streaming query using foreachBatch.
      query2_part2 = (
          df_stream.writeStream
          .trigger(once=True) 
          .foreachBatch(process_batch_query2_part2)
          .outputMode("append")
          .start()
      )
      
      query2_part2.awaitTermination()
      
      spark.sql("SELECT * FROM profitable_areas_streaming_result").show()

When only a single cell qualifies, the remaining nine columns are NULL. This is the intended behavior whenever fewer than ten distinct profitable cells are found in the batch and we only used 10% of the whole dataset.

---

## Running the Project with Docker

1. **Ensure Docker is installed** (and Docker Compose if not included by default).
2. **Place** your CSV data in the `./data` folder.
3. Create a new docker image using `docker build . -t "pyspark-kafka:0.0.1"`
4. **Launch** the container:
   
       docker-compose up -d

   This starts a Jupyter + PySpark and kafka environment in the background.

5. **Check logs** (optional) to confirm Jupyter is running:
   
       docker-compose logs -f jupyter

6. **Open** your browser at [http://localhost:8888](http://localhost:8888).  
   - By default, no token/password is required (see `docker-compose.yml`).

7. **Run** the notebook/script:
   - In Jupyter’s file browser, open `project1.ipynb` (or your `.py` file).
   - Execute the cells to load data from `data/`, and run the queries.

**Done!** You’re now running the project via Docker. 

## Detailed Report

A more detailed write-up (including figures, tables, and extended explanations)  
is available in our PDF report: [pdfs/report.pdf](./pdf/report.pdf)

Simply open or download `pdfs/report.pdf` to read the full documentation.  


