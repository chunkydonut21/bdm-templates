# BigData 2025 Project Repository

![TartuLogo](./images/logo_ut_0.png)

Project [Big Data](https://courses.cs.ut.ee/2025/bdm/spring/Main/HomePage) is provided by [University of Tartu](https://courses.cs.ut.ee/).

Students: **[To be added]**
---
author:
- Juan Gonzalo Quiroz Cadavid
- Priit Peterson
- Shivam Maheshwari
- Venkata Narayana Bommanaboina
date: 2025-03-09
title:  Project 1 Analyzing New York City Taxi Data
---

# Master plan

-   **Geospatial preparation**: Load into memory the geo-json, store in
    decremental order base on the area of each borough, broadcast it to
    be read-only across all worker nodes under the spark context.

-   **Data features**:

    -   **Borough** name for each tuple of lat lon on pickup and drop
        off, using the previous data generated on geospatial preparation
        and a UDF to add a new field.

    -   **Duration** using the delta time between the date time feature
        for both pick-up and drop-off.

-   **Cleansing data**: Remove rows where lat/lon parameters are 0;
    duration is negative and where its duration is bigger than 4 hours
    (which means there could be an error as it is a long trip)

-   **Window data**: Aggregate over each drive per same taxi driver,
    calculate the idle when this is less than 4 hours, more than 4 would
    be considered as a recession.

# Geospatial preparation

The Geojson is a json file coded following the RFC 7946 standard. It
contains 5 borough across New york city, represented by 104 different
polygons, whose area are in the range \[0.027193754466707243 Km,
5.094442942481346$e^9$ Km\].

The GeoJson file was read using json, each feature were loaded into a
shapely shape object. For each feature a dict element were created
containing the shape, borough name and code and the shape instance. This
Dictionary will we later sorted and broadcasted across nodes.

Figure [1](#fig:geojson){reference-type="ref" reference="fig:geojson"}
shows the data store on the geojson, code
[\[code:geojson_dict\]](#code:geojson_dict){reference-type="ref"
reference="code:geojson_dict"} illustrate the process of extracting
features, and loading them into the dictionary. The resulting dictionary
is sorted on code
[\[code:geojson_sort\]](#code:geojson_sort){reference-type="ref"
reference="code:geojson_sort"} using python sort function, and broadcast
using pyspak sparkContext broadcast function on code
[\[code:geojson_broadcast\]](#code:geojson_broadcast){reference-type="ref"
reference="code:geojson_broadcast"}.

<figure id="fig:geojson">

<figcaption> </figcaption>
</figure>

::: minipage
``` {#code:geojson_dict .numberLines .python language="python" caption="data geometrics creation" label="code:geojson_dict" numbers="left"}
goeJson =(
    open("./data/nyc-boroughs.geojson", "r")
    .read()
)
y = json.loads(goeJson)
data_geomtries = []

for feature in y['features']:
    boroughCode = feature['properties']['boroughCode']
    borough = feature['properties']['borough']
    poly = feature['geometry']
    poly_shape = shape(poly)
    data_geomtries.append({
        "boroughCode": boroughCode,
        "borough": borough,
        "shape": poly_shape.area,
        "thing": poly_shape,
    })
```
:::

::: minipage
``` {#code:geojson_sort .numberLines .python language="python" caption="Sort geometries by shape" label="code:geojson_sort" numbers="left"}
sorted_geoms = sorted(
    data_geomtries,
    key=lambda poly: poly['shape'], 
    reverse=True, 
)
```
:::

::: minipage
``` {#code:geojson_broadcast .numberLines .python language="python" caption="Broadcast dict across worker nodes using sparkContext broadcast feature." label="code:geojson_broadcast" numbers="left"}
broadcast_geoms = spark.sparkContext.broadcast(sorted_geoms)
```
:::

# Data features

## Borough name

A UDF function were created. The function received lat and lon (See code
[\[code:geojson_udf\]](#code:geojson_udf){reference-type="ref"
reference="code:geojson_udf"}), converted them into a shapely Point,
iterated over all the geoms checking if the point is contained by any
polygon, if so the name of the polygon would be returned. The method is
registered as a UDF function (See code
[\[code:geojson_udf_2\]](#code:geojson_udf_2){reference-type="ref"
reference="code:geojson_udf_2"}) to be used later to create two columns
for pickup and dropoff locations (See code
[\[code:geojson_udf_3\]](#code:geojson_udf_3){reference-type="ref"
reference="code:geojson_udf_3"})

::: minipage
``` {#code:geojson_udf .numberLines .python language="python" caption="UDF method" label="code:geojson_udf" numbers="left"}
def getBorough(lat:float, lon:float) -> str:
    geoms = broadcast_geoms.value
    p = Point(lon, lat)
    for geom in geoms:
        if geom["thing"].covers(p):
            return geom["borough"]
    return None
```
:::

::: minipage
``` {#code:geojson_udf_2 .numberLines .python language="python" caption="UDF creation" label="code:geojson_udf_2" numbers="left"}
getBoroughUDF = F.udf(getBorough, StringType())
```
:::

::: minipage
``` {#code:geojson_udf_3 .numberLines .python language="python" caption="UDF usage" label="code:geojson_udf_3" numbers="left"}
trip_df = ( trip_df
    .withColumn(
        "pickup_borough", 
        getBoroughUDF(
            F.col("pickup_latitude"),
            F.col("pickup_longitude")
        )
    )
    .withColumn(
        "dropoff_borough", 
        getBoroughUDF(
            F.col("dropoff_latitude"), 
            F.col("dropoff_longitude")
        )
    )
)
```
:::

## Duration name

Duration (See Code
[\[code:duration_feature\]](#code:duration_feature){reference-type="ref"
reference="code:duration_feature"}) is calculated by transforming the
dropoff and pickup datetime into seconds using unix_timestamp function
from PySpark following the format \"dd-MM-yy HH:mm\". The duration is
calculated by subtracting pickup to dropoff the once transformed into
seconds.

::: minipage
``` {#code:duration_feature .numberLines .python language="python" caption="Duration creation" label="code:duration_feature" numbers="left"}
trip_df = (
    .withColumn(
        "duration",
        F.unix_timestamp(
            F.col("dropoff_datetime"), "dd-MM-yy HH:mm"
        ) 
        - F.unix_timestamp(
            F.col("pickup_datetime"), "dd-MM-yy HH:mm"
        )
    )
)
```
:::

# Cleansing data

First, it will be removed all rows that does not contain a TaxiID, after
it will remove rows where the lat and lon attributes are 0, and finally
all rows whose duration is less than 0 would be removed. See code
[\[code:overall_cleaning\]](#code:overall_cleaning){reference-type="ref"
reference="code:overall_cleaning"}

::: minipage
``` {#code:overall_cleaning .numberLines .python language="python" caption="Duration creation" label="code:overall_cleaning" numbers="left"}
trip_df = (
    trip_df.filter(
        ~F.isnull(F.col("taxiId"))
    )
    .filter(
        (F.col("pickup_latitude") != 0) & 
        (F.col("pickup_longitude") != 0) & 
        (F.col("dropoff_longitude") != 0) & 
        (F.col("dropoff_latitude") != 0)
    )
    .filter(
        F.col("duration") > 0
    )
)
```
:::

# Query one: Utilization

For query one, we partitioned the data using the taxiID, ordered by
their pickup ts, this will allow us to work per taxi ID; Then a new
column is added for the previous pickup time for each row using Lag
function. Later on we compute the idle for the trip, we summarized the
total ride time, total idle time and computed utilization as
$\frac{\text{total ride time}}{\text{total ride time} + \text{total idle time}}$.
Code
[\[code:q1_utilization\]](#code:q1_utilization){reference-type="ref"
reference="code:q1_utilization"} Shows the process. Table
[1](#tab:q1_utilization){reference-type="ref"
reference="tab:q1_utilization"} shows the first 10 rows of the resulting
operation. Image [2](#fig:q1_utilization){reference-type="ref"
reference="fig:q1_utilization"} plot the distribution of the utilization
across all taxi drivers, the top 10 and the buttom 10.

::: minipage
``` {#code:q1_utilization .numberLines .python language="python" caption="Query 1. Utilization" label="code:q1_utilization" numbers="left"}
# Window partitioned by taxi_id, ordered by pickup_ts
w = Window.partitionBy("taxi_id").orderBy("pickup_ts")

trip_df = trip_df.withColumn("prev_dropoff_ts", F.lag("dropoff_ts").over(w))

# Idle time between consecutive trips (if gap <= 4 hours)
idle_expr = F.when(
    (F.col("pickup_ts") - F.col("prev_dropoff_ts") <= 4 * 3600) &
    (F.col("pickup_ts") - F.col("prev_dropoff_ts") >= 0),
    F.col("pickup_ts") - F.col("prev_dropoff_ts")
).otherwise(F.lit(0))

trip_df = trip_df.withColumn("idle_time", idle_expr)

util_df = trip_df.groupBy("taxi_id").agg(
    F.sum("Duration").alias("total_ride_time"),
    F.sum("idle_time").alias("total_idle_time")
)

util_df = util_df.withColumn(
    "utilization",
    F.col("total_ride_time") / 
    (F.col("total_ride_time") + F.col("total_idle_time"))
)

print("=== Utilization per Taxi ===")
util_df.show(10, truncate=False)
```
:::

::: {#tab:q1_utilization}
             **taxi_id**              **total_ride_time**   **total_idle_time**   **utilization**
  ---------------------------------- --------------------- --------------------- -----------------
   000318C2E3E6381580E5C99910A60668          1560                  2640               0.3714
   002B4CFC5B8920A87065FC131F9732D1          1140                  7020               0.1397
   002E3B405B6ABEA23B6305D3766140F1          2760                  2040               0.5750
   0030AD2648D81EE87796445DB61FCF20          1980                   720               0.7333
   0035520A854E4F2769B37DAF5357426F          3060                  5160               0.3723
   0036961468659D0BFC7241D92E8ED865          1920                  2160               0.4706
   0038EF45118925A510975FD0CCD67192          1440                  6240               0.1875
   003D87DB553C6F00F774C8575BC8444A           420                    0                1.0000
   003EEA559FA61800874D4F6805C4A084          1020                  12600              0.0749
   0053334C798EC6C8E637657962030F99           420                    0                1.0000

  : Taxi Utilization Data
:::

<figure id="fig:q1_utilization">

<figcaption>Newyork taxi utilization</figcaption>
</figure>

# Query 2: Average Time to Find Next Fare per Destination Borough

Taxi drivers are spited by their respective ID using Window (See code
[\[code:q2\]](#code:q2){reference-type="ref" reference="code:q2"}),
Compute $time_to_next_fare = next_pickup_ts - dropoff_ts$ if the delta
is less than 4 hours, finally we group them by $dropoff_borough$ to get
the average. Table [2](#tab:q2){reference-type="ref" reference="tab:q2"}
presents the results for all 5 borough in New york, Unknown are the
points whose lat lon attributes does not fit on the geojson features.
The same data is represented on figure [3](#fig:q2){reference-type="ref"
reference="fig:q2"}.

::: minipage
``` {#code:q2 .numberLines .python language="python" caption="Duration creation" label="code:q2" numbers="left"}
w2 = Window.partitionBy("taxi_id").orderBy("pickup_ts")

trip_df = trip_df.withColumn("next_pickup_ts", F.lead("pickup_ts").over(w2))

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

print("=== Average Time to Find Next Fare per Destination Borough ===")
wait_time_df.show(10, truncate=False)
```
:::

::: {#tab:q2}
   **Dropoff Borough**   **Avg. Time to Next Fare (s)**
  --------------------- --------------------------------
        Manhattan                   2677.15
         Unknown                    3298.60
         Queens                     4611.89
        Brooklyn                    4214.11
          Bronx                     3926.67
      Staten Island                 9630.00

  : Average Time to Find Next Fare per Destination Borough
:::

<figure id="fig:q2">

<figcaption>Average Time to Find Next Fare per Destination Borough(in
minutes)</figcaption>
</figure>

# Query 3: Number of Trips that Started and Ended in the Same Borough

There is a total of **16821** trips that start and end in the same
borough, code [\[code:q3\]](#code:q3){reference-type="ref"
reference="code:q3"} filter by comparing pickup and dropoff borough
feature, then using count we could extract the number of occurrences.

::: minipage
``` {#code:q3 .numberLines .python language="python" caption="Same borough" label="code:q3" numbers="left"}
same_borough_count = trip_df.filter(
    F.col("pickup_borough") == F.col("dropoff_borough")
).count()

print(
    "Number of trips that start and end in the same borough:", 
    same_borough_count
)
```
:::

# Query 4: Number of Trips that Started in One Borough and Ended in Another

There is a total of **3179** trips that start and end in different
borough, code [\[code:q3\]](#code:q3){reference-type="ref"
reference="code:q3"} filter by comparing pickup and dropoff borough
feature, then using count we could extract the number of occurrences.

::: minipage
``` {#code:q3 .numberLines .python language="python" caption="Different borough" label="code:q3" numbers="left"}
diff_borough_count = trip_df.filter(
    F.col("pickup_borough") != F.col("dropoff_borough")
).count()
```
:::

# Conclusion

We have:

1.  Loaded and cleansed NYC Taxi data

2.  Enriched it with borough information (pickup and dropoff)

3.  Computed key metrics:

    -   Utilization per taxi

    -   Average wait time for next fare

    -   Same vs. different borough trip counts

4.  Created basic visualizations (bar chart, histogram)
