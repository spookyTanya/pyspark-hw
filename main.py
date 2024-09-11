from pyspark.sql import SparkSession, functions as sf, types
import os

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PYSPARK_PYTHON"] = "C:\\Users\\Таня\\AppData\\Local\\Programs\\Python\\Python312\\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:\\Users\\Таня\\AppData\\Local\\Programs\\Python\\Python312\\python.exe"


def setup_session():
    spark = (SparkSession.builder
             .appName("SimpleApp")
             .config("spark.ui.enabled", "true")
             .config("spark.hadoop.fs.defaultFS", "file:///")
             .getOrCreate())

    schema = types.StructType([
        types.StructField("id", types.IntegerType(), True),
        types.StructField("name", types.StringType(), True),
        types.StructField("host_id", types.IntegerType(), True),
        types.StructField("host_name", types.StringType(), True),
        types.StructField("neighbourhood_group", types.StringType(), True),
        types.StructField("neighbourhood", types.StringType(), True),
        types.StructField("latitude", types.FloatType(), True),
        types.StructField("longitude", types.FloatType(), True),
        types.StructField("room_type", types.StringType(), True),
        types.StructField("price", types.IntegerType(), True),
        types.StructField("minimum_nights", types.IntegerType(), True),
        types.StructField("number_of_reviews", types.IntegerType(), True),
        types.StructField("last_review", types.StringType(), True),
        types.StructField("reviews_per_month", types.FloatType(), True),
        types.StructField("calculated_host_listings_count", types.IntegerType(), True),
        types.StructField("availability_365", types.IntegerType(), True)
    ])

    data = (spark.readStream
             .option('sep', ',')
             .option('header', True)
             .schema(schema)
             .csv('file:///C:/Users/Таня/PycharmProjects/spark-test/raw'))

    return spark, data


def clean_data(data):
    unique_data = data.dropDuplicates(['name', 'host_id', 'latitude', 'longitude'])

    filtered_by_price = unique_data.filter(data.price > 0)
    converted_date = filtered_by_price.withColumn('last_review_date',
                                                  sf.to_date(filtered_by_price['last_review'], 'yyyy-MM-dd')).drop(
        'last_review')
    filled = converted_date.fillna({'last_review_date': '2010-01-01', 'reviews_per_month': 0})
    cleaned_data = filled.dropna(subset=['latitude', 'longitude'])

    return cleaned_data


def transform_data(cleaned_data):
    with_price_range = cleaned_data.withColumn("price_range", sf.when(cleaned_data.price < 100,"Budget")
                                     .when(cleaned_data.price < 250,"Mid-range")
                                     .otherwise('Luxury'))

    df_with_price_per_review = with_price_range.withColumn(
        'price_per_review',
        sf.when(sf.col('number_of_reviews') > 0, sf.round(sf.col('price') / sf.col('number_of_reviews'), 2))
          .otherwise(None)
    )

    with_processing_time = df_with_price_per_review.withColumn("processing_time", sf.current_timestamp())

    return with_processing_time


def query_data(data, session):
    data.createOrReplaceTempView('temp_table_1')

    listings_count = session.sql("""SELECT COUNT(*) as listings_count, neighbourhood_group
        FROM temp_table_1 
        group by neighbourhood_group
    """)

    listings_count.writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    # 2
    windowed_df = data.groupBy(
        sf.window("processing_time", "10 minutes"),
        "id", "name"
    ).agg(sf.max("price").alias("max_price"))

    sorted_df = windowed_df.orderBy(sf.desc("max_price"))
    sorted_df.createOrReplaceTempView('temp_table_2')

    expensive_listings = session.sql("""SELECT name, id, max_price
        FROM temp_table_2
        LIMIT 10
    """)

    expensive_listings.writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()

    # 3
    grouped_avg_prices = data.groupBy(
        sf.window("processing_time", "10 minutes"),
        "neighbourhood_group", "room_type"
    ).agg(sf.avg("price").alias("avg_price"))
    grouped_avg_prices.createOrReplaceTempView('temp_table_3')

    prices_per_room = session.sql("""SELECT neighbourhood_group, room_type, avg_price
        FROM temp_table_3
    """)

    prices_per_room.writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()


def check_data(data):
    null_data = data.filter(data.price.isNull() & data.minimum_nights.isNull() & data.availability_365.isNull())

    # should be empty
    null_data.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()


spark_session, raw_data = setup_session()
cleaned_data = clean_data(raw_data)
transformed_data = transform_data(cleaned_data)
query_data(transformed_data, spark_session)
check_data(transformed_data)

transformed_data.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()


repartitioned = transformed_data.repartition(5, 'neighbourhood_group')

repartitioned.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

repartitioned.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "file:///C:/Users/Таня/PycharmProjects/spark-test/processed") \
    .option("checkpointLocation", "file:///C:/Users/Таня/PycharmProjects/spark-test/checkpoint") \
    .trigger(processingTime="10 seconds") \
    .start()

spark_session.stop()
