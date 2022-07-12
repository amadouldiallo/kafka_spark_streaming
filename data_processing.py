# PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
# Time
import time


KAFKA_TOPIC_NAME = "orderstopicdemo"  # topic name
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"
CUSTOMER_FILE = "data/customers.csv"

# MYSQL
mysql_host_name = "localhost"
mysql_port_no = "3306"
mysql_database_name = "sales_db"  # database
mysql_driver_class = "com.mysql.cj.jdbc.Driver"
mysql_table_name = "total_sales_by_source_state" # table
mysql_user = "root"
mysql_password = 'PASSWORD'  # your password
mysql_jdbc_url = "jdbc:mysql://"+mysql_host_name+":"+mysql_port_no+"/"+mysql_database_name

# Cassandra

cassandra_host_name= "localhost"
cassandra_port_no = "9042"
cassandra_keyspace_name = "sales_ks"
cassandra_table_name = "orders"


def save_to_cassandra(current_df, epoch_id):
    print(f"Print epoch id :{epoch_id}")
    print(f"Print BEFORE cassandra table save : {str(epoch_id)}")
    current_df\
        .write\
        .format("org.apache.spark.sql.cassandra")\
        .mode("append")\
        .options(table=cassandra_table_name, keyspace=cassandra_keyspace_name, )\
        .save()
    print(f"Print AFTER cassandra table save : {str(epoch_id)}")


def save_to_mysql(current_df, epoch_id):
    db_credentials = {
        "user": mysql_user,
        "password": mysql_password,
        "driver": mysql_driver_class
    }
    print(f"Print epoch_id: {str(epoch_id)}")
    processed_at = time.strftime("%Y-%m-%d %H:%M:%S")

    current_df_final = current_df\
        .withColumn("processed_at",lit(processed_at))\
        .withColumn("batch_id", lit(epoch_id))
    print(f"Printing BEFORE MySql table save : {str(epoch_id)}")
    current_df_final.write.jdbc(url=mysql_jdbc_url,
                                table=mysql_table_name,
                                mode="append",
                                properties=db_credentials)

    print(f"Printing AFTER MySql table save : {str(epoch_id)}")


if __name__ == '__main__':
    print("Welcome to Data Enginner")
    print("Data processing started....")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))
    jars = ["mysql:mysql-connector-java:8.0.29",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0",
            "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0"]
    # create spark Session
    spark = SparkSession\
        .builder\
        .appName("Pyspark Kafka Cassandra")\
        .master("local[*]") \
        .config("spark.jars.packages", ",".join(jars)) \
        .config("spark.cassandra.connection.host", cassandra_host_name)\
        .config("spark.cassandra.connection.port", cassandra_port_no)\
        .getOrCreate()

    print('Change log level')
    spark.sparkContext.setLogLevel("ERROR")
    # get and stream data from kafka topics
    # in your console , you must run kafka producer and feed data in to topic
    orders_df = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)\
        .option('subscribe', KAFKA_TOPIC_NAME)\
        .option('startingOffsets', 'latest')\
        .load()

    orders_df1 = orders_df.selectExpr("CAST(value AS STRING)", "timestamp")
    orders_df1.printSchema()
    print("Printing of Schema of ordered_df :")
    orders_df.printSchema()

    orders_schema = StructType()\
        .add('order_id', StringType())\
        .add("created_at", StringType())\
        .add("discount", StringType())\
        .add("product_id", StringType())\
        .add("quantity", StringType())\
        .add("subtotal",StringType())\
        .add("tax", StringType())\
        .add("total", StringType())\
        .add("customer_id", StringType())

    orders_df2 = orders_df1\
        .select(from_json(col("value"), orders_schema).alias("orders"), "timestamp")

    orders_df3 = orders_df2.select("orders.*","timestamp")
    orders_df3\
        .writeStream\
        .trigger(processingTime="15 seconds")\
        .outputMode("update")\
        .foreachBatch(save_to_cassandra)\
        .start()
    print("df3: ")
    orders_df3.printSchema()
    customers_df = spark.read.csv(CUSTOMER_FILE, header=True, inferSchema=True)
    print(customers_df.printSchema())
    customers_df.show(10)

    #merge
    orders_df4 = orders_df3.join(customers_df, orders_df3.customer_id == customers_df.customer_id, how='inner')

    print("Printing Schema of orders df4")

    orders_df4.printSchema()

    ## aggregate amount by source
    orders_df5 = orders_df4.groupBy("source", "state")\
        .agg({"total":'sum'}).select("source", "state", col("sum(total)").alias('total_sum_amount'))

    print('Print schema of ordered df5')
    orders_df5.printSchema()

    trans_detail_write_stream = orders_df5\
        .writeStream\
        .trigger(processingTime='15 seconds')\
        .outputMode('update')\
        .option('truncate', 'false')\
        .format('console')\
        .start()

    orders_df5 \
        .writeStream \
        .trigger(processingTime='15 seconds') \
        .outputMode('update') \
        .foreachBatch(save_to_mysql)\
        .start()
    trans_detail_write_stream.awaitTermination()
    print("Pyspark Kafka Complete")
    spark.stop() # stop spark session..
