package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.*;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class KafkaMessagesConsume {

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {

        // 1. Create Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("KafkaSparkMongo")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // 2. Define Kafka JSON Schema
        StructType employeeSchema = new StructType()
                .add("id", DataTypes.IntegerType)
                .add("name", DataTypes.StringType)
                .add("age", DataTypes.IntegerType)
                .add("country", DataTypes.StringType);

        // 3. Read Kafka Stream
        Dataset<Row> kafkaDF = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "Emp")
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST(value AS STRING) as json");

        // 4. Parse JSON
        Dataset<Row> parsedDF = kafkaDF
                .select(from_json(col("json"), employeeSchema).as("data"))
                .select("data.*");

        // 5. Filter India records and write to MongoDB using foreachBatch
        StreamingQuery mongoQuery = parsedDF
                .filter(col("country").equalTo("India"))
                .writeStream()
                .foreachBatch((batchDF, batchId) -> {

                    System.out.println("\n==============================");
                    System.out.println("Batch Triggered: " + batchId);
                    System.out.println("==============================");

                    long count = batchDF.count();
                    System.out.println("Batch size: " + count);

                    if (count > 0) {

                        System.out.println("Sample Data:");
                        batchDF.show(false);

                        try {
                            System.out.println("Writing to MongoDB...");

                            batchDF.write()
                                    .format("mongodb")
                                    .option("spark.mongodb.connection.uri", "mongodb://localhost:27017")
                                    .option("spark.mongodb.database", "adarsh")
                                    .option("spark.mongodb.collection", "india")
                                    .mode("append")
                                    .save();

                            System.out.println("Write SUCCESS for batch: " + batchId);

                        } catch (Exception e) {
                            System.out.println("ERROR writing to MongoDB for batch: " + batchId);
                            e.printStackTrace();
                        }

                    } else {
                        System.out.println("Empty batch: " + batchId);
                    }

                    System.out.println("==============================\n");
                })
                .option("checkpointLocation", "/tmp/spark-checkpoints")
                .start();

        mongoQuery.awaitTermination();
    }
}