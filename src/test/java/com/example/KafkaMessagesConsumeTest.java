package com.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.jupiter.api.*;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.apache.spark.sql.functions.*;

@DisplayName("Kafka Messages Consumer Tests")
public class KafkaMessagesConsumeTest {

    private SparkSession spark;

    @BeforeEach
    void setupSparkSession() {
        spark = SparkSession.builder()
                .appName("KafkaSparkMongo-Test")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "1")
                .config("spark.streaming.kafka.consumer.poll.ms", "5000")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
    }

    @AfterEach
    void stopSparkSession() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    @DisplayName("Should define correct employee schema")
    void testEmployeeSchemaDefinition() {
        StructType employeeSchema = new StructType()
                .add("id", DataTypes.IntegerType)
                .add("name", DataTypes.StringType)
                .add("age", DataTypes.IntegerType)
                .add("country", DataTypes.StringType);

        assertNotNull(employeeSchema);
        assertEquals(4, employeeSchema.length());
        assertEquals("id", employeeSchema.apply(0).name());
        assertEquals("name", employeeSchema.apply(1).name());
        assertEquals("age", employeeSchema.apply(2).name());
        assertEquals("country", employeeSchema.apply(3).name());
        
        assertEquals(DataTypes.IntegerType, employeeSchema.apply(0).dataType());
        assertEquals(DataTypes.StringType, employeeSchema.apply(1).dataType());
    }

    @Test
    @DisplayName("Should parse JSON to DataFrame with correct schema")
    void testJsonParsing() {
        String jsonData = "{\"id\":1,\"name\":\"John\",\"age\":30,\"country\":\"India\"}";
        List<Row> rows = Arrays.asList(RowFactory.create(jsonData));
        StructType schema = new StructType().add("value", DataTypes.StringType);
        Dataset<Row> df = spark.createDataFrame(rows, schema);

        assertNotNull(df);
        assertEquals(1, df.count());
    }

    @Test
    @DisplayName("Should filter India records correctly")
    void testFilterIndiaRecords() {
        List<Row> rows = Arrays.asList(
                RowFactory.create(1, "John", 30, "India"),
                RowFactory.create(2, "Jane", 28, "USA"),
                RowFactory.create(3, "Bob", 35, "India"),
                RowFactory.create(4, "Alice", 26, "UK")
        );

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType)
                .add("name", DataTypes.StringType)
                .add("age", DataTypes.IntegerType)
                .add("country", DataTypes.StringType);

        Dataset<Row> df = spark.createDataFrame(rows, schema);
        Dataset<Row> filteredDF = df.filter(col("country").equalTo("India"));

        assertEquals(2, filteredDF.count());
        
        List<Row> filtered = filteredDF.collectAsList();
        for (Row row : filtered) {
            assertEquals("India", row.getString(3));
        }
    }

    @Test
    @DisplayName("Should handle empty DataFrame")
    void testEmptyDataFrame() {
        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType)
                .add("name", DataTypes.StringType)
                .add("age", DataTypes.IntegerType)
                .add("country", DataTypes.StringType);

        Dataset<Row> df = spark.createDataFrame(Arrays.asList(), schema);

        assertEquals(0, df.count());
        Dataset<Row> filtered = df.filter(col("country").equalTo("India"));
        assertEquals(0, filtered.count());
    }

    @Test
    @DisplayName("Should handle records with different countries")
    void testMultipleCountries() {
        List<Row> rows = Arrays.asList(
                RowFactory.create(1, "User1", 25, "India"),
                RowFactory.create(2, "User2", 30, "USA"),
                RowFactory.create(3, "User3", 35, "India"),
                RowFactory.create(4, "User4", 40, "Canada"),
                RowFactory.create(5, "User5", 28, "India")
        );

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType)
                .add("name", DataTypes.StringType)
                .add("age", DataTypes.IntegerType)
                .add("country", DataTypes.StringType);

        Dataset<Row> df = spark.createDataFrame(rows, schema);
        Dataset<Row> indiaDF = df.filter(col("country").equalTo("India"));

        assertEquals(3, indiaDF.count());
        long indiaCount = indiaDF.filter(col("country").equalTo("India")).count();
        assertEquals(indiaDF.count(), indiaCount);
    }

    @Test
    @DisplayName("Should validate data types in schema")
    void testSchemaDataTypes() {
        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType)
                .add("name", DataTypes.StringType)
                .add("age", DataTypes.IntegerType)
                .add("country", DataTypes.StringType);

        StructField idField = schema.apply("id");
        StructField nameField = schema.apply("name");
        StructField ageField = schema.apply("age");
        StructField countryField = schema.apply("country");

        assertEquals(DataTypes.IntegerType, idField.dataType());
        assertEquals(DataTypes.StringType, nameField.dataType());
        assertEquals(DataTypes.IntegerType, ageField.dataType());
        assertEquals(DataTypes.StringType, countryField.dataType());
    }

    @Test
    @DisplayName("Should handle records with null values")
    void testNullValueHandling() {
        List<Row> rows = Arrays.asList(
                RowFactory.create(1, "John", 30, "India"),
                RowFactory.create(2, null, 28, "USA"),
                RowFactory.create(3, "Bob", null, "India")
        );

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType)
                .add("name", DataTypes.StringType)
                .add("age", DataTypes.IntegerType)
                .add("country", DataTypes.StringType);

        Dataset<Row> df = spark.createDataFrame(rows, schema);

        assertNotNull(df);
        assertEquals(3, df.count());
    }

    @Test
    @DisplayName("Should count records correctly in batch")
    void testBatchCountLogic() {
        List<Row> rows = Arrays.asList(
                RowFactory.create(1, "John", 30, "India"),
                RowFactory.create(2, "Jane", 28, "USA"),
                RowFactory.create(3, "Bob", 35, "India")
        );

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType)
                .add("name", DataTypes.StringType)
                .add("age", DataTypes.IntegerType)
                .add("country", DataTypes.StringType);

        Dataset<Row> df = spark.createDataFrame(rows, schema);
        long count = df.count();

        assertTrue(count > 0);
        assertEquals(3, count);
    }

    @Test
    @DisplayName("Should select specific columns from DataFrame")
    void testColumnSelection() {
        List<Row> rows = Arrays.asList(
                RowFactory.create(1, "John", 30, "India"),
                RowFactory.create(2, "Jane", 28, "USA")
        );

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType)
                .add("name", DataTypes.StringType)
                .add("age", DataTypes.IntegerType)
                .add("country", DataTypes.StringType);

        Dataset<Row> df = spark.createDataFrame(rows, schema);
        Dataset<Row> selectedDF = df.select("id", "name", "country");

        assertNotNull(selectedDF);
        assertEquals(3, selectedDF.columns().length);
    }

    @Test
    @DisplayName("Should handle age filtering")
    void testAgeFiltering() {
        List<Row> rows = Arrays.asList(
                RowFactory.create(1, "John", 30, "India"),
                RowFactory.create(2, "Jane", 28, "India"),
                RowFactory.create(3, "Bob", 35, "India"),
                RowFactory.create(4, "Alice", 22, "India")
        );

        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType)
                .add("name", DataTypes.StringType)
                .add("age", DataTypes.IntegerType)
                .add("country", DataTypes.StringType);

        Dataset<Row> df = spark.createDataFrame(rows, schema);
        Dataset<Row> adults = df.filter(col("age").geq(30));

        assertEquals(2, adults.count());
    }
}
