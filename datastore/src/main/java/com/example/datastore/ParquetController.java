package com.example.datastore;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;


public class ParquetController extends Thread {
    List<Row> Records;

    public ParquetController() {
        Records = new ArrayList<>();
    }

    public ParquetController(List<Row> records) {
        Records = records;
    }

    public void run() {
        WriteToParquet(Records);
        Records.clear();
    }

    public void AddToParquet(StationMessage message) {
        // Create a Row using RowFactory
        Row record = RowFactory.create(
                message.getStation_ID(),
                message.getS_No(),
                message.getBattery_Status(),
                message.getStatus_Timestamp(),
                RowFactory.create(message.getWeather().getHumidity(), message.getWeather().getTemprature(), message.getWeather().getWind_Speed())
        );
        Records.add(record);

        if (Records.size() == 3) {
            List<Row> newlist = Records;
            Records = new ArrayList<>();
            ParquetController parquetwriter = new ParquetController(newlist);
            parquetwriter.start();
        }
    }

    public void WriteToParquet(List<Row> records) {
        SparkSession spark = SparkSession.builder().appName("AppendToParquet").master("local").getOrCreate();
        System.out.println("Hello" + records);
        // Create fields for the struct type
        StructField[] fields = {
                DataTypes.createStructField("Station_ID", DataTypes.IntegerType, true),
                DataTypes.createStructField("S_No", DataTypes.IntegerType, true),
                DataTypes.createStructField("Battery_Status", DataTypes.StringType, true),
                DataTypes.createStructField("Status_Timestamp", DataTypes.LongType, true),
                DataTypes.createStructField("Weather", DataTypes.createStructType(new StructField[]{
                        DataTypes.createStructField("Humidity", DataTypes.IntegerType, true),
                        DataTypes.createStructField("Temperature", DataTypes.IntegerType, true),
                        DataTypes.createStructField("Wind_Speed", DataTypes.IntegerType, true)
                }), true)
        };
        try {
            StructType schema = DataTypes.createStructType(fields);
            Dataset<Row> existingData = spark.createDataFrame(records, schema);

            Path currentDir = Paths.get("").toAbsolutePath();
            Row minMaxRow = existingData.agg(min("Status_Timestamp"), max("Status_Timestamp")).collectAsList().get(0);
            long minVal = minMaxRow.getLong(0);
            System.out.println(minVal);
            existingData = existingData.withColumn("Status_Timestamp_Ranges",
                    functions.when(col("Status_Timestamp").between(minVal, minVal + 1000), "Range1")
                            .when(col("Status_Timestamp").between(minVal + 1001, minVal + 2000), "Range2")
                            .when(col("Status_Timestamp").between(minVal + 2001, minVal + 3000), "Range3")
                            .when(col("Status_Timestamp").between(minVal + 3001, minVal + 4000), "Range4")
                            .when(col("Status_Timestamp").between(minVal + 4001, minVal + 5000), "Range5")
                            .when(col("Status_Timestamp").between(minVal + 5001, minVal + 6000), "Range6")
                            .when(col("Status_Timestamp").between(minVal + 6001, minVal + 7000), "Range7")
                            .when(col("Status_Timestamp").between(minVal + 7001, minVal + 8000), "Range8")
                            .when(col("Status_Timestamp").between(minVal + 8001, minVal + 9000), "Range9")
                            .otherwise("Other"));

            String outputfile = String.valueOf(currentDir) + "/Data";
            System.out.println(outputfile);
            existingData.write().partitionBy("Station_ID", "Status_Timestamp_Ranges").parquet(outputfile);
            System.out.println("Data Written ");
            spark.stop();
        } catch (java.lang.Exception e) {
            System.out.println("Error Occured");
        }
    }

    public void ReadParquet() {
        SparkSession ss = SparkSession.builder().appName("Reader").master("local").getOrCreate();
        String path = String.valueOf(Paths.get("").toAbsolutePath()) + "/Data/Station_ID=1/" + "part-00000-d0f994c2-bd74-4185-a469-d4ef3918d78a.c000.snappy.parquet";
        Dataset<Row> ds = ss.read().parquet(path);

        ds.printSchema();
        ds.show(false);
    }

}