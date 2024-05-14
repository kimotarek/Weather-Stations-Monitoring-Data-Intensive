package org.example;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.ElasticSearch.ElasticSearchApi;
import org.example.Parquet.ParquetController;
import org.example.models.StationMessage;
import org.example.models.Weather;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {
      ElasticSearchApi elasticSearchApi=new ElasticSearchApi();
     elasticSearchApi.ReadParquetAndPutInEL();


    }


}