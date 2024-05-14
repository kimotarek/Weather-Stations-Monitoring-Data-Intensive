package org.example.ElasticSearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.cluster.HealthResponse;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.client.RestClient;
import org.example.models.StationMessage;
import org.example.models.Weather;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;

public class ElasticSearchApi  implements Serializable{
private  transient ElasticsearchClient client;


    private ElasticsearchClient getClient() throws IOException {
        if (client == null) {
            client = connectToElasticSearch();
        }
        return client;
    }

    public ElasticsearchClient connectToElasticSearch() throws IOException {


        //Abstract credentials provider that maintains a collection of usercredentials
        BasicCredentialsProvider credsProv = new BasicCredentialsProvider();
        credsProv.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic","NlfGhKDDy=36FpvM*o0l"));

        //Client that connects to an Elasticsearch
        RestClient restClient = RestClient
                .builder(new HttpHost("localhost", 9200, "http"))
                .setHttpClientConfigCallback(hc -> hc
                        .setDefaultCredentialsProvider(credsProv)
                )
                .build();

        // Create the transport and the API client
        //A transport layer that implements Elasticsearch specificities
        ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
        ElasticsearchClient client = new ElasticsearchClient(transport);

        //to check Elastic server health and connection status
        HealthResponse healthResponse = client.cluster().health();
        System.out.printf("Elasticsearch status is: [%s]\n", healthResponse.status());
        return client;
    }

    public void createIndex() throws IOException {
        CreateIndexRequest.Builder createIndexBuilder = new CreateIndexRequest.Builder();
        createIndexBuilder.index("weather_station_index");
        CreateIndexRequest createIndexRequest = createIndexBuilder.build();

        ElasticsearchIndicesClient indices = client.indices();
        CreateIndexResponse createIndexResponse = indices.create(createIndexRequest);

        System.out.println("Index Created Successfully: "+createIndexResponse.acknowledged());
    }

    public void createDocument(StationMessage stationMessage) throws IOException {
 ElasticsearchClient client = getClient();
        IndexRequest.Builder<StationMessage> indexReqBuilder = new IndexRequest.Builder<>();

        indexReqBuilder.index("weather_station_index");
        indexReqBuilder.id(String.valueOf(stationMessage.getStationID()+"_"+stationMessage.getStatusTimestamp()));
        indexReqBuilder.document(stationMessage);
        IndexRequest<StationMessage> indexRequest = indexReqBuilder.build();

        IndexResponse response = client.index(indexRequest);

        System.out.println("Indexed with version " + response.version());
    }
    public void search() throws IOException {
        GetRequest.Builder getRequestBuilder = new GetRequest.Builder();
        getRequestBuilder.index("weather_station_index");
        getRequestBuilder.id("5");

        GetRequest getRequest = getRequestBuilder.build();

        GetResponse<StationMessage> getResponse = client.get(getRequest, StationMessage.class);

          StationMessage stationMessage = getResponse.source();
        System.out.println("Station ID: "+stationMessage.getStationID());
        System.out.println("S No: "+stationMessage.getsNo());
        System.out.println("Battery Status: "+stationMessage.getBatteryStatus());
        System.out.println("Status Timestamp: "+stationMessage.getStatusTimestamp());
        System.out.println("Weather Humidity: "+stationMessage.getWeather().getHumidity());
        System.out.println("Weather Temp: "+stationMessage.getWeather().getTemperature());
        System.out.println("Weather Wind Speed: "+stationMessage.getWeather().getWindSpeed());

    }
    public void ReadParquetAndPutInEL() {
        SparkSession ss = SparkSession.builder().appName("Reader").master("local").getOrCreate();
        String path = String.valueOf(Paths.get("").toAbsolutePath()) + "/Data/Station_ID=1/Status_Timestamp_Ranges=Range1/" + "part-00000-604d6ad0-fac3-4b68-84a1-6b023bd5a998.c000.snappy.parquet";
        Dataset<Row> ds = ss.read().parquet(path);

        // Map columns to fields of StationMessage objects
        ds.foreach(row -> {
            int sNo = row.getAs("S_No");
            String batteryStatus = row.getAs("Battery_Status");
            long statusTimestamp = row.getAs("Status_Timestamp");
            Row weatherRow = row.getAs("Weather");
            int humidity = weatherRow.getAs("Humidity");
            int temperature = weatherRow.getAs("Temperature");
            int windSpeed = weatherRow.getAs("Wind_Speed");
            Weather weather = new Weather(humidity, temperature, windSpeed);
            StationMessage stationMessage = new StationMessage(1, sNo, batteryStatus, statusTimestamp, weather);
            createDocument(stationMessage);

        });

    }
}
