package com.github.pk.elastic;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PutElasticDemo {

    public static RestHighLevelClient CreateElasticRestClient(){

        String hostname = "kafka-twitter-poc-556813528.us-east-1.bonsaisearch.net"; // localhost or bonsai url
        String username = "1pvp1ze414"; // needed only for bonsai
        String password = "j0c81ge7nw"; // needed only for bonsai

        // credentials provider help supply username and password
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider ();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials (username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost (hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;

    }

    public static void main(String[] args) {

        //Logger logger = LoggerFactory.getLogger (PutElasticDemo.class.getName ());

        // Create elastic client
        RestHighLevelClient client = CreateElasticRestClient();
        String SampleRecord = "{\"name \": \"FOO\",\"id# \": \"BAR\"}";
        // Push Sample record to Elastic Search
         //a. create the REST request
        IndexRequest indexRequest = new IndexRequest ("twitter","tweets").source(SampleRecord, XContentType.JSON);
        //b. send the request to ES
        try {
            IndexResponse indexResponse = client.index (indexRequest, RequestOptions.DEFAULT);
            String id = indexResponse.getId ();
            System.out.println ("ID Written to ES : " + id);
            client.close();
        } catch (IOException e) {
            e.printStackTrace ( );
        }




    }
}
