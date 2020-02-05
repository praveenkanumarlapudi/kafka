package com.github.pk.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 Steps -
    1. Create Twitter Client
        - Followed https://github.com/twitter/hbc example for Kafka-Twitter Connect
    2. Create Kafka Producer
        a. Create Properties
        b. Create Producer
        C. Create Producer Record
        d. send ProducerRecord -- (Possibly looping)
    3. Send records to Kafka

  */

public class TwitterProducer {
    public static void main(String[] args){
        new TwitterProducer ().run ();


    }
    public void run(){

        // 1.Create Twitter Client
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String> (1000);
        //creating Twitter Client
        // Attempts to establish a connection.
        Client hosebirdClient = createTwitterClient (msgQueue);
        hosebirdClient.connect();
        //2. Create Kafka Producer
        KafkaProducer<String, String> producer = createKafkaProducer();


        // Send messages to Kafka topic OR to Console
        while (!hosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll (5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace ( );
                hosebirdClient.stop ();
            } if(msg != null) {
                System.out.println (msg);
                producer.send (new ProducerRecord<String, String> ("Twitter_Tweets", null, msg), new Callback ( ) {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e != null){
                           e.printStackTrace ();
                        }
                    }
                });
            }
        }

    }
    public Client createTwitterClient(BlockingQueue<String> msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts (Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        //List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("kafka");
        //hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1 ("consumerKey", "consumerSecret", "token", "secret");
        /**
         *  Creating the client
         */
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor (msgQueue));
                                                                        // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;

    }

    public KafkaProducer createKafkaProducer(){
        // a. creating producer properties
        Properties props = new Properties ();
        props.setProperty (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName ());
        props.setProperty (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName ());
        props.setProperty (ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        return new KafkaProducer<String, String> (props);


    }
}
