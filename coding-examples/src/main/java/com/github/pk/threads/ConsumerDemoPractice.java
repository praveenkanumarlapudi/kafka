package com.github.pk.threads;


//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoPractice {

    public static void main(String[] args){
        // Properties
        Properties consumerProp = new Properties ();
        consumerProp.setProperty (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProp.setProperty (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProp.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        consumerProp.setProperty (ConsumerConfig.GROUP_ID_CONFIG, "java-consumer");

        //Define Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String> (consumerProp);

        //subscribe
        consumer.subscribe (Arrays.asList ("first_topic"));
        //poll
        while(true){
            ConsumerRecords<String, String> records = consumer.poll (Duration.ofMillis (100));
            for(ConsumerRecord<String, String>record:records){
                System.out.println ("Key :"+ record.key ()+"\t"+
                                       "value :" + record.value ()+"\t" +
                                     "partition :"+ record.partition ());

            }
        }
    }
}
