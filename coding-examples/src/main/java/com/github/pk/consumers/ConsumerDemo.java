package com.github.pk.consumers;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String[] args){

        //Logger
        //Logger logger = LoggerFactory.getLogger (ConsumerDemo.class.getName());
        Properties prop = new Properties ();
        //Consumer Properties
         prop.setProperty (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
         prop.setProperty (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
         prop.setProperty (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
         prop.setProperty (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
         prop.setProperty (ConsumerConfig.GROUP_ID_CONFIG, "Java-Consumer-with-parts");

        //Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String> (prop);

        //Subscribe consumer
        consumer.subscribe (Arrays.asList("topic_partitions_3"));

        //Poll Consumer
        while(true){
            ConsumerRecords<String, String> records= consumer.poll (Duration.ofMillis(100 ));

            for (ConsumerRecord<String, String> record: records){
                System.out.println("key :"+ record.key ()+"\\t"+"value :"+record.value ()+"\\t"+"Partition :"+ record.partition () );

            }

        }

    }
}
