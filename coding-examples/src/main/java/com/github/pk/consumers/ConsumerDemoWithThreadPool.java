package com.github.pk.consumers;

//follow the example from
// https://www.confluent.io/blog/tutorial-getting-started-with-the-new-apache-kafka-0-9-consumer-client/

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

public class ConsumerDemoWithThreadPool implements Runnable{

    //create topic
    private final KafkaConsumer consumer;
    private int id;
    private List<String> topic;


    public ConsumerDemoWithThreadPool(String groupId,
                                      int id,
                                      List<String> topic) {
        this. id = id;
        this.topic = topic;
        Properties props = new Properties ();
        props.setProperty (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty (ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092" );
        this.consumer = new KafkaConsumer<String, String> (props);
    }


    @Override
    public void run() {
     // Subscribing and polling goes here
        try {
            consumer.subscribe (topic);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll (Long.MAX_VALUE);
                for (ConsumerRecord<String, String> record : records) {
                    Map<String, Object> data = new HashMap ( );
                    data.put ("Partition", record.partition ( ));
                    data.put ("Key", record.key ( ));
                    data.put ("value", record.value ( ));
                    data.put ("offset", record.offset ( ));
                }
            }
        }catch(WakeupException e){
            //ignore for shutdown

        }finally{
            consumer.close ();
        }
    }
    public void shutDown(){
        consumer.wakeup ();
    }
}
