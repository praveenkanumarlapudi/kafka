package com.github.pk.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import sun.jvm.hotspot.jdi.IntegerTypeImpl;

import java.util.Properties;

public class ProducerWithKeyPartitions {

    public static void main(String[] args){
        //define Producer Properties
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //define producer
        KafkaProducer<String, String> producer = new KafkaProducer<String,String>(prop);
        for(int i=10; i<20 ;i++) {
            String key = "Key_"+ Integer.toString (i);
            String value = "Hello World_"+Integer.toString(i);
            //define producer record
            ProducerRecord<String, String> pr = new ProducerRecord<String, String> ("topic_partitions_3",key, value);
            System.out.println("Published a record with value_"+ i);
            //Send data
            producer.send (pr);
        }
        producer.flush ( );
        producer.close ( );
    }
}
