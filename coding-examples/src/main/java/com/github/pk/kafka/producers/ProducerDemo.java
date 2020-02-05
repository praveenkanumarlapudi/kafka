package com.github.pk.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args){
        //define Producer Properties
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //define producer
        KafkaProducer<String, String> producer = new KafkaProducer<String,String>(prop);
        //define producer record
        ProducerRecord<String, String> pr = new ProducerRecord<String, String>("first_topic", "Hi This Message is from Java");
        //Send data
        producer.send(pr);
        producer.flush();
        producer.close();

    }
}
