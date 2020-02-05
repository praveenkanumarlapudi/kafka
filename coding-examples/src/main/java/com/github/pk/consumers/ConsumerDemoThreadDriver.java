package com.github.pk.consumers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.ExecutorService.*;

public class ConsumerDemoThreadDriver {

    public static void main(String[] args){
        int threadCount = 3;
        List<String> topic = Arrays.asList("topic_partitions_3");
        String groupId = "Java-Consumer-with-MultiThread";
        ExecutorService executor = Executors.newFixedThreadPool (threadCount);

        final List<ConsumerDemoWithThreadPool> consumers = new ArrayList<> ();
        for(int i = 0; i< threadCount; i++){
            ConsumerDemoWithThreadPool consumer = new ConsumerDemoWithThreadPool (groupId, i, topic );
            consumers.add (consumer);
            executor.submit (consumer);
        }
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (ConsumerDemoWithThreadPool consumer : consumers) {
                    consumer.shutDown ();
                }
                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace ();
                }
            }
        });


    }
}
