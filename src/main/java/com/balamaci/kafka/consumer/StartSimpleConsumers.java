package com.balamaci.kafka.consumer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author sbalamaci
 */
public class StartSimpleConsumers {

    private static final Config config = ConfigFactory.load("app");

    private static final String CONSUMER_GROUP_ID = "consumer-logs-processing";

    public static void main(String[] args) {
        int numConsumers = 3;

        List<String> topics = Arrays.asList(config.getString("kafka.topics"));
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);


        final List<SimpleConsumer> consumers = new ArrayList<>();
        for (int i = 1; i <= numConsumers; i++) {
            SimpleConsumer consumer = new SimpleConsumer(i, topics, properties(CONSUMER_GROUP_ID));
            consumers.add(consumer);

            executor.submit(consumer);
        }

        //we simulate consumer 1 to die after 7 processed records
        //to see standby consumers pick up from him
        consumers.get(0).setConsumerDieAfterRecords(7L);

        registerShutdownHook(executor, consumers);
    }

    private static void registerShutdownHook(ExecutorService executor, List<SimpleConsumer> consumers) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (SimpleConsumer consumer : consumers) {
                    consumer.shutdown();
                }
                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private static Properties properties(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrap.servers"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        //
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5");

        return props;
    }

}
