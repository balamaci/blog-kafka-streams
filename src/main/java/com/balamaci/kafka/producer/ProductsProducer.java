package com.balamaci.kafka.producer;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

/**
 * @author sbalamaci
 */
public class ProductsProducer {

    private static final Config config = ConfigFactory.load("app");

    private static final Logger log = LoggerFactory.getLogger(ProductsProducer.class);

    private static final String PRODUCTS_TOPIC = "products";
    private static final int NR_PRODUCTS = 100;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrap.servers"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<Long, String> producer = new KafkaProducer<>(props);

        try {
            for(long i = 1; i <= NR_PRODUCTS; i++) {
                Future<RecordMetadata> responseFuture = producer.
                        send(new ProducerRecord<>(PRODUCTS_TOPIC, i, UUID.randomUUID().toString()));
                responseFuture.get();
            }
        } catch (Exception e) {
            log.error("Exception encountered", e);
        }
    }

}