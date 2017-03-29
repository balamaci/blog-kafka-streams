package com.balamaci.kafka.streams;

import com.balamaci.kafka.streams.dsl.CountWithWindowDSL;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author sbalamaci
 */
public class StreamsStart {

    private static final Logger log = LoggerFactory.getLogger(StreamsStart.class);

    private static final Config config = ConfigFactory.load("app");

    public static void main(String[] args) {
        Properties streamsConfiguration = streamConfig();


        String[] topics = config.getString("kafka.topics").split(",");
//        KStreamBuilder streamsBuilder = new SimpleAggregationDSL(topics).buildStream();
//        KStreamBuilder streamsBuilder = new KTablesJoinedDSL(topics).buildStream();
//        KStreamBuilder streamsBuilder = new SimpleJoinTableDSL(topics).buildStream();
        KStreamBuilder streamsBuilder = new CountWithWindowDSL(topics).buildStream();


        KafkaStreams streams = new KafkaStreams(streamsBuilder, streamsConfiguration);

        streams.cleanUp();
        streams.start();

//        printTopology(streams);

//         Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static void printTopology(KafkaStreams streams) {
        Executors.newScheduledThreadPool(1).schedule(() -> {
            log.info("Topology ********* \n {}", streams.toString());
        }, 10, TimeUnit.SECONDS);
    }

    private static Properties streamConfig() {
        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getString("kafka.app.id"));
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                config.getString("kafka.bootstrap.servers"));

        String uuid = UUID.randomUUID().toString();
        String stateDir = "/tmp/" + uuid;
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);

        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Set the commit interval to 500ms so that any changes are flushed frequently. The low latency
        // would be important for anomaly detection.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000);

        //what to do when there is no offset data in Kafka brokers
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return streamsConfiguration;
    }

}
