package com.balamaci.kafka.streams;

import com.balamaci.kafka.util.Json;
import com.google.gson.JsonParser;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * An example of Low Level processing API
 *
 * @author sbalamaci
 */
public class StartLowLevelProcessor {

    private static final Logger log = LoggerFactory.getLogger(StreamsStart.class);

    private static final Config config = ConfigFactory.load("app");

    private static final String COUNT_STORE = "Counts";

    public static void main(String[] args) {
        Properties streamsConfiguration = streamConfig();

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        StateStoreSupplier countStore = Stores.create(COUNT_STORE)
                .withKeys(Serdes.Long())
                .withValues(Serdes.Long())
                .persistent()
                .build();

        topologyBuilder.addSource("SOURCE", config.getString("kafka.topics"));

        topologyBuilder.addProcessor("JSON_PRODUCT_EXTRACTOR", JsonProductIdExtractor::new, "SOURCE");
        topologyBuilder
                .addProcessor("PRODUCT_COUNTER", MyProductCountProcessor::new, "JSON_PRODUCT_EXTRACTOR")
                .addStateStore(countStore, "PRODUCT_COUNTER");

        //write to another Kafka topic "counter_topic"
        topologyBuilder.addSink("SINK_COUNTER", "counter_topic",
                Serdes.Long().serializer(), Serdes.Long().serializer(), "PRODUCT_COUNTER");

        //a sub topology, another Kafka consumer
        topologyBuilder.addSource("SOURCE_COUNTER", Serdes.Long().deserializer(), Serdes.Long().deserializer(),
                "counter_topic");
        topologyBuilder.addProcessor("PRINT", PrintProcessor::new, "SOURCE_COUNTER");



        KafkaStreams streams = new KafkaStreams(topologyBuilder, streamsConfiguration);
        streams.cleanUp();
        streams.start();

//        registerPeriodicStoreQuery(streams);
    }

    private static class PrintProcessor extends AbstractProcessor<Long, Long> {

        @Override
        public void process(Long key, Long value) {
            log.info("PRINT {}-{}", key, value);
        }
    }

    private static class JsonProductIdExtractor extends AbstractProcessor<String, String> {
        private ProcessorContext context;
        private JsonParser jsonParser = new JsonParser();

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }

        @Override
        public void process(String key, String value) {
            Json json = new Json(jsonParser.parse(value).getAsJsonObject());
            Long productId = json.propertyLongValue("productId");
            String browserHash = json.propertyStringValue("browserHash");

            String loggerName = json.propertyStringValue("logger_name");
            boolean isViewProductEvent = loggerName.contains("ViewProductEvent");
            if(isViewProductEvent) {
                log.info("JsonProductExtractor sending downstream {}", productId);
                context.forward(productId, browserHash);
            }
        }
    }

    private static class MyProductCountProcessor extends AbstractProcessor<Long, String> {
        private ProcessorContext context;

        //
        private KeyValueStore<Long, Long> kvStore;

        @Override
        public void init(ProcessorContext context) {
            // keep the processor context locally because we need it in punctuate() and commit()
            this.context = context;

            // call this processor's punctuate() method every 5000 milliseconds.
            // as a way to do something periodically
            this.context.schedule(5000);

            // retrieve the key-value store named "Counts"
            this.kvStore = (KeyValueStore<Long, Long>) context.getStateStore(COUNT_STORE);
        }

        @Override
        public void process(Long productId, String browser) {
            Long oldValue = this.kvStore.get(productId);

            if (oldValue == null) {
                this.kvStore.put(productId, 1L);
            } else {
                this.kvStore.put(productId, oldValue + 1L);
            }
        }

        //This method will be called as a way to do something periodically
        @Override
        public void punctuate(long timestamp) {
            KeyValueIterator<Long, Long> iter = this.kvStore.all();

            log.info("Sending downstream each counted value");
            while (iter.hasNext()) {
                KeyValue<Long, Long> entry = iter.next();

                //send downstream
                context.forward(entry.key, entry.value);
            }

            iter.close();

            log.info("Committing context");
            // commit the current processing progress
            context.commit();
        }
    }


    private static Properties streamConfig() {
        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, config.getString("kafka.app.id"));
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                config.getString("kafka.bootstrap.servers"));

        /* We use dynamic directory files for Store as we plan to start multiple instances on the same machine */
        String uuid = UUID.randomUUID().toString();
        String stateDir = "/tmp/" + uuid;
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);

        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Set the commit interval to 500ms so that any changes are flushed frequently. The low latency
        // would be important for anomaly detection.
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000);

        //what to do
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return streamsConfiguration;
    }

    /**
     * Simulate an external thread reading from the KV state store
     *
     * @param streams KafkaStreams
     */
    private static void registerPeriodicStoreQuery(KafkaStreams streams) {
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
            try {
                final ReadOnlyKeyValueStore<Long, Long> countsStore = streams
                        .store(COUNT_STORE, QueryableStoreTypes.keyValueStore());


                //A state store will not contain all the data other processors are
                //this is a way to find out where are.
                Collection<StreamsMetadata> storeMetadata = streams.allMetadataForStore(COUNT_STORE);

                KeyValueIterator<Long, Long> keyValueIterator = countsStore.all();
                while (keyValueIterator.hasNext()) {
                    KeyValue<Long, Long> entry = keyValueIterator.next();
                    log.info("Store {}-{}", entry.key, entry.value);
                }
            } catch (Exception e) {
                log.error("Periodic scheduler encountered Exception", e);
            }

        }, 0, 5, TimeUnit.SECONDS);
    }

}
