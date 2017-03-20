package com.balamaci.kafka;

import com.balamaci.kafka.util.Json;
import com.balamaci.kafka.util.TimeUtil;
import com.google.gson.JsonParser;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.UUID;

/**
 * @author sbalamaci
 */
public class StreamsStart {

    private static final Logger log = LoggerFactory.getLogger(StreamsStart.class);

    private static final Config config = ConfigFactory.load("app");

    public static void main(String[] args) {
        Properties streamsConfiguration = streamConfig();

        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<String, String> events = builder.stream(config.getString("kafka.topics"));

        events
                .mapValues(new ValueMapper<String, Json>() {
                    private JsonParser jsonParser = new JsonParser();

                    @Override
                    public Json apply(String value) {
                        Json json = new Json(jsonParser.parse(value).getAsJsonObject());
                        log.info("Processing {}", json.propertyLongValue("eventId"));
                        return json;
                    }
                })
                .filter((key, jsonValue) -> jsonValue.propertyStringValue("logger_name")
                        .contains("ViewProductEvent"))
                .map((key, jsonValue) -> {
                    Long eventId = jsonValue.propertyLongValue("eventId");
                    String productId = jsonValue.propertyStringValue("productId");
                    String browserHash = jsonValue.propertyStringValue("browserHash");
/*
                    if(eventId == 20) {
                        log.info("Encountered event with number 20, dieing unexpectedly");
                        System.exit(1);
                        throw new RuntimeException("Exception");
                    }
*/
                    log.info("{} - ProductId={} - Browser={}", eventId, productId, browserHash);
                    return new KeyValue<>(productId, browserHash);
                })
                .map((product, browser) -> new KeyValue<>(product + "#" + browser, product + "#" + browser))
                .groupByKey()
                .reduce((val1, val2) -> {
                            log.info("Found a duplicate and ignored {}", val1);
                            return val1;
                        },
                        TimeWindows.of(5 * 1000L), "uniques")
                .toStream()
                .map((kWindow, v) -> {
                    String[] keyValue = v.split("#");
                    log.info("New Value {}-{}", keyValue[0], keyValue[1]);

                    return KeyValue.pair(keyValue[0], keyValue[1]);
                })
                .groupByKey()
                .count(TimeWindows.of(15 * 1000L), "browserHashStore")
                .foreach((window, value) ->
                        log.info("[{}-{}] --- Got Product_{}={} times",
                                TimeUtil.millisFormat(window.window().start()),
                                TimeUtil.millisFormat(window.window().end()), window.key(), value)
                );



        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.cleanUp();

        streams.start();

//         Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
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

        //what to do
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return streamsConfiguration;
    }

}
