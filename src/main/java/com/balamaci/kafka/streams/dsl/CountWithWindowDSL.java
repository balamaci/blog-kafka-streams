package com.balamaci.kafka.streams.dsl;

import com.balamaci.kafka.streams.dsl.mapper.JsonMapper;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import static com.balamaci.kafka.util.TimeUtil.millisFormat;

/**
 * @author sbalamaci
 */
public class CountWithWindowDSL extends BaseDSL {

    public CountWithWindowDSL(String[] topics) {
        super(topics);
    }

    @Override
    public KStreamBuilder buildStream() {
        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<String, String> kStream = builder.stream(topics);

        KTable<Windowed<String>, Long> browserActions = kStream.mapValues(new JsonMapper())
                .filter((key, jsonValue) -> jsonValue.propertyStringValue("logger_name")
                        .contains("ViewProductEvent"))
                .map((key, jsonValue) -> {
                    String productId = jsonValue.propertyStringValue("productId");
                    String browserHash = jsonValue.propertyStringValue("browserHash");

                    return new KeyValue<String, String>(browserHash, productId);
                })
                .groupByKey()
                .count(TimeWindows.of(5 * 1000L).advanceBy(2 * 1000L), "counterStore");

        browserActions
                .toStream()
                .foreach((windowKey, count) -> {
                    long windowStart = windowKey.window().start();
                    long windowEnd = windowKey.window().end();
                    log.info("Window [{}-{}] Browser:{} count={}", millisFormat(windowStart),
                            millisFormat(windowEnd), windowKey.key(), count);
                });
        return builder;
    }
}
