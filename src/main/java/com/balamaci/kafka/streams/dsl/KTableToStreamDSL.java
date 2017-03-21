package com.balamaci.kafka.streams.dsl;

import com.balamaci.kafka.streams.dsl.mapper.JsonMapper;
import com.balamaci.kafka.util.TimeUtil;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.TimeWindows;

/**
 * @author sbalamaci
 */
public class KTableToStreamDSL extends BaseDSL {


    public KTableToStreamDSL(String[] topics) {
        super(topics);
    }

    @Override
    public KStreamBuilder buildStream() {
        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<String, String> kStream = builder.stream(topics);

        kStream
                .mapValues(new JsonMapper())
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
                    log.info("EvId={} - ProductId={} - Browser={}", eventId, productId, browserHash);
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
                .count(TimeWindows.of(15 * 1000L),"browserHashStore")
                .foreach((windowKey, value) ->
                        log.info("[{}-{}] --- Got Product_{}={} times",
                                TimeUtil.millisFormat(windowKey.window().start()),
                                TimeUtil.millisFormat(windowKey.window().end()), windowKey.key(), value)
                );

        return builder;
    }

}
