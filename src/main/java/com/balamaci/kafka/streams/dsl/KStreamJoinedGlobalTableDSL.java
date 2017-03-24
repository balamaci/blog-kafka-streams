package com.balamaci.kafka.streams.dsl;

import com.balamaci.kafka.streams.dsl.mapper.JsonMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

/**
 * Joining a KStream to a GlobalKTable
 *
 * @author sbalamaci
 */
public class KStreamJoinedGlobalTableDSL extends BaseDSL {

    public KStreamJoinedGlobalTableDSL(String[] topics) {
        super(topics);
    }

    @Override
    public KStreamBuilder buildStream() {
        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<String, String> kStream = builder.stream(topics);

        KTable<String, Long> browserActions = kStream.mapValues(new JsonMapper())
                .filter((key, jsonValue) -> jsonValue.propertyStringValue("logger_name")
                        .contains("ViewProductEvent"))
                .map((key, jsonValue) -> {
                    Long productId = jsonValue.propertyLongValue("productId");
                    String browserHash = jsonValue.propertyStringValue("browserHash");

                    return new KeyValue<String, Long>(browserHash, productId);
                })
                .groupByKey(Serdes.String(), Serdes.Long())
                .count("counterStore");

        kStream.mapValues(new JsonMapper())
                .filter((key, jsonValue) -> jsonValue.propertyStringValue("logger_name")
                        .contains("BrowserCaptchaVerified"))
                .map((key, jsonValue) -> {
                    String browserHash = jsonValue.propertyStringValue("browserHash");

                    return new KeyValue<String, Integer>(browserHash, 1);
                })
                .to(Serdes.String(), Serdes.Integer(),"captchaStream");


        GlobalKTable<String, Integer> captchaTable = builder.globalTable(Serdes.String(), Serdes.Integer(),
                "captchaStream",
                "captchaStreamStore");

        browserActions
                .toStream()
                .leftJoin(captchaTable,
                        (browserKey, streamValue) -> browserKey,
                        (actionsCount, captchaVal) -> {
                            if(captchaVal == null) {
                                return actionsCount;
                            }
                            return 0L;
                        })
                .filter((browser, count) -> count >= 5)
                .groupByKey(Serdes.String(), Serdes.Long())
                .reduce((val1, val2) -> val1, "captchaAskedFor")
                .toStream()
                .foreach((k, v) -> log.info("Asking captcha for {}", k));

        return builder;
    }
}
