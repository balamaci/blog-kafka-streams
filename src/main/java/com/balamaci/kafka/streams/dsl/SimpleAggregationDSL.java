package com.balamaci.kafka.streams.dsl;

import com.balamaci.kafka.streams.dsl.mapper.JsonMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

/**
 * @author sbalamaci
 */
public class SimpleAggregationDSL extends BaseDSL {

    public SimpleAggregationDSL(String[] topics) {
        super(topics);
    }

    public KStreamBuilder buildStream() {
        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<String, String> kStream = builder.stream(topics);

        KTable<Long, Integer> kTable =
              kStream
                .mapValues(new JsonMapper())
                .filter((key, jsonValue) -> jsonValue.propertyStringValue("logger_name")
                        .contains("ViewProductEvent"))
                .map((key, jsonValue) -> {
                    Long productId = jsonValue.propertyLongValue("productId");
                    String browserHash = jsonValue.propertyStringValue("browserHash");

                    return new KeyValue<Long, String>(productId, browserHash);
                })
                .groupByKey(Serdes.Long(), Serdes.String())
                    .aggregate(() -> 0, (key, val, agg) -> (agg + 1), Serdes.Integer(), "counterStore");

        kTable.print();

        return builder;
    }
}
