package com.balamaci.kafka.streams.dsl;

import com.balamaci.kafka.streams.dsl.mapper.JsonMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

/**
 * @author sbalamaci
 */
public class SimpleJoinTableDSL extends BaseDSL {


    public SimpleJoinTableDSL(String[] topics) {
        super(topics);
    }

    @Override
    public KStreamBuilder buildStream() {
        final KStreamBuilder builder = new KStreamBuilder();

        final GlobalKTable<Long, String> kProductsTable = builder.globalTable(Serdes.Long(), Serdes.String(),
                "products", "productsStore");

        final KStream<String, String> kStream = builder.stream(topics);

            kStream
                        .mapValues(new JsonMapper())
                        .filter((key, jsonValue) -> jsonValue.propertyStringValue("logger_name")
                                .contains("ViewProductEvent"))
                        .map((key, jsonValue) -> {
                            Long eventId = jsonValue.propertyLongValue("eventId");
                            Long productId = jsonValue.propertyLongValue("productId");
                            String browserHash = jsonValue.propertyStringValue("browserHash");

                            return new KeyValue<Long, String>(productId, browserHash);
                        })
                        .groupByKey(Serdes.Long(), Serdes.String())
                        .aggregate(() -> 0, (key, val, agg) -> (agg + 1), Serdes.Integer(), "counterStore")
                .toStream()
                        .leftJoin(kProductsTable, (key, value) -> key,
                                (val, globalVal) -> "Product:" + globalVal + "=" + val)
                        .print(Serdes.Long(), Serdes.String());



//        KTable<Long, String> kProductNamesView = kProductsViewTable.join(kProductsTable, (name, count) -> name + "=" + count);

//        kProductNamesView.print(Serdes.Long(), Serdes.String());

        return builder;
    }
}
