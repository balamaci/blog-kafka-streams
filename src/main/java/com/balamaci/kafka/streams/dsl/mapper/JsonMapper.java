package com.balamaci.kafka.streams.dsl.mapper;

import com.balamaci.kafka.util.Json;
import com.google.gson.JsonParser;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sbalamaci
 */
public class JsonMapper implements ValueMapper<String, Json> {

    private static final Logger log = LoggerFactory.getLogger(JsonMapper.class);

    private JsonParser jsonParser = new JsonParser();

    @Override
    public Json apply(String value) {
        Json json = new Json(jsonParser.parse(value).getAsJsonObject());

        log.info("Processing EventID={}", json.propertyLongValue("eventId"));
        return json;
    }
}
