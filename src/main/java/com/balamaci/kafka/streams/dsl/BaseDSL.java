package com.balamaci.kafka.streams.dsl;

import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author sbalamaci
 */
public abstract class BaseDSL {

    protected static final Logger log = LoggerFactory.getLogger(BaseDSL.class);

    protected String[] topics;

    public BaseDSL(String[] topics) {
        this.topics = topics;
    }

    public abstract KStreamBuilder buildStream();
}
