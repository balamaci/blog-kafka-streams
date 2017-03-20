package com.balamaci.kafka.configuration;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;
import rx.Observable;
import rx.subjects.PublishSubject;

import java.util.HashMap;
import java.util.Map;

/**
 * @author sbalamaci
 */
@Configuration
@Profile("kafka")
public class KafkaEmitterSourceConfiguration {

    //comma separated host:port,host2:port
    @Value("${kafka.servers}")
    private String kafkaServers;

    @Value("${kafka.topic}")
    private String kafkaTopic;

    private PublishSubject<JsonObject> publishSubject = PublishSubject.create();

    private Gson gson = new Gson();


    private static final Logger log = LoggerFactory.getLogger(KafkaEmitterSourceConfiguration.class);

    private Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "logs-processor");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return props;
    }

    @Bean
    public KafkaMessageListenerContainer<Integer, String> createContainer() {
        Map<String, Object> props = consumerProps();
        DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<>(props);
        ContainerProperties containerProps = new ContainerProperties(kafkaTopic);

        containerProps.setMessageListener(messageListener());

        KafkaMessageListenerContainer<Integer, String> container =
                new KafkaMessageListenerContainer<>(cf, containerProps);
        return container;
    }

    private MessageListener<Integer, String> messageListener() {
        return new MessageListener<Integer, String>() {
            @Override
            public void onMessage(ConsumerRecord<Integer, String> record) {
//                log.info("received: {}", record);

                JsonElement remoteJsonElement = gson.fromJson(record.value(), JsonElement.class);
                JsonObject jsonObj = remoteJsonElement.getAsJsonObject();

                publishSubject.onNext(jsonObj);
            }
        };
    }

    @Bean(name = "events")
    public Observable<JsonObject> eventsStream() {
        return publishSubject;
    }
}
