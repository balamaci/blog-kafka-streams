package com.balamaci.kafka.consumer;

import com.balamaci.kafka.util.SleepUtil;
import com.balamaci.kafka.util.TimeUtil;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @author sbalamaci
 */
public class SimpleConsumerCustomOffset implements Runnable {

    private final KafkaConsumer<String, String> consumer;
    private final List<String> topics;
    private final int id;

    private static final Logger log = LoggerFactory.getLogger(SimpleConsumer.class);

    private long startOffset;

    public SimpleConsumerCustomOffset(int id, long startOffset, List<String> topics, Properties consumerProps) {
        this.id = id;
        this.topics = topics;
        this.startOffset = startOffset;
        this.consumer = new KafkaConsumer<>(consumerProps);
    }


    @Override
    public void run() {
        try {
            consumer.subscribe(topics, new CustomOffsetsRebalanceListener(startOffset));
            log.info("Consumer {} subscribed ...", id);

            while (true) {
                log.info("Consumer {} polling ...", id);
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                log.info("Received {} records", records.count());

                for (TopicPartition topicPartition : records.partitions()) {
                    List<ConsumerRecord<String, String>> topicRecords = records.records(topicPartition);

                    for (ConsumerRecord<String, String> record : topicRecords) {

                        log.info("ConsumerId:{}-Topic:{} => Partition={}, Offset={}, EventTime:[{}] Val={}",
                                id, topicPartition.topic(), record.partition(), record.offset(),
                                TimeUtil.millisFormat(record.timestamp()), record.value());

                        SleepUtil.sleepMillis(2000);
                    }

                    long lastPartitionOffset = topicRecords.get(topicRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(topicPartition,
                            new OffsetAndMetadata(lastPartitionOffset + 1)));
                }
            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } catch (Exception e) {
            log.error("Consumer encountered error", e);
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }


    private class CustomOffsetsRebalanceListener implements ConsumerRebalanceListener {

        private long offset;

        public CustomOffsetsRebalanceListener(long offset) {
            this.offset = offset;
        }

        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            for(TopicPartition partition: partitions) {
                log.info("*** Partitions revoked {}", partition);
            }
        }

        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            for(TopicPartition partition: partitions) {
                log.info("*** Partitions assigned {}", partition);

                consumer.seek(partition, offset);
            }
        }
    }


}
