### 
This is the accompanying source for the Blog post.

Kafka can be run as a docker image as presented [kafka-docker](https://github.com/wurstmeister/kafka-docker).
We can play around with topics with different partitions by
```
    environment:
      KAFKA_CREATE_TOPICS: "logs:1:3,Topic2:1:1:compact"
```

[StartLowLevelProcessor](https://github.com/balamaci/blog-kafka-streams/blob/master/src/main/java/com/balamaci/kafka/streams/StartLowLevelProcessor.java) is used to showcase a simple scenario using the low-level Processor API.
