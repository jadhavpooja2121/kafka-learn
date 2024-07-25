package com.example.kafka_demo.demo2;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerWithAutoCommit {


  public static void main(String[] args) {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "multipartition-consumer");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", 1000);
    props.put("auto.offset.reset", "earliest"); // Add this property

    // String topics[] = {"multipartition-topic"};
    // String topic = "social-media-app";
    String topics[] = {"social-media-app"};
    // TopicPartition[] topicPartitions = {new TopicPartition(), new TopicPartition()};

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Arrays.asList(topics));
    try {
      while (true) {
        ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record : consumerRecords) {
          String message = String.format("offset: %d, key: %s, value: %s, partition: %d%n",
              record.offset(), record.key(), record.value(), record.partition());
          System.out.println(message);
        }
      }
    } catch (Exception e) {
      e.printStackTrace(); // Print the stack trace for better error handling
    } finally {
      consumer.close();
    }
  }
}
