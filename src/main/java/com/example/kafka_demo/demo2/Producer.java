package com.example.kafka_demo.demo2;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {
  public static void main(String[] args) {
    String clientId = "multipartition-consumer"; // consumer group name

    Properties props = new Properties();
    // single broker topic
    // props.put("bootstrap.servers", "localhost:9092");
    // multi-broker
    props.put("bootstrap.servers", "localhost:9093, localhost:9094, localhost:9094");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("acks", "all");
    props.put("client.id", clientId);

    try {
      KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
      int nums = 10;

      // String topic= "multipartition-topic";
      String topic = "social-media-app"; // topic using multiple brokers and multi partitions and
                                         // replication factor 2


      for (int i = 0; i < nums; i++) {
        kafkaProducer
            .send(new ProducerRecord<String, String>(topic, Integer.toString(i), "msg" + i));
        System.out.println("Message " + i + "is sent");
      }
      kafkaProducer.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
