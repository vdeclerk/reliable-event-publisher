package com.v15k.events.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;

public class KafkaEventListener implements Runnable {

  private final KafkaConsumer<String, byte[]> consumer;
  private final KafkaEventConsumer eventConsumer;
  private final Class<?> eventType;

  public KafkaEventListener(
      KafkaConsumer<String, byte[]> consumer,
      KafkaEventConsumer eventConsumer,
      Class<?> eventType) {
    this.consumer = consumer;
    this.eventConsumer = eventConsumer;
    this.eventType = eventType;
  }

  @Override
  public void run() {
    while (true) {
      var records = consumer.poll(Duration.ofMillis(200));

      for (ConsumerRecord<String, byte[]> record : records) {
        try {
          eventConsumer.consume(record.value(), eventType);
        } catch (Exception e) {
          // acá podés enviar a DLQ
          e.printStackTrace();
        }
      }
    }
  }
}
