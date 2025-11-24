package com.v15k.events.impl.kafka;

import com.v15k.events.TransportPublisher;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;

public class KafkaPublisher implements TransportPublisher {

  private final KafkaProducer<String, byte[]> producer;

  public KafkaPublisher(KafkaProducer<String, byte[]> producer) {
    this.producer = producer;
  }

  @Override
  public void send(String topic, String key, byte[] payload) throws Exception {

    ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, payload);

    final Exception[] exceptionHolder = new Exception[1];

    producer.send(record, new Callback() {
      @Override
      public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
          exceptionHolder[0] = exception;
        }
      }
    });

    // Flush asegura que si hubo error se detecte aquí
    producer.flush();

    if (exceptionHolder[0] != null) {
      throw exceptionHolder[0];
    }
  }
}
