package com.v15k.events.impl.kafka;

import com.v15k.events.dlq.DeadLetterPublisher;
import com.v15k.events.EventSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaDeadLetterPublisher implements DeadLetterPublisher {

  private final KafkaProducer<String, byte[]> producer;
  private final EventSerializer serializer;

  public KafkaDeadLetterPublisher(KafkaProducer<String, byte[]> producer,
      EventSerializer serializer) {
    this.producer = producer;
    this.serializer = serializer;
  }

  @Override
  public void sendToDLQ(String originalTopic, Object event, Exception cause) {

    String dlqTopic = originalTopic + ".DLQ";

    DeadLetterEnvelope envelope = new DeadLetterEnvelope(
        originalTopic,
        event,
        cause.toString(),
        System.currentTimeMillis());

    byte[] payload = serializer.serialize(envelope);

    ProducerRecord<String, byte[]> record = new ProducerRecord<>(dlqTopic, null, payload);

    producer.send(record);
    producer.flush();
  }

  public static class DeadLetterEnvelope {
    private final String topic;
    private final Object event;
    private final String error;
    private final long timestamp;

    public DeadLetterEnvelope(String topic, Object event, String error, long timestamp) {
      this.topic = topic;
      this.event = event;
      this.error = error;
      this.timestamp = timestamp;
    }

    public String getTopic() {
      return topic;
    }

    public Object getEvent() {
      return event;
    }

    public String getError() {
      return error;
    }

    public long getTimestamp() {
      return timestamp;
    }
  }
}
