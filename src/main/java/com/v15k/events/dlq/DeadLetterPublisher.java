package com.v15k.events.dlq;

public interface DeadLetterPublisher {
  void sendToDLQ(String originalTopic, Object event, Exception cause);
}
