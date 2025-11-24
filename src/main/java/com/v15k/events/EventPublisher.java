package com.v15k.events;

public interface EventPublisher {
  void publish(String topic, Object event);
}
