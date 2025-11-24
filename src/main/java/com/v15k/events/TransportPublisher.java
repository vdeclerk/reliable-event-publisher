package com.v15k.events;

public interface TransportPublisher {
  void send(String topic, String key, byte[] payload) throws Exception;
}
