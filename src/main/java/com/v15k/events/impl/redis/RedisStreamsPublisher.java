package com.v15k.events.impl.redis;

import java.util.Map;

import com.v15k.events.TransportPublisher;

import io.lettuce.core.api.sync.RedisCommands;

public class RedisStreamsPublisher implements TransportPublisher {

  private final RedisCommands<String, String> redis;

  public RedisStreamsPublisher(RedisCommands<String, String> redis) {
    this.redis = redis;
  }

  @Override
  public void send(String topic, String key, byte[] payload) {
    redis.xadd(topic, Map.of("payload", new String(payload)));
  }
}
