package com.v15k.events.impl.redis;

import java.util.Map;

import com.v15k.events.dlq.DeadLetterPublisher;

import io.lettuce.core.api.sync.RedisCommands;

public class RedisDeadLetterPublisher implements DeadLetterPublisher {

  private final RedisCommands<String, String> redis;

  public RedisDeadLetterPublisher(RedisCommands<String, String> redis) {
    this.redis = redis;
  }

  @Override
  public void sendToDLQ(String originalTopic, Object event, Exception cause) {
    redis.xadd(originalTopic + ".DLQ", Map.of(
        "error", cause.toString(),
        "event", event.toString()));
  }
}
