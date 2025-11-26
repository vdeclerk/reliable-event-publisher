package com.v15k.events.consumer;

import com.v15k.events.EventSerializer;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.List;
import java.util.Map;

public class RedisEventListener implements Runnable {

  private final UnifiedJedis redis;
  private final EventSerializer serializer;
  private final String stream;
  private final String group;
  private final String consumer;
  private final KafkaEventConsumer eventConsumer;
  private final Class<?> eventType;
  private final String dlqStream;

  public RedisEventListener(
      UnifiedJedis redis,
      EventSerializer serializer,
      String stream,
      String group,
      String consumer,
      KafkaEventConsumer eventConsumer,
      Class<?> eventType) {
    this.redis = redis;
    this.serializer = serializer;
    this.stream = stream;
    this.group = group;
    this.consumer = consumer;
    this.eventConsumer = eventConsumer;
    this.eventType = eventType;
    this.dlqStream = stream + ".DLQ";

    ensureConsumerGroupExists();
  }

  private void ensureConsumerGroupExists() {
    try {
      redis.xgroupCreate(stream, group, new StreamEntryID("0-0"), true);
    } catch (Exception ignored) {
      // El grupo ya existe, no pasa nada
    }
  }

  @Override
  public void run() {
    while (true) {
      try {
        List<Map.Entry<String, List<StreamEntry>>> result = redis.xreadGroup(
            group,
            consumer,
            XReadGroupParams.xReadGroupParams().count(1).block(200),
            Map.of(stream, StreamEntryID.XREADGROUP_UNDELIVERED_ENTRY));

        if (result == null || result.isEmpty()) {
          continue;
        }

        for (Map.Entry<String, List<StreamEntry>> entry : result) {
          for (StreamEntry streamEntry : entry.getValue()) {
            processEntry(streamEntry);
          }
        }

      } catch (Exception e) {
        e.printStackTrace();
        try {
          Thread.sleep(500);
        } catch (InterruptedException ignored) {
        }
      }
    }
  }

  private void processEntry(StreamEntry entry) {
    try {
      byte[] eventBytes = entry.getFields().get("event").getBytes();
      Object event = serializer.deserialize(eventBytes, eventType);

      eventConsumer.dispatch(eventType, event);

      redis.xack(stream, group, entry.getID());

    } catch (Exception ex) {
      ex.printStackTrace();

      redis.xadd(dlqStream, entry.getID(), Map.of(
          "error", ex.getMessage(),
          "eventId", entry.getID().toString(),
          "rawEvent", entry.getFields().get("event").toString()));

      redis.xack(stream, group, entry.getID());
    }
  }
}
