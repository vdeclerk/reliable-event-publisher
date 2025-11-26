package com.v15k.events.consumer;

import com.v15k.events.EventSerializer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaEventConsumer {

  private final EventSerializer serializer;
  private final Map<Class<?>, EventHandler<?>> handlers = new ConcurrentHashMap<>();

  public KafkaEventConsumer(EventSerializer serializer) {
    this.serializer = serializer;
  }

  /**
   * Registrar un handler para un tipo de evento
   */
  public <T> void registerHandler(Class<T> eventType, EventHandler<T> handler) {
    handlers.put(eventType, handler);
  }

  /**
   * Procesar un mensaje recibido del broker
   */
  public void consume(byte[] data, Class<?> eventType) {
    try {
      Object event = serializer.deserialize(data, eventType);
      dispatch(eventType, event);
    } catch (Exception e) {
      throw new RuntimeException("Error consuming event", e);
    }
  }

  @SuppressWarnings("unchecked")
  <T> void dispatch(Class<T> eventType, Object event) {
    EventHandler<T> handler = (EventHandler<T>) handlers.get(eventType);
    if (handler != null) {
      handler.onEvent(eventType.cast(event));
    }
  }
}
