package com.v15k.events;

public interface EventSerializer {
  byte[] serialize(Object event);

  <T> T deserialize(byte[] data, Class<T> clazz);
}
