package com.v15k.events;

public interface EventSerializer {
  byte[] serialize(Object event);

}
