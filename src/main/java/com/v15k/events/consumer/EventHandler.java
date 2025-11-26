package com.v15k.events.consumer;

public interface EventHandler<T> {
  void onEvent(T event);
}
