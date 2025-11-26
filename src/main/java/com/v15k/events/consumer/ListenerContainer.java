package com.v15k.events.consumer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ListenerContainer {

  private final ExecutorService executor = Executors.newCachedThreadPool();

  public void startListener(KafkaEventListener listener) {
    executor.submit(listener);
  }

  public void shutdown() {
    executor.shutdown();
  }
}
