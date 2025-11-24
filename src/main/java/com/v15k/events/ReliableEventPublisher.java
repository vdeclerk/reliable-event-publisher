package com.v15k.events;

import com.v15k.events.dlq.DeadLetterPublisher;

public class ReliableEventPublisher implements EventPublisher {

  private final TransportPublisher transport;
  private final DeadLetterPublisher dlq;
  private final EventSerializer serializer;
  private final RetryStrategy retryStrategy;

  public ReliableEventPublisher(
      TransportPublisher transport,
      DeadLetterPublisher dlq,
      EventSerializer serializer,
      RetryStrategy retryStrategy) {
    this.transport = transport;
    this.dlq = dlq;
    this.serializer = serializer;
    this.retryStrategy = retryStrategy;
  }

  @Override
  public void publish(String topic, Object event) {
    byte[] payload = serializer.serialize(event);

    int attempt = 0;
    while (true) {
      try {
        transport.send(topic, null, payload);
        return;
      } catch (Exception ex) {
        attempt++;

        if (!retryStrategy.shouldRetry(attempt)) {
          dlq.sendToDLQ(topic, event, ex);
          return;
        }

        try {
          Thread.sleep(retryStrategy.backoff(attempt));
        } catch (InterruptedException ignored) {
        }
      }
    }
  }
}
