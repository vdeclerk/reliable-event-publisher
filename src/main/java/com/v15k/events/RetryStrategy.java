package com.v15k.events;

public interface RetryStrategy {
  boolean shouldRetry(int attempt);

  long backoff(int attempt);
}
