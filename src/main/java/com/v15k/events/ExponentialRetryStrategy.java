package com.v15k.events;

public class ExponentialRetryStrategy implements RetryStrategy {

  private final int maxRetries;
  private final long baseDelay;
  private final long maxDelay;

  public ExponentialRetryStrategy(int maxRetries, long baseDelay, long maxDelay) {
    this.maxRetries = maxRetries;
    this.baseDelay = baseDelay;
    this.maxDelay = maxDelay;
  }

  @Override
  public boolean shouldRetry(int attempt) {
    return attempt <= maxRetries;
  }

  @Override
  public long backoff(int attempt) {
    long delay = (long) (baseDelay * Math.pow(2, attempt - 1));
    return Math.min(delay, maxDelay);
  }
}
