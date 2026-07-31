package com.mycompany.app;

import java.util.concurrent.CompletionStage;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class SimpleFutureMetrics implements FutureMetrics {

  private static final long WINDOW_MILLIS = 5000L;

  private final LongSupplier clock;

  public SimpleFutureMetrics() {
    this(System::currentTimeMillis);
  }

  SimpleFutureMetrics(LongSupplier clock) {
    this.clock = clock;
  }

  @Override
  public <T> CompletionStage<T> runMetered(Supplier<CompletionStage<T>> func) {
    return func.get();
  }

  @Override
  public double getCurrentAverage() {
    return -1;
  }
}