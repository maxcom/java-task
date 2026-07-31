package com.mycompany.app;

import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

public interface FutureMetrics {
  <T> CompletionStage<T> runMetered(Supplier<CompletionStage<T>> func);
  double getCurrentAverage();
}