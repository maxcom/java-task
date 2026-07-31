Задача
======

Сделать реализацию интерфейса FutureMetrics, которая будет запускать переданные асинхронные функции и считать
среднее количество одновременно работающих функций за последние 5 секунд.

Асинхронная функция может производить часть свой работы в потоке вызова, эта работа так же должна учитываться.

Реализация должна поддерживать 1000 запросов в секунду. Реализация не должна создавать дополнительных потоков
и использовать таймеры/планировщики.

Нужно реализовать в классе SimpleFutureMetrics. Для получения текущего времени нужно использовать
переданный LongSupplier (это нужно для тестов).

```java
public interface FutureMetrics {
  <T> CompletionStage<T> runMetered(Supplier<CompletionStage<T>> func);
  double getCurrentAverage();
}
```

```java
public class SimpleFutureMetrics implements FutureMetrics {
  private final LongSupplier clock;

  public SimpleFutureMetrics() {
    this(System::currentTimeMillis);
  }

  SimpleFutureMetrics(LongSupplier clock) {
    this.clock = clock;
  }

  @Override
  public <T> CompletionStage<T> runMetered(Supplier<CompletionStage<T>> func) {
    // TODO ...
  }

  @Override
  public double getCurrentAverage() {
    // TODO ...
  }
}
```