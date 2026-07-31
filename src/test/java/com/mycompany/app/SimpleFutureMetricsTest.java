package com.mycompany.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

class SimpleFutureMetricsTest {

  private static final class MutableClock {
    private long now = 0L;

    void set(long v) {
      now = v;
    }

    void advance(long ms) {
      now += ms;
    }

    LongSupplier supplier() {
      return () -> now;
    }
  }

  private static Supplier<CompletionStage<Void>> hungTask() {
    return CompletableFuture::new;
  }

  @Test
  void emptyAverageIsZero() {
    SimpleFutureMetrics m = new SimpleFutureMetrics(new MutableClock().supplier());
    assertEquals(0.0, m.getCurrentAverage(), 1e-9);
  }

  @Test
  void singleRunningTask() {
    MutableClock clock = new MutableClock();
    SimpleFutureMetrics m = new SimpleFutureMetrics(clock.supplier());

    m.runMetered(hungTask());
    clock.advance(5000);

    assertEquals(1.0, m.getCurrentAverage(), 1e-9);
  }

  @Test
  void completedTaskOutsideWindow() {
    MutableClock clock = new MutableClock();
    SimpleFutureMetrics m = new SimpleFutureMetrics(clock.supplier());

    CompletableFuture<Void> f = new CompletableFuture<>();
    m.runMetered(() -> f);
    f.complete(null);
    clock.advance(6000);

    assertEquals(0.0, m.getCurrentAverage(), 1e-9);
  }

  @Test
  void completedTaskInsideWindow() {
    MutableClock clock = new MutableClock();
    SimpleFutureMetrics m = new SimpleFutureMetrics(clock.supplier());

    CompletableFuture<Void> f = new CompletableFuture<>();
    m.runMetered(() -> f);
    clock.advance(2000);
    f.complete(null);
    clock.advance(3000);

    assertEquals(0.4, m.getCurrentAverage(), 1e-9);
  }

  @Test
  void multipleConcurrentTasks() {
    MutableClock clock = new MutableClock();
    SimpleFutureMetrics m = new SimpleFutureMetrics(clock.supplier());

    int n = 10;
    List<CompletableFuture<Void>> tasks = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      CompletableFuture<Void> f = new CompletableFuture<>();
      m.runMetered(() -> f);
      tasks.add(f);
    }
    clock.advance(5000);

    assertEquals((double) n, m.getCurrentAverage(), 1e-9);
    tasks.forEach(f -> f.complete(null));
  }

  @Test
  void mixedOverlappingTasks() {
    MutableClock clock = new MutableClock();
    SimpleFutureMetrics m = new SimpleFutureMetrics(clock.supplier());

    CompletableFuture<Void> a = new CompletableFuture<>();
    m.runMetered(() -> a);
    clock.advance(1000);

    CompletableFuture<Void> b = new CompletableFuture<>();
    m.runMetered(() -> b);
    clock.advance(1000);

    a.complete(null);
    clock.advance(1000);

    b.complete(null);
    clock.advance(2000);

    assertEquals(0.8, m.getCurrentAverage(), 1e-9);
  }

  @Test
  void windowSlides() {
    MutableClock clock = new MutableClock();
    SimpleFutureMetrics m = new SimpleFutureMetrics(clock.supplier());

    CompletableFuture<Void> f = new CompletableFuture<>();
    m.runMetered(() -> f);
    f.complete(null);
    clock.advance(6000);

    assertEquals(0.0, m.getCurrentAverage(), 1e-9);
  }

  @Test
  void resultAndExceptionPropagation() {
    MutableClock clock = new MutableClock();
    SimpleFutureMetrics m = new SimpleFutureMetrics(clock.supplier());

    CompletableFuture<String> okF = new CompletableFuture<>();
    CompletionStage<String> okStage = m.runMetered(() -> okF);
    okF.complete("done");
    assertEquals("done", okStage.toCompletableFuture().join());

    CompletableFuture<String> failF = new CompletableFuture<>();
    CompletionStage<String> failStage = m.runMetered(() -> failF);
    failF.completeExceptionally(new RuntimeException("boom"));
    assertThrows(RuntimeException.class,
        () -> failStage.toCompletableFuture().join());

    assertThrows(RuntimeException.class,
        () -> m.runMetered(() -> {
          throw new RuntimeException("sync");
        }));

    clock.advance(6000);
    assertEquals(0.0, m.getCurrentAverage(), 1e-9);
  }

  @Test
  void throughput1000rps() throws Exception {
    SimpleFutureMetrics m = new SimpleFutureMetrics();
    ExecutorService exec = Executors.newFixedThreadPool(8);
    AtomicInteger completed = new AtomicInteger();

    try {
      int total = 3000;
      List<CompletableFuture<Void>> all = new ArrayList<>();
      for (int i = 0; i < total; i++) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        all.add(m.runMetered(() -> f).toCompletableFuture()
            .thenRun(completed::incrementAndGet));
        exec.submit(() -> {
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          f.complete(null);
        });
      }

      CompletableFuture.allOf(all.toArray(new CompletableFuture[0]))
          .get(30, TimeUnit.SECONDS);
      assertEquals(total, completed.get());

      double avg = m.getCurrentAverage();
      assertTrue(avg >= 0.0, "avg should be non-negative, got " + avg);
      assertFalse(Double.isNaN(avg) || Double.isInfinite(avg),
          "avg should be finite, got " + avg);
    } finally {
      exec.shutdownNow();
      exec.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void concurrentCallsSafety() throws Exception {
    MutableClock clock = new MutableClock();
    SimpleFutureMetrics m = new SimpleFutureMetrics(clock.supplier());

    int threads = 16;
    int perThread = 50;
    int totalTasks = threads * perThread;

    ExecutorService exec = Executors.newFixedThreadPool(threads);
    List<CompletableFuture<Void>> submitted = new ArrayList<>();
    List<CompletableFuture<Void>> cfs = new ArrayList<>();
    try {
      for (int i = 0; i < totalTasks; i++) {
        CompletableFuture<Void> cf = new CompletableFuture<>();
        cfs.add(cf);
        CompletableFuture<Void> s = CompletableFuture.runAsync(() -> {
          try {
            m.runMetered(() -> cf);
          } catch (Throwable t) {
            throw new RuntimeException(t);
          }
        }, exec);
        submitted.add(s);
        exec.submit(m::getCurrentAverage);
      }

      for (CompletableFuture<Void> s : submitted) {
        s.get(10, TimeUnit.SECONDS);
      }
      clock.set(10_000);

      double avgBefore = m.getCurrentAverage();
      assertTrue(avgBefore >= 0.0, "avg should be non-negative, got " + avgBefore);
      assertFalse(Double.isNaN(avgBefore) || Double.isInfinite(avgBefore),
          "avg should be finite, got " + avgBefore);

      for (CompletableFuture<Void> cf : cfs) {
        cf.complete(null);
      }
      clock.set(16_000);
      assertEquals(0.0, m.getCurrentAverage(), 1e-9);
    } finally {
      exec.shutdownNow();
      exec.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void nullStageFromFuncClosesMeter() {
    MutableClock clock = new MutableClock();
    SimpleFutureMetrics m = new SimpleFutureMetrics(clock.supplier());

    assertThrows(NullPointerException.class,
        () -> m.runMetered(() -> null));

    clock.advance(6000);
    assertEquals(0.0, m.getCurrentAverage(), 1e-9);
  }

  @Test
  void rampUpAndDown() {
    MutableClock clock = new MutableClock();
    SimpleFutureMetrics m = new SimpleFutureMetrics(clock.supplier());

    // 5 tasks start at t=0, finish staggered
    List<CompletableFuture<Void>> tasks = IntStream.range(0, 5)
        .mapToObj(i -> new CompletableFuture<Void>())
        .toList();
    for (CompletableFuture<Void> f : tasks) {
      m.runMetered(() -> f);
    }
    clock.advance(1000);
    tasks.get(0).complete(null);
    clock.advance(1000);
    tasks.get(1).complete(null);
    clock.advance(1000);

    // remaining 3 tasks active during [2000,3000]; advance to t=5000
    clock.advance(2000);

    // Expected integral: with clock t=0..1000 level=5; 1000..2000 level=4;
    // 2000..3000 level=3. Window [0,5000].
    // area = 5*1000 + 4*1000 + 3*1000 + 3*2000 (tail from 3000..5000
    // still has 3 running) = 5000+4000+3000+6000 = 18000; /5000 = 3.6
    assertEquals(3.6, m.getCurrentAverage(), 1e-9);
  }

  @Test
  void getCurrentAverageIsIdempotentAtSameClock() {
    MutableClock clock = new MutableClock();
    SimpleFutureMetrics m = new SimpleFutureMetrics(clock.supplier());

    CompletableFuture<Void> a = new CompletableFuture<>();
    m.runMetered(() -> a);
    clock.advance(4000);
    a.complete(null);
    clock.advance(1000);

    double first = m.getCurrentAverage();
    double second = m.getCurrentAverage();
    double third = m.getCurrentAverage();

    assertEquals(0.8, first, 1e-9);
    assertEquals(first, second, 1e-9);
    assertEquals(second, third, 1e-9);
  }

  @Test
  void prunePreservesBaseAcrossSlidingWindow() {
    MutableClock clock = new MutableClock();
    SimpleFutureMetrics m = new SimpleFutureMetrics(clock.supplier());

    CompletableFuture<Void> a = new CompletableFuture<>();
    m.runMetered(() -> a);
    clock.advance(4000);
    a.complete(null);
    clock.advance(1000);

    assertEquals(0.8, m.getCurrentAverage(), 1e-9);
    clock.advance(1000);
    assertEquals(0.6, m.getCurrentAverage(), 1e-9);
    clock.advance(4000);
    assertEquals(0.0, m.getCurrentAverage(), 1e-9);
    assertEquals(0.0, m.getCurrentAverage(), 1e-9);
  }

  @Test
  void repeatedQueriesDoNotDriftDuringActiveTask() {
    MutableClock clock = new MutableClock();
    SimpleFutureMetrics m = new SimpleFutureMetrics(clock.supplier());

    m.runMetered(hungTask());

    // growing phase: window [now-5000, now] not yet saturated by the task
    for (int t = 1000; t <= 5000; t += 1000) {
      clock.advance(1000);
      assertEquals(t / 5000.0, m.getCurrentAverage(), 1e-9,
          "avg at t=" + t);
    }
    // steady phase: start event pruned into base; avg must stay 1.0
    for (int t = 6000; t <= 9000; t += 1000) {
      clock.advance(1000);
      assertEquals(1.0, m.getCurrentAverage(), 1e-9,
          "avg at t=" + t);
    }
  }

  @Test
  void pruneBoundaryEventAtWindowStart() {
    MutableClock clock = new MutableClock();
    SimpleFutureMetrics m = new SimpleFutureMetrics(clock.supplier());

    CompletableFuture<Void> a = new CompletableFuture<>();
    m.runMetered(() -> a);
    a.complete(null);

    assertEquals(0.0, m.getCurrentAverage(), 1e-9);
    clock.advance(5000);
    assertEquals(0.0, m.getCurrentAverage(), 1e-9);
    assertEquals(0.0, m.getCurrentAverage(), 1e-9);
  }
}