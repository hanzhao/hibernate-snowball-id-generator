package com.github.magicae;

import org.assertj.core.api.Assertions;
import org.jboss.logging.Logger;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SnowballIdWorkerTest {

  private static final Logger LOG = Logger.getLogger(SnowballIdWorkerTest.class);

  long sequenceMask = -1L ^ (-1L << 12);
  long workerMask = 0x00000000003FF000L;
  long timestampMask = 0xFFFFFFFFFFC00000L;

  class EasyTimeWorker extends SnowballIdWorker {
    protected Callable<Long> timeMaker = () -> System.currentTimeMillis();

    @Override
    protected long timeGen() {
      Future<Long> future = Executors.newSingleThreadExecutor().submit(timeMaker);
      try {
        return future.get();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (ExecutionException e) {
        e.printStackTrace();
      }
      return -1;
    }
  }

  class WakingIdWorker extends EasyTimeWorker {
    private long slept = 0;

    @Override
    protected long tilNextMillis(long lastTimestamp) {
      slept += 1;
      return super.tilNextMillis(lastTimestamp);
    }
  }

  class StaticTimeWorker extends SnowballIdWorker {
    private long time = 1L;

    @Override
    protected long timeGen() {
      return time + twepoch;
    }
  }

  @Test
  public void generateAnId() {
    SnowballIdWorker worker = new SnowballIdWorker();
    worker.workerId = 1;
    long id = worker.nextId();
    Assertions.assertThat(id).isGreaterThan(0L);
  }

  @Test
  public void properlyMaskWorkerId() {
    SnowballIdWorker worker = new SnowballIdWorker();
    long workerId = 1023;
    worker.workerId = workerId;
    for (int i = 0; i < 1000; ++i) {
      Assertions.assertThat((worker.nextId() & workerMask) >> 12).isEqualTo(workerId);
    }
  }

  @Test
  public void properlyMaskTimestamp() {
    EasyTimeWorker worker = new EasyTimeWorker();
    worker.workerId = 1023;
    for (int i = 0; i < 100; ++i) {
      long t = System.currentTimeMillis();
      worker.timeMaker = () -> t;
      Assertions.assertThat((worker.nextId() & timestampMask) >> 22).isEqualTo(t - worker.twepoch);
    }
  }

  @Test
  public void rollOverSequenceId() {
    SnowballIdWorker worker = new SnowballIdWorker();
    long workerId = 4;
    worker.workerId = workerId;
    int startSequence = 0xFFFFFF - 20, endSequence = 0xFFFFFF + 20;
    worker.sequence = startSequence;
    for (int i = startSequence; i <= endSequence; ++i) {
      Assertions.assertThat((worker.nextId() & workerMask) >> 12).isEqualTo(workerId);
    }
  }

  @Test
  public void generateIncreasingId() {
    long lastId = 0L;
    SnowballIdWorker worker = new SnowballIdWorker();
    for (int i = 0; i < 100; ++i) {
      long id = worker.nextId();
      Assertions.assertThat(id).isGreaterThan(lastId);
      lastId = id;
    }
  }

  @Test
  public void generateMillionsOfIdQuickly() {
    SnowballIdWorker worker = new SnowballIdWorker();
    long t = System.currentTimeMillis();
    for (int i = 0; i < 1000000; ++i) {
      worker.nextId();
    }
    long t2 = System.currentTimeMillis();
    LOG.infof("Generated 1000000 ids in %d ms, or %,.0f ids/second.", t2 - t, 1000000000.0 / (t2 - t));
  }

  @Test
  public void sleepIfWeRollOverTwiceInSameMillisecond() {
    WakingIdWorker worker = new WakingIdWorker();
    List<Long> times = Arrays.asList(2L, 2L, 3L);
    ListIterator<Long> iterator = times.listIterator();
    worker.timeMaker = () -> iterator.next();
    worker.sequence = 4095;
    worker.nextId();
    worker.sequence = 4095;
    worker.nextId();
    Assertions.assertThat(worker.slept).isEqualTo(1);
  }

  @Test
  public void generateOnlyUniqueIds() {
    SnowballIdWorker worker = new SnowballIdWorker();
    worker.workerId = 1023;
    HashSet<Long> set = new HashSet<>();
    int n = 2000000;
    for (int i = 0; i < n; ++i) {
      long id = worker.nextId();
      if (set.contains(id)) {
        LOG.errorf("Generated duplicated id %s.", Long.toString(id, 2));
      } else {
        set.add(id);
      }
    }
    Assertions.assertThat(set.size()).isEqualTo(n);
  }

  @Test
  public void generateIdsOver50Billion() {
    SnowballIdWorker worker = new SnowballIdWorker();
    worker.workerId = 0;
    Assertions.assertThat(worker.nextId()).isGreaterThan(50000000000L);
  }

  @Test
  public void generateOnlyUniqueIdsEvenWhenTimeGoesBackwards() {
    StaticTimeWorker worker = new StaticTimeWorker();
    worker.workerId = 0;

    Assertions.assertThat(worker.sequence).isEqualTo(0);
    Assertions.assertThat(worker.time).isEqualTo(1);
    long id1 = worker.nextId();
    Assertions.assertThat(id1 >> 22).isEqualTo(1);
    Assertions.assertThat(id1 & sequenceMask).isEqualTo(0);

    Assertions.assertThat(worker.sequence).isEqualTo(0);
    Assertions.assertThat(worker.time).isEqualTo(1);
    long id2 = worker.nextId();
    Assertions.assertThat(id2 >> 22).isEqualTo(1);
    Assertions.assertThat(id2 & sequenceMask).isEqualTo(1);

    worker.time = 0;
    Assertions.assertThat(worker.sequence).isEqualTo(1);
    try {
      worker.nextId();
    } catch (Exception e) {
      Assertions.assertThat(e).isInstanceOf(RuntimeException.class);
      Assertions.assertThat(e).hasMessage("Clock moved backwards. Refusing to generate id for 1 milliseconds");
    }

    worker.time = 1;
    long id3 = worker.nextId();
    Assertions.assertThat(id3 >> 22).isEqualTo(1);
    Assertions.assertThat(id3 & sequenceMask).isEqualTo(2);
  }

}
