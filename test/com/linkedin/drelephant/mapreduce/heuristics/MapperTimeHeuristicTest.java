package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.MapReduceCounterHolder;
import com.linkedin.drelephant.mapreduce.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.MapReduceTaskData;
import com.linkedin.drelephant.math.Statistics;
import java.io.IOException;
import junit.framework.TestCase;


public class MapperTimeHeuristicTest extends TestCase {

  private static final long DUMMY_INPUT_SIZE = 0;
  Heuristic _heuristic = new MapperTimeHeuristic();

  // Test batch 1: Large runtime. Heuristic is not affected by various number of tasks */

  public void testLongRuntimeTasksCritical() throws IOException {
    // Should decrease split size and increase number of tasks
    assertEquals(Severity.CRITICAL, analyzeJob(10, 120 * Statistics.MINUTE_IN_MS));
  }

  public void testLongRuntimeTasksCriticalMore() throws IOException {
    // Should decrease split size and increase number of tasks
    assertEquals(Severity.CRITICAL, analyzeJob(1000, 120 * Statistics.MINUTE_IN_MS));
  }

  public void testLongRuntimeTasksSevere() throws IOException {
    // Should decrease split size and increase number of tasks
    assertEquals(Severity.SEVERE, analyzeJob(10, 60 * Statistics.MINUTE_IN_MS));
  }

  public void testLongRuntimeTasksSevereMore() throws IOException {
    // Should decrease split size and increase number of tasks
    assertEquals(Severity.SEVERE, analyzeJob(1000, 60 * Statistics.MINUTE_IN_MS));
  }

  // Test batch 2: Short runtime and various number of tasks

  public void testShortRuntimeTasksCritical() throws IOException {
    // Should increase split size and decrease number of tasks
    assertEquals(Severity.CRITICAL, analyzeJob(1000, 1 * Statistics.MINUTE_IN_MS));
  }

  public void testShortRuntimeTasksSevere() throws IOException {
    // Should increase split size and decrease number of tasks
    assertEquals(Severity.SEVERE, analyzeJob(500, 1 * Statistics.MINUTE_IN_MS));
  }

  public void testShortRuntimeTasksModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(101, 1 * Statistics.MINUTE_IN_MS));
  }

  public void testShortRuntimeTasksLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(50, 1 * Statistics.MINUTE_IN_MS));
  }

  public void testShortRuntimeTasksNone() throws IOException {
    // Small file with small number of tasks and short runtime. This should be the common case.
    assertEquals(Severity.NONE, analyzeJob(5, 1 * Statistics.MINUTE_IN_MS));
  }

  private Severity analyzeJob(int numTasks, long runtime) throws IOException {
    MapReduceCounterHolder jobCounter = new MapReduceCounterHolder();
    MapReduceTaskData[] mappers = new MapReduceTaskData[numTasks];

    MapReduceCounterHolder taskCounter = new MapReduceCounterHolder();
    taskCounter.set(MapReduceCounterHolder.CounterName.HDFS_BYTES_READ, DUMMY_INPUT_SIZE);

    int i = 0;
    for (; i < numTasks; i++) {
      mappers[i] = new MapReduceTaskData(taskCounter, new long[] { runtime, 0, 0 });
    }

    MapReduceApplicationData data = new MapReduceApplicationData().setCounters(jobCounter).setMapperData(mappers);
    HeuristicResult result = _heuristic.apply(data);
    return result.getSeverity();
  }
}
