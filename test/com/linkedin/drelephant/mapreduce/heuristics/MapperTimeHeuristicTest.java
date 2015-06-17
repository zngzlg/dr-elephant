package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.analysis.HadoopSystemContext;
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

  // Test batch 1: Large runtime and various number of tasks */

  public void testLongRuntimeTasksCritical() throws IOException {
    // Should decrease split size and increase number of tasks
    assertEquals(Severity.CRITICAL, analyzeJob(10, 120 * Statistics.MINUTE_IN_MS));
  }

  public void testLongRuntimeTasksSevere() throws IOException {
    // Should decrease split size and increase number of tasks
    assertEquals(Severity.SEVERE, analyzeJob(50, 120 * Statistics.MINUTE_IN_MS));
  }

  public void testLongRuntimeTasksModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(100, 120 * Statistics.MINUTE_IN_MS));
  }

  public void testLongRuntimeTasksLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(500, 120 * Statistics.MINUTE_IN_MS));
  }

  public void testLongRuntimeTasksNone() throws IOException {
    // Large file with large number of tasks and long runtime.
    // Looks horrible but nothing we could suggest in this case (Probably suggest something other than hadoop?)
    assertEquals(Severity.NONE, analyzeJob(1000, 120 * Statistics.MINUTE_IN_MS));
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

  // Test batch 3: Large number of tasks and various runtime

  public void testLargeNumTasksCritical() throws IOException {
    // Should increase split size and decrease number of tasks
    assertEquals(Severity.CRITICAL, analyzeJob(1000, 1 * Statistics.MINUTE_IN_MS));
  }

  public void testLargeNumTasksSevere() throws IOException {
    // Should increase split size and decrease number of tasks
    assertEquals(Severity.SEVERE, analyzeJob(1000, 2 * Statistics.MINUTE_IN_MS));
  }

  public void testLargeNumTasksModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(1000, 4 * Statistics.MINUTE_IN_MS));
  }

  public void testLargeNumTasksLow() throws IOException {
    // This should be common case
    assertEquals(Severity.LOW, analyzeJob(1000, 5 * Statistics.MINUTE_IN_MS));
  }

  public void testLargeNumTasksNone() throws IOException {
    // This should be a common case
    assertEquals(Severity.NONE, analyzeJob(1000, 12 * Statistics.MINUTE_IN_MS));
  }

  // Test batch 4: Small number of tasks and various runtime

  public void testSmallNumTasksCritical() throws IOException {
    // Should decrease split size and increase number of tasks
    assertEquals(Severity.CRITICAL, analyzeJob(10, 120 * Statistics.MINUTE_IN_MS));
  }

  public void testSmallNumTasksSevere() throws IOException {
    // Should decrease split size and increase number of tasks
    assertEquals(Severity.SEVERE, analyzeJob(10, 60 * Statistics.MINUTE_IN_MS));
  }

  public void testSmallNumTasksModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(10, 30 * Statistics.MINUTE_IN_MS));
  }

  public void testSmallNumTasksLow() throws IOException {
    // This should be common case
    assertEquals(Severity.LOW, analyzeJob(10, 15 * Statistics.MINUTE_IN_MS));
  }

  public void testSmallNumTasksNone() throws IOException {
    // This should be a common case
    assertEquals(Severity.NONE, analyzeJob(10, 12 * Statistics.MINUTE_IN_MS));
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
