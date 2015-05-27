package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.HadoopCounterHolder;
import com.linkedin.drelephant.mapreduce.MapreduceApplicationData;
import com.linkedin.drelephant.mapreduce.HadoopTaskData;
import com.linkedin.drelephant.mapreduce.heuristics.MapperInputSizeHeuristic;
import com.linkedin.drelephant.math.Statistics;
import java.io.IOException;
import junit.framework.TestCase;


public class MapperInputSizeHeuristicTest extends TestCase {

  private static final long UNITSIZE = Constants.HDFS_BLOCK_SIZE;
  Heuristic _heuristic = new MapperInputSizeHeuristic();

  // Test batch 1: Large file test with large runtime and various number of tasks */

  public void testLargeFileNumTasksCritical() throws IOException {
    // Should decrease file size and increase number of tasks
    assertEquals(Severity.CRITICAL, analyzeJob(10, 5 * UNITSIZE, 60 * Statistics.MINUTE_IN_MS));
  }

  public void testLargeFileNumTasksSevere() throws IOException {
    // Should decrease file size and increase number of tasks
    assertEquals(Severity.SEVERE, analyzeJob(200, 5 * UNITSIZE, 60 * Statistics.MINUTE_IN_MS));
  }

  public void testLargeFileNumTasksModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(500, 5 * UNITSIZE, 60 * Statistics.MINUTE_IN_MS));
  }

  public void testLargeFileNumTasksLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(1000, 5 * UNITSIZE, 60 * Statistics.MINUTE_IN_MS));
  }

  public void testLargeFileNumTasksNone() throws IOException {
    // Large file with large number of tasks and long runtime.
    // Looks horrible but nothing we could suggest in this case (Probably suggest something other than hadoop?)
    assertEquals(Severity.NONE, analyzeJob(2000, 5 * UNITSIZE, 60 * Statistics.MINUTE_IN_MS));
  }

  // Test batch 2: Large file test with various runtime and small number of tasks

  public void testLargeFileRuntimeCritical() throws IOException {
    // Should decrease file size and increase number of tasks
    assertEquals(Severity.CRITICAL, analyzeJob(100, 5 * UNITSIZE, 60 * Statistics.MINUTE_IN_MS));
  }

  public void testLargeFileRuntimeSevere() throws IOException {
    // Should decrease file size and increase number of tasks
    assertEquals(Severity.SEVERE, analyzeJob(100, 5 * UNITSIZE, 20 * Statistics.MINUTE_IN_MS));
  }

  public void testLargeFileRuntimeModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(100, 5 * UNITSIZE, 15 * Statistics.MINUTE_IN_MS));
  }

  public void testLargeFileRuntimeLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(100, 5 * UNITSIZE, 10 * Statistics.MINUTE_IN_MS));
  }

  public void testLargeFileRuntimeNone() throws IOException {
    // Large file, but already has small number of tasks and small runtime. Looks good.
    assertEquals(Severity.NONE, analyzeJob(100, 5 * UNITSIZE, 5 * Statistics.MINUTE_IN_MS));
  }

  // Test batch 3: Small file test with short runtime and various number of tasks

  public void testSmallFileNumTasksCritical() throws IOException {
    // Should increase file size and decrease number of tasks
    assertEquals(Severity.CRITICAL, analyzeJob(1000, UNITSIZE / 32, 2 * Statistics.MINUTE_IN_MS));
  }

  public void testSmallFileNumTasksSevere() throws IOException {
    // Should increase file size and decrease number of tasks
    assertEquals(Severity.SEVERE, analyzeJob(200, UNITSIZE / 32, 2 * Statistics.MINUTE_IN_MS));
  }

  public void testSmallFileNumTasksModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(50, UNITSIZE / 32, 2 * Statistics.MINUTE_IN_MS));
  }

  public void testSmallFileNumTasksLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(10, UNITSIZE / 32, 2 * Statistics.MINUTE_IN_MS));
  }

  public void testSmallFileNumTasksNone() throws IOException {
    // Small file with small number of tasks and short runtime. This should be the common case.
    assertEquals(Severity.NONE, analyzeJob(5, UNITSIZE / 32, 2 * Statistics.MINUTE_IN_MS));
  }

  // Test batch 4: Small file test with large number of tasks and various runtime

  public void testRuntimeSmallFileCritical() throws IOException {
    // Should increase file size and decrease number of tasks
    assertEquals(Severity.CRITICAL, analyzeJob(500, UNITSIZE / 32, 1 * Statistics.MINUTE_IN_MS));
  }

  public void testRuntimeSmallFileSevere() throws IOException {
    // Should increase file size and decrease number of tasks
    assertEquals(Severity.SEVERE, analyzeJob(500, UNITSIZE / 32, 3 * Statistics.MINUTE_IN_MS));
  }

  public void testRuntimeSmallFileModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(500, UNITSIZE / 32, 4 * Statistics.MINUTE_IN_MS));
  }

  public void testRuntimeSmallFileLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(500, UNITSIZE / 32, 5 * Statistics.MINUTE_IN_MS));
  }

  public void testRuntimeSmallFileNone() throws IOException {
    // Small file with large number of tasks and long runtime. This should be a CPU-bound job
    // Probably not a good idea to suggest file size increase(even longer runtime) or decrease(even more mappers)
    assertEquals(Severity.NONE, analyzeJob(500, UNITSIZE / 32, 10 * Statistics.MINUTE_IN_MS));
  }

  // Test batch 5: Test with normal file size (one HDFS block). This should be a common case

  public void testNormalFileSmallNumTasks() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(10, UNITSIZE, 10 * Statistics.MINUTE_IN_MS));
  }

  public void testNormalFileLargeNumTasks() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(300, UNITSIZE, 10 * Statistics.MINUTE_IN_MS));
  }

  public void testNormalFileLongRuntime() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(100, UNITSIZE, 30 * Statistics.MINUTE_IN_MS));
  }

  public void testNormalFileShortRuntime() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(100, UNITSIZE, 1 * Statistics.MINUTE_IN_MS));
  }

  private Severity analyzeJob(int numTasks, long inputSize, long runtime) throws IOException {
    HadoopCounterHolder jobCounter = new HadoopCounterHolder();
    HadoopTaskData[] mappers = new HadoopTaskData[numTasks];

    HadoopCounterHolder taskCounter = new HadoopCounterHolder();
    taskCounter.set(HadoopCounterHolder.CounterName.HDFS_BYTES_READ, inputSize);

    int i = 0;
    for (; i < numTasks; i++) {
      mappers[i] = new HadoopTaskData(taskCounter, new long[] { runtime, 0, 0 });
    }

    MapreduceApplicationData data = new MapreduceApplicationData().setCounters(jobCounter).setMapperData(mappers);
    HeuristicResult result = _heuristic.apply(data);
    return result.getSeverity();
  }
}
