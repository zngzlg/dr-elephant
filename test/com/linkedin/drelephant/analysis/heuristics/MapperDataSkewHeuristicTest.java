package com.linkedin.drelephant.analysis.heuristics;

import java.io.IOException;
import java.util.HashMap;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder.CounterName;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;

import junit.framework.TestCase;


public class MapperDataSkewHeuristicTest extends TestCase {

  private static final long unitSize = Constants.HDFS_BLOCK_SIZE / 64;
  Heuristic heuristic = new MapperDataSkewHeuristic();

  public void testCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(200, 200, 1 * unitSize, 100 * unitSize));
  }

  public void testSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(200, 200, 10 * unitSize, 100 * unitSize));
  }

  public void testModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(200, 200, 20 * unitSize, 100 * unitSize));
  }

  public void testLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(200, 200, 30 * unitSize, 100 * unitSize));
  }

  public void testNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(200, 200, 50 * unitSize, 100 * unitSize));
  }

  public void testSmallFiles() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(200, 200, 1 * unitSize, 5 * unitSize));
  }

  public void testSmallTasks() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(5, 5, 10 * unitSize, 100 * unitSize));
  }

  private Severity analyzeJob(int numSmallTasks, int numLargeTasks, long smallInputSize, long largeInputSize)
      throws IOException {
    HadoopCounterHolder jobCounter = new HadoopCounterHolder(null);
    HadoopTaskData[] mappers = new HadoopTaskData[numSmallTasks + numLargeTasks];

    HadoopCounterHolder smallCounter = new HadoopCounterHolder(new HashMap<CounterName,Long>());
    smallCounter.set(HadoopCounterHolder.CounterName.HDFS_BYTES_READ, smallInputSize);

    HadoopCounterHolder largeCounter = new HadoopCounterHolder(new HashMap<CounterName,Long>());
    largeCounter.set(HadoopCounterHolder.CounterName.HDFS_BYTES_READ, largeInputSize);

    int i = 0;
    for (; i < numSmallTasks; i++) {
      mappers[i] = new HadoopTaskData(smallCounter, new long[4]);
    }
    for (; i < numSmallTasks + numLargeTasks; i++) {
      mappers[i] = new HadoopTaskData(largeCounter, new long[4]);
    }

    HadoopJobData data = new HadoopJobData().setCounters(jobCounter).setMapperData(mappers);
    HeuristicResult result = heuristic.apply(data);
    return result.getSeverity();

  }
}
