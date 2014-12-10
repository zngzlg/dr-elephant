package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;
import java.io.IOException;
import junit.framework.TestCase;


public class ReducerDataSkewHeuristicTest extends TestCase {
  private static final long UNITSIZE = Constants.HDFS_BLOCK_SIZE / 64;
  Heuristic _heuristic = new ReducerDataSkewHeuristic();

  public void testCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(200, 200, 1 * UNITSIZE, 100 * UNITSIZE));
  }

  public void testSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(200, 200, 10 * UNITSIZE, 100 * UNITSIZE));
  }

  public void testModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(200, 200, 20 * UNITSIZE, 100 * UNITSIZE));
  }

  public void testLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(200, 200, 30 * UNITSIZE, 100 * UNITSIZE));
  }

  public void testNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(200, 200, 50 * UNITSIZE, 100 * UNITSIZE));
  }

  public void testSmallFiles() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(200, 200, 1 * UNITSIZE, 5 * UNITSIZE));
  }

  public void testSmallTasks() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(5, 5, 10 * UNITSIZE, 100 * UNITSIZE));
  }

  private Severity analyzeJob(int numSmallTasks, int numLargeTasks, long smallInputSize, long largeInputSize)
      throws IOException {
    HadoopCounterHolder jobCounter = new HadoopCounterHolder();
    HadoopTaskData[] reducers = new HadoopTaskData[numSmallTasks + numLargeTasks];

    HadoopCounterHolder smallCounter = new HadoopCounterHolder();
    smallCounter.set(HadoopCounterHolder.CounterName.REDUCE_SHUFFLE_BYTES, smallInputSize);

    HadoopCounterHolder largeCounter = new HadoopCounterHolder();
    largeCounter.set(HadoopCounterHolder.CounterName.REDUCE_SHUFFLE_BYTES, largeInputSize);

    int i = 0;
    for (; i < numSmallTasks; i++) {
      reducers[i] = new HadoopTaskData(smallCounter, new long[3]);
    }
    for (; i < numSmallTasks + numLargeTasks; i++) {
      reducers[i] = new HadoopTaskData(largeCounter, new long[3]);
    }

    HadoopJobData data = new HadoopJobData().setCounters(jobCounter).setReducerData(reducers);
    HeuristicResult result = _heuristic.apply(data);
    return result.getSeverity();
  }
}
