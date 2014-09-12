package com.linkedin.drelephant.analysis.heuristics;

import java.io.IOException;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;
import com.linkedin.drelephant.math.Statistics;

import junit.framework.TestCase;


public class ShuffleSortHeuristicTest extends TestCase {
  Heuristic _heuristic = new ShuffleSortHeuristic();
  private static final int NUMTASKS = Constants.SHUFFLE_SORT_MAX_SAMPLE_SIZE;
  private static final long MINUTE = Statistics.MINUTE;;

  public void testLongShuffleCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(30 * MINUTE, 0, 5 * MINUTE));
  }

  public void testLongShuffleSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(30 * MINUTE, 0, 10 * MINUTE));
  }

  public void testLongShuffleModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(30 * MINUTE, 0, 20 * MINUTE));
  }

  public void testLongShuffleLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(30 * MINUTE, 0, 40 * MINUTE));
  }

  public void testLongShuffleNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(30 * MINUTE, 0, 80 * MINUTE));
  }

  public void testLongSortCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(0, 30 * MINUTE, 5 * MINUTE));
  }

  public void testLongSortSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(0, 30 * MINUTE, 10 * MINUTE));
  }

  public void testLongSortModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(0, 30 * MINUTE, 20 * MINUTE));
  }

  public void testLongSortLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(0, 30 * MINUTE, 40 * MINUTE));
  }

  public void testLongSortNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(0, 30 * MINUTE, 80 * MINUTE));
  }

  public void testShortShuffle() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(MINUTE / 2, 0, MINUTE / 2));
  }

  public void testShortSort() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(0, MINUTE / 2, MINUTE / 2));
  }

  private Severity analyzeJob(long shuffleTime, long sortTime, long reduceTime) throws IOException {
    HadoopCounterHolder dummyCounter = new HadoopCounterHolder(null);
    HadoopTaskData[] reducers = new HadoopTaskData[NUMTASKS];

    int i = 0;
    for (; i < NUMTASKS; i++) {
      reducers[i] =
          new HadoopTaskData(dummyCounter, new long[] { 0, shuffleTime + sortTime + reduceTime, shuffleTime, sortTime });
    }
    HadoopJobData data = new HadoopJobData().setCounters(dummyCounter).setReducerData(reducers);
    HeuristicResult result = _heuristic.apply(data);
    return result.getSeverity();
  }

}
