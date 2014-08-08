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
  Heuristic heuristic = new ShuffleSortHeuristic();
  private static final int numTasks = Constants.SHUFFLE_SORT_MAX_SAMPLE_SIZE;
  private static final long minute = Statistics.MINUTE;;

  public void testLongShuffleCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(30 * minute, 0, 5 * minute));
  }

  public void testLongShuffleSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(30 * minute, 0, 10 * minute));
  }

  public void testLongShuffleModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(30 * minute, 0, 20 * minute));
  }

  public void testLongShuffleLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(30 * minute, 0, 40 * minute));
  }

  public void testLongShuffleNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(30 * minute, 0, 80 * minute));
  }

  public void testLongSortCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(0, 30 * minute, 5 * minute));
  }

  public void testLongSortSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(0, 30 * minute, 10 * minute));
  }

  public void testLongSortModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(0, 30 * minute, 20 * minute));
  }

  public void testLongSortLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(0, 30 * minute, 40 * minute));
  }

  public void testLongSortNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(0, 30 * minute, 80 * minute));
  }

  public void testShortShuffle() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(minute / 2, 0, minute / 2));
  }

  public void testShortSort() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(0, minute / 2, minute / 2));
  }

  private Severity analyzeJob(long shuffleTime, long sortTime, long reduceTime) throws IOException {
    HadoopCounterHolder dummyCounter = new HadoopCounterHolder(null);
    HadoopTaskData[] reducers = new HadoopTaskData[numTasks];

    int i = 0;
    for (; i < numTasks; i++) {
      reducers[i] = new HadoopTaskData(dummyCounter,  new long[]{ 0, shuffleTime + sortTime + reduceTime, shuffleTime, sortTime});
    }
    HadoopJobData data = new HadoopJobData().setCounters(dummyCounter).setReducerData(reducers);
    HeuristicResult result = heuristic.apply(data);
    return result.getSeverity();
  }

}
