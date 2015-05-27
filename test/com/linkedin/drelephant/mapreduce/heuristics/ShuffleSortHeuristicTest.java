package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.mapreduce.heuristics.ShuffleSortHeuristic;
import java.io.IOException;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.HadoopCounterHolder;
import com.linkedin.drelephant.mapreduce.MapreduceApplicationData;
import com.linkedin.drelephant.mapreduce.HadoopTaskData;
import com.linkedin.drelephant.math.Statistics;

import junit.framework.TestCase;


public class ShuffleSortHeuristicTest extends TestCase {
  Heuristic _heuristic = new ShuffleSortHeuristic();
  private static final int NUMTASKS = Constants.SHUFFLE_SORT_MAX_SAMPLE_SIZE;
  private static final long MINUTE_IN_MS = Statistics.MINUTE_IN_MS;;

  public void testLongShuffleCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(30 * MINUTE_IN_MS, 0, 5 * MINUTE_IN_MS));
  }

  public void testLongShuffleSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(30 * MINUTE_IN_MS, 0, 10 * MINUTE_IN_MS));
  }

  public void testLongShuffleModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(30 * MINUTE_IN_MS, 0, 20 * MINUTE_IN_MS));
  }

  public void testLongShuffleLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(30 * MINUTE_IN_MS, 0, 40 * MINUTE_IN_MS));
  }

  public void testLongShuffleNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(30 * MINUTE_IN_MS, 0, 80 * MINUTE_IN_MS));
  }

  public void testLongSortCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(0, 30 * MINUTE_IN_MS, 5 * MINUTE_IN_MS));
  }

  public void testLongSortSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(0, 30 * MINUTE_IN_MS, 10 * MINUTE_IN_MS));
  }

  public void testLongSortModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(0, 30 * MINUTE_IN_MS, 20 * MINUTE_IN_MS));
  }

  public void testLongSortLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(0, 30 * MINUTE_IN_MS, 40 * MINUTE_IN_MS));
  }

  public void testLongSortNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(0, 30 * MINUTE_IN_MS, 80 * MINUTE_IN_MS));
  }

  public void testShortShuffle() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(MINUTE_IN_MS / 2, 0, MINUTE_IN_MS / 2));
  }

  public void testShortSort() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(0, MINUTE_IN_MS / 2, MINUTE_IN_MS / 2));
  }

  private Severity analyzeJob(long shuffleTimeMs, long sortTimeMs, long reduceTimeMs) throws IOException {
    HadoopCounterHolder dummyCounter = new HadoopCounterHolder();
    HadoopTaskData[] reducers = new HadoopTaskData[NUMTASKS];

    int i = 0;
    for (; i < NUMTASKS; i++) {
      reducers[i] = new HadoopTaskData(dummyCounter,
        new long[] { shuffleTimeMs + sortTimeMs + reduceTimeMs, shuffleTimeMs, sortTimeMs });
    }
    MapreduceApplicationData data = new MapreduceApplicationData().setCounters(dummyCounter).setReducerData(reducers);
    HeuristicResult result = _heuristic.apply(data);
    return result.getSeverity();
  }

}
