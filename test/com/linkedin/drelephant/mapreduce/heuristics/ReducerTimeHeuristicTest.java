package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.mapreduce.heuristics.ReducerTimeHeuristic;
import java.io.IOException;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.HadoopCounterHolder;
import com.linkedin.drelephant.mapreduce.MapreduceApplicationData;
import com.linkedin.drelephant.mapreduce.HadoopTaskData;
import com.linkedin.drelephant.math.Statistics;

import junit.framework.TestCase;


public class ReducerTimeHeuristicTest extends TestCase {
  Heuristic _heuristic = new ReducerTimeHeuristic();
  private static final long MINUTE_IN_MS = Statistics.MINUTE_IN_MS;;

  public void testShortRunetimeCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(1 * MINUTE_IN_MS, 500));
  }

  public void testShortRunetimeSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(1 * MINUTE_IN_MS, 200));
  }

  public void testShortRunetimeModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(1 * MINUTE_IN_MS, 51));
  }

  public void testShortRunetimeLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(1 * MINUTE_IN_MS, 50));
  }

  public void testShortRunetimeNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(1 * MINUTE_IN_MS, 2));
  }

  public void testLongRunetimeCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(120 * MINUTE_IN_MS, 10));
  }

  public void testLongRunetimeSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(120 * MINUTE_IN_MS, 20));
  }

  public void testLongRunetimeModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(120 * MINUTE_IN_MS, 40));
  }

  public void testLongRunetimeLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(120 * MINUTE_IN_MS, 100));
  }

  public void testLongRunetimeNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(120 * MINUTE_IN_MS, 200));
  }

  private Severity analyzeJob(long runtimeMs, int numTasks) throws IOException {
    HadoopCounterHolder dummyCounter = new HadoopCounterHolder();
    HadoopTaskData[] reducers = new HadoopTaskData[numTasks];

    int i = 0;
    for (; i < numTasks; i++) {
      reducers[i] = new HadoopTaskData(dummyCounter, new long[] { runtimeMs, 0, 0 });
    }

    MapreduceApplicationData data = new MapreduceApplicationData().setCounters(dummyCounter).setReducerData(reducers);
    HeuristicResult result = _heuristic.apply(data);
    return result.getSeverity();
  }
}
