package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.MapReduceCounterHolder;
import com.linkedin.drelephant.mapreduce.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.MapReduceTaskData;

import java.io.IOException;
import junit.framework.TestCase;


public class MapperGCHeuristicTest extends TestCase {
  Heuristic _heuristic = new MapperGCHeuristic();
  private static int NUMTASKS = 100;

  public void testGCCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(1000000, 50000, 2000));
  }

  public void testGCSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(1000000, 50000, 1500));
  }

  public void testGCModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(1000000, 50000, 1000));
  }

  public void testGCNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(1000000, 50000, 300));
  }

  public void testShortTasksNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(100000, 50000, 2000));
  }


  private Severity analyzeJob(long runtimeMs, long cpuMs, long gcMs) throws IOException {
    MapReduceCounterHolder jobCounter = new MapReduceCounterHolder();
    MapReduceTaskData[] mappers = new MapReduceTaskData[NUMTASKS];

    MapReduceCounterHolder counter = new MapReduceCounterHolder();
    counter.set(MapReduceCounterHolder.CounterName.CPU_MILLISECONDS, cpuMs);
    counter.set(MapReduceCounterHolder.CounterName.GC_MILLISECONDS, gcMs);

    int i = 0;
    for (; i < NUMTASKS; i++) {
      mappers[i] = new MapReduceTaskData(counter, new long[]{runtimeMs, 0 , 0});
    }

    MapReduceApplicationData data = new MapReduceApplicationData().setCounters(jobCounter).setMapperData(mappers);
    HeuristicResult result = _heuristic.apply(data);
    return result.getSeverity();
  }
}
