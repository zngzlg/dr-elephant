package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.MapReduceCounterHolder;
import com.linkedin.drelephant.mapreduce.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.MapReduceTaskData;
import java.io.IOException;
import junit.framework.TestCase;


public class MapperSpillHeuristicTest extends TestCase {

  Heuristic heuristic = new MapperSpillHeuristic();
  private static final int numTasks = 100;

  public void testCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(3000, 1000));
  }

  public void testSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(2000, 1000));
  }

  public void testModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(1980, 1000));
  }

  public void testLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(1900, 1000));
  }

  public void testNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(1000, 1000));
  }

  private Severity analyzeJob(long spilledRecords, long mapRecords) throws IOException {
    MapReduceCounterHolder jobCounter = new MapReduceCounterHolder();
    MapReduceTaskData[] mappers = new MapReduceTaskData[numTasks];

    MapReduceCounterHolder counter = new MapReduceCounterHolder();
    counter.set(MapReduceCounterHolder.CounterName.SPILLED_RECORDS, spilledRecords);
    counter.set(MapReduceCounterHolder.CounterName.MAP_OUTPUT_RECORDS, mapRecords);

    for (int i=0; i < numTasks; i++) {
      mappers[i] = new MapReduceTaskData(counter, new long[4]);
    }

    MapReduceApplicationData data = new MapReduceApplicationData().setCounters(jobCounter).setMapperData(mappers);
    HeuristicResult result = heuristic.apply(data);
    return result.getSeverity();
  }
}
