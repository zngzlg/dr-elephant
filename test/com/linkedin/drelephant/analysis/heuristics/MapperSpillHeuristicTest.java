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


public class MapperSpillHeuristicTest extends TestCase {

  Heuristic heuristic = new MapperSpillHeuristic();
  private static final int numTasks = Constants.SHUFFLE_SORT_MAX_SAMPLE_SIZE;

  public void testCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(2200, 1000));
  }

  public void testSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(1755, 1000));
  }

  public void testModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(1555, 1000));
  }

  public void testLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(1352, 1000));
  }

  public void testNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(1000, 1000));
  }

  private Severity analyzeJob(long spilledRecords, long mapRecords) throws IOException {
    HadoopCounterHolder jobCounter = new HadoopCounterHolder(null);
    HadoopTaskData[] mappers = new HadoopTaskData[numTasks];

    HadoopCounterHolder counter = new HadoopCounterHolder(new HashMap<CounterName, Long>());
    counter.set(HadoopCounterHolder.CounterName.SPILLED_RECORDS, spilledRecords);
    counter.set(HadoopCounterHolder.CounterName.MAP_OUTPUT_RECORDS, mapRecords);

    for (int i=0; i < numTasks; i++) {
      mappers[i] = new HadoopTaskData(counter, new long[] { 0, 5, 5, 5 });
    }

    HadoopJobData data = new HadoopJobData().setCounters(jobCounter).setMapperData(mappers);
    HeuristicResult result = heuristic.apply(data);
    return result.getSeverity();
  }
}
