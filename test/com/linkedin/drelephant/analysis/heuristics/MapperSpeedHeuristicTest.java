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
import com.linkedin.drelephant.math.Statistics;

import junit.framework.TestCase;


public class MapperSpeedHeuristicTest extends TestCase {
  Heuristic _heuristic = new MapperSpeedHeuristic();
  private static final long UNITSIZE = Constants.HDFS_BLOCK_SIZE / 64;
  private static final long MINUTE = Statistics.MINUTE;
  private static final int NUMTASKS = Constants.SHUFFLE_SORT_MAX_SAMPLE_SIZE;

  public void testCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(120 * MINUTE, 10000 * UNITSIZE));
  }

  public void testSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(120 * MINUTE, 50000 * UNITSIZE));
  }

  public void testModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(120 * MINUTE, 100000 * UNITSIZE));
  }

  public void testLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(120 * MINUTE, 200000 * UNITSIZE));
  }

  public void testNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(120 * MINUTE, 500000 * UNITSIZE));
  }

  public void testShortTask() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(2 * MINUTE, 10 * UNITSIZE));
  }

  private Severity analyzeJob(long runtime, long readBytes) throws IOException {
    HadoopCounterHolder jobCounter = new HadoopCounterHolder(null);
    HadoopTaskData[] mappers = new HadoopTaskData[NUMTASKS];

    HadoopCounterHolder counter = new HadoopCounterHolder(new HashMap<CounterName, Long>());
    counter.set(HadoopCounterHolder.CounterName.HDFS_BYTES_READ, readBytes);

    int i = 0;
    for (; i < NUMTASKS; i++) {
      mappers[i] = new HadoopTaskData(counter, new long[] { 0, runtime, 0, 0 });
    }

    HadoopJobData data = new HadoopJobData().setCounters(jobCounter).setMapperData(mappers);
    HeuristicResult result = _heuristic.apply(data);
    return result.getSeverity();
  }
}
