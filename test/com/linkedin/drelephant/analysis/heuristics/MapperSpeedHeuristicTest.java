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
  Heuristic heuristic = new MapperSpeedHeuristic();
  private static final long unitSize = Constants.HDFS_BLOCK_SIZE / 64;
  private static final long minute = Statistics.MINUTE;
  private static final int numTasks = Constants.SHUFFLE_SORT_MAX_SAMPLE_SIZE;

  public void testCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(120 * minute, 10000 * unitSize));
  }

  public void testSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(120 * minute, 50000 * unitSize));
  }

  public void testModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(120 * minute, 100000 * unitSize));
  }

  public void testLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(120 * minute, 200000 * unitSize));
  }

  public void testNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(120 * minute, 500000 * unitSize));
  }

  public void testShortTask() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(2 * minute, 10 * unitSize));
  }

  private Severity analyzeJob(long runtime, long readBytes) throws IOException {
    HadoopCounterHolder jobCounter = new HadoopCounterHolder(null);
    HadoopTaskData[] mappers = new HadoopTaskData[numTasks];

    HadoopCounterHolder counter = new HadoopCounterHolder(new HashMap<CounterName,Long>());
    counter.set(HadoopCounterHolder.CounterName.HDFS_BYTES_READ, readBytes);

    int i = 0;
    for (; i < numTasks; i++) {
      mappers[i] = new HadoopTaskData(counter, new long[]{0,runtime,0,0});
    }

    HadoopJobData data = new HadoopJobData().setCounters(jobCounter).setMapperData(mappers);
    HeuristicResult result = heuristic.apply(data);
    return result.getSeverity();
  }
}
