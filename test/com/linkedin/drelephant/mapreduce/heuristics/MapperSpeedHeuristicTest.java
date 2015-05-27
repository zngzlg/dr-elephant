package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.HadoopCounterHolder;
import com.linkedin.drelephant.mapreduce.MapreduceApplicationData;
import com.linkedin.drelephant.mapreduce.HadoopTaskData;
import com.linkedin.drelephant.mapreduce.heuristics.MapperSpeedHeuristic;
import com.linkedin.drelephant.math.Statistics;
import java.io.IOException;
import junit.framework.TestCase;


public class MapperSpeedHeuristicTest extends TestCase {
  Heuristic _heuristic = new MapperSpeedHeuristic();
  private static final long UNITSIZE = Constants.HDFS_BLOCK_SIZE / 64;
  private static final long MINUTE_IN_MS = Statistics.MINUTE_IN_MS;
  private static final int NUMTASKS = Constants.SHUFFLE_SORT_MAX_SAMPLE_SIZE;

  public void testCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(120 * MINUTE_IN_MS, 10000 * UNITSIZE));
  }

  public void testSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(120 * MINUTE_IN_MS, 50000 * UNITSIZE));
  }

  public void testModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(120 * MINUTE_IN_MS, 100000 * UNITSIZE));
  }

  public void testLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(120 * MINUTE_IN_MS, 200000 * UNITSIZE));
  }

  public void testNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(120 * MINUTE_IN_MS, 500000 * UNITSIZE));
  }

  public void testShortTask() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(2 * MINUTE_IN_MS, 10 * UNITSIZE));
  }

  private Severity analyzeJob(long runtimeMs, long readBytes) throws IOException {
    HadoopCounterHolder jobCounter = new HadoopCounterHolder();
    HadoopTaskData[] mappers = new HadoopTaskData[NUMTASKS];

    HadoopCounterHolder counter = new HadoopCounterHolder();
    counter.set(HadoopCounterHolder.CounterName.HDFS_BYTES_READ, readBytes);

    int i = 0;
    for (; i < NUMTASKS; i++) {
      mappers[i] = new HadoopTaskData(counter, new long[] { runtimeMs, 0, 0 });
    }

    MapreduceApplicationData data = new MapreduceApplicationData().setCounters(jobCounter).setMapperData(mappers);
    HeuristicResult result = _heuristic.apply(data);
    return result.getSeverity();
  }
}
