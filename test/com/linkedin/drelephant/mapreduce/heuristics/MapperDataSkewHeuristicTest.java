package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.analysis.HDFSContext;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.MapReduceCounterHolder;
import com.linkedin.drelephant.mapreduce.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.MapReduceTaskData;

import java.io.IOException;

import junit.framework.TestCase;


public class MapperDataSkewHeuristicTest extends TestCase {

  private static final long UNITSIZE = HDFSContext.HDFS_BLOCK_SIZE / 64; //1MB
  Heuristic _heuristic = new MapperDataSkewHeuristic();

  public void testCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(200, 200, 1 * UNITSIZE, 100 * UNITSIZE));
  }

  public void testSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(200, 200, 10 * UNITSIZE, 100 * UNITSIZE));
  }

  public void testModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(200, 200, 20 * UNITSIZE, 100 * UNITSIZE));
  }

  public void testLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(200, 200, 30 * UNITSIZE, 100 * UNITSIZE));
  }

  public void testNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(200, 200, 50 * UNITSIZE, 100 * UNITSIZE));
  }

  public void testSmallFiles() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(200, 200, 1 * UNITSIZE, 5 * UNITSIZE));
  }

  public void testSmallTasks() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(5, 5, 10 * UNITSIZE, 100 * UNITSIZE));
  }

  private Severity analyzeJob(int numSmallTasks, int numLargeTasks, long smallInputSize, long largeInputSize)
      throws IOException {
    MapReduceCounterHolder jobCounter = new MapReduceCounterHolder();
    MapReduceTaskData[] mappers = new MapReduceTaskData[numSmallTasks + numLargeTasks];

    MapReduceCounterHolder smallCounter = new MapReduceCounterHolder();
    smallCounter.set(MapReduceCounterHolder.CounterName.HDFS_BYTES_READ, smallInputSize);

    MapReduceCounterHolder largeCounter = new MapReduceCounterHolder();
    largeCounter.set(MapReduceCounterHolder.CounterName.HDFS_BYTES_READ, largeInputSize);

    int i = 0;
    for (; i < numSmallTasks; i++) {
      mappers[i] = new MapReduceTaskData(smallCounter, new long[3]);
    }
    for (; i < numSmallTasks + numLargeTasks; i++) {
      mappers[i] = new MapReduceTaskData(largeCounter, new long[3]);
    }

    MapReduceApplicationData data = new MapReduceApplicationData().setCounters(jobCounter).setMapperData(mappers);
    HeuristicResult result = _heuristic.apply(data);
    return result.getSeverity();

  }
}
