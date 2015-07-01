package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.MapReduceCounterHolder;
import com.linkedin.drelephant.mapreduce.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.MapReduceTaskData;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;

import junit.framework.TestCase;


public class MapperMemoryHeuristicTest extends TestCase {
  Heuristic _heuristic = new MapperMemoryHeuristic();
  private int NUMTASKS = 100;

  public void testLargeContainerSizeCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(2048, 8192));
  }

  public void testLargeContainerSizeSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(3072, 8192));
  }

  public void testLargeContainerSizeModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(4096, 8192));
  }

  public void testLargeContainerSizeNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(6144, 8192));
  }

  // If the task use default container size, it should not be flagged
  public void testDefaultContainerNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(256, 2048));
  }

  public void testDefaultContainerNoneMore() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(1024, 2048));
  }

  private Severity analyzeJob(long taskAvgMemMB, long containerMemMB) throws IOException {
    MapReduceCounterHolder jobCounter = new MapReduceCounterHolder();
    MapReduceTaskData[] mappers = new MapReduceTaskData[NUMTASKS];

    MapReduceCounterHolder counter = new MapReduceCounterHolder();
    counter.set(MapReduceCounterHolder.CounterName.PHYSICAL_MEMORY_BYTES, taskAvgMemMB* FileUtils.ONE_MB);

    Properties p = new Properties();
    p.setProperty(MapperMemoryHeuristic.MAPPER_MEMORY_CONF, Long.toString(containerMemMB));

    int i = 0;
    for (; i < NUMTASKS; i++) {
      mappers[i] = new MapReduceTaskData(counter, new long[3]);
    }

    MapReduceApplicationData data = new MapReduceApplicationData().setCounters(jobCounter).setMapperData(mappers);
    data.setJobConf(p);
    HeuristicResult result = _heuristic.apply(data);
    return result.getSeverity();
  }
}
