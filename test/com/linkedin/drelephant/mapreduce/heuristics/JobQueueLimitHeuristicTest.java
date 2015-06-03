package com.linkedin.drelephant.mapreduce.heuristics;

import java.io.IOException;
import java.util.Properties;

import org.junit.Test;

import junit.framework.TestCase;

import com.linkedin.drelephant.analysis.HadoopSystemContext;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.MapReduceCounterHolder;
import com.linkedin.drelephant.mapreduce.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.MapReduceTaskData;


public class JobQueueLimitHeuristicTest extends TestCase {

  Heuristic _heuristic = new JobQueueLimitHeuristic();
  private static final int NUM_TASKS = HadoopSystemContext.SHUFFLE_SORT_MAX_SAMPLE_SIZE;

  @Test
  public void testRuntimeCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob((long) (14.5 * 60 * 1000), "default"));
  }

  public void testRuntimeSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(14 * 60 * 1000, "default"));
  }

  public void testRuntimeModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob((long) (13.5 * 60 * 1000), "default"));
  }

  public void testRuntimeLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(13 * 60 * 1000, "default"));
  }

  public void testRuntimeNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(12 * 60 * 1000, "default"));
  }

  public void testNonDefaultRuntimeNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(15 * 60 * 1000, "non-default"));
  }

  private Severity analyzeJob(long runtimeMs, String queueName) throws IOException {
    MapReduceCounterHolder dummyCounter = new MapReduceCounterHolder();
    MapReduceTaskData[] mappers = new MapReduceTaskData[2 * NUM_TASKS / 3];
    MapReduceTaskData[] reducers = new MapReduceTaskData[NUM_TASKS / 3];
    Properties jobConf = new Properties();
    jobConf.put("mapred.job.queue.name", queueName);
    int i = 0;
    for (; i < 2 * NUM_TASKS / 3; i++) {
      mappers[i] = new MapReduceTaskData(dummyCounter, new long[] { runtimeMs, 0, 0 });
    }
    for (i = 0; i < NUM_TASKS / 3; i++) {
      reducers[i] = new MapReduceTaskData(dummyCounter, new long[] { runtimeMs, 0, 0 });
    }
    MapReduceApplicationData data =
        new MapReduceApplicationData().setCounters(dummyCounter).setReducerData(reducers).setMapperData(mappers)
            .setJobConf(jobConf);
    HeuristicResult result = _heuristic.apply(data);
    return result.getSeverity();
  }
}
