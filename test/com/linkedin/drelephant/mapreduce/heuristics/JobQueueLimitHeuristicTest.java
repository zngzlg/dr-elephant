package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.mapreduce.heuristics.JobQueueLimitHeuristic;
import java.io.IOException;
import java.util.Properties;

import org.junit.Test;

import junit.framework.TestCase;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.HadoopCounterHolder;
import com.linkedin.drelephant.mapreduce.MapreduceApplicationData;
import com.linkedin.drelephant.mapreduce.HadoopTaskData;


public class JobQueueLimitHeuristicTest extends TestCase {

  Heuristic _heuristic = new JobQueueLimitHeuristic();
  private static final int NUM_TASKS = Constants.SHUFFLE_SORT_MAX_SAMPLE_SIZE;

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
    HadoopCounterHolder dummyCounter = new HadoopCounterHolder();
    HadoopTaskData[] mappers = new HadoopTaskData[2 * NUM_TASKS / 3];
    HadoopTaskData[] reducers = new HadoopTaskData[NUM_TASKS / 3];
    Properties jobConf = new Properties();
    jobConf.put("mapred.job.queue.name", queueName);
    int i = 0;
    for (; i < 2 * NUM_TASKS / 3; i++) {
      mappers[i] = new HadoopTaskData(dummyCounter, new long[] { runtimeMs, 0, 0 });
    }
    for (i = 0; i < NUM_TASKS / 3; i++) {
      reducers[i] = new HadoopTaskData(dummyCounter, new long[] { runtimeMs, 0, 0 });
    }
    MapreduceApplicationData data =
        new MapreduceApplicationData().setCounters(dummyCounter).setReducerData(reducers).setMapperData(mappers)
            .setJobConf(jobConf);
    HeuristicResult result = _heuristic.apply(data);
    return result.getSeverity();
  }
}
