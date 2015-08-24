/*
 * Copyright 2015 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.linkedin.drelephant.mapreduce.heuristics;

import java.io.IOException;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.MapReduceCounterHolder;
import com.linkedin.drelephant.mapreduce.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.MapReduceTaskData;
import com.linkedin.drelephant.math.Statistics;

import junit.framework.TestCase;


public class ReducerTimeHeuristicTest extends TestCase {
  Heuristic _heuristic = new ReducerTimeHeuristic();
  private static final long MINUTE_IN_MS = Statistics.MINUTE_IN_MS;;

  public void testShortRunetimeCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(1 * MINUTE_IN_MS, 1000));
  }

  public void testShortRunetimeSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(1 * MINUTE_IN_MS, 500));
  }

  public void testShortRunetimeModerate() throws IOException {
    assertEquals(Severity.MODERATE, analyzeJob(1 * MINUTE_IN_MS, 101));
  }

  public void testShortRunetimeLow() throws IOException {
    assertEquals(Severity.LOW, analyzeJob(1 * MINUTE_IN_MS, 50));
  }

  public void testShortRunetimeNone() throws IOException {
    assertEquals(Severity.NONE, analyzeJob(1 * MINUTE_IN_MS, 2));
  }

  public void testLongRunetimeCritical() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(120 * MINUTE_IN_MS, 10));
  }

  // Long runtime severity is not affected by number of tasks
  public void testLongRunetimeCriticalMore() throws IOException {
    assertEquals(Severity.CRITICAL, analyzeJob(120 * MINUTE_IN_MS, 1000));
  }

  public void testLongRunetimeSevere() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(60 * MINUTE_IN_MS, 10));
  }

  public void testLongRunetimeSevereMore() throws IOException {
    assertEquals(Severity.SEVERE, analyzeJob(60 * MINUTE_IN_MS, 1000));
  }

  private Severity analyzeJob(long runtimeMs, int numTasks) throws IOException {
    MapReduceCounterHolder dummyCounter = new MapReduceCounterHolder();
    MapReduceTaskData[] reducers = new MapReduceTaskData[numTasks];

    int i = 0;
    for (; i < numTasks; i++) {
      reducers[i] = new MapReduceTaskData(dummyCounter, new long[] { runtimeMs, 0, 0 });
    }

    MapReduceApplicationData data = new MapReduceApplicationData().setCounters(dummyCounter).setReducerData(reducers);
    HeuristicResult result = _heuristic.apply(data);
    return result.getSeverity();
  }
}
