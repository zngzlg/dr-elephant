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

  public void testCritical() throws IOException {
    // Spill ratio 3.0, 1000 tasks
    assertEquals(Severity.CRITICAL, analyzeJob(3000, 1000, 1000));
  }

  public void testSevere() throws IOException {
    // Spill ratio 2.5, 1000 tasks
    assertEquals(Severity.SEVERE, analyzeJob(2500, 1000, 1000));
  }

  public void testModerate() throws IOException {
    // Spill ratio 2.3, 1000 tasks
    assertEquals(Severity.MODERATE, analyzeJob(2300, 1000, 1000));
  }

  public void testLow() throws IOException {
    // Spill ratio 2.1, 1000 tasks
    assertEquals(Severity.LOW, analyzeJob(2100, 1000, 1000));
  }

  public void testNone() throws IOException {
    // Spill ratio 1.0, 1000 tasks
    assertEquals(Severity.NONE, analyzeJob(1000, 1000, 1000));
  }

  public void testSmallNumTasks() throws IOException {
    // Spill ratio 3.0, should be critical, but number of task is small(10), final result is NONE
    assertEquals(Severity.NONE, analyzeJob(3000, 1000, 10));
  }

  private Severity analyzeJob(long spilledRecords, long mapRecords, int numTasks) throws IOException {
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
