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

import com.linkedin.drelephant.mapreduce.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.MapReduceTaskData;
import java.util.ArrayList;
import java.util.List;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.math.Statistics;


public class ReducerTimeHeuristic implements Heuristic<MapReduceApplicationData> {
  public static final String HEURISTIC_NAME = "Reducer Time";

  @Override
  public String getHeuristicName() {
    return HEURISTIC_NAME;
  }

  @Override
  public HeuristicResult apply(MapReduceApplicationData data) {

    if(!data.getSucceeded()) {
      return null;
    }

    MapReduceTaskData[] tasks = data.getReducerData();

    List<Long> runTimesMs = new ArrayList<Long>();
    long taskMinMs = Long.MAX_VALUE;
    long taskMaxMs = 0;

    for (MapReduceTaskData task : tasks) {
      if (task.timed()) {
        long taskTime = task.getTotalRunTimeMs();
        runTimesMs.add(taskTime);
        taskMinMs = Math.min(taskMinMs, taskTime);
        taskMaxMs = Math.max(taskMaxMs, taskTime);
      }
    }

    if(taskMinMs == Long.MAX_VALUE) {
      taskMinMs = 0;
    }

    //Analyze data
    long averageRuntimeMs = Statistics.average(runTimesMs);

    Severity shortTimeSeverity = shortTimeSeverity(averageRuntimeMs, tasks.length);
    Severity longTimeSeverity = longTimeSeverity(averageRuntimeMs, tasks.length);
    Severity severity = Severity.max(shortTimeSeverity, longTimeSeverity);

    HeuristicResult result = new HeuristicResult(HEURISTIC_NAME, severity);

    result.addDetail("Number of tasks", Integer.toString(tasks.length));
    result.addDetail("Average task runtime", Statistics.readableTimespan(averageRuntimeMs));
    result.addDetail("Max task runtime", Statistics.readableTimespan(taskMaxMs));
    result.addDetail("Min task runtime", Statistics.readableTimespan(taskMinMs));
    return result;
  }

  private Severity shortTimeSeverity(long runtimeMs, long numTasks) {
    Severity timeSeverity = getShortRuntimeSeverity(runtimeMs);
    // Severity is adjusted based on number of tasks
    Severity taskSeverity = getNumTasksSeverity(numTasks);
    return Severity.min(timeSeverity, taskSeverity);
  }

  private Severity longTimeSeverity(long runtimeMs, long numTasks) {
    // Severity is NOT adjusted based on number of tasks
    return getLongRuntimeSeverity(runtimeMs);
  }

  private Severity getShortRuntimeSeverity(long runtimeMs) {
    return Severity.getSeverityDescending(runtimeMs, 10 * Statistics.MINUTE_IN_MS, 4 * Statistics.MINUTE_IN_MS,
        2 * Statistics.MINUTE_IN_MS, 1 * Statistics.MINUTE_IN_MS);
  }

  private Severity getLongRuntimeSeverity(long runtimeMs) {
    return Severity.getSeverityAscending(runtimeMs, 15 * Statistics.MINUTE_IN_MS, 30 * Statistics.MINUTE_IN_MS, 1 * Statistics.HOUR_IN_MS,
        2 * Statistics.HOUR_IN_MS);
  }

  private Severity getNumTasksSeverity(long numTasks) {
    return Severity.getSeverityAscending(numTasks, 50, 101, 500, 1000);
  }
}
