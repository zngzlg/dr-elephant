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
import com.linkedin.drelephant.mapreduce.MapReduceCounterHolder;
import java.util.ArrayList;
import java.util.List;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.MapReduceTaskData;
import com.linkedin.drelephant.math.Statistics;

import org.apache.commons.io.FileUtils;


public class MapperTimeHeuristic implements Heuristic<MapReduceApplicationData> {
  public static final String HEURISTIC_NAME = "Mapper Time";

  @Override
  public String getHeuristicName() {
    return HEURISTIC_NAME;
  }

  @Override
  public HeuristicResult apply(MapReduceApplicationData data) {

    if(!data.getSucceeded()) {
      return null;
    }

    MapReduceTaskData[] tasks = data.getMapperData();

    List<Long> inputBytes = new ArrayList<Long>();
    List<Long> runtimesMs = new ArrayList<Long>();
    long taskMinMs = Long.MAX_VALUE;
    long taskMaxMs = 0;

    for (MapReduceTaskData task : tasks) {
      inputBytes.add(task.getCounters().get(MapReduceCounterHolder.CounterName.HDFS_BYTES_READ));
      if (task.timed()) {
        long taskTime = task.getTotalRunTimeMs();
        runtimesMs.add(taskTime);
        taskMinMs = Math.min(taskMinMs, taskTime);
        taskMaxMs = Math.max(taskMaxMs, taskTime);
      }
    }

    if(taskMinMs == Long.MAX_VALUE) {
      taskMinMs = 0;
    }

    long averageSize = Statistics.average(inputBytes);
    long averageTimeMs = Statistics.average(runtimesMs);

    Severity shortTaskSeverity = shortTaskSeverity(tasks.length, averageTimeMs);
    Severity longTaskSeverity = longTaskSeverity(tasks.length, averageTimeMs);
    Severity severity = Severity.max(shortTaskSeverity, longTaskSeverity);

    HeuristicResult result = new HeuristicResult(HEURISTIC_NAME, severity);

    result.addDetail("Number of tasks", Integer.toString(tasks.length));
    result.addDetail("Average task input size", FileUtils.byteCountToDisplaySize(averageSize));
    result.addDetail("Average task runtime", Statistics.readableTimespan(averageTimeMs));
    result.addDetail("Max task runtime", Statistics.readableTimespan(taskMaxMs));
    result.addDetail("Min task runtime", Statistics.readableTimespan(taskMinMs));

    return result;
  }

  private Severity shortTaskSeverity(long numTasks, long averageTimeMs) {
    // We want to identify jobs with short task runtime
    Severity severity = getShortRuntimeSeverity(averageTimeMs);
    // Severity is reduced if number of tasks is small.
    Severity numTaskSeverity = getNumTasksSeverity(numTasks);
    return Severity.min(severity, numTaskSeverity);
  }

  private Severity longTaskSeverity(long numTasks, long averageTimeMs) {
    // We want to identify jobs with long task runtime. Severity is NOT reduced if num of tasks is large
    return getLongRuntimeSeverity(averageTimeMs);
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
