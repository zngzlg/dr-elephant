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

import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceCounterData;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.util.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.data.MapReduceTaskData;
import com.linkedin.drelephant.math.Statistics;

import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;


public class MapperTimeHeuristic implements Heuristic<MapReduceApplicationData> {
  private static final Logger logger = Logger.getLogger(MapperTimeHeuristic.class);
  public static final String HEURISTIC_NAME = "Mapper Time";

  // Severity parameters.
  private static final String SHORT_RUNTIME_SEVERITY = "short_runtime_severity_in_min";
  private static final String LONG_RUNTIME_SEVERITY = "long_runtime_severity_in_min";
  private static final String NUM_TASKS_SEVERITY = "num_tasks_severity";

  // Default value of parameters
  private double[] shortRuntimeLimits = {10, 4, 2, 1};     // Limits(ms) for tasks with shorter runtime
  private double[] longRuntimeLimits = {15, 30, 60, 120};  // Limits(ms) for tasks with longer runtime
  private double[] numTasksLimits = {50, 101, 500, 1000};  // Number of Map tasks.

  private HeuristicConfigurationData _heuristicConfData;

  private void loadParameters() {
    Map<String, String> paramMap = _heuristicConfData.getParamMap();

    if(paramMap.get(SHORT_RUNTIME_SEVERITY) != null) {
      double[] confShortThreshold = Utils.getParam(paramMap.get(SHORT_RUNTIME_SEVERITY), shortRuntimeLimits.length);
      if (confShortThreshold != null) {
        shortRuntimeLimits = confShortThreshold;
      }
    }
    logger.info(HEURISTIC_NAME + " will use " + SHORT_RUNTIME_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(shortRuntimeLimits));
    for (int i = 0; i < shortRuntimeLimits.length; i++) {
      shortRuntimeLimits[i] = shortRuntimeLimits[i] * Statistics.MINUTE_IN_MS;
    }

    if(paramMap.get(LONG_RUNTIME_SEVERITY) != null) {
      double[] confLongThreshold = Utils.getParam(paramMap.get(LONG_RUNTIME_SEVERITY), longRuntimeLimits.length);
      if (confLongThreshold != null) {
        longRuntimeLimits = confLongThreshold;
      }
    }
    logger.info(HEURISTIC_NAME + " will use " + LONG_RUNTIME_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(longRuntimeLimits));
    for (int i = 0; i < longRuntimeLimits.length; i++) {
      longRuntimeLimits[i] = longRuntimeLimits[i] * Statistics.MINUTE_IN_MS;
    }

    if(paramMap.get(NUM_TASKS_SEVERITY) != null) {
      double[] confNumTasksThreshold = Utils.getParam(paramMap.get(NUM_TASKS_SEVERITY), numTasksLimits.length);
      if (confNumTasksThreshold != null) {
        numTasksLimits = confNumTasksThreshold;
      }
    }
    logger.info(HEURISTIC_NAME + " will use " + NUM_TASKS_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(numTasksLimits));
  }

  public MapperTimeHeuristic(HeuristicConfigurationData heuristicConfData) {
    this._heuristicConfData = heuristicConfData;
    loadParameters();
  }

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
      inputBytes.add(task.getCounters().get(MapReduceCounterData.CounterName.HDFS_BYTES_READ));
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
    return Severity.getSeverityDescending(
            runtimeMs, shortRuntimeLimits[0], shortRuntimeLimits[1], shortRuntimeLimits[2], shortRuntimeLimits[3]);
  }

  private Severity getLongRuntimeSeverity(long runtimeMs) {
    return Severity.getSeverityAscending(
        runtimeMs, longRuntimeLimits[0], longRuntimeLimits[1], longRuntimeLimits[2], longRuntimeLimits[3]);
  }

  private Severity getNumTasksSeverity(long numTasks) {
    return Severity.getSeverityAscending(
        numTasks, numTasksLimits[0], numTasksLimits[1], numTasksLimits[2], numTasksLimits[3]);
  }
}
