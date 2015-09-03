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

import java.util.ArrayList;
import java.util.List;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.MapReduceCounterHolder;
import com.linkedin.drelephant.mapreduce.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.MapReduceTaskData;
import com.linkedin.drelephant.math.Statistics;

import org.apache.commons.io.FileUtils;


public abstract class GenericMemoryHeuristic implements Heuristic<MapReduceApplicationData> {
  private String _containerMemConf;
  private String _heuristicName;
  private long CONTAINER_MEMORY_DEFAULT_BYTES = 2048L * FileUtils.ONE_MB;

  @Override
  public String getHeuristicName() {
    return _heuristicName;
  }

  protected GenericMemoryHeuristic(String containerMemConf, String heuristicName) {
    this._heuristicName = heuristicName;
    this._containerMemConf = containerMemConf;
  }

  protected abstract MapReduceTaskData[] getTasks(MapReduceApplicationData data);

  @Override
  public HeuristicResult apply(MapReduceApplicationData data) {
    long containerMem = Long.parseLong(data.getConf().getProperty(_containerMemConf)) * FileUtils.ONE_MB;

    MapReduceTaskData[] tasks = getTasks(data);
    List<Long> taskPMems = new ArrayList<Long>();
    List<Long> taskVMems = new ArrayList<Long>();
    List<Long> runtimesMs = new ArrayList<Long>();
    long taskPMin = Long.MAX_VALUE;
    long taskPMax = 0;
    for (MapReduceTaskData task : tasks) {
      long taskPMem = task.getCounters().get(MapReduceCounterHolder.CounterName.PHYSICAL_MEMORY_BYTES);
      long taskVMem = task.getCounters().get(MapReduceCounterHolder.CounterName.VIRTUAL_MEMORY_BYTES);
      taskPMems.add(taskPMem);
      taskPMin = Math.min(taskPMin, taskPMem);
      taskPMax = Math.max(taskPMax, taskPMem);
      if (task.timed()) {
        runtimesMs.add(task.getTotalRunTimeMs());
      }
      taskVMems.add(taskVMem);
    }

    if(taskPMin == Long.MAX_VALUE) {
      taskPMin = 0;
    }

    long taskPMemAvg = Statistics.average(taskPMems);
    long taskVMemAvg = Statistics.average(taskVMems);
    long averageTimeMs = Statistics.average(runtimesMs);

    Severity severity;
    if (tasks.length == 0) {
      severity = Severity.NONE;
    } else {
      severity = getTaskMemoryUtilSeverity(taskPMemAvg, containerMem);
    }

    HeuristicResult result = new HeuristicResult(_heuristicName, severity);

    result.addDetail("Number of tasks", Integer.toString(tasks.length));
    result.addDetail("Avg task runtime", Statistics.readableTimespan(averageTimeMs));
    result.addDetail("Avg Physical Memory (MB)", Long.toString(taskPMemAvg/FileUtils.ONE_MB));
    result.addDetail("Max Physical Memory (MB)", Long.toString(taskPMax/FileUtils.ONE_MB));
    result.addDetail("Min Physical Memory (MB)", Long.toString(taskPMin/FileUtils.ONE_MB));
    result.addDetail("Avg Virtual Memory (MB)", Long.toString(taskVMemAvg/FileUtils.ONE_MB));
    result.addDetail("Requested Container Memory", FileUtils.byteCountToDisplaySize(containerMem));

    return result;
  }

  private Severity getTaskMemoryUtilSeverity(long taskMemAvg, long taskMemMax) {
    double ratio = ((double)taskMemAvg) / taskMemMax;
    Severity sevRatio = getMemoryRatioSeverity(ratio);
    // Severity is reduced if the requested container memory is close to default
    Severity sevMax = getContainerMemorySeverity(taskMemMax);

    return Severity.min(sevRatio, sevMax);
  }

  private Severity getContainerMemorySeverity(long taskMemMax) {
    return Severity.getSeverityAscending(taskMemMax, CONTAINER_MEMORY_DEFAULT_BYTES * 1.1,
        CONTAINER_MEMORY_DEFAULT_BYTES * 1.5, CONTAINER_MEMORY_DEFAULT_BYTES * 2, CONTAINER_MEMORY_DEFAULT_BYTES * 2.5);
  }

  private Severity getMemoryRatioSeverity(double ratio) {
    return Severity.getSeverityDescending(ratio, 0.6, 0.5, 0.4, 0.3);
  }
}
