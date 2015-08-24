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

import com.linkedin.drelephant.analysis.HDFSContext;
import com.linkedin.drelephant.analysis.HadoopSystemContext;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.MapReduceCounterHolder;
import com.linkedin.drelephant.mapreduce.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.MapReduceTaskData;
import com.linkedin.drelephant.math.Statistics;

import org.apache.commons.io.FileUtils;


public abstract class GenericDataSkewHeuristic implements Heuristic<MapReduceApplicationData> {
  private MapReduceCounterHolder.CounterName _counterName;
  private String _heuristicName;

  @Override
  public String getHeuristicName() {
    return _heuristicName;
  }

  protected GenericDataSkewHeuristic(MapReduceCounterHolder.CounterName counterName, String heuristicName) {
    this._counterName = counterName;
    this._heuristicName = heuristicName;
  }

  protected abstract MapReduceTaskData[] getTasks(MapReduceApplicationData data);

  @Override
  public HeuristicResult apply(MapReduceApplicationData data) {
    MapReduceTaskData[] tasks = getTasks(data);

    //Gather data
    long[] inputBytes = new long[tasks.length];

    for (int i = 0; i < tasks.length; i++) {
      inputBytes[i] = tasks[i].getCounters().get(_counterName);
    }

    //Analyze data
    long[][] groups = Statistics.findTwoGroups(inputBytes);

    long avg1 = Statistics.average(groups[0]);
    long avg2 = Statistics.average(groups[1]);

    long min = Math.min(avg1, avg2);
    long diff = Math.abs(avg2 - avg1);

    Severity severity = getDeviationSeverity(min, diff);

    //This reduces severity if the largest file sizes are insignificant
    severity = Severity.min(severity, getFilesSeverity(avg2));

    //This reduces severity if number of tasks is insignificant
    severity = Severity.min(severity, Statistics.getNumTasksSeverity(groups[0].length));

    HeuristicResult result = new HeuristicResult(_heuristicName, severity);

    result.addDetail("Number of tasks", Integer.toString(tasks.length));
    result.addDetail("Group A", groups[0].length + " tasks @ " + FileUtils.byteCountToDisplaySize(avg1) + " avg");
    result.addDetail("Group B", groups[1].length + " tasks @ " + FileUtils.byteCountToDisplaySize(avg2) + " avg");

    return result;
  }

  public static Severity getDeviationSeverity(long averageMin, long averageDiff) {
    if (averageMin <= 0) {
      averageMin = 1;
    }
    long value = averageDiff / averageMin;
    return Severity.getSeverityAscending(value, 2, 4, 8, 16);
  }

  public static Severity getFilesSeverity(long value) {
    return Severity.getSeverityAscending(value, HDFSContext.HDFS_BLOCK_SIZE / 8, HDFSContext.HDFS_BLOCK_SIZE / 4,
        HDFSContext.HDFS_BLOCK_SIZE / 2, HDFSContext.HDFS_BLOCK_SIZE);
  }
}
