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
import java.util.ArrayList;
import java.util.List;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.MapReduceTaskData;
import com.linkedin.drelephant.math.Statistics;


public class ShuffleSortHeuristic implements Heuristic<MapReduceApplicationData> {
  public static final String HEURISTIC_NAME = "Shuffle & Sort";

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

    List<Long> execTimeMs = new ArrayList<Long>();
    List<Long> shuffleTimeMs = new ArrayList<Long>();
    List<Long> sortTimeMs = new ArrayList<Long>();

    for (MapReduceTaskData task : tasks) {
      if (task.timed()) {
        execTimeMs.add(task.getCodeExecutionTimeMs());
        shuffleTimeMs.add(task.getShuffleTimeMs());
        sortTimeMs.add(task.getSortTimeMs());
      }
    }

    //Analyze data
    long avgExecTimeMs = Statistics.average(execTimeMs);
    long avgShuffleTimeMs = Statistics.average(shuffleTimeMs);
    long avgSortTimeMs = Statistics.average(sortTimeMs);

    Severity shuffleSeverity = getShuffleSortSeverity(avgShuffleTimeMs, avgExecTimeMs);
    Severity sortSeverity = getShuffleSortSeverity(avgSortTimeMs, avgExecTimeMs);
    Severity severity = Severity.max(shuffleSeverity, sortSeverity);

    HeuristicResult result = new HeuristicResult(HEURISTIC_NAME, severity);

    result.addDetail("Number of tasks", Integer.toString(data.getReducerData().length));
    result.addDetail("Average code runtime", Statistics.readableTimespan(avgExecTimeMs));
    String shuffleFactor = Statistics.describeFactor(avgShuffleTimeMs, avgExecTimeMs, "x");
    result.addDetail("Average shuffle time", Statistics.readableTimespan(avgShuffleTimeMs) + " " + shuffleFactor);
    String sortFactor = Statistics.describeFactor(avgSortTimeMs, avgExecTimeMs, "x");
    result.addDetail("Average sort time", Statistics.readableTimespan(avgSortTimeMs) + " " + sortFactor);

    return result;
  }

  public static Severity getShuffleSortSeverity(long runtimeMs, long codetimeMs) {
    Severity runtimeSeverity =
        Severity.getSeverityAscending(runtimeMs, 1 * Statistics.MINUTE_IN_MS, 5 * Statistics.MINUTE_IN_MS, 10 * Statistics.MINUTE_IN_MS,
            30 * Statistics.MINUTE_IN_MS);

    if (codetimeMs <= 0) {
      return runtimeSeverity;
    }
    long value = runtimeMs * 2 / codetimeMs;
    Severity runtimeRatioSeverity = Severity.getSeverityAscending(value, 1, 2, 4, 8);

    return Severity.min(runtimeSeverity, runtimeRatioSeverity);
  }
}
