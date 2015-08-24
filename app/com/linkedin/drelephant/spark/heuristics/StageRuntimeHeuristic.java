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
package com.linkedin.drelephant.spark.heuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.math.Statistics;
import com.linkedin.drelephant.spark.SparkApplicationData;
import com.linkedin.drelephant.spark.SparkJobProgressData;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang.StringUtils;


/**
 * Spark heuristic that detects stage runtime anomalies.
 *
 */
public class StageRuntimeHeuristic implements Heuristic<SparkApplicationData> {
  public static final String HEURISTIC_NAME = "Spark Stage Runtime";

  @Override
  public HeuristicResult apply(SparkApplicationData data) {
    SparkJobProgressData jobProgressData = data.getJobProgressData();
    Severity endSeverity = Severity.NONE;

    Set<SparkJobProgressData.StageAttemptId> completedStages = jobProgressData.getCompletedStages();
    Set<SparkJobProgressData.StageAttemptId> failedStages = jobProgressData.getFailedStages();

    // Average stage failure rate
    double avgStageFailureRate = jobProgressData.getStageFailureRate();
    endSeverity = Severity.max(endSeverity, getStageFailureRateSeverity(avgStageFailureRate));

    // For each completed stage, the task failure rate
    List<String> problematicStages = new ArrayList<String>();

    for (SparkJobProgressData.StageAttemptId id : completedStages) {
      SparkJobProgressData.StageInfo info = jobProgressData.getStageInfo(id.stageId, id.attemptId);
      double stageTasksFailureRate = info.getFailureRate();
      Severity tasksFailureRateSeverity = getSingleStageTasksFailureRate(stageTasksFailureRate);

      if (tasksFailureRateSeverity.getValue() > Severity.MODERATE.getValue()) {
        problematicStages.add(String.format("%s (task failure rate: %1.3f)", id, stageTasksFailureRate));
      }

      long duration = info.duration;
      Severity runtimeSeverity = getStageRuntimeSeverity(duration);
      if (runtimeSeverity.getValue() > Severity.MODERATE.getValue()) {
        problematicStages
            .add(String.format("%s (runtime: %s)", id, Statistics.readableTimespan(duration)));
      }

      endSeverity = Severity.max(endSeverity, tasksFailureRateSeverity, runtimeSeverity);
    }

    HeuristicResult result = new HeuristicResult(getHeuristicName(), endSeverity);

    result.addDetail("Spark stage completed", String.valueOf(completedStages.size()));
    result.addDetail("Spark stage failed", String.valueOf(failedStages.size()));
    result.addDetail("Spark average stage failure rate", String.format("%.3f", avgStageFailureRate));
    result.addDetail("Spark problematic stages:", getStageListString(problematicStages));

    return result;
  }

  @Override
  public String getHeuristicName() {
    return HEURISTIC_NAME;
  }

  private static Severity getStageRuntimeSeverity(long runtime) {
    return Severity.getSeverityAscending(runtime, 15 * Statistics.MINUTE_IN_MS, 30 * Statistics.MINUTE_IN_MS,
        60 * Statistics.MINUTE_IN_MS, 60 * Statistics.MINUTE_IN_MS);
  }

  private static Severity getStageFailureRateSeverity(double rate) {
    return Severity.getSeverityAscending(rate, 0.3d, 0.3d, 0.5d, 0.5d);
  }

  private static Severity getSingleStageTasksFailureRate(double rate) {
    return Severity.getSeverityAscending(rate, 0.0d, 0.3d, 0.5d, 0.5d);
  }

  private static String getStageListString(Collection<String> names) {
    return "[" + StringUtils.join(names, ",") + "]";
  }
}
