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
