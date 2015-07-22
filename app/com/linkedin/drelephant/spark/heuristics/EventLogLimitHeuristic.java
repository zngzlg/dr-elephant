package com.linkedin.drelephant.spark.heuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.spark.SparkApplicationData;


/**
 * This is a safeguard heuristic rule that makes sure if a log size passes the limit, we do not automatically
 * approve it.
 *
 * @author yizhou
 */
public class EventLogLimitHeuristic implements Heuristic<SparkApplicationData> {
  public static final String HEURISTIC_NAME = "Spark Event Log Limit";

  @Override
  public HeuristicResult apply(SparkApplicationData data) {
    Severity severity = getSeverity(data);
    HeuristicResult result = new HeuristicResult(getHeuristicName(), severity);
    if (severity == Severity.CRITICAL) {
      result.addDetail("Spark job's event log passes the limit. No actual log data is fetched."
          + " All other heuristic rules will not make sense.");
    }
    return result;
  }

  @Override
  public String getHeuristicName() {
    return HEURISTIC_NAME;
  }

  private Severity getSeverity(SparkApplicationData data) {
    if (data.isThrottled()) {
      return Severity.CRITICAL;
    } else {
      return Severity.NONE;
    }
  }
}
