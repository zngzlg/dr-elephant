package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.MapReduceApplicationData;

public class ExceptionHeuristic implements Heuristic<MapReduceApplicationData> {

  public static final String HEURISTIC_NAME = "Exception";

  @Override
  public String getHeuristicName() {
    return HEURISTIC_NAME;
  }

  @Override
  public HeuristicResult apply(MapReduceApplicationData data) {
    if (data.getSucceeded()) {
      return null;
    }
    HeuristicResult result = new HeuristicResult(HEURISTIC_NAME, Severity.MODERATE);
    String diagnosticInfo = data.getDiagnosticInfo();
    if (diagnosticInfo != null) {
      result.addDetail(diagnosticInfo);
    } else {
      String msg = "Unable to find stacktrace info. Please find the real problem in the Jobhistory link above.\nException can happen either in task log or Application Master log.";
      result.addDetail(msg);
    }
    return result;
  }
}
