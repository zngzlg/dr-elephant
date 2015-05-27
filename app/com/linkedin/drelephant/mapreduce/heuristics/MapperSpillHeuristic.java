package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.HadoopCounterHolder;
import com.linkedin.drelephant.mapreduce.HadoopTaskData;
import com.linkedin.drelephant.mapreduce.MapreduceApplicationData;


public class MapperSpillHeuristic implements Heuristic<MapreduceApplicationData> {
  public static final String HEURISTIC_NAME = "Mapper Spill";
  private static final long THRESHOLD_SPILL_FACTOR = 10000;

  @Override
  public HeuristicResult apply(MapreduceApplicationData data) {
    HadoopTaskData[] tasks = data.getMapperData();

    long totalSpills = 0;
    long totalOutputRecords = 0;
    double ratioSpills = 0.0;

    for (HadoopTaskData task : tasks) {
      totalSpills += task.getCounters().get(HadoopCounterHolder.CounterName.SPILLED_RECORDS);
      totalOutputRecords += task.getCounters().get(HadoopCounterHolder.CounterName.MAP_OUTPUT_RECORDS);
    }

    //If both totalSpills and totalOutputRecords are zero then set ratioSpills to zero.
    if (totalSpills == 0) {
      ratioSpills = 0;
    } else {
      ratioSpills = (double) totalSpills / (double) totalOutputRecords;
    }

    Severity severity = getSpillSeverity(ratioSpills);

    HeuristicResult result = new HeuristicResult(HEURISTIC_NAME, severity);

    result.addDetail("Number of spilled records ", Long.toString(totalSpills));
    result.addDetail("Number of Mapper output records ", Long.toString(totalOutputRecords));
    result.addDetail("Ratio of spilled records to mapper output records", Double.toString(ratioSpills));

    return result;

  }

  @Override
  public String getHeuristicName() {
    return HEURISTIC_NAME;
  }

  /**
   *
   * @param ratioSpills
   * @return Severity
   *
   * ratioSpills: Severity
   *
   * <1.25: None
   * 1.25-1.5: Low
   * 1.5-1.75: Moderate
   * 1.75-2: Severe
   * >=2: Critical
   *
   */

  public static Severity getSpillSeverity(double ratioSpills) {
    long normalizedSpillRatio = 0;
    //Normalize the ratio to integer.
    normalizedSpillRatio = (long) (ratioSpills * THRESHOLD_SPILL_FACTOR);
    return Severity.getSeverityAscending(normalizedSpillRatio, (long) (1.25 * THRESHOLD_SPILL_FACTOR),
        (long) (1.5 * THRESHOLD_SPILL_FACTOR), (long) (1.75 * THRESHOLD_SPILL_FACTOR),
        (long) (2 * THRESHOLD_SPILL_FACTOR));
  }
}
