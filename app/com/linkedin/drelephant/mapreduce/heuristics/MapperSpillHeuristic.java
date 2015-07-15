package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.MapReduceCounterHolder;
import com.linkedin.drelephant.mapreduce.MapReduceTaskData;
import com.linkedin.drelephant.mapreduce.MapReduceApplicationData;


public class MapperSpillHeuristic implements Heuristic<MapReduceApplicationData> {
  public static final String HEURISTIC_NAME = "Mapper Spill";
  private static final long THRESHOLD_SPILL_FACTOR = 10000;

  @Override
  public HeuristicResult apply(MapReduceApplicationData data) {
    MapReduceTaskData[] tasks = data.getMapperData();

    long totalSpills = 0;
    long totalOutputRecords = 0;
    double ratioSpills = 0.0;

    for (MapReduceTaskData task : tasks) {
      totalSpills += task.getCounters().get(MapReduceCounterHolder.CounterName.SPILLED_RECORDS);
      totalOutputRecords += task.getCounters().get(MapReduceCounterHolder.CounterName.MAP_OUTPUT_RECORDS);
    }

    //If both totalSpills and totalOutputRecords are zero then set ratioSpills to zero.
    if (totalSpills == 0) {
      ratioSpills = 0;
    } else {
      ratioSpills = (double) totalSpills / (double) totalOutputRecords;
    }

    Severity severity = getSpillSeverity(ratioSpills);

    HeuristicResult result = new HeuristicResult(HEURISTIC_NAME, severity);

    result.addDetail("Number of tasks", Integer.toString(tasks.length));
    result.addDetail("Avg spilled records per task", tasks.length == 0 ? "0" : Long.toString(totalSpills/tasks.length));
    result.addDetail("Avg output records per task", tasks.length == 0 ? "0" : Long.toString(totalOutputRecords/tasks.length));
    result.addDetail("Ratio of spilled records to output records", Double.toString(ratioSpills));

    return result;

  }

  @Override
  public String getHeuristicName() {
    return HEURISTIC_NAME;
  }

  public static Severity getSpillSeverity(double ratioSpills) {
    long normalizedSpillRatio = 0;
    //Normalize the ratio to integer.
    normalizedSpillRatio = (long) (ratioSpills * THRESHOLD_SPILL_FACTOR);
    return Severity.getSeverityAscending(normalizedSpillRatio, (long) (2.01 * THRESHOLD_SPILL_FACTOR),
        (long) (2.2 * THRESHOLD_SPILL_FACTOR), (long) (2.5 * THRESHOLD_SPILL_FACTOR),
        (long) (3 * THRESHOLD_SPILL_FACTOR));
  }
}
