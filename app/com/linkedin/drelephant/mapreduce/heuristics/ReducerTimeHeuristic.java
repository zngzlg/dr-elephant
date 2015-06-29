package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.mapreduce.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.MapReduceTaskData;
import java.util.ArrayList;
import java.util.List;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.math.Statistics;


public class ReducerTimeHeuristic implements Heuristic<MapReduceApplicationData> {
  public static final String HEURISTIC_NAME = "Reducer Time";

  @Override
  public String getHeuristicName() {
    return HEURISTIC_NAME;
  }

  @Override
  public HeuristicResult apply(MapReduceApplicationData data) {
    MapReduceTaskData[] tasks = data.getReducerData();

    List<Long> runTimesMs = new ArrayList<Long>();

    for (MapReduceTaskData task : tasks) {
      if (task.timed()) {
        runTimesMs.add(task.getTotalRunTimeMs());
      }
    }

    //Analyze data
    long averageRuntimeMs = Statistics.average(runTimesMs);

    Severity shortTimeSeverity = shortTimeSeverity(averageRuntimeMs, tasks.length);
    Severity longTimeSeverity = longTimeSeverity(averageRuntimeMs, tasks.length);
    Severity severity = Severity.max(shortTimeSeverity, longTimeSeverity);

    HeuristicResult result = new HeuristicResult(HEURISTIC_NAME, severity);

    result.addDetail("Number of tasks", Integer.toString(tasks.length));
    result.addDetail("Average task time", Statistics.readableTimespan(averageRuntimeMs));

    return result;
  }

  private Severity shortTimeSeverity(long runtimeMs, long numTasks) {
    Severity timeSeverity = getShortRuntimeSeverity(runtimeMs);
    // Severity is adjusted based on number of tasks
    Severity taskSeverity = getNumTasksSeverity(numTasks);
    return Severity.min(timeSeverity, taskSeverity);
  }

  private Severity longTimeSeverity(long runtimeMs, long numTasks) {
    // Severity is NOT adjusted based on number of tasks
    return getLongRuntimeSeverity(runtimeMs);
  }

  private Severity getShortRuntimeSeverity(long runtimeMs) {
    return Severity.getSeverityDescending(runtimeMs, 10 * Statistics.MINUTE_IN_MS, 4 * Statistics.MINUTE_IN_MS,
        2 * Statistics.MINUTE_IN_MS, 1 * Statistics.MINUTE_IN_MS);
  }

  private Severity getLongRuntimeSeverity(long runtimeMs) {
    return Severity.getSeverityAscending(runtimeMs, 15 * Statistics.MINUTE_IN_MS, 30 * Statistics.MINUTE_IN_MS, 1 * Statistics.HOUR_IN_MS,
        2 * Statistics.HOUR_IN_MS);
  }

  private Severity getNumTasksSeverity(long numTasks) {
    return Severity.getSeverityAscending(numTasks, 50, 101, 500, 1000);
  }
}
