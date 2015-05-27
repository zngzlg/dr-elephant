package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.mapreduce.MapreduceApplicationData;
import com.linkedin.drelephant.mapreduce.HadoopTaskData;
import java.util.ArrayList;
import java.util.List;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.math.Statistics;


public class ReducerTimeHeuristic implements Heuristic<MapreduceApplicationData> {
  public static final String HEURISTIC_NAME = "Reducer Time";

  @Override
  public String getHeuristicName() {
    return HEURISTIC_NAME;
  }

  @Override
  public HeuristicResult apply(MapreduceApplicationData data) {
    HadoopTaskData[] tasks = data.getReducerData();

    List<Long> runTimesMs = new ArrayList<Long>();

    for (HadoopTaskData task : tasks) {
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
    Severity taskSeverity = getNumTasksSeverity(numTasks);
    return Severity.min(timeSeverity, taskSeverity);
  }

  private Severity longTimeSeverity(long runtimeMs, long numTasks) {
    Severity timeSeverity = getLongRuntimeSeverity(runtimeMs);
    Severity taskSeverity = getNumTasksSeverityReverse(numTasks);
    return Severity.min(timeSeverity, taskSeverity);
  }

  public static Severity getShortRuntimeSeverity(long runtimeMs) {
    return Severity.getSeverityDescending(runtimeMs, 10 * Statistics.MINUTE_IN_MS, 5 * Statistics.MINUTE_IN_MS,
        2 * Statistics.MINUTE_IN_MS, 1 * Statistics.MINUTE_IN_MS);
  }

  public static Severity getLongRuntimeSeverity(long runtimeMs) {
    return Severity.getSeverityAscending(runtimeMs, 15 * Statistics.MINUTE_IN_MS, 30 * Statistics.MINUTE_IN_MS, 1 * Statistics.HOUR_IN_MS,
        2 * Statistics.HOUR_IN_MS);
  }

  public static Severity getNumTasksSeverity(long numTasks) {
    return Severity.getSeverityAscending(numTasks, 10, 51, 200, 500);
  }

  public static Severity getNumTasksSeverityReverse(long numTasks) {
    return Severity.getSeverityDescending(numTasks, 100, 49, 20, 10);
  }
}
