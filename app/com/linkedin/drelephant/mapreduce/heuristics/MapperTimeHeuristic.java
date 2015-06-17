package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.mapreduce.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.MapReduceCounterHolder;
import java.util.ArrayList;
import java.util.List;

import com.linkedin.drelephant.analysis.HadoopSystemContext;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.MapReduceTaskData;
import com.linkedin.drelephant.math.Statistics;

import org.apache.commons.io.FileUtils;


public class MapperTimeHeuristic implements Heuristic<MapReduceApplicationData> {
  public static final String HEURISTIC_NAME = "Mapper Time";

  @Override
  public String getHeuristicName() {
    return HEURISTIC_NAME;
  }

  @Override
  public HeuristicResult apply(MapReduceApplicationData data) {
    MapReduceTaskData[] tasks = data.getMapperData();

    List<Long> inputBytes = new ArrayList<Long>();
    List<Long> runtimesMs = new ArrayList<Long>();

    for (MapReduceTaskData task : tasks) {
      inputBytes.add(task.getCounters().get(MapReduceCounterHolder.CounterName.HDFS_BYTES_READ));
      if (task.timed()) {
        runtimesMs.add(task.getTotalRunTimeMs());
      }
    }

    long averageSize = Statistics.average(inputBytes);
    long averageTimeMs = Statistics.average(runtimesMs);

    Severity shortTaskSeverity = shortTaskSeverity(tasks.length, averageTimeMs);
    Severity longTaskSeverity = longTaskSeverity(tasks.length, averageTimeMs);
    Severity severity = Severity.max(shortTaskSeverity, longTaskSeverity);

    HeuristicResult result = new HeuristicResult(HEURISTIC_NAME, severity);

    result.addDetail("Number of tasks", Integer.toString(tasks.length));
    result.addDetail("Average task input size", FileUtils.byteCountToDisplaySize(averageSize));
    result.addDetail("Average task runtime", Statistics.readableTimespan(averageTimeMs));

    return result;
  }

  private Severity shortTaskSeverity(long numTasks, long averageTimeMs) {
    // We want to identify jobs with short task runtime
    Severity severity = getShortRuntimeSeverity(averageTimeMs);
    // Severity is reduced if number of tasks is small.
    Severity numTaskSeverity = getNumTasksSeverity(numTasks);
    return Severity.min(severity, numTaskSeverity);
  }

  private Severity longTaskSeverity(long numTasks, long averageTimeMs) {
    // We want to identify jobs with long task runtime
    Severity severity = getLongRuntimeSeverity(averageTimeMs);
    // Severity is reduced if number of tasks is large
    Severity numTaskSeverity = getNumTasksSeverityReverse(numTasks);
    return Severity.min(severity, numTaskSeverity);
  }

  public static Severity getShortRuntimeSeverity(long runtimeMs) {
    return Severity.getSeverityDescending(runtimeMs, 10 * Statistics.MINUTE_IN_MS, 4 * Statistics.MINUTE_IN_MS,
        2 * Statistics.MINUTE_IN_MS, 1 * Statistics.MINUTE_IN_MS);
  }

  public static Severity getLongRuntimeSeverity(long runtimeMs) {
    return Severity.getSeverityAscending(runtimeMs, 15 * Statistics.MINUTE_IN_MS, 30 * Statistics.MINUTE_IN_MS, 1 * Statistics.HOUR_IN_MS,
        2 * Statistics.HOUR_IN_MS);
  }

  public static Severity getNumTasksSeverity(long numTasks) {
    return Severity.getSeverityAscending(numTasks, 50, 101, 500, 1000);
  }

  public static Severity getNumTasksSeverityReverse(long numTasks) {
    return Severity.getSeverityDescending(numTasks, 500, 100, 50, 10);
  }
}
