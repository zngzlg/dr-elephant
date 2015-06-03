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


public class MapperInputSizeHeuristic implements Heuristic<MapReduceApplicationData> {
  public static final String HEURISTIC_NAME = "Mapper Input Size";

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

    Severity smallFilesSeverity = smallFilesSeverity(averageSize, tasks.length, averageTimeMs);
    Severity largeFilesSeverity = largeFilesSeverity(averageSize, tasks.length, averageTimeMs);
    Severity severity = Severity.max(smallFilesSeverity, largeFilesSeverity);

    HeuristicResult result = new HeuristicResult(HEURISTIC_NAME, severity);

    result.addDetail("Number of tasks", Integer.toString(tasks.length));
    result.addDetail("Average task input size", FileUtils.byteCountToDisplaySize(averageSize));
    result.addDetail("Average task runtime", Statistics.readableTimespan(averageTimeMs));

    return result;
  }

  private Severity smallFilesSeverity(long value, long numTasks, long averageTimeMs) {
    // We want to identify jobs with small task input, large number of tasks, and low task runtime
    Severity severity = getSmallFilesSeverity(value);
    // Severity is reduced if number of tasks is small
    Severity numTaskSeverity = getNumTasksSeverity(numTasks);
    severity = Severity.min(severity, numTaskSeverity);
    // Severity is reduced if task runtime is long
    Severity runtimeSeverity = getRuntimeSeverityReverse(averageTimeMs);
    return Severity.min(severity, runtimeSeverity);
  }

  private Severity largeFilesSeverity(long value, long numTasks, long averageTimeMs) {
    // We want to identify jobs with large task input, small number of tasks, and long task runtime
    Severity severity = getLargeFilesSeverity(value);
    // Severity is reduced if number of tasks is large
    Severity numTaskSeverity = getNumTasksSeverityReverse(numTasks);
    severity = Severity.min(severity, numTaskSeverity);
    // Severity is reduced if task runtime is short
    Severity runtimeSeverity = getRuntimeSeverity(averageTimeMs);
    return Severity.min(severity, runtimeSeverity);
  }

  public static Severity getSmallFilesSeverity(long value) {
    return Severity.getSeverityDescending(value, HadoopSystemContext.HDFS_BLOCK_SIZE / 2, HadoopSystemContext.HDFS_BLOCK_SIZE / 4,
        HadoopSystemContext.HDFS_BLOCK_SIZE / 8, HadoopSystemContext.HDFS_BLOCK_SIZE / 32);
  }

  public static Severity getLargeFilesSeverity(long value) {
    return Severity.getSeverityAscending(value, HadoopSystemContext.HDFS_BLOCK_SIZE * 2, HadoopSystemContext.HDFS_BLOCK_SIZE * 3,
        HadoopSystemContext.HDFS_BLOCK_SIZE * 4, HadoopSystemContext.HDFS_BLOCK_SIZE * 5);
  }

  public static Severity getNumTasksSeverity(long numTasks) {
    return Severity.getSeverityAscending(numTasks, 10, 50, 200, 500);
  }

  public static Severity getNumTasksSeverityReverse(long numTasks) {
    return Severity.getSeverityDescending(numTasks, 1000, 500, 200, 100);
  }

  public static Severity getRuntimeSeverity(long runtimeMs) {
    return Severity.getSeverityAscending(runtimeMs, 10 * Statistics.MINUTE_IN_MS, 15 * Statistics.MINUTE_IN_MS,
        20 * Statistics.MINUTE_IN_MS, 30 * Statistics.MINUTE_IN_MS);
  }

  public static Severity getRuntimeSeverityReverse(long runtimeMs) {
    return Severity.getSeverityDescending(runtimeMs, 5 * Statistics.MINUTE_IN_MS, 4 * Statistics.MINUTE_IN_MS,
        3 * Statistics.MINUTE_IN_MS, 2 * Statistics.MINUTE_IN_MS);
  }

}
