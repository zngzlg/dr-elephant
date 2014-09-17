package com.linkedin.drelephant.analysis.heuristics;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;
import com.linkedin.drelephant.math.Statistics;

import org.apache.commons.io.FileUtils;


public class MapperSpeedHeuristic implements Heuristic {
  public static final String HEURISTIC_NAME = "Mapper Speed";

  @Override
  public String getHeuristicName() {
    return HEURISTIC_NAME;
  }

  @Override
  public HeuristicResult apply(HadoopJobData data) {

    HadoopTaskData[] tasks = data.getMapperData();

    List<Long> inputByteSizes = new ArrayList<Long>();
    List<Long> speeds = new ArrayList<Long>();
    List<Long> runtimesMs = new ArrayList<Long>();

    for (HadoopTaskData task : tasks) {
      if (task.timed()) {
        long inputBytes = task.getCounters().get(HadoopCounterHolder.CounterName.HDFS_BYTES_READ);
        long runtimeMs = task.getTotalRunTimeMs();
        //Apply 1 minute buffer
        runtimeMs -= 60 * 1000;
        if (runtimeMs < 1000) {
          runtimeMs = 1000;
        }
        inputByteSizes.add(inputBytes);
        runtimesMs.add(runtimeMs);
        //Speed is bytes per second
        speeds.add((1000 * inputBytes) / (runtimeMs));
      }
    }

    //Analyze data
    long averageSpeed = Statistics.average(speeds);
    long averageSize = Statistics.average(inputByteSizes);
    long averageRuntimeMs = Statistics.average(runtimesMs);

    Severity severity = getDiskSpeedSeverity(averageSpeed);

    //This reduces severity if task runtime is insignificant
    severity = Severity.min(severity, getRuntimeSeverity(averageRuntimeMs));

    HeuristicResult result = new HeuristicResult(HEURISTIC_NAME, severity);

    result.addDetail("Number of tasks", Integer.toString(tasks.length));
    result.addDetail("Average task input size", FileUtils.byteCountToDisplaySize(averageSize));
    result.addDetail("Average task speed", FileUtils.byteCountToDisplaySize(averageSpeed) + "/s");
    result.addDetail("Average task runtime", Statistics.readableTimespan(averageRuntimeMs));

    return result;
  }

  public static Severity getDiskSpeedSeverity(long speed) {
    return Severity.getSeverityDescending(speed, Constants.DISK_READ_SPEED / 2, Constants.DISK_READ_SPEED / 4,
        Constants.DISK_READ_SPEED / 8, Constants.DISK_READ_SPEED / 32);
  }

  public static Severity getRuntimeSeverity(long runtimeMs) {
    return Severity.getSeverityAscending(runtimeMs, 5 * Statistics.MINUTE_IN_MS, 20 * Statistics.MINUTE_IN_MS,
        40 * Statistics.MINUTE_IN_MS, 1 * Statistics.HOUR_IN_MS);
  }
}
