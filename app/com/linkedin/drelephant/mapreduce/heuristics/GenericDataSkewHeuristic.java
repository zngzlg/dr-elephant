package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.HadoopCounterHolder;
import com.linkedin.drelephant.mapreduce.MapreduceApplicationData;
import com.linkedin.drelephant.mapreduce.HadoopTaskData;
import com.linkedin.drelephant.math.Statistics;
import org.apache.commons.io.FileUtils;


public abstract class GenericDataSkewHeuristic implements Heuristic<MapreduceApplicationData> {
  private HadoopCounterHolder.CounterName _counterName;
  private String _heuristicName;

  @Override
  public String getHeuristicName() {
    return _heuristicName;
  }

  protected GenericDataSkewHeuristic(HadoopCounterHolder.CounterName counterName, String heuristicName) {
    this._counterName = counterName;
    this._heuristicName = heuristicName;
  }

  protected abstract HadoopTaskData[] getTasks(MapreduceApplicationData data);

  @Override
  public HeuristicResult apply(MapreduceApplicationData data) {
    HadoopTaskData[] tasks = getTasks(data);

    //Gather data
    long[] inputBytes = new long[tasks.length];

    for (int i = 0; i < tasks.length; i++) {
      inputBytes[i] = tasks[i].getCounters().get(_counterName);
    }

    //Analyze data
    long[][] groups = Statistics.findTwoGroups(inputBytes);

    long avg1 = Statistics.average(groups[0]);
    long avg2 = Statistics.average(groups[1]);

    long min = Math.min(avg1, avg2);
    long diff = Math.abs(avg2 - avg1);

    Severity severity = getDeviationSeverity(min, diff);

    //This reduces severity if the largest file sizes are insignificant
    severity = Severity.min(severity, getFilesSeverity(avg2));

    //This reduces severity if number of tasks is insignificant
    severity = Severity.min(severity, Statistics.getNumTasksSeverity(groups[0].length));

    HeuristicResult result = new HeuristicResult(_heuristicName, severity);

    result.addDetail("Number of tasks", Integer.toString(tasks.length));
    result.addDetail("Group A", groups[0].length + " tasks @ " + FileUtils.byteCountToDisplaySize(avg1) + " avg");
    result.addDetail("Group B", groups[1].length + " tasks @ " + FileUtils.byteCountToDisplaySize(avg2) + " avg");

    return result;
  }

  public static Severity getDeviationSeverity(long averageMin, long averageDiff) {
    if (averageMin <= 0) {
      averageMin = 1;
    }
    long value = averageDiff / averageMin;
    return Severity.getSeverityAscending(value, 2, 4, 8, 16);
  }

  public static Severity getFilesSeverity(long value) {
    return Severity.getSeverityAscending(value, Constants.HDFS_BLOCK_SIZE / 8, Constants.HDFS_BLOCK_SIZE / 4,
        Constants.HDFS_BLOCK_SIZE / 2, Constants.HDFS_BLOCK_SIZE);
  }
}
