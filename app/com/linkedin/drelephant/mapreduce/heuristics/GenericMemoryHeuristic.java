package com.linkedin.drelephant.mapreduce.heuristics;

import java.util.ArrayList;
import java.util.List;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.MapReduceCounterHolder;
import com.linkedin.drelephant.mapreduce.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.MapReduceTaskData;
import com.linkedin.drelephant.math.Statistics;

import org.apache.commons.io.FileUtils;


public abstract class GenericMemoryHeuristic implements Heuristic<MapReduceApplicationData> {
  private String _containerMemConf;
  private String _heuristicName;
  private long CONTAINER_MEMORY_DEFAULT_BYTES = 2048L * FileUtils.ONE_MB;

  @Override
  public String getHeuristicName() {
    return _heuristicName;
  }

  protected GenericMemoryHeuristic(String containerMemConf, String heuristicName) {
    this._heuristicName = heuristicName;
    this._containerMemConf = containerMemConf;
  }

  protected abstract MapReduceTaskData[] getTasks(MapReduceApplicationData data);

  @Override
  public HeuristicResult apply(MapReduceApplicationData data) {
    long containerMem = Long.parseLong(data.getConf().getProperty(_containerMemConf)) * FileUtils.ONE_MB;

    MapReduceTaskData[] tasks = getTasks(data);
    List<Long> taskMems = new ArrayList<Long>();
    long taskMin = Long.MAX_VALUE;
    long taskMax = 0;
    for (MapReduceTaskData task : tasks) {
      long taskMem = task.getCounters().get(MapReduceCounterHolder.CounterName.PHYSICAL_MEMORY_BYTES);
      taskMems.add(taskMem);
      taskMin = Math.min(taskMin, taskMem);
      taskMax = Math.max(taskMax, taskMem);
    }

    if(taskMin == Long.MAX_VALUE) {
      taskMin = 0;
    }

    long taskAvg = Statistics.average(taskMems);

    Severity severity = getTaskMemoryUtilSeverity(taskAvg, containerMem);

    HeuristicResult result = new HeuristicResult(_heuristicName, severity);

    result.addDetail("Number of tasks", Integer.toString(tasks.length));
    result.addDetail("Avg Physical Memory (MB)", Long.toString(taskAvg/FileUtils.ONE_MB));
    result.addDetail("Max Physical Memory (MB)", Long.toString(taskMax/FileUtils.ONE_MB));
    result.addDetail("Min Physical Memory (MB)", Long.toString(taskMin/FileUtils.ONE_MB));
    result.addDetail("Requested Container Memory", FileUtils.byteCountToDisplaySize(containerMem));

    return result;
  }

  private Severity getTaskMemoryUtilSeverity(long taskMemAvg, long taskMemMax) {
    double ratio = ((double)taskMemAvg) / taskMemMax;
    Severity sevRatio = getMemoryRatioSeverity(ratio);
    // Severity is reduced if the requested container memory is close to default
    Severity sevMax = getContainerMemorySeverity(taskMemMax);

    return Severity.min(sevRatio, sevMax);
  }

  private Severity getContainerMemorySeverity(long taskMemMax) {
    return Severity.getSeverityAscending(taskMemMax, CONTAINER_MEMORY_DEFAULT_BYTES * 1.1,
        CONTAINER_MEMORY_DEFAULT_BYTES * 1.5, CONTAINER_MEMORY_DEFAULT_BYTES * 2, CONTAINER_MEMORY_DEFAULT_BYTES * 2.5);
  }

  private Severity getMemoryRatioSeverity(double ratio) {
    return Severity.getSeverityDescending(ratio, 0.6, 0.5, 0.4, 0.3);
  }
}
