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

public abstract class GenericGCHeuristic implements Heuristic<MapReduceApplicationData> {
  private String _heuristicName;

  @Override
  public String getHeuristicName() {
    return _heuristicName;
  }

  protected GenericGCHeuristic(String heuristicName) {
    this._heuristicName = heuristicName;
  }

  protected abstract MapReduceTaskData[] getTasks(MapReduceApplicationData data);

  @Override
  public HeuristicResult apply(MapReduceApplicationData data) {

    if(!data.getSucceeded()) {
      return null;
    }

    MapReduceTaskData[] tasks = getTasks(data);
    List<Long> gcMs = new ArrayList<Long>();
    List<Long> cpuMs = new ArrayList<Long>();
    List<Long> runtimesMs = new ArrayList<Long>();

    for (MapReduceTaskData task : tasks) {
      if (task.timed()) {
        runtimesMs.add(task.getTotalRunTimeMs());
      }
      gcMs.add(task.getCounters().get(MapReduceCounterHolder.CounterName.GC_MILLISECONDS));
      cpuMs.add(task.getCounters().get(MapReduceCounterHolder.CounterName.CPU_MILLISECONDS));
    }

    long avgRuntimeMs = Statistics.average(runtimesMs);
    long avgCpuMs = Statistics.average(cpuMs);
    long avgGcMs = Statistics.average(gcMs);
    double ratio = avgCpuMs != 0 ? avgGcMs*(1.0)/avgCpuMs: 0;

    Severity severity;
    if (tasks.length == 0) {
      severity = Severity.NONE;
    } else {
      severity = getGcRatioSeverity(avgRuntimeMs, avgCpuMs, avgGcMs);
    }

    HeuristicResult result = new HeuristicResult(_heuristicName, severity);

    result.addDetail("Number of tasks", Integer.toString(tasks.length));
    result.addDetail("Avg task runtime (ms)", Long.toString(avgRuntimeMs));
    result.addDetail("Avg task CPU time (ms)", Long.toString(avgCpuMs));
    result.addDetail("Avg task GC time (ms)", Long.toString(avgGcMs));
    result.addDetail("Task GC/CPU ratio", Double.toString(ratio));
    return result;
  }

  private Severity getGcRatioSeverity(long runtimeMs, long cpuMs, long gcMs) {
    double gcRatio = ((double)gcMs)/cpuMs;
    Severity ratioSeverity = Severity.getSeverityAscending(gcRatio, 0.01, 0.02, 0.03, 0.04);
    // Severity is reduced if task runtime is insignificant
    Severity runtimeSeverity = getRuntimeSeverity(runtimeMs);
    return Severity.min(ratioSeverity, runtimeSeverity);
  }

  private Severity getRuntimeSeverity(long runtimeMs) {
    return Severity.getSeverityAscending(runtimeMs, 5 * Statistics.MINUTE_IN_MS, 10 * Statistics.MINUTE_IN_MS,
        12 * Statistics.MINUTE_IN_MS, 15 * Statistics.MINUTE_IN_MS);
  }

}
