package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.util.HeuristicConfigurationData;
import com.linkedin.drelephant.util.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.mapreduce.MapReduceCounterHolder;
import com.linkedin.drelephant.mapreduce.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.MapReduceTaskData;
import com.linkedin.drelephant.math.Statistics;
import java.util.Map;
import org.apache.log4j.Logger;


public abstract class GenericGCHeuristic implements Heuristic<MapReduceApplicationData> {
  private static final Logger logger = Logger.getLogger(GenericGCHeuristic.class);

  // Severity Parameters
  private static final String GC_RATIO_SEVERITY = "gc_ratio_severity";
  private static final String RUNTIME_SEVERITY = "runtime_severity_in_min";

  // Default value of parameters
  private double[] gcRatioLimits = {0.01d, 0.02d, 0.03d, 0.04d};   // Garbage Collection Time / CPU Time
  private double[] runtimeLimits = {5, 10, 12, 15};                // Task Runtime in milli sec

  private String _heuristicName;
  private HeuristicConfigurationData _heuristicConfData;

  @Override
  public String getHeuristicName() {
    return _heuristicName;
  }

  private void loadParameters() {
    Map<String, String> paramMap = _heuristicConfData.getParamMap();

    if(paramMap.get(GC_RATIO_SEVERITY) != null) {
      double[] confGcRatioThreshold = Utils.getParam(paramMap.get(GC_RATIO_SEVERITY), gcRatioLimits.length);
      if (confGcRatioThreshold != null) {
        gcRatioLimits = confGcRatioThreshold;
      }
    }
    logger.info(_heuristicName + " will use " + GC_RATIO_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(gcRatioLimits));

    if(paramMap.get(RUNTIME_SEVERITY) != null) {
      double[] confRuntimeThreshold = Utils.getParam(paramMap.get(RUNTIME_SEVERITY), runtimeLimits.length);
      if (confRuntimeThreshold != null) {
        runtimeLimits = confRuntimeThreshold;
      }
    }
    logger.info(_heuristicName + " will use " + RUNTIME_SEVERITY + " with the following threshold settings: "
        + Arrays.toString(runtimeLimits));
    for (int i = 0; i < runtimeLimits.length; i++) {
      runtimeLimits[i] = runtimeLimits[i] * Statistics.MINUTE_IN_MS;
    }
  }

  protected GenericGCHeuristic(String heuristicName, HeuristicConfigurationData heuristicConfData) {
    this._heuristicName = heuristicName;
    this._heuristicConfData = heuristicConfData;

    loadParameters();
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
    Severity ratioSeverity = Severity.getSeverityAscending(
        gcRatio, gcRatioLimits[0], gcRatioLimits[1], gcRatioLimits[2], gcRatioLimits[3]);

    // Severity is reduced if task runtime is insignificant
    Severity runtimeSeverity = getRuntimeSeverity(runtimeMs);

    return Severity.min(ratioSeverity, runtimeSeverity);
  }

  private Severity getRuntimeSeverity(long runtimeMs) {
    return Severity.getSeverityAscending(
        runtimeMs, runtimeLimits[0], runtimeLimits[1], runtimeLimits[2], runtimeLimits[3]);
  }

}
