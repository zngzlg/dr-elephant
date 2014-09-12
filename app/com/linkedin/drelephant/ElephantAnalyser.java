package com.linkedin.drelephant;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.analysis.heuristics.MapperDataSkewHeuristic;
import com.linkedin.drelephant.analysis.heuristics.MapperInputSizeHeuristic;
import com.linkedin.drelephant.analysis.heuristics.MapperSpeedHeuristic;
import com.linkedin.drelephant.analysis.heuristics.ReducerDataSkewHeuristic;
import com.linkedin.drelephant.analysis.heuristics.ReducerTimeHeuristic;
import com.linkedin.drelephant.analysis.heuristics.ShuffleSortHeuristic;
import com.linkedin.drelephant.hadoop.HadoopJobData;

import model.JobType;


public class ElephantAnalyser {
  public static final String NO_DATA = "No Data Received";
  private static final ElephantAnalyser ANALYZER = new ElephantAnalyser();

  private HeuristicResult _nodata;
  private List<Heuristic> _heuristics = new ArrayList<Heuristic>();
  public List<String> _heuristicNames = new ArrayList<String>();

  public ElephantAnalyser() {
    _nodata = new HeuristicResult(NO_DATA, Severity.LOW);
    addHeuristic(new MapperDataSkewHeuristic());
    addHeuristic(new ReducerDataSkewHeuristic());
    addHeuristic(new MapperInputSizeHeuristic());
    addHeuristic(new MapperSpeedHeuristic());
    addHeuristic(new ReducerTimeHeuristic());
    addHeuristic(new ShuffleSortHeuristic());
  }

  public void addHeuristic(Heuristic heuristic) {
    _heuristics.add(heuristic);
    _heuristicNames.add(heuristic.getHeuristicName());
  }

  public HeuristicResult[] analyse(HadoopJobData data) {
    if (data.getMapperData().length == 0 && data.getReducerData().length == 0) {
      return new HeuristicResult[] { _nodata };
    }

    List<HeuristicResult> results = new ArrayList<HeuristicResult>();
    for (Heuristic heuristic : _heuristics) {
      results.add(heuristic.apply(data));
    }
    return results.toArray(new HeuristicResult[results.size()]);
  }

  public JobType getJobType(HadoopJobData data) {
    String pigVersion = data.getJobConf().getProperty("pig.version");
    if (pigVersion != null && !pigVersion.isEmpty()) {
      return JobType.PIG;
    }
    String hiveMapredMode = data.getJobConf().getProperty("hive.mapred.mode");
    if (hiveMapredMode != null && !hiveMapredMode.isEmpty()) {
      return JobType.HIVE;
    }
    return JobType.HADOOPJAVA;
  }

  public Map<String, String> getMetaUrls(HadoopJobData data) {
    Map<String, String> result = new HashMap<String, String>();
    final String prefix = "meta.url.";
    Properties jobConf = data.getJobConf();
    for (Map.Entry<Object, Object> entry : jobConf.entrySet()) {
      if (entry.getKey().toString().startsWith(prefix)) {
        String key = entry.getKey().toString();
        String value = jobConf.getProperty(key);
        result.put(key.substring(prefix.length()), value);
      }
    }
    return result;
  }

  public static ElephantAnalyser instance() {
    return ANALYZER;
  }
}
