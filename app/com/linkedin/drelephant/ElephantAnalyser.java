package com.linkedin.drelephant;

import java.util.ArrayList;
import java.util.List;
import model.JobType;

import org.apache.hadoop.conf.Configuration;
import play.api.Play;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.util.HeuristicConf;
import com.linkedin.drelephant.util.HeuristicConfData;


public class ElephantAnalyser {
  public static final String NO_DATA = "No Data Received";
  private static final ElephantAnalyser ANALYZER = new ElephantAnalyser();

  private HeuristicResult _nodata;
  private List<Heuristic> _heuristics = new ArrayList<Heuristic>();
  private List<String> _heuristicNames = new ArrayList<String>();

  private ElephantAnalyser() {
    _nodata = new HeuristicResult(NO_DATA, Severity.LOW);
    List<Heuristic> heuristics = getHeuristics();
    for (Heuristic heuristic : heuristics) {
      addHeuristic(heuristic);
    }
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

  public static ElephantAnalyser instance() {
    return ANALYZER;
  }

  public List<String> getHeuristicNames() {
    return this._heuristicNames;
  }

  private List<Heuristic> getHeuristics() {
    List<Heuristic> heuristics = new ArrayList<Heuristic>();
    HeuristicConf conf = HeuristicConf.instance();
    List<HeuristicConfData> heuristicsConfData = conf.getHeuristicsConfData();
    Configuration hadoopConf = new Configuration();
    String framework = hadoopConf.get("mapreduce.framework.name");

    for (HeuristicConfData data : heuristicsConfData) {
      if (data.getHadoopVersions().contains(framework)) {
        try {
          Class<?> heuristicClass = Play.current().classloader().loadClass(data.getClassName());
          heuristics.add((Heuristic) heuristicClass.newInstance());
        } catch (ClassNotFoundException e) {
          throw new RuntimeException("Could not find class " + data.getClassName()
              + ". Make sure your class is in the classpath", e);
        } catch (InstantiationException e) {
          throw new RuntimeException(
              "Could not instantiate class "
                  + data.getClassName()
                  + ". Check that your class is not an abstract class, an interface, an array class, a primitive type, or void. Also make sure the class has a nullary constructor.",
              e);
        } catch (IllegalAccessException e) {
          throw new RuntimeException("Error in accessing the class " + data.getClassName() + ". Make sure "
              + data.getClassName() + " is public", e);
        }
      }
    }
    return heuristics;

  }
}
