package com.linkedin.drelephant;

import java.util.ArrayList;
import java.util.List;
import model.JobType;
import play.api.Play;
import org.apache.log4j.Logger;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.util.HeuristicConf;
import com.linkedin.drelephant.util.HeuristicConfData;


public class ElephantAnalyser {
  public static final String NO_DATA = "No Data Received";

  private List<String> _heuristicNames = new ArrayList<String>();
  private static final Logger logger = Logger.getLogger(ElephantAnalyser.class);
  private static ElephantAnalyser _analyzer = null;
  private HeuristicResult _nodata;
  private List<Heuristic> _heuristics = new ArrayList<Heuristic>();

  public static void init() {
    if (_analyzer == null) {
      _analyzer = new ElephantAnalyser();
    }
  }

  public static ElephantAnalyser instance() {
    if (_analyzer == null) {
      init();
    }
    return _analyzer;
  }

  private ElephantAnalyser() {
    logger.info("Initialize Analyzer... Loading heuristics....");
    _nodata = new HeuristicResult(NO_DATA, Severity.LOW);
    List<Heuristic> heuristics = loadHeuristics();
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
      logger.info("No Data Received for job: " + data.getJobId());
      return new HeuristicResult[] { _nodata };
    }

    List<HeuristicResult> results = new ArrayList<HeuristicResult>();
    for (Heuristic heuristic : _heuristics) {
      results.add(heuristic.apply(data));
    }
    return results.toArray(new HeuristicResult[results.size()]);
  }

  public JobType getJobType(HadoopJobData data) {
    String pigVersion = data.getJobConf().getProperty("pig.script");
    if (pigVersion != null && !pigVersion.isEmpty()) {
      return JobType.PIG;
    }
    String hiveMapredMode = data.getJobConf().getProperty("hive.mapred.mode");
    if (hiveMapredMode != null && !hiveMapredMode.isEmpty()) {
      return JobType.HIVE;
    }
    return JobType.HADOOPJAVA;
  }

  public List<String> getHeuristicNames() {
    return this._heuristicNames;
  }

  private List<Heuristic> loadHeuristics() {

    List<Heuristic> heuristics = new ArrayList<Heuristic>();
    HeuristicConf conf = HeuristicConf.instance();
    List<HeuristicConfData> heuristicsConfData = conf.getHeuristicsConfData();

    for (HeuristicConfData data : heuristicsConfData) {
      try {
        Class<?> heuristicClass = Play.current().classloader().loadClass(data.getClassName());
        heuristics.add((Heuristic) heuristicClass.newInstance());
        logger.info("Load Heuristic : " + data.getClassName());
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Could not find class " + data.getClassName(), e);
      } catch (InstantiationException e) {
        throw new RuntimeException("Could not instantiate class " + data.getClassName(), e);
      } catch (IllegalAccessException e) {
        throw new RuntimeException("Could not access constructor for class" + data.getClassName(), e);
      } catch (RuntimeException e) {
        //More descriptive on other runtime exception such as ClassCastException
        throw new RuntimeException(data.getClassName() + " is not a valid Heuristic class.", e);
      }
    }
    return heuristics;
  }
}
