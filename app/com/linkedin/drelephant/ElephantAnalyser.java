package com.linkedin.drelephant;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import play.api.Play;

import org.apache.log4j.Logger;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.JobType;
import com.linkedin.drelephant.util.HeuristicConf;
import com.linkedin.drelephant.util.HeuristicConfData;
import com.linkedin.drelephant.util.JobTypeConf;


public class ElephantAnalyser {
  public static final String NO_DATA = "No Data Received";
  public static final String DEFAULT_TYPE = "HadoopJava";
  private static final Logger logger = Logger.getLogger(ElephantAnalyser.class);
  private static ElephantAnalyser _analyzer = null;

  private List<String> _heuristicNames = new ArrayList<String>();
  private HeuristicResult _nodata;
  private List<Heuristic> _heuristics = new ArrayList<Heuristic>();
  private List<JobType> _jobTypeList;

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
    loadHeuristics();
    loadJobType();
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

  public String getJobType(HadoopJobData data) {
    Properties conf = data.getJobConf();
    for (JobType type: _jobTypeList) {
      if(type.matchType(conf)) {
        return type.getName();
      }
    }
    // Return default type. This line shouldn't reached as the job type list
    // has a default type as its last element.
    return DEFAULT_TYPE;
  }

  public List<String> getHeuristicNames() {
    return this._heuristicNames;
  }

  public List<JobType> getJobTypes() {
    return _jobTypeList;
  }

  private void loadJobType() {
    _jobTypeList = JobTypeConf.instance().getJobTypeList();
  }

  private void loadHeuristics() {

    HeuristicConf conf = HeuristicConf.instance();
    List<HeuristicConfData> heuristicsConfData = conf.getHeuristicsConfData();

    for (HeuristicConfData data : heuristicsConfData) {
      try {
        Class<?> heuristicClass = Play.current().classloader().loadClass(data.getClassName());
        Heuristic heuristicInstance = (Heuristic) heuristicClass.newInstance();
        _heuristics.add(heuristicInstance);
        _heuristicNames.add(heuristicInstance.getHeuristicName());
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
  }
}
