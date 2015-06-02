package com.linkedin.drelephant.spark.heuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.spark.SparkApplicationData;
import com.linkedin.drelephant.spark.SparkEnvironmentData;
import com.linkedin.drelephant.util.MemoryFormatUtils;


/**
 * This heuristic rule check some of the most commonly set spark properties and make sure the user is following
 * a best convention of them.
 *
 * @author yizhou
 */
public class BestPropertiesConventionHeuristic implements Heuristic<SparkApplicationData> {
  public static final String HEURISTIC_NAME = "Spark Configuration Best Practice";
  public static final String SPARK_SERIALIZER = "spark.serializer";
  public static final String SPARK_DRIVER_MEMORY = "spark.driver.memory";
  public static final String SPARK_SHUFFLE_MANAGER = "spark.shuffle.manager";
  public static final String SPARK_EXECUTOR_CORES = "spark.executor.cores";

  @Override
  public HeuristicResult apply(SparkApplicationData data) {
    SparkEnvironmentData env = data.getEnvironmentData();
    String sparkSerializer = env.getSparkProperty(SPARK_SERIALIZER);
    String sparkDriverMemory = env.getSparkProperty(SPARK_DRIVER_MEMORY);
    String sparkShuffleManager = env.getSparkProperty(SPARK_SHUFFLE_MANAGER);
    String sparkExecutorCores = env.getSparkProperty(SPARK_EXECUTOR_CORES);
    int coreNum = sparkExecutorCores == null ? 1 : Integer.parseInt(sparkExecutorCores);

    Severity kryoSeverity =
        binarySeverity("org.apache.spark.serializer.KryoSerializer", sparkSerializer, false, Severity.MODERATE);
    Severity driverMemSeverity = getDriverMemorySeverity(MemoryFormatUtils.stringToBytes(sparkDriverMemory));
    Severity sortSeverity = binarySeverity("sort", sparkShuffleManager, true, Severity.MODERATE);
    Severity executorCoreSeverity = getCoreNumSeverity(coreNum);

    HeuristicResult result = new HeuristicResult(getHeuristicName(),
        Severity.max(kryoSeverity, driverMemSeverity, sortSeverity, executorCoreSeverity));

    result.addDetail(SPARK_SERIALIZER, propertyToString(sparkSerializer));
    result.addDetail(SPARK_DRIVER_MEMORY, propertyToString(sparkDriverMemory));
    result.addDetail(SPARK_SHUFFLE_MANAGER, propertyToString(sparkShuffleManager));
    result.addDetail(SPARK_EXECUTOR_CORES, propertyToString(sparkExecutorCores));

    return result;
  }

  private static Severity getCoreNumSeverity(int cores) {
    if (cores > 2) {
      return Severity.CRITICAL;
    } else {
      return Severity.NONE;
    }
  }

  private static Severity getDriverMemorySeverity(long mem) {
    return Severity
        .getSeverityAscending(mem, MemoryFormatUtils.stringToBytes("4G"), MemoryFormatUtils.stringToBytes("4G"),
            MemoryFormatUtils.stringToBytes("8G"), MemoryFormatUtils.stringToBytes("8G"));
  }

  private static Severity binarySeverity(String expectedValue, String actualValue, boolean ignoreNull,
      Severity severity) {
    if (actualValue == null) {
      if (ignoreNull) {
        return Severity.NONE;
      } else {
        return severity;
      }
    }

    if (actualValue.equals(expectedValue)) {
      return Severity.NONE;
    } else {
      return severity;
    }
  }

  private static String propertyToString(String val) {
    return val == null ? "not presented, using default" : val;
  }

  @Override
  public String getHeuristicName() {
    return HEURISTIC_NAME;
  }
}
