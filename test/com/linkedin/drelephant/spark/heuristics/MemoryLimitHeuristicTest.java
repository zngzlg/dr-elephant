package com.linkedin.drelephant.spark.heuristics;

import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.spark.MockSparkApplicationData;
import com.linkedin.drelephant.spark.SparkApplicationData;
import com.linkedin.drelephant.spark.SparkEnvironmentData;
import com.linkedin.drelephant.spark.SparkExecutorData;
import com.linkedin.drelephant.util.MemoryFormatUtils;
import java.util.HashSet;
import java.util.Set;
import junit.framework.TestCase;

import static com.linkedin.drelephant.spark.SparkExecutorData.EXECUTOR_DRIVER_NAME;
import static com.linkedin.drelephant.spark.heuristics.MemoryLimitHeuristic.SPARK_DRIVER_MEMORY;
import static com.linkedin.drelephant.spark.heuristics.MemoryLimitHeuristic.SPARK_EXECUTOR_INSTANCES;
import static com.linkedin.drelephant.spark.heuristics.MemoryLimitHeuristic.SPARK_EXECUTOR_MEMORY;
import static com.linkedin.drelephant.spark.heuristics.MemoryLimitHeuristic.DEFAULT_SPARK_STORAGE_MEMORY_FRACTION;


/**
 * This class tests the heuristic rule: MemoryLimitHeuristic
 *
 * @author yizhou
 */
public class MemoryLimitHeuristicTest extends TestCase {
  public void testTotalMemoryRule() {
    // Test if the total memory limit is working, set all peak memory to arbirarity 100%
    assertEquals(Severity.NONE, analyzeJob(1, "1G", "1G", "1G"));
    assertEquals(Severity.NONE, analyzeJob(100, "1G", "1G", "100G"));
    assertEquals(Severity.NONE, analyzeJob(10, "10G", "1G", "100G"));
    assertEquals(Severity.LOW, analyzeJob(600, "1G", "1G", "600G"));
    assertEquals(Severity.MODERATE, analyzeJob(2400, "512M", "1G", "1.2T"));
    assertEquals(Severity.SEVERE, analyzeJob(1600, "1G", "1G", "1.6T"));
    assertEquals(Severity.CRITICAL, analyzeJob(4200, "512M", "1G", "2.1T"));
  }

  public void testMemoryUtilizationRule() {
    // Test if the total memory utilization is working

    // When the total memory is too low, ignore the ratio calculation
    assertEquals(Severity.NONE, analyzeJob(1, "1G", "1G", "0B"));
    // When we barely pass the safe zone
    assertEquals(Severity.CRITICAL, analyzeJob(20, "1G", "1G", "0B"));

    // Normal situations
    assertEquals(Severity.NONE, analyzeJob(100, "1G", "1G", getPeakMemory(1.0d, 100, "1G")));
    assertEquals(Severity.LOW, analyzeJob(100, "1G", "1G", getPeakMemory(0.7d, 100, "1G")));
    assertEquals(Severity.MODERATE, analyzeJob(100, "1G", "1G", getPeakMemory(0.5d, 100, "1G")));
    assertEquals(Severity.SEVERE, analyzeJob(100, "1G", "1G", getPeakMemory(0.3d, 100, "1G")));
    assertEquals(Severity.CRITICAL, analyzeJob(100, "1G", "1G", getPeakMemory(0.1d, 100, "1G")));
  }

  public void testCombinedRules() {
    // Mix multiple rules together, majorly check the combined logic
    assertEquals(Severity.CRITICAL, analyzeJob(1, "1G", "10T", "0B"));
    assertEquals(Severity.CRITICAL, analyzeJob(2500, "1G", "1G", getPeakMemory(0.7d, 2500, "1G")));
  }

  public void testMissingSparkDriverMemoryProperty() {
    HeuristicResult result = getJobresult(100, "1G", "700M", getPeakMemory(1.0d, 100, "1G"), SPARK_DRIVER_MEMORY);
    for (String detail : result.getDetails()) {
      System.out.println(detail);
      if (detail.startsWith("\"Total driver memory allocated")) {
        assertEquals("\"Total driver memory allocated\",\"700 MB\"", detail);
      }
    }
  }

  private static String getPeakMemory(double utilRatio, int executors, String memPerExecutor) {
    long totalMem =
        (long) (executors * MemoryFormatUtils.stringToBytes(memPerExecutor) * DEFAULT_SPARK_STORAGE_MEMORY_FRACTION);
    return MemoryFormatUtils.bytesToString((long) (utilRatio * totalMem));
  }

  private Severity analyzeJob(int executors, String memPerExecutor, String driverMem, String peakTotalMem,
      String... skippedProperties) {
    return getJobresult(executors, memPerExecutor, driverMem, peakTotalMem, skippedProperties).getSeverity();
  }

  private HeuristicResult getJobresult(int executors, String memPerExecutor, String driverMem, String peakTotalMem,
      String... skippedProperties) {
    Set<String> filters = new HashSet<String>();
    for (int i = 0; i < skippedProperties.length; i++) {
      filters.add(skippedProperties[i]);
    }

    SparkApplicationData data = new MockSparkApplicationData();
    SparkEnvironmentData env = data.getEnvironmentData();
    if (!filters.contains(SPARK_EXECUTOR_INSTANCES)) {
      env.addSparkProperty(SPARK_EXECUTOR_INSTANCES, String.valueOf(executors));
    }
    if (!filters.contains(SPARK_EXECUTOR_MEMORY)) {
      env.addSparkProperty(SPARK_EXECUTOR_MEMORY, memPerExecutor);
    }
    if (!filters.contains(SPARK_DRIVER_MEMORY)) {
      env.addSparkProperty(SPARK_DRIVER_MEMORY, driverMem);
    }

    SparkExecutorData exe = data.getExecutorData();
    SparkExecutorData.ExecutorInfo driverInfo = new SparkExecutorData.ExecutorInfo();
    driverInfo.maxMem = (long) (MemoryFormatUtils.stringToBytes(driverMem) * DEFAULT_SPARK_STORAGE_MEMORY_FRACTION);
    exe.setExecutorInfo(EXECUTOR_DRIVER_NAME, driverInfo);

    long bytesPerExecutor = MemoryFormatUtils.stringToBytes(memPerExecutor);

    /* Assign evenly the peak memory to each executor, in practical cases, we might observe the executor peak memory
     * can vary a bit due to data skewness and imperfect partitioning.
     */
    long peakMemToGenerate = MemoryFormatUtils.stringToBytes(peakTotalMem) / executors;
    for (int i = 0; i < executors; i++) {
      SparkExecutorData.ExecutorInfo info = new SparkExecutorData.ExecutorInfo();
      info.maxMem = (long) (bytesPerExecutor * DEFAULT_SPARK_STORAGE_MEMORY_FRACTION);
      info.memUsed = peakMemToGenerate;
      exe.setExecutorInfo(String.valueOf(i), info);
    }
    return new MemoryLimitHeuristic().apply(data);
  }
}
