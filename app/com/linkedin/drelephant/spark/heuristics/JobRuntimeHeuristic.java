/*
 * Copyright 2015 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.linkedin.drelephant.spark.heuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.spark.SparkApplicationData;
import com.linkedin.drelephant.spark.SparkJobProgressData;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang.StringUtils;


/**
 * This heuristic rule observes the runtime characteristics of the spark application run.
 *
 * @author yizhou
 */
public class JobRuntimeHeuristic implements Heuristic<SparkApplicationData> {
  public static final String HEURISTIC_NAME = "Spark Job Runtime";

  @Override
  public HeuristicResult apply(SparkApplicationData data) {
    SparkJobProgressData jobProgressData = data.getJobProgressData();
    Severity endSeverity = Severity.NONE;

    Set<Integer> completedJobs = jobProgressData.getCompletedJobs();
    Set<Integer> failedJobs = jobProgressData.getFailedJobs();

    // Average job failure rate
    double avgJobFailureRate = jobProgressData.getJobFailureRate();
    Severity jobFailureRateSeverity = getAvgJobFailureRateSeverity(avgJobFailureRate);
    endSeverity = Severity.max(endSeverity, jobFailureRateSeverity);

    // For each completed individual job
    List<String> highFailureRateJobs = new ArrayList<String>();
    for (int jobId : completedJobs) {
      SparkJobProgressData.JobInfo job = jobProgressData.getJobInfo(jobId);
      double jobFailureRate = job.getFailureRate();
      Severity severity = getSingleJobFailureRateSeverity(jobFailureRate);
      if (severity.getValue() > Severity.MODERATE.getValue()) {
        highFailureRateJobs.add(
            jobProgressData.getJobDescription(jobId) + " (task failure rate:" + String.format("%1.3f", jobFailureRate)
                + ")");
      }
      endSeverity = Severity.max(endSeverity, severity);
    }

    HeuristicResult result = new HeuristicResult(getHeuristicName(), endSeverity);

    result.addDetail("Spark completed jobs number", String.valueOf(completedJobs.size()));
    result.addDetail("Spark failed jobs number", String.valueOf(failedJobs.size()));
    result.addDetail("Spark failed jobs list", getJobListString(jobProgressData.getFailedJobDescriptions()));
    result.addDetail("Spark average job failure rate", String.format("%.3f", avgJobFailureRate));
    result.addDetail("Spark jobs with high task failure rate", getJobListString(highFailureRateJobs));

    return result;
  }

  @Override
  public String getHeuristicName() {
    return HEURISTIC_NAME;
  }

  private static Severity getAvgJobFailureRateSeverity(double rate) {
    return Severity.getSeverityAscending(rate, 0.1d, 0.3d, 0.5d, 0.5d);
  }

  private static Severity getSingleJobFailureRateSeverity(double rate) {
    return Severity.getSeverityAscending(rate, 0.0d, 0.3d, 0.5d, 0.5d);
  }

  private static String getJobListString(Collection<String> names) {
    return "[" + StringUtils.join(names, ",") + "]";
  }
}
