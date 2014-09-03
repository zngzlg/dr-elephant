package com.linkedin.drelephant.analysis.heuristics;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;


public class JobQueueLimitHeuristic implements Heuristic {
  public static final String heuristicName = "Job Queue Timeout Limit";

  @Override
  public String getHeuristicName() {
    return heuristicName;
  }

  @Override
  public HeuristicResult apply(HadoopJobData data) {
    HeuristicResult result = new HeuristicResult(heuristicName, Severity.NONE);
    Properties jobConf = data.getJobConf();
    long queueTimeoutLimit = TimeUnit.MINUTES.toMillis(15);

    // Fetch the Queue to which the job is submitted.
    String queueName = jobConf.getProperty("mapred.job.queue.name");
    if (queueName == null) {
      throw new IllegalStateException("Queue Name not found.");
    }

    // Compute severity if job is submitted to default queue else set severity to NONE.
    HadoopTaskData[] mapTasks = data.getMapperData();
    HadoopTaskData[] redTasks = data.getReducerData();
    Severity[] mapTasksSeverity = new Severity[mapTasks.length];
    Severity[] redTasksSeverity = new Severity[redTasks.length];
    if (queueName.equals("default")) {
      result.addDetail("Queue: ", queueName);
      result.addDetail("Number of Map tasks", Integer.toString(mapTasks.length));
      result.addDetail("Number of Reduce tasks", Integer.toString(redTasks.length));

      // Calculate Severity of Mappers
      mapTasksSeverity = getTasksSeverity(mapTasks, queueTimeoutLimit);
      result.addDetail("Number of Map tasks that are in severe state (14 to 14.5 min)",
          Long.toString(getSeverityFrequency(Severity.SEVERE, mapTasksSeverity)));
      result.addDetail("Number of Map tasks that are in critical state (over 14.5 min)",
          Long.toString(getSeverityFrequency(Severity.CRITICAL, mapTasksSeverity)));

      // Calculate Severity of Reducers
      redTasksSeverity = getTasksSeverity(redTasks, queueTimeoutLimit);
      result.addDetail("Number of Reduce tasks that are in severe state (14 to 14.5 min)",
          Long.toString(getSeverityFrequency(Severity.SEVERE, redTasksSeverity)));
      result.addDetail("Number of Reduce tasks that are in critical state (over 14.5 min)",
          Long.toString(getSeverityFrequency(Severity.CRITICAL, redTasksSeverity)));

      // Calculate Job severity
      result.setSeverity(Severity.max(Severity.max(mapTasksSeverity), Severity.max(redTasksSeverity)));

    } else {
      result.addDetail("This Heuristic is not applicable to " + queueName + " queue");
      result.setSeverity(Severity.NONE);
    }
    return result;
  }

  private Severity[] getTasksSeverity(HadoopTaskData[] tasks, long queueTimeout) {
    Severity[] tasksSeverity = new Severity[tasks.length];
    int i = 0;
    for (HadoopTaskData task : tasks) {
      tasksSeverity[i] = getQueueLimitSeverity(task.getRunTime(), queueTimeout);
      i++;
    }
    return tasksSeverity;
  }

  private long getSeverityFrequency(Severity severity, Severity[] tasksSeverity) {
    long count = 0;
    for (Severity taskSeverity : tasksSeverity) {
      if (taskSeverity.equals(severity)) {
        count++;
      }
    }
    return count;
  }

  private Severity getQueueLimitSeverity(long taskTime, long queueTimeout) {
    long timeUnit = TimeUnit.SECONDS.toMillis(30); // 30s
    if (queueTimeout == 0) {
      return Severity.NONE;
    }
    return Severity.getSeverityAscending(taskTime, queueTimeout - 4 * timeUnit, queueTimeout - 3 * timeUnit,
        queueTimeout - 2 * timeUnit, queueTimeout - timeUnit);
  }

}
