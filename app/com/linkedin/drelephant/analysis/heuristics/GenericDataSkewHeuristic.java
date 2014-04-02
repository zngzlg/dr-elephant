package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.analysis.Severity;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;
import com.linkedin.drelephant.math.Statistics;
import org.apache.commons.io.FileUtils;

public abstract class GenericDataSkewHeuristic implements Heuristic {
    private HadoopCounterHolder.CounterName counterName;
    private String heuristicName;

    @Override
    public String getHeuristicName() {
        return heuristicName;
    }

    protected GenericDataSkewHeuristic(HadoopCounterHolder.CounterName counterName, String heuristicName) {
        this.counterName = counterName;
        this.heuristicName = heuristicName;
    }

    protected abstract HadoopTaskData[] getTasks(HadoopJobData data);

    @Override
    public HeuristicResult apply(HadoopJobData data) {
        HadoopTaskData[] tasks = getTasks(data);

        //Gather data
        long[] inputBytes = new long[tasks.length];

        for (int i = 0; i < tasks.length; i++) {
            inputBytes[i] = tasks[i].getCounters().get(counterName);
        }

        //Analyze data
        long[][] groups = Statistics.findTwoGroups(inputBytes);

        long avg1 = Statistics.average(groups[0]);
        long avg2 = Statistics.average(groups[1]);

        long min = Math.min(avg1, avg2);
        long diff = Math.abs(avg2 - avg1);

        Severity severity = getDeviationSeverity(min, diff);

        //This reduces severity if the largest file sizes are insignificant
        severity = Severity.min(severity, getFilesSeverity(avg2));

        //This reduces severity if number of tasks is insignificant
        severity = Severity.min(severity, Statistics.getNumTasksSeverity(groups[0].length));

        HeuristicResult result = new HeuristicResult(heuristicName, severity);

        result.addDetail("Number of tasks", Integer.toString(tasks.length));
        result.addDetail("Group A: Number of tasks", Integer.toString(groups[0].length));
        result.addDetail("Group A: Average input size", FileUtils.byteCountToDisplaySize(avg1));
        result.addDetail("Group B: Number of tasks", Integer.toString(groups[1].length));
        result.addDetail("Group B: Average input size", FileUtils.byteCountToDisplaySize(avg2));

        return result;
    }

    public static Severity getDeviationSeverity(long averageMin, long averageDiff) {
        if (averageMin <= 0) {
            averageMin = 1;
        }
        long value = averageDiff / averageMin;
        return Severity.getSeverityAscending(value,
                2, 4, 8, 16);
    }

    public static Severity getFilesSeverity(long value) {
        return Severity.getSeverityAscending(value,
                Constants.HDFS_BLOCK_SIZE / 8,
                Constants.HDFS_BLOCK_SIZE / 4,
                Constants.HDFS_BLOCK_SIZE / 2,
                Constants.HDFS_BLOCK_SIZE);
    }
}
