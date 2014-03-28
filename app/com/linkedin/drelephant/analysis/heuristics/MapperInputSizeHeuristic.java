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

public class MapperInputSizeHeuristic implements Heuristic {
    private static final String analysisName = "Mapper input size";

    @Override
    public HeuristicResult apply(HadoopJobData data) {
        HadoopTaskData[] tasks = data.getMapperData();

        //Gather data
        long[] inputBytes = new long[tasks.length];

        for (int i = 0; i < tasks.length; i++) {
            inputBytes[i] = tasks[i].getCounters().get(HadoopCounterHolder.CounterName.HDFS_BYTES_READ);
        }

        //Analyze data
        long average = Statistics.average(inputBytes);

        Severity smallFilesSeverity = getSmallFilesSeverity(average);
        Severity largeFilesSeverity = getLargeFilesSeverity(average);
        Severity severity = Severity.max(smallFilesSeverity, largeFilesSeverity);

        //This reduces severity if number of tasks is insignificant
        severity = Severity.min(severity, Statistics.getNumTasksSeverity(tasks.length));

        HeuristicResult result = new HeuristicResult(analysisName, severity);

        result.addDetail("Number of tasks", Integer.toString(tasks.length));
        String deviationFactor = Statistics.describeFactor(average, Constants.HDFS_BLOCK_SIZE, "x");
        result.addDetail("Average task input", FileUtils.byteCountToDisplaySize(average) + " " + deviationFactor);
        result.addDetail("HDFS Block size", FileUtils.byteCountToDisplaySize(Constants.HDFS_BLOCK_SIZE));

        return result;
    }

    public static Severity getSmallFilesSeverity(long value) {
        return Severity.getSeverityDescending(value,
                Constants.HDFS_BLOCK_SIZE / 2,
                Constants.HDFS_BLOCK_SIZE / 4,
                Constants.HDFS_BLOCK_SIZE / 8,
                Constants.HDFS_BLOCK_SIZE / 32);
    }

    public static Severity getLargeFilesSeverity(long value) {
        return Severity.getSeverityAscending(value,
                Constants.HDFS_BLOCK_SIZE * 2,
                Constants.HDFS_BLOCK_SIZE * 3,
                Constants.HDFS_BLOCK_SIZE * 4,
                Constants.HDFS_BLOCK_SIZE * 5);
    }
}
