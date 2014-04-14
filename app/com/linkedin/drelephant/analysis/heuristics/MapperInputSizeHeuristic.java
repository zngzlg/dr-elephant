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
    public static final String heuristicName = "Mapper Input Size";

    @Override
    public String getHeuristicName() {
        return heuristicName;
    }

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

        Severity smallFilesSeverity = smallFilesSeverity(average, tasks.length);
        Severity largeFilesSeverity = largeFilesSeverity(average, tasks.length);
        Severity severity = Severity.max(smallFilesSeverity, largeFilesSeverity);

        HeuristicResult result = new HeuristicResult(heuristicName, severity);

        result.addDetail("Number of tasks", Integer.toString(tasks.length));
        result.addDetail("Average task input", FileUtils.byteCountToDisplaySize(average));

        return result;
    }

    private Severity smallFilesSeverity(long value, long numTasks) {
        Severity severity = getSmallFilesSeverity(value);
        Severity taskSeverity = getNumTasksSeverity(numTasks);
        return Severity.min(severity, taskSeverity);
    }

    private Severity largeFilesSeverity(long value, long numTasks) {
        Severity severity = getLargeFilesSeverity(value);
        Severity taskSeverity = getNumTasksSeverityReverse(numTasks);
        return Severity.min(severity, taskSeverity);
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

    public static Severity getNumTasksSeverity(long numTasks) {
        return Severity.getSeverityAscending(numTasks,
                10, 50, 200, 500);
    }

    public static Severity getNumTasksSeverityReverse(long numTasks) {
        return Severity.getSeverityDescending(numTasks,
                1000, 500, 200, 100);
    }
}
