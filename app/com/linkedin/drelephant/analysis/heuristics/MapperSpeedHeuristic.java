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

public class MapperSpeedHeuristic implements Heuristic {
    private static final String heuristicName = "Mapper Speed";

    @Override
    public String getHeuristicName() {
        return heuristicName;
    }

    @Override
    public HeuristicResult apply(HadoopJobData data) {
        HadoopTaskData[] tasks = data.getMapperData();

        //Gather data
        long[] speeds = new long[tasks.length];
        long[] runtimes = new long[tasks.length];

        for (int i = 0; i < tasks.length; i++) {
            long input_bytes = tasks[i].getCounters().get(HadoopCounterHolder.CounterName.HDFS_BYTES_READ);
            runtimes[i] = tasks[i].getEndTime() - tasks[i].getStartTime();
            //Apply 1 minute buffer
            runtimes[i] -= 60 * 1000;
            if (runtimes[i] < 1000) {
                runtimes[i] = 1000;
            }
            //Speed is bytes per second
            speeds[i] = (1000 * input_bytes) / (runtimes[i]);
        }

        //Analyze data
        long averageSpeed = Statistics.average(speeds);
        long averageRuntime = Statistics.average(runtimes);

        Severity severity = getDiskSpeedSeverity(averageSpeed);

        //This reduces severity if task runtime is insignificant
        severity = Severity.min(severity, getRuntimeSeverity(averageRuntime));

        HeuristicResult result = new HeuristicResult(heuristicName, severity);

        result.addDetail("Number of tasks", Integer.toString(tasks.length));
        result.addDetail("Average mapper speed", FileUtils.byteCountToDisplaySize(averageSpeed) + "/s");
        result.addDetail("Average mapper runtime", Statistics.readableTimespan(averageRuntime));

        return result;
    }

    public static Severity getDiskSpeedSeverity(long speed) {
        return Severity.getSeverityDescending(speed,
                Constants.DISK_READ_SPEED / 2,
                Constants.DISK_READ_SPEED / 4,
                Constants.DISK_READ_SPEED / 8,
                Constants.DISK_READ_SPEED / 32);
    }

    public static Severity getRuntimeSeverity(long runtime) {
        return Severity.getSeverityAscending(runtime,
                5 * Statistics.MINUTE,
                20 * Statistics.MINUTE,
                40 * Statistics.MINUTE,
                1 * Statistics.HOUR);
    }
}
