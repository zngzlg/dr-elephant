package com.linkedin.drelephant.analysis.heuristics;

import java.util.ArrayList;
import java.util.List;

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
    public static final String heuristicName = "Mapper Speed";

    @Override
    public String getHeuristicName() {
        return heuristicName;
    }

    @Override
    public HeuristicResult apply(HadoopJobData data) {

        HadoopTaskData[] tasks = data.getMapperData();

        List<Long> input_byte_sizes = new ArrayList<Long>();
        List<Long> speeds = new ArrayList<Long>();
        List<Long> runtimes = new ArrayList<Long>();

        for(HadoopTaskData task : tasks) {
          if(task.timed()) {
            long input_bytes = task.getCounters().get(HadoopCounterHolder.CounterName.HDFS_BYTES_READ);
            long runtime = task.getEndTime() - task.getStartTime();
            //Apply 1 minute buffer
            runtime -= 60 * 1000;
            if (runtime < 1000) {
                runtime = 1000;
            }
            input_byte_sizes.add(input_bytes);
            runtimes.add(runtime);
            //Speed is bytes per second
            speeds.add((1000 * input_bytes) / (runtime));
          }
        }

        //Analyze data
        long averageSpeed = Statistics.average(speeds);
        long averageSize = Statistics.average(input_byte_sizes);
        long averageRuntime = Statistics.average(runtimes);

        Severity severity = getDiskSpeedSeverity(averageSpeed);

        //This reduces severity if task runtime is insignificant
        severity = Severity.min(severity, getRuntimeSeverity(averageRuntime));

        HeuristicResult result = new HeuristicResult(heuristicName, severity);

        result.addDetail("Number of tasks", Integer.toString(tasks.length));
        result.addDetail("Average task input size", FileUtils.byteCountToDisplaySize(averageSize));
        result.addDetail("Average task speed", FileUtils.byteCountToDisplaySize(averageSpeed) + "/s");
        result.addDetail("Average task runtime", Statistics.readableTimespan(averageRuntime));

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
