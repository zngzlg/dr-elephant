package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;
import com.linkedin.drelephant.math.Statistics;
import org.apache.commons.io.FileUtils;

public class GeneralMapperSlowHeuristic implements Heuristic {
    @Override
    public HeuristicResult apply(HadoopJobData data) {
        HadoopTaskData[] tasks = data.getMapperData();

        //Gather data
        long[] speeds = new long[tasks.length];
        long[] runtime = new long[tasks.length];

        for (int i = 0; i < tasks.length; i++) {
            long input_bytes = tasks[i].getCounters().get(HadoopCounterHolder.CounterName.HDFS_BYTES_READ);
            runtime[i] = tasks[i].getEndTime() - tasks[i].getStartTime();
            //Apply 1 minute buffer
            runtime[i] -= 60 * 1000;
            if (runtime[i] < 1000) {
                runtime[i] = 1000;
            }
            //Speed is bytes per second
            speeds[i] = (1000 * input_bytes) / (runtime[i]);
        }

        //Analyze data
        long average = Statistics.average(speeds);
        long avgRuntime = Statistics.average(runtime);

        //Ignore jobs less than 10 minutes
        if (avgRuntime < 10 * 60 * 1000) {
            return HeuristicResult.SUCCESS;
        }

        long limit = Constants.DISK_READ_SPEED / 4;

        if (average < limit) {
            HeuristicResult result = new HeuristicResult("Mapper side is unusually slow", false);

            result.addDetail("Average mapper speed", FileUtils.byteCountToDisplaySize(average) + "/s");

            return result;
        }

        return HeuristicResult.SUCCESS;
    }
}
