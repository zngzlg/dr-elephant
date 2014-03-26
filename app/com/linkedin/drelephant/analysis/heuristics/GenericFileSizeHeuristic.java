package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.analysis.Constants;
import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;
import com.linkedin.drelephant.math.Statistics;
import org.apache.commons.io.FileUtils;

public abstract class GenericFileSizeHeuristic implements Heuristic {
    private String failMessage;

    protected GenericFileSizeHeuristic(String failMessage) {
        this.failMessage = failMessage;
    }

    /**
     * Checks if the average is out of ordinary
     *
     * @param average
     * @return true if the average is bad/problematic
     */
    protected abstract boolean checkAverage(long average);

    @Override
    public HeuristicResult apply(HadoopJobData data) {
        HadoopTaskData[] tasks = data.getMapperData();

        //Ignore tasks with little amount of data anyway
        if (tasks.length <= 5) {
            return HeuristicResult.SUCCESS;
        }

        //Gather data
        long[] inputBytes = new long[tasks.length];

        for (int i = 0; i < tasks.length; i++) {
            inputBytes[i] = tasks[i].getCounters().get(HadoopCounterHolder.CounterName.HDFS_BYTES_READ);
        }

        //Analyze data
        long average = Statistics.average(inputBytes);

        if (checkAverage(average)) {
            HeuristicResult result = new HeuristicResult(failMessage, false);

            result.addDetail("Number of tasks", Integer.toString(tasks.length));
            String deviationFactor = Statistics.describeFactor(average, Constants.HDFS_BLOCK_SIZE, "x");
            result.addDetail("Average task input", FileUtils.byteCountToDisplaySize(average) + " " + deviationFactor);
            result.addDetail("HDFS Block size", FileUtils.byteCountToDisplaySize(Constants.HDFS_BLOCK_SIZE));

            return result;
        }

        return HeuristicResult.SUCCESS;
    }
}
