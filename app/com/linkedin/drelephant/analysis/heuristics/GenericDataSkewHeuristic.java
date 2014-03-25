package com.linkedin.drelephant.analysis.heuristics;

import com.linkedin.drelephant.analysis.Heuristic;
import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.hadoop.HadoopCounterHolder;
import com.linkedin.drelephant.hadoop.HadoopJobData;
import com.linkedin.drelephant.hadoop.HadoopTaskData;
import com.linkedin.drelephant.math.Statistics;
import org.apache.commons.io.FileUtils;

public abstract class GenericDataSkewHeuristic implements Heuristic {
    private HadoopCounterHolder.CounterName counterName;
    private String failMessage;

    protected GenericDataSkewHeuristic(HadoopCounterHolder.CounterName counterName, String failMessage) {
        this.counterName = counterName;
        this.failMessage = failMessage;
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
        int buffer = 50 * 1024 * 1024;
        int[] deviations = Statistics.deviates(inputBytes, buffer, 0.5D);

        if (deviations.length > 0) {
            HeuristicResult result = new HeuristicResult(failMessage, false);

            long average = Statistics.average(inputBytes);
            result.addDetail("Number of tasks", Integer.toString(tasks.length));
            result.addDetail("Average task input", FileUtils.byteCountToDisplaySize(average));

            int num = 0;
            for (int i : deviations) {
                if (num >= 5) {
                    result.addDetail("... and " + (deviations.length - num) + " more", "");
                    break;
                }
                String inputByteString = FileUtils.byteCountToDisplaySize(inputBytes[i]);
                String deviationFactor = Statistics.describeFactor(inputBytes[i], average, "x avg");
                result.addDetail(tasks[i].getTaskId() + " input", inputByteString + " " + deviationFactor);
                num++;
            }

            return result;
        }

        return HeuristicResult.SUCCESS;
    }
}
