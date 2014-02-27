package com.linkedin.drelephant.hadoop;


import org.apache.hadoop.mapred.TaskReport;

public class HadoopTaskData {
    private HadoopCounterHolder counterHolder;
    private long startTime;
    private long endTime;

    public HadoopTaskData(TaskReport task) {
        counterHolder = new HadoopCounterHolder(task.getCounters());
        startTime = task.getStartTime();
        endTime = task.getFinishTime();
    }

    public HadoopCounterHolder getCounters() {
        return counterHolder;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }
}
