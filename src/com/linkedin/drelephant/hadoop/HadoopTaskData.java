package com.linkedin.drelephant.hadoop;


import org.apache.hadoop.mapred.TaskReport;

public class HadoopTaskData {
    private HadoopCounterHolder counterHolder;

    public HadoopTaskData(TaskReport task) {
        counterHolder = new HadoopCounterHolder(task.getCounters());
    }

    public HadoopCounterHolder getCounters() {
        return counterHolder;
    }
}
