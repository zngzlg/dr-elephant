package com.linkedin.drelephant.hadoop;

public class HadoopTaskData{
    private HadoopCounterHolder counterHolder;
    private long startTime = 0;
    private long endTime = 0;
    private long shuffleTime = 0;
    private long sortTime = 0;
    private boolean timed = false;

    public HadoopTaskData(HadoopCounterHolder counterHolder, long[] time) {
        this.counterHolder = counterHolder;
        this.startTime = time[0];
        this.endTime = time[1];
        this.shuffleTime = time[2];
        this.sortTime = time[3];
        this.timed = true;
    }

    public HadoopTaskData(HadoopCounterHolder counterHolder) {
      this.counterHolder = counterHolder;
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

    public long getRunTime() {
        return endTime - startTime;
    }

    public long getExecutionTime() {
        return endTime - startTime - shuffleTime - sortTime;
    }

    public long getShuffleTime() {
        return shuffleTime;
    }

    public long getSortTime() {
        return sortTime;
    }

    public boolean timed() {
      return timed;
    }
}
