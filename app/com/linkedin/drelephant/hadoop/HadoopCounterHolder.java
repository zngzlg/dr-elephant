package com.linkedin.drelephant.hadoop;

import java.util.Map;


public class HadoopCounterHolder {

  private Map<CounterName, Long> _counters;

  public HadoopCounterHolder(Map<CounterName, Long> counterMap) {
    _counters = counterMap;
  }

  public long get(CounterName counterName) {
    Long value = _counters.get(counterName);
    if (value == null) {
      return 0;
    }
    return value;
  }

  public void set(CounterName counterName, long value) {
    _counters.put(counterName, value);
  }

  public static enum GroupName {
    FileInput("org.apache.hadoop.mapred.FileInputFormat$Counter"),
    FileSystemCounters("FileSystemCounters"),
    MapReduce("org.apache.hadoop.mapred.Task$Counter"),
    FileOutput("org.apache.hadoop.mapred.FileOutputFormat$Counter");

    String _name;

    GroupName(String name) {
      this._name = name;
    }

    public String getName() {
      return _name;
    }
  }

  public static enum CounterName {
    BYTES_READ(GroupName.FileInput, "BYTES_READ"),
    BYTES_WRITTEN(GroupName.FileOutput, "BYTES_WRITTEN"),

    FILE_BYTES_READ(GroupName.FileSystemCounters, "FILE_BYTES_READ"),
    FILE_BYTES_WRITTEN(GroupName.FileSystemCounters, "FILE_BYTES_WRITTEN"),
    HDFS_BYTES_READ(GroupName.FileSystemCounters, "HDFS_BYTES_READ"),
    HDFS_BYTES_WRITTEN(GroupName.FileSystemCounters, "HDFS_BYTES_WRITTEN"),

    MAP_INPUT_RECORDS(GroupName.MapReduce, "MAP_INPUT_RECORDS"),
    MAP_OUTPUT_RECORDS(GroupName.MapReduce, "MAP_OUTPUT_RECORDS"),
    MAP_OUTPUT_BYTES(GroupName.MapReduce, "MAP_OUTPUT_BYTES"),
    MAP_OUTPUT_MATERIALIZED_BYTES(GroupName.MapReduce, "MAP_OUTPUT_MATERIALIZED_BYTES"),
    SPLIT_RAW_BYTES(GroupName.MapReduce, "SPLIT_RAW_BYTES"),

    REDUCE_INPUT_GROUPS(GroupName.MapReduce, "REDUCE_INPUT_GROUPS"),
    REDUCE_SHUFFLE_BYTES(GroupName.MapReduce, "REDUCE_SHUFFLE_BYTES"),
    REDUCE_OUTPUT_RECORDS(GroupName.MapReduce, "REDUCE_OUTPUT_RECORDS"),
    REDUCE_INPUT_RECORDS(GroupName.MapReduce, "REDUCE_INPUT_RECORDS"),

    COMBINE_INPUT_RECORDS(GroupName.MapReduce, "COMBINE_INPUT_RECORDS"),
    COMBINE_OUTPUT_RECORDS(GroupName.MapReduce, "COMBINE_OUTPUT_RECORDS"),
    SPILLED_RECORDS(GroupName.MapReduce, "SPILLED_RECORDS"),

    CPU_MILLISECONDS(GroupName.MapReduce, "CPU_MILLISECONDS"),
    COMMITTED_HEAP_BYTES(GroupName.MapReduce, "COMMITTED_HEAP_BYTES"),
    PHYSICAL_MEMORY_BYTES(GroupName.MapReduce, "PHYSICAL_MEMORY_BYTES"),
    VIRTUAL_MEMORY_BYTES(GroupName.MapReduce, "VIRTUAL_MEMORY_BYTES");

    GroupName _group;
    String _name;

    CounterName(GroupName group, String name) {
      this._group = group;
      this._name = name;
    }

    public GroupName getGroup() {
      return _group;
    }

    public String getName() {
      return _name;
    }
  }
}
