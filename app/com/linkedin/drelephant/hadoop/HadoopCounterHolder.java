package com.linkedin.drelephant.hadoop;

import java.util.HashMap;
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
  }

  public static enum CounterName {
    BYTES_READ(GroupName.FileInput, "BYTES_READ", "Bytes Read"),
    BYTES_WRITTEN(GroupName.FileOutput, "BYTES_WRITTEN", "Bytes Written"),

    FILE_BYTES_READ(GroupName.FileSystemCounters, "FILE_BYTES_READ", "FILE_BYTES_READ"),
    FILE_BYTES_WRITTEN(GroupName.FileSystemCounters, "FILE_BYTES_WRITTEN", "FILE_BYTES_WRITTEN"),
    HDFS_BYTES_READ(GroupName.FileSystemCounters, "HDFS_BYTES_READ", "HDFS_BYTES_READ"),
    HDFS_BYTES_WRITTEN(GroupName.FileSystemCounters, "HDFS_BYTES_WRITTEN", "HDFS_BYTES_WRITTEN"),

    MAP_INPUT_RECORDS(GroupName.MapReduce, "MAP_INPUT_RECORDS", "Map input records"),
    MAP_OUTPUT_RECORDS(GroupName.MapReduce, "MAP_OUTPUT_RECORDS", "Map output records"),
    MAP_OUTPUT_BYTES(GroupName.MapReduce, "MAP_OUTPUT_BYTES", "Map output bytes"),
    MAP_OUTPUT_MATERIALIZED_BYTES(GroupName.MapReduce, "MAP_OUTPUT_MATERIALIZED_BYTES", "Map output materialized bytes"),
    SPLIT_RAW_BYTES(GroupName.MapReduce, "SPLIT_RAW_BYTES", "SPLIT_RAW_BYTES"),

    REDUCE_INPUT_GROUPS(GroupName.MapReduce, "REDUCE_INPUT_GROUPS", "Reduce input groups"),
    REDUCE_SHUFFLE_BYTES(GroupName.MapReduce, "REDUCE_SHUFFLE_BYTES", "Reduce shuffle bytes"),
    REDUCE_OUTPUT_RECORDS(GroupName.MapReduce, "REDUCE_OUTPUT_RECORDS", "Reduce output records"),
    REDUCE_INPUT_RECORDS(GroupName.MapReduce, "REDUCE_INPUT_RECORDS", "Reduce input records"),

    COMBINE_INPUT_RECORDS(GroupName.MapReduce, "COMBINE_INPUT_RECORDS", "Combine input records"),
    COMBINE_OUTPUT_RECORDS(GroupName.MapReduce, "COMBINE_OUTPUT_RECORDS", "Combine output records"),
    SPILLED_RECORDS(GroupName.MapReduce, "SPILLED_RECORDS", "Spilled Records"),

    CPU_MILLISECONDS(GroupName.MapReduce, "CPU_MILLISECONDS", "CPU time spent (ms)"),
    COMMITTED_HEAP_BYTES(GroupName.MapReduce, "COMMITTED_HEAP_BYTES", "Total committed heap usage (bytes)"),
    PHYSICAL_MEMORY_BYTES(GroupName.MapReduce, "PHYSICAL_MEMORY_BYTES", "Physical memory (bytes) snapshot"),
    VIRTUAL_MEMORY_BYTES(GroupName.MapReduce, "VIRTUAL_MEMORY_BYTES", "Virtual memory (bytes) snapshot");

    GroupName _group;
    String _name;
    String _displayName;

    CounterName(GroupName group, String name, String displayName) {
      this._group = group;
      this._name = name;
      this._displayName = displayName;
    }

    static Map<String, CounterName> _counterDisplayNameMap;
    static Map<String, CounterName> _counterNameMap;
    static {
      _counterDisplayNameMap = new HashMap<String, CounterName>();
      _counterNameMap = new HashMap<String, CounterName>();
      for (CounterName cn : CounterName.values()) {
        _counterDisplayNameMap.put(cn._displayName, cn);
        _counterNameMap.put(cn._name, cn);
      }
    }

    public static CounterName getCounterFromName(String name) {
      if (_counterNameMap.containsKey(name)) {
        return _counterNameMap.get(name);
      }
      return null;
    }

    public static CounterName getCounterFromDisplayName(String displayName) {
      if (_counterDisplayNameMap.containsKey(displayName)) {
        return _counterDisplayNameMap.get(displayName);
      }
      return null;
    }

    public String getName() {
      return _name;
    }

    public String getDisplayName() {
      return _displayName;
    }

  }
}
