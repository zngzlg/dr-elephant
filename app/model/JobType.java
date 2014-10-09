package model;

import java.util.HashMap;
import java.util.Map;

import com.avaje.ebean.annotation.EnumValue;


public enum JobType {
  @EnumValue("Hadoop")
  HADOOPJAVA("HadoopJava", "Hadoop"),

  @EnumValue("Pig")
  PIG("Pig", "Pig"),

  @EnumValue("Hive")
  HIVE("Hive", "Hive");

  private String _name;
  private String _dbName;

  JobType(String name, String dbName) {
    this._name = name;
    this._dbName = dbName;
  }

  private static Map<String, String> _jobTypeNameMap;
  static {
    _jobTypeNameMap = new HashMap<String, String>();
    for (JobType jobType : JobType.values()) {
      _jobTypeNameMap.put(jobType._name, jobType._dbName);
    }
  }

  public String getName() {
    return _name;
  }

  public static String getDbName(String jobType) {
    if (_jobTypeNameMap.containsKey(jobType)) {
      return _jobTypeNameMap.get(jobType);
    }
    return null;
  }
}
