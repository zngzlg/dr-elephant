package com.linkedin.drelephant.analysis;

import java.util.Properties;
import java.util.regex.Pattern;


public class JobType {
  public static final JobType NO_DATA = new JobType("No data received");

  private final String _type;
  private final String _confName;
  private final Pattern _confPattern;

  public JobType(String type) {
    _type = type;
    _confName = null;
    _confPattern = null;
  }

  public JobType(String type, String confName, String confPattern) {
    _type = type;
    _confName = confName;
    _confPattern = Pattern.compile(confPattern);
  }

  public boolean matchType(Properties jobProp) {
    // Always return false if confName/confPattern is undefined,
    // which means we cannot tell if the properties are matching the pattern
    if (_confName == null || _confPattern == null) {
      return false;
    }

    return jobProp.containsKey(_confName) && _confPattern.matcher((String) jobProp.get(_confName)).matches();
  }

  public String getName() {
    return _type;
  }

  public String toString() {
    return _type;
  }
}
