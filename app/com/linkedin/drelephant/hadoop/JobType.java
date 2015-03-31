package com.linkedin.drelephant.hadoop;

import java.util.Properties;
import java.util.regex.Pattern;

public class JobType {

  private String _type;
  private String _confName;
  private Pattern _confPattern;

  public JobType(String type, String confName, String confPattern) {
    this._type = type;
    this._confName = confName;
    this._confPattern = Pattern.compile(confPattern);
  }

  public boolean matchType(Properties jobProp) {
    return jobProp.containsKey(_confName) &&
        _confPattern.matcher((String)jobProp.get(_confName)).matches();
  }

  public String getName() {
    return _type;
  }

  public String toString() {
    return _type;
  }
}
